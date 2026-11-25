package com.tradeexception.platform.enrichment;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.tradeexception.platform.audit.domain.AuditEvent;
import com.tradeexception.platform.audit.domain.EventType;
import com.tradeexception.platform.audit.repository.AuditEventRepository;
import com.tradeexception.platform.common.couchdb.CouchDbConnectionException;
import com.tradeexception.platform.common.kafka.EventEnvelope;
import com.tradeexception.platform.common.kafka.KafkaProperties;
import com.tradeexception.platform.enrichment.TradeEnrichmentService.EnrichmentOutcome;
import com.tradeexception.platform.enrichment.domain.EnrichedTrade;
import com.tradeexception.platform.enrichment.domain.EnrichmentOrigin;
import com.tradeexception.platform.enrichment.domain.ReferenceData;
import com.tradeexception.platform.enrichment.domain.ReferenceDataCategory;
import com.tradeexception.platform.enrichment.repository.ReferenceDataRepository;
import com.tradeexception.platform.ingestion.SyntheticTradeProducer;
import com.tradeexception.platform.ingestion.SyntheticTradeScenario;
import com.tradeexception.platform.replay.DeadLetterPublisher;
import com.tradeexception.platform.trade.domain.Trade;
import com.tradeexception.platform.trade.domain.TradeStatus;
import com.tradeexception.platform.trade.repository.TradeRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TradeEnrichmentServiceTest {

    @Mock private ReferenceDataRepository referenceDataRepository;
    @Mock private TradeRepository tradeRepository;
    @Mock private AuditEventRepository auditEventRepository;
    @Mock private KafkaTemplate<String, String> kafkaTemplate;
    @Mock private DeadLetterPublisher deadLetterPublisher;

    private ObjectMapper mapper;
    private SyntheticTradeProducer producer;
    private TradeEnrichmentService service;
    private Map<String, ReferenceData> catalog;

    @BeforeEach
    void setUp() {
        mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        KafkaProperties props = new KafkaProperties();
        producer = new SyntheticTradeProducer(kafkaTemplate, mapper, props);
        service = new TradeEnrichmentService(
                mapper, referenceDataRepository, tradeRepository, auditEventRepository,
                kafkaTemplate, deadLetterPublisher, props);

        catalog = ReferenceDataSeeder.defaultCatalog().stream()
                .collect(Collectors.toMap(ReferenceData::getId, r -> r));

        lenient().when(referenceDataRepository.findByCategoryAndCode(any(), anyString()))
                .thenAnswer(inv -> {
                    ReferenceDataCategory cat = inv.getArgument(0);
                    String code = inv.getArgument(1);
                    return Optional.ofNullable(catalog.get("refdata:" + cat.name() + ":" + code));
                });
    }

    @Test
    void enrichesValidFxSpotWithFullProvenanceAndPublishesTradeEnriched() throws Exception {
        EventEnvelope<Trade> accepted = producer.buildEnvelopeForScenario(SyntheticTradeScenario.VALID_FX_SPOT);
        Trade trade = accepted.payload();
        trade.setStatus(TradeStatus.ACCEPTED);
        String json = mapper.writeValueAsString(accepted);

        when(tradeRepository.findByTradeId(trade.getTradeId())).thenReturn(Optional.of(trade));
        when(tradeRepository.save(any(Trade.class))).thenAnswer(inv -> inv.getArgument(0));

        EnrichmentOutcome outcome = service.processAcceptedMessage(json, trade.getTradeId(), "trade.accepted", 0, 1L);

        assertThat(outcome).isEqualTo(EnrichmentOutcome.ENRICHED);
        assertThat(trade.getStatus()).isEqualTo(TradeStatus.ENRICHED);

        ArgumentCaptor<AuditEvent> auditCaptor = ArgumentCaptor.forClass(AuditEvent.class);
        verify(auditEventRepository).save(auditCaptor.capture());
        assertThat(auditCaptor.getValue().getEventType()).isEqualTo(EventType.TRADE_ENRICHED);
        assertThat(auditCaptor.getValue().getCorrelationId()).isEqualTo(accepted.correlationId());

        verify(kafkaTemplate).send(eq("trade.enriched"), eq(trade.getTradeId()), anyString());
    }

    @Test
    void recordsProvenanceAndStructuredFailuresWithoutInventingMissingReferenceData() {
        EventEnvelope<Trade> accepted = producer.buildEnvelopeForScenario(SyntheticTradeScenario.VALID_FX_SPOT);
        // Remove instrument and seller from reference catalog to simulate missing reference data
        catalog.remove("refdata:INSTRUMENT:FX_SPOT:USD/EUR");
        catalog.remove("refdata:COUNTERPARTY:SYNTHCORP00000000002");

        EnrichedTrade enriched = service.enrich(accepted.payload(), accepted.correlationId());

        assertThat(enriched.buyerFound()).isTrue();
        assertThat(enriched.sellerFound()).isFalse();
        assertThat(enriched.instrumentFound()).isFalse();
        assertThat(enriched.hasFailures()).isTrue();
        assertThat(enriched.enrichmentFailures())
                .extracting(f -> f.code())
                .containsExactlyInAnyOrder("COUNTERPARTY_NOT_FOUND", "INSTRUMENT_NOT_FOUND");
        assertThat(enriched.enrichedFields().get("buyer.legalName").origin()).isEqualTo(EnrichmentOrigin.SUPPLIED);
        assertThat(enriched.enrichedFields().get("confirmation.templateAvailable").origin()).isEqualTo(EnrichmentOrigin.DERIVED);
    }

    @Test
    void skipsDuplicateEnrichmentWhenTradeAlreadyEnriched() throws Exception {
        EventEnvelope<Trade> accepted = producer.buildEnvelopeForScenario(SyntheticTradeScenario.VALID_FX_FORWARD);
        Trade existing = accepted.payload();
        existing.setStatus(TradeStatus.ENRICHED);

        when(tradeRepository.findByTradeId(existing.getTradeId())).thenReturn(Optional.of(existing));

        EnrichmentOutcome outcome = service.processAcceptedMessage(
                mapper.writeValueAsString(accepted), existing.getTradeId(), "trade.accepted", 0, 2L);

        assertThat(outcome).isEqualTo(EnrichmentOutcome.DUPLICATE_IGNORED);
        verify(tradeRepository, never()).save(any());
        verify(kafkaTemplate, never()).send(eq("trade.enriched"), any(), any());
    }

    @Test
    void propagatesCouchDbConnectionExceptionForBoundedRetry() throws Exception {
        EventEnvelope<Trade> accepted = producer.buildEnvelopeForScenario(SyntheticTradeScenario.VALID_FX_SPOT);
        when(tradeRepository.findByTradeId(anyString()))
                .thenThrow(new CouchDbConnectionException("CouchDB unavailable", null));

        assertThatThrownBy(() -> service.processAcceptedMessage(
                mapper.writeValueAsString(accepted), accepted.payload().getTradeId(), "trade.accepted", 0, 3L))
                .isInstanceOf(CouchDbConnectionException.class);
    }
}
