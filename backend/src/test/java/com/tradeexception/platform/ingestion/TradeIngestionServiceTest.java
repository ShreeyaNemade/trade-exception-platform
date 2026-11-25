package com.tradeexception.platform.ingestion;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.tradeexception.platform.audit.domain.AuditEvent;
import com.tradeexception.platform.audit.domain.EventType;
import com.tradeexception.platform.audit.repository.AuditEventRepository;
import com.tradeexception.platform.common.couchdb.CouchDbConflictException;
import com.tradeexception.platform.common.couchdb.CouchDbConnectionException;
import com.tradeexception.platform.common.kafka.EventEnvelope;
import com.tradeexception.platform.common.kafka.KafkaProperties;
import com.tradeexception.platform.ingestion.TradeIngestionService.IngestionOutcome;
import com.tradeexception.platform.replay.DeadLetterPublisher;
import com.tradeexception.platform.replay.DeadLetterRecord.DeadLetterErrorCategory;
import com.tradeexception.platform.trade.domain.Trade;
import com.tradeexception.platform.trade.domain.TradeStatus;
import com.tradeexception.platform.trade.repository.TradeRepository;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TradeIngestionServiceTest {

    @Mock private TradeRepository tradeRepository;
    @Mock private AuditEventRepository auditEventRepository;
    @Mock private KafkaTemplate<String, String> kafkaTemplate;
    @Mock private DeadLetterPublisher deadLetterPublisher;

    private ObjectMapper mapper;
    private SyntheticTradeProducer producer;
    private TradeIngestionService service;

    @BeforeEach
    void setUp() {
        mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        Validator validator = Validation.buildDefaultValidatorFactory().getValidator();
        KafkaProperties props = new KafkaProperties();
        producer = new SyntheticTradeProducer(kafkaTemplate, mapper, props);
        service = new TradeIngestionService(
                mapper, validator, tradeRepository, auditEventRepository,
                kafkaTemplate, deadLetterPublisher, props);
    }

    @Test
    void acceptsValidTradePersistsRecordsAuditAndPublishesTradeAccepted() throws Exception {
        EventEnvelope<Trade> envelope = producer.buildEnvelopeForScenario(SyntheticTradeScenario.VALID_FX_SPOT);
        String json = mapper.writeValueAsString(envelope);
        String tradeId = envelope.payload().getTradeId();

        when(tradeRepository.findByTradeId(tradeId)).thenReturn(Optional.empty());
        when(tradeRepository.save(any(Trade.class))).thenAnswer(inv -> {
            Trade t = inv.getArgument(0);
            t.setRevision("1-rev");
            return t;
        });

        IngestionOutcome outcome = service.processRawMessage(json, tradeId, "trade.raw", 0, 1L);

        assertThat(outcome).isEqualTo(IngestionOutcome.ACCEPTED);

        ArgumentCaptor<Trade> tradeCaptor = ArgumentCaptor.forClass(Trade.class);
        verify(tradeRepository).save(tradeCaptor.capture());
        assertThat(tradeCaptor.getValue().getStatus()).isEqualTo(TradeStatus.ACCEPTED);

        ArgumentCaptor<AuditEvent> auditCaptor = ArgumentCaptor.forClass(AuditEvent.class);
        verify(auditEventRepository).save(auditCaptor.capture());
        assertThat(auditCaptor.getValue().getEventType()).isEqualTo(EventType.TRADE_ACCEPTED);
        assertThat(auditCaptor.getValue().getCorrelationId()).isEqualTo(envelope.correlationId());
        assertThat(auditCaptor.getValue().getTradeId()).isEqualTo(tradeId);

        verify(kafkaTemplate).send(eq("trade.accepted"), eq(tradeId), any(String.class));
        verify(deadLetterPublisher, never()).publish(any(), any(), any(), any(), any(), any(), any(), any(), any());
    }

    @Test
    void acceptsTradesWithBusinessRuleDefectsSoStpEngineHandlesThem() throws Exception {
        EventEnvelope<Trade> missingEcon = producer.buildEnvelopeForScenario(SyntheticTradeScenario.MISSING_MANDATORY_ECONOMIC_FIELD);
        String json1 = mapper.writeValueAsString(missingEcon);
        when(tradeRepository.findByTradeId(missingEcon.payload().getTradeId())).thenReturn(Optional.empty());
        when(tradeRepository.save(any(Trade.class))).thenAnswer(inv -> inv.getArgument(0));

        assertThat(service.processRawMessage(json1, missingEcon.payload().getTradeId(), "trade.raw", 0, 2L))
                .isEqualTo(IngestionOutcome.ACCEPTED);

        EventEnvelope<Trade> badParty = producer.buildEnvelopeForScenario(SyntheticTradeScenario.INVALID_COUNTERPARTY_REFERENCE);
        String json2 = mapper.writeValueAsString(badParty);
        when(tradeRepository.findByTradeId(badParty.payload().getTradeId())).thenReturn(Optional.empty());

        assertThat(service.processRawMessage(json2, badParty.payload().getTradeId(), "trade.raw", 0, 3L))
                .isEqualTo(IngestionOutcome.ACCEPTED);

        verify(deadLetterPublisher, never()).publish(any(), any(), any(), any(), any(), any(), any(), any(), any());
        verify(kafkaTemplate).send(eq("trade.accepted"), eq(missingEcon.payload().getTradeId()), any(String.class));
        verify(kafkaTemplate).send(eq("trade.accepted"), eq(badParty.payload().getTradeId()), any(String.class));
    }

    @Test
    void routesMissingCoreIdentifiersToDeadLetter() throws Exception {
        EventEnvelope<Trade> missingId = producer.buildEnvelopeForScenario(SyntheticTradeScenario.VALID_FX_SPOT);
        missingId.payload().setTradeId(" ");
        String json = mapper.writeValueAsString(missingId);

        IngestionOutcome outcome = service.processRawMessage(json, "NO-ID", "trade.raw", 0, 4L);
        assertThat(outcome).isEqualTo(IngestionOutcome.DEAD_LETTERED);
        verify(deadLetterPublisher).publish(
                eq("trade.raw"), eq(0), eq(4L), eq("NO-ID"),
                eq(missingId.correlationId()), eq(DeadLetterErrorCategory.VALIDATION_ERROR),
                any(), anyList(), eq(json));
        verify(tradeRepository, never()).save(any());
        verify(kafkaTemplate, never()).send(eq("trade.accepted"), any(), any());
    }

    @Test
    void routesMalformedJsonToDeadLetterWithoutRetrying() {
        String broken = "{not-valid-json";
        IngestionOutcome outcome = service.processRawMessage(broken, "BAD-1", "trade.raw", 1, 5L);

        assertThat(outcome).isEqualTo(IngestionOutcome.DEAD_LETTERED);
        verify(deadLetterPublisher).publish(
                eq("trade.raw"), eq(1), eq(5L), eq("BAD-1"), eq(null),
                eq(DeadLetterErrorCategory.DESERIALIZATION_ERROR), any(), eq(List.of()), eq(broken));
        verify(tradeRepository, never()).save(any());
    }

    @Test
    void ignoresDuplicateTradeOnExistingLookupOrConflict() throws Exception {
        EventEnvelope<Trade> envelope = producer.buildEnvelopeForScenario(SyntheticTradeScenario.VALID_FX_FORWARD);
        String json = mapper.writeValueAsString(envelope);
        String tradeId = envelope.payload().getTradeId();

        when(tradeRepository.findByTradeId(tradeId)).thenReturn(Optional.of(envelope.payload()));
        assertThat(service.processRawMessage(json, tradeId, "trade.raw", 0, 10L))
                .isEqualTo(IngestionOutcome.DUPLICATE_IGNORED);

        // Also verify concurrent 409 conflict on save is treated as duplicate
        when(tradeRepository.findByTradeId(tradeId)).thenReturn(Optional.empty());
        when(tradeRepository.save(any(Trade.class)))
                .thenThrow(new CouchDbConflictException("trades", "trade:" + tradeId, null, null));
        assertThat(service.processRawMessage(json, tradeId, "trade.raw", 0, 11L))
                .isEqualTo(IngestionOutcome.DUPLICATE_IGNORED);

        verify(auditEventRepository, never()).save(any());
        verify(kafkaTemplate, never()).send(eq("trade.accepted"), any(), any());
    }

    @Test
    void propagatesTransientCouchDbConnectionExceptionForKafkaRetry() throws Exception {
        EventEnvelope<Trade> envelope = producer.buildEnvelopeForScenario(SyntheticTradeScenario.VALID_FX_SPOT);
        String json = mapper.writeValueAsString(envelope);
        String tradeId = envelope.payload().getTradeId();

        when(tradeRepository.findByTradeId(tradeId))
                .thenThrow(new CouchDbConnectionException("CouchDB temporarily unreachable", null));

        assertThatThrownBy(() -> service.processRawMessage(json, tradeId, "trade.raw", 0, 20L))
                .isInstanceOf(CouchDbConnectionException.class);
        verify(deadLetterPublisher, never()).publish(any(), any(), any(), any(), any(), any(), any(), any(), any());
    }
}
