package com.tradeexception.platform.confirmation;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.tradeexception.platform.audit.domain.AuditEvent;
import com.tradeexception.platform.audit.domain.EventType;
import com.tradeexception.platform.audit.repository.AuditEventRepository;
import com.tradeexception.platform.common.kafka.EventEnvelope;
import com.tradeexception.platform.common.kafka.KafkaProperties;
import com.tradeexception.platform.confirmation.ConfirmationGenerationService.ConfirmationOutcome;
import com.tradeexception.platform.confirmation.domain.Confirmation;
import com.tradeexception.platform.confirmation.domain.ConfirmationStatus;
import com.tradeexception.platform.confirmation.repository.ConfirmationRepository;
import com.tradeexception.platform.enrichment.ReferenceDataSeeder;
import com.tradeexception.platform.enrichment.domain.ReferenceData;
import com.tradeexception.platform.enrichment.domain.ReferenceDataCategory;
import com.tradeexception.platform.enrichment.repository.ReferenceDataRepository;
import com.tradeexception.platform.exception.domain.ExceptionRecord;
import com.tradeexception.platform.exception.repository.ExceptionRepository;
import com.tradeexception.platform.ingestion.SyntheticTradeProducer;
import com.tradeexception.platform.ingestion.SyntheticTradeScenario;
import com.tradeexception.platform.replay.DeadLetterPublisher;
import com.tradeexception.platform.stp.domain.StpDecision;
import com.tradeexception.platform.stp.domain.StpValidationResult;
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

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ConfirmationGenerationServiceTest {

    @Mock private TradeRepository tradeRepository;
    @Mock private ReferenceDataRepository referenceDataRepository;
    @Mock private ConfirmationRepository confirmationRepository;
    @Mock private ExceptionRepository exceptionRepository;
    @Mock private AuditEventRepository auditEventRepository;
    @Mock private KafkaTemplate<String, String> kafkaTemplate;
    @Mock private DeadLetterPublisher deadLetterPublisher;

    private ObjectMapper mapper;
    private SyntheticTradeProducer producer;
    private ConfirmationGenerationService service;
    private Map<String, ReferenceData> catalog;

    @BeforeEach
    void setUp() {
        mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        KafkaProperties props = new KafkaProperties();
        producer = new SyntheticTradeProducer(kafkaTemplate, mapper, props);
        service = new ConfirmationGenerationService(
                mapper, tradeRepository, referenceDataRepository, confirmationRepository,
                exceptionRepository, auditEventRepository, new ConfirmationTemplateRenderer(),
                kafkaTemplate, deadLetterPublisher, props);

        catalog = ReferenceDataSeeder.defaultCatalog().stream()
                .collect(Collectors.toMap(ReferenceData::getId, r -> r));
        lenient().when(referenceDataRepository.findByCategoryAndCode(any(), anyString()))
                .thenAnswer(inv -> Optional.ofNullable(
                        catalog.get("refdata:" + ((ReferenceDataCategory) inv.getArgument(0)).name() + ":" + inv.getArgument(1))));
    }

    private EventEnvelope<StpValidationResult> validatedEnvelope(String tradeId, String corrId) {
        StpValidationResult res = new StpValidationResult(
                tradeId, corrId, StpDecision.STP_ELIGIBLE, Instant.now(),
                List.of("ALL"), List.of(), List.of(), "All passed");
        return EventEnvelope.of("StpValidated", corrId, "1.0", "stp-validation", res);
    }

    @Test
    void generatesConfirmationPersistsUpdatesTradeAndPublishesEvent() throws Exception {
        Trade trade = producer.buildEnvelopeForScenario(SyntheticTradeScenario.VALID_FX_SPOT).payload();
        trade.setStatus(TradeStatus.VALIDATED);
        EventEnvelope<StpValidationResult> env = validatedEnvelope(trade.getTradeId(), trade.getHeader().correlationId());

        when(confirmationRepository.findById("confirmation:CNF-" + trade.getTradeId())).thenReturn(Optional.empty());
        when(tradeRepository.findByTradeId(trade.getTradeId())).thenReturn(Optional.of(trade));
        when(confirmationRepository.save(any(Confirmation.class))).thenAnswer(inv -> inv.getArgument(0));

        ConfirmationOutcome out = service.processValidatedMessage(
                mapper.writeValueAsString(env), trade.getTradeId(), "stp.validated", 0, 1L);

        assertThat(out).isEqualTo(ConfirmationOutcome.GENERATED);
        assertThat(trade.getStatus()).isEqualTo(TradeStatus.CONFIRMED);

        ArgumentCaptor<Confirmation> cnfCaptor = ArgumentCaptor.forClass(Confirmation.class);
        verify(confirmationRepository).save(cnfCaptor.capture());
        Confirmation saved = cnfCaptor.getValue();
        assertThat(saved.getConfirmationId()).isEqualTo("CNF-" + trade.getTradeId());
        assertThat(saved.getStatus()).isEqualTo(ConfirmationStatus.GENERATED);
        assertThat(saved.getTemplateId()).isEqualTo("TMPL_FX_SPOT_V1");
        assertThat(saved.getRenderedContent()).contains("Not Legally Binding").contains(trade.getTradeId());

        ArgumentCaptor<AuditEvent> auditCaptor = ArgumentCaptor.forClass(AuditEvent.class);
        verify(auditEventRepository).save(auditCaptor.capture());
        assertThat(auditCaptor.getValue().getEventType()).isEqualTo(EventType.CONFIRMATION_GENERATED);
        assertThat(auditCaptor.getValue().getEntityId()).isEqualTo(saved.getConfirmationId());

        verify(kafkaTemplate).send(eq("confirmation.generated"), eq(trade.getTradeId()), anyString());
    }

    @Test
    void missingTemplateRoutesBusinessExceptionInsteadOfDeadLetter() throws Exception {
        Trade trade = producer.buildEnvelopeForScenario(SyntheticTradeScenario.VALID_FX_SPOT).payload();
        catalog.remove("refdata:CONFIRMATION_TEMPLATE:TMPL_FX_SPOT_V1");
        EventEnvelope<StpValidationResult> env = validatedEnvelope(trade.getTradeId(), trade.getHeader().correlationId());

        when(confirmationRepository.findById(anyString())).thenReturn(Optional.empty());
        when(tradeRepository.findByTradeId(trade.getTradeId())).thenReturn(Optional.of(trade));
        when(exceptionRepository.findById(anyString())).thenReturn(Optional.empty());
        when(exceptionRepository.save(any(ExceptionRecord.class))).thenAnswer(inv -> inv.getArgument(0));

        ConfirmationOutcome out = service.processValidatedMessage(
                mapper.writeValueAsString(env), trade.getTradeId(), "stp.validated", 0, 2L);

        assertThat(out).isEqualTo(ConfirmationOutcome.TEMPLATE_UNAVAILABLE);
        verify(exceptionRepository).save(any(ExceptionRecord.class));
        verify(kafkaTemplate).send(eq("exception.created"), eq(trade.getTradeId()), anyString());
        verify(deadLetterPublisher, never()).publish(any(), any(), any(), any(), any(), any(), any(), any(), any());
    }

    @Test
    void missingTradeRoutesToDeadLetterAndDuplicateConfirmationIsIgnored() throws Exception {
        EventEnvelope<StpValidationResult> env1 = validatedEnvelope("TRD-MISSING", "CORR-M");
        when(confirmationRepository.findById("confirmation:CNF-TRD-MISSING")).thenReturn(Optional.empty());
        when(tradeRepository.findByTradeId("TRD-MISSING")).thenReturn(Optional.empty());

        assertThat(service.processValidatedMessage(mapper.writeValueAsString(env1), "TRD-MISSING", "stp.validated", 0, 3L))
                .isEqualTo(ConfirmationOutcome.MISSING_TRADE);
        verify(deadLetterPublisher).publish(any(), any(), any(), any(), any(), any(), any(), any(), any());

        EventEnvelope<StpValidationResult> env2 = validatedEnvelope("TRD-DUP", "CORR-D");
        Confirmation existing = new Confirmation();
        existing.setConfirmationId("CNF-TRD-DUP");
        when(confirmationRepository.findById("confirmation:CNF-TRD-DUP")).thenReturn(Optional.of(existing));

        assertThat(service.processValidatedMessage(mapper.writeValueAsString(env2), "TRD-DUP", "stp.validated", 0, 4L))
                .isEqualTo(ConfirmationOutcome.DUPLICATE_IGNORED);
        verify(confirmationRepository, never()).save(any());
    }
}
