package com.tradeexception.platform.stp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.tradeexception.platform.audit.repository.AuditEventRepository;
import com.tradeexception.platform.common.kafka.EventEnvelope;
import com.tradeexception.platform.common.kafka.KafkaProperties;
import com.tradeexception.platform.enrichment.domain.EnrichedTrade;
import com.tradeexception.platform.exception.domain.ExceptionCategory;
import com.tradeexception.platform.exception.domain.ExceptionPriority;
import com.tradeexception.platform.exception.domain.ExceptionRecord;
import com.tradeexception.platform.exception.repository.ExceptionRepository;
import com.tradeexception.platform.ingestion.SyntheticTradeProducer;
import com.tradeexception.platform.ingestion.SyntheticTradeScenario;
import com.tradeexception.platform.replay.DeadLetterPublisher;
import com.tradeexception.platform.stp.StpValidationService.ValidationOutcome;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StpValidationServiceTest {

    @Mock private StpRuleEngine ruleEngine;
    @Mock private TradeRepository tradeRepository;
    @Mock private ExceptionRepository exceptionRepository;
    @Mock private AuditEventRepository auditEventRepository;
    @Mock private KafkaTemplate<String, String> kafkaTemplate;
    @Mock private DeadLetterPublisher deadLetterPublisher;

    private ObjectMapper mapper;
    private SyntheticTradeProducer producer;
    private StpValidationService service;

    @BeforeEach
    void setUp() {
        mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        KafkaProperties props = new KafkaProperties();
        producer = new SyntheticTradeProducer(kafkaTemplate, mapper, props);
        service = new StpValidationService(
                mapper, ruleEngine, tradeRepository, exceptionRepository,
                auditEventRepository, kafkaTemplate, deadLetterPublisher, props);
    }

    private EventEnvelope<EnrichedTrade> sampleEnrichedEnvelope() {
        Trade trade = producer.buildEnvelopeForScenario(SyntheticTradeScenario.VALID_FX_SPOT).payload();
        trade.setStatus(TradeStatus.ENRICHED);
        EnrichedTrade enriched = new EnrichedTrade(
                trade, trade.getHeader().correlationId(), Instant.now(), Map.of(),
                true, true, true, true, true, true, true, true, false, true, true, List.of());
        return EventEnvelope.of("TradeEnriched", enriched.correlationId(), "1.0", "trade-enrichment", enriched);
    }

    @Test
    void publishesStpValidatedWhenTradeIsStpEligible() throws Exception {
        EventEnvelope<EnrichedTrade> env = sampleEnrichedEnvelope();
        String tradeId = env.payload().trade().getTradeId();
        when(tradeRepository.findByTradeId(tradeId)).thenReturn(Optional.of(env.payload().trade()));
        when(ruleEngine.validate(any())).thenReturn(new StpValidationResult(
                tradeId, env.correlationId(), StpDecision.STP_ELIGIBLE, Instant.now(),
                List.of("ALL"), List.of(), List.of(), "All passed"));

        ValidationOutcome out = service.processEnrichedMessage(
                mapper.writeValueAsString(env), tradeId, "trade.enriched", 0, 1L);

        assertThat(out).isEqualTo(ValidationOutcome.STP_ELIGIBLE);
        verify(kafkaTemplate).send(eq("stp.validated"), eq(tradeId), anyString());
        verify(exceptionRepository, never()).save(any());
        verify(deadLetterPublisher, never()).publish(any(), any(), any(), any(), any(), any(), any(), any(), any());
    }

    @Test
    void createsExceptionAndDoesNotSendBusinessFailureToDeadLetter() throws Exception {
        EventEnvelope<EnrichedTrade> env = sampleEnrichedEnvelope();
        String tradeId = env.payload().trade().getTradeId();
        when(tradeRepository.findByTradeId(tradeId)).thenReturn(Optional.of(env.payload().trade()));
        when(exceptionRepository.findById("exception:EXC-" + tradeId)).thenReturn(Optional.empty());
        when(exceptionRepository.save(any(ExceptionRecord.class))).thenAnswer(inv -> inv.getArgument(0));
        when(ruleEngine.validate(any())).thenReturn(new StpValidationResult(
                tradeId, env.correlationId(), StpDecision.MANUAL_REVIEW_REQUIRED, Instant.now(),
                List.of("CURRENCY_SUPPORT"), List.of("COUNTERPARTY_VALIDATION"),
                List.of("COUNTERPARTY_NOT_STP_ELIGIBLE"), "Manual review required: restricted counterparty"));

        ValidationOutcome out = service.processEnrichedMessage(
                mapper.writeValueAsString(env), tradeId, "trade.enriched", 0, 2L);

        assertThat(out).isEqualTo(ValidationOutcome.MANUAL_REVIEW_REQUIRED);

        ArgumentCaptor<ExceptionRecord> exCaptor = ArgumentCaptor.forClass(ExceptionRecord.class);
        verify(exceptionRepository).save(exCaptor.capture());
        assertThat(exCaptor.getValue().getExceptionId()).isEqualTo("EXC-" + tradeId);
        assertThat(exCaptor.getValue().getCategory()).isEqualTo(ExceptionCategory.UNCLASSIFIED);
        assertThat(exCaptor.getValue().getPriority()).isEqualTo(ExceptionPriority.MEDIUM);
        assertThat(exCaptor.getValue().getDescription()).contains("Manual review required");

        verify(kafkaTemplate).send(eq("exception.created"), eq(tradeId), anyString());
        verify(kafkaTemplate).send(eq("manual.review.required"), eq(tradeId), anyString());
        verify(deadLetterPublisher, never()).publish(any(), any(), any(), any(), any(), any(), any(), any(), any());
    }

    @Test
    void skipsDuplicateValidationAndPreservesExistingException() throws Exception {
        EventEnvelope<EnrichedTrade> env = sampleEnrichedEnvelope();
        Trade alreadyValidated = env.payload().trade();
        alreadyValidated.setStatus(TradeStatus.VALIDATED);
        when(tradeRepository.findByTradeId(alreadyValidated.getTradeId())).thenReturn(Optional.of(alreadyValidated));

        ValidationOutcome out = service.processEnrichedMessage(
                mapper.writeValueAsString(env), alreadyValidated.getTradeId(), "trade.enriched", 0, 3L);

        assertThat(out).isEqualTo(ValidationOutcome.DUPLICATE_IGNORED);
        verify(ruleEngine, never()).validate(any());
        verify(exceptionRepository, never()).save(any());
    }
}
