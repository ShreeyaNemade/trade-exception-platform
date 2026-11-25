package com.tradeexception.platform.replay;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.tradeexception.platform.audit.domain.AuditEvent;
import com.tradeexception.platform.audit.domain.EventType;
import com.tradeexception.platform.audit.repository.AuditEventRepository;
import com.tradeexception.platform.common.kafka.EventEnvelope;
import com.tradeexception.platform.replay.DeadLetterRecord.DeadLetterErrorCategory;
import com.tradeexception.platform.replay.api.ReplayResultDto;
import com.tradeexception.platform.replay.domain.DeadLetterDocument;
import com.tradeexception.platform.replay.domain.ReplayStatus;
import com.tradeexception.platform.replay.repository.DeadLetterRepository;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DeadLetterManagementServiceTest {

    @Mock private DeadLetterRepository deadLetterRepository;
    @Mock private AuditEventRepository auditEventRepository;
    @Mock private KafkaTemplate<String, String> kafkaTemplate;

    private ObjectMapper mapper;
    private DeadLetterManagementService service;

    @BeforeEach
    void setUp() {
        mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        service = new DeadLetterManagementService(mapper, deadLetterRepository, auditEventRepository, kafkaTemplate);
    }

    @Test
    void persistsDeadLetterIdempotentlyAndDistinguishesBusinessVsTechnicalFailure() throws Exception {
        DeadLetterRecord rec = new DeadLetterRecord(
                "DLQ-trade-raw-p0-o10", "trade.raw", 0, 10L, "TRD-1", "CORR-1",
                DeadLetterErrorCategory.INFRASTRUCTURE_ERROR, "CouchDB timeout", List.of(),
                "{\"eventId\":\"EVT-99\",\"payload\":{}}", Instant.now());
        String json = mapper.writeValueAsString(EventEnvelope.of("TradeDeadLettered", "CORR-1", "1.0", "dlq", rec));

        when(deadLetterRepository.findByDeadLetterId("DLQ-trade-raw-p0-o10")).thenReturn(Optional.empty());
        when(deadLetterRepository.save(any(DeadLetterDocument.class))).thenAnswer(inv -> inv.getArgument(0));

        assertThat(service.persistFromKafka(json)).isTrue();

        ArgumentCaptor<DeadLetterDocument> cap = ArgumentCaptor.forClass(DeadLetterDocument.class);
        verify(deadLetterRepository).save(cap.capture());
        assertThat(cap.getValue().getOriginalEventId()).isEqualTo("EVT-99");
        assertThat(cap.getValue().getFailureType()).isEqualTo("TECHNICAL");
        assertThat(cap.getValue().getReplayStatus()).isEqualTo(ReplayStatus.PENDING);

        verify(auditEventRepository).save(any(AuditEvent.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    void replaysEligibleTechnicalMessagePreservingOriginalPayloadAndRecordsAudits() {
        DeadLetterDocument doc = new DeadLetterDocument();
        doc.setDeadLetterId("DLQ-1");
        doc.setOriginalEventId("EVT-1");
        doc.setOriginalTopic("trade.raw");
        doc.setOriginalKey("TRD-1");
        doc.setCorrelationId("CORR-1");
        doc.setErrorCategory(DeadLetterErrorCategory.INFRASTRUCTURE_ERROR);
        doc.setFailureType("TECHNICAL");
        doc.setRawPayload("{\"eventId\":\"EVT-1\"}");
        doc.setFailedAt(Instant.now());
        doc.setReplayStatus(ReplayStatus.PENDING);
        doc.setReplayAttempts(0);

        when(deadLetterRepository.findByDeadLetterId("DLQ-1")).thenReturn(Optional.of(doc));
        when(deadLetterRepository.save(any(DeadLetterDocument.class))).thenAnswer(inv -> inv.getArgument(0));
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        ReplayResultDto result = service.replay("DLQ-1", "operations");

        assertThat(result.replayStatus()).isEqualTo(ReplayStatus.REPLAYED);
        assertThat(result.attemptNumber()).isEqualTo(1);
        assertThat(result.replayedBy()).isEqualTo("operations");
        assertThat(doc.getHistory()).hasSize(1);

        ArgumentCaptor<AuditEvent> audits = ArgumentCaptor.forClass(AuditEvent.class);
        verify(auditEventRepository, times(2)).save(audits.capture());
        assertThat(audits.getAllValues())
                .extracting(AuditEvent::getEventType)
                .containsExactly(EventType.DLQ_REPLAY_REQUESTED, EventType.DLQ_REPLAY_SUCCEEDED);
    }

    @Test
    void blocksReplayOfMalformedAlreadyReplayedOrExhaustedRecords() {
        DeadLetterDocument malformed = new DeadLetterDocument();
        malformed.setDeadLetterId("DLQ-BAD");
        malformed.setErrorCategory(DeadLetterErrorCategory.DESERIALIZATION_ERROR);
        malformed.setReplayStatus(ReplayStatus.NOT_ELIGIBLE);
        when(deadLetterRepository.findByDeadLetterId("DLQ-BAD")).thenReturn(Optional.of(malformed));

        assertThatThrownBy(() -> service.replay("DLQ-BAD", "operations"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("without payload correction");

        DeadLetterDocument replayed = new DeadLetterDocument();
        replayed.setDeadLetterId("DLQ-DONE");
        replayed.setErrorCategory(DeadLetterErrorCategory.INFRASTRUCTURE_ERROR);
        replayed.setReplayStatus(ReplayStatus.REPLAYED);
        when(deadLetterRepository.findByDeadLetterId("DLQ-DONE")).thenReturn(Optional.of(replayed));

        assertThatThrownBy(() -> service.replay("DLQ-DONE", "operations"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already been replayed");

        DeadLetterDocument maxed = new DeadLetterDocument();
        maxed.setDeadLetterId("DLQ-MAX");
        maxed.setErrorCategory(DeadLetterErrorCategory.INFRASTRUCTURE_ERROR);
        maxed.setReplayStatus(ReplayStatus.FAILED);
        maxed.setReplayAttempts(3);
        maxed.setMaxReplayAttempts(3);
        when(deadLetterRepository.findByDeadLetterId("DLQ-MAX")).thenReturn(Optional.of(maxed));

        assertThatThrownBy(() -> service.replay("DLQ-MAX", "operations"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Maximum replay attempts");
    }
}
