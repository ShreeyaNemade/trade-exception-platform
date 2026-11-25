package com.tradeexception.platform.classification;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.tradeexception.platform.audit.domain.AuditEvent;
import com.tradeexception.platform.audit.domain.EventType;
import com.tradeexception.platform.audit.repository.AuditEventRepository;
import com.tradeexception.platform.classification.ExceptionClassificationService.ClassificationOutcome;
import com.tradeexception.platform.common.kafka.EventEnvelope;
import com.tradeexception.platform.common.kafka.KafkaProperties;
import com.tradeexception.platform.exception.domain.ExceptionCategory;
import com.tradeexception.platform.exception.domain.ExceptionPriority;
import com.tradeexception.platform.exception.domain.ExceptionRecord;
import com.tradeexception.platform.exception.domain.ExceptionSeverity;
import com.tradeexception.platform.exception.domain.ExceptionStatus;
import com.tradeexception.platform.exception.repository.ExceptionRepository;
import com.tradeexception.platform.replay.DeadLetterPublisher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Instant;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ExceptionClassificationServiceTest {

    @Mock private ExceptionRepository exceptionRepository;
    @Mock private AuditEventRepository auditEventRepository;
    @Mock private KafkaTemplate<String, String> kafkaTemplate;
    @Mock private DeadLetterPublisher deadLetterPublisher;

    private ObjectMapper mapper;
    private ExceptionClassifier classifier;
    private ExceptionClassificationService service;

    @BeforeEach
    void setUp() {
        mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        classifier = new ExceptionClassifier();
        service = new ExceptionClassificationService(
                mapper, classifier, exceptionRepository, auditEventRepository,
                kafkaTemplate, deadLetterPublisher, new KafkaProperties());
    }

    private ExceptionRecord sampleException(String id, String reasonCode) {
        ExceptionRecord ex = new ExceptionRecord();
        ex.setExceptionId(id);
        ex.setTradeId("TRD-1");
        ex.setCorrelationId("CORR-1");
        ex.setCategory(ExceptionCategory.UNCLASSIFIED);
        ex.setPriority(ExceptionPriority.MEDIUM);
        ex.setStatus(ExceptionStatus.OPEN);
        ex.setReasonCode(reasonCode);
        ex.setDescription("Validation failed");
        ex.setDetectedAt(Instant.now());
        return ex;
    }

    @Test
    void classifiesKnownAndUnknownReasonCodesDeterministically() {
        var econ = classifier.classify(sampleException("EXC-1", "INVALID_ECONOMIC_TERMS"));
        assertThat(econ.category()).isEqualTo(ExceptionCategory.INVALID_ECONOMIC_TERMS);
        assertThat(econ.priority()).isEqualTo(ExceptionPriority.CRITICAL);
        assertThat(econ.severity()).isEqualTo(ExceptionSeverity.CRITICAL);

        var cpty = classifier.classify(sampleException("EXC-2", "COUNTERPARTY_NOT_FOUND"));
        assertThat(cpty.category()).isEqualTo(ExceptionCategory.INVALID_COUNTERPARTY);
        assertThat(cpty.priority()).isEqualTo(ExceptionPriority.HIGH);

        var unknown = classifier.classify(sampleException("EXC-3", ""));
        assertThat(unknown.category()).isEqualTo(ExceptionCategory.OTHER);
        assertThat(unknown.recommendedAction()).isNotBlank();
    }

    @Test
    void persistsClassificationRecordsAuditAndPublishesExceptionClassified() throws Exception {
        ExceptionRecord ex = sampleException("EXC-TRD-1", "CURRENCY_UNSUPPORTED");
        EventEnvelope<ExceptionRecord> env = EventEnvelope.of("ExceptionCreated", "CORR-1", "1.0", "stp-validation", ex);

        when(exceptionRepository.findById("exception:EXC-TRD-1")).thenReturn(Optional.of(ex));
        when(exceptionRepository.save(any(ExceptionRecord.class))).thenAnswer(inv -> inv.getArgument(0));

        ClassificationOutcome out = service.processExceptionCreated(
                mapper.writeValueAsString(env), "TRD-1", "exception.created", 0, 1L);

        assertThat(out).isEqualTo(ClassificationOutcome.CLASSIFIED);
        assertThat(ex.getCategory()).isEqualTo(ExceptionCategory.CURRENCY_MISMATCH);
        assertThat(ex.getRootCause()).isNotBlank();
        assertThat(ex.getClassifiedAt()).isNotNull();

        ArgumentCaptor<AuditEvent> auditCaptor = ArgumentCaptor.forClass(AuditEvent.class);
        verify(auditEventRepository).save(auditCaptor.capture());
        assertThat(auditCaptor.getValue().getEventType()).isEqualTo(EventType.EXCEPTION_CLASSIFIED);

        verify(kafkaTemplate).send(eq("exception.classified"), eq("TRD-1"), anyString());
    }

    @Test
    void skipsDuplicateClassificationWhenAlreadyClassified() throws Exception {
        ExceptionRecord ex = sampleException("EXC-DUP", "CURRENCY_UNSUPPORTED");
        ex.setClassifiedAt(Instant.now());
        EventEnvelope<ExceptionRecord> env = EventEnvelope.of("ExceptionCreated", "CORR-1", "1.0", "stp-validation", ex);

        when(exceptionRepository.findById("exception:EXC-DUP")).thenReturn(Optional.of(ex));

        ClassificationOutcome out = service.processExceptionCreated(
                mapper.writeValueAsString(env), "TRD-1", "exception.created", 0, 2L);

        assertThat(out).isEqualTo(ClassificationOutcome.DUPLICATE_IGNORED);
        verify(exceptionRepository, never()).save(any());
        verify(kafkaTemplate, never()).send(eq("exception.classified"), any(), any());
    }
}
