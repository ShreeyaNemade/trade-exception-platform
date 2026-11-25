package com.tradeexception.platform.classification;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tradeexception.platform.audit.domain.AuditEvent;
import com.tradeexception.platform.audit.domain.EventType;
import com.tradeexception.platform.audit.repository.AuditEventRepository;
import com.tradeexception.platform.classification.ExceptionClassifier.ClassificationResult;
import com.tradeexception.platform.common.couchdb.CouchDbConflictException;
import com.tradeexception.platform.common.kafka.EventEnvelope;
import com.tradeexception.platform.common.kafka.KafkaProperties;
import com.tradeexception.platform.exception.domain.ExceptionRecord;
import com.tradeexception.platform.exception.repository.ExceptionRepository;
import com.tradeexception.platform.replay.DeadLetterPublisher;
import com.tradeexception.platform.replay.DeadLetterRecord.DeadLetterErrorCategory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Consumes {@code exception.created} events, applies deterministic root-cause classification,
 * updates the persisted {@link ExceptionRecord} while preserving original fields, records an
 * immutable audit event, and publishes {@code exception.classified}.
 */
@Service
public class ExceptionClassificationService {

    private static final Logger log = LoggerFactory.getLogger(ExceptionClassificationService.class);
    private static final TypeReference<EventEnvelope<ExceptionRecord>> EXCEPTION_ENVELOPE_TYPE = new TypeReference<>() {};
    private static final String SCHEMA_VERSION = "1.0";
    private static final String SOURCE = "exception-classification";

    private final ObjectMapper objectMapper;
    private final ExceptionClassifier classifier;
    private final ExceptionRepository exceptionRepository;
    private final AuditEventRepository auditEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final DeadLetterPublisher deadLetterPublisher;
    private final KafkaProperties kafkaProperties;

    public ExceptionClassificationService(
            ObjectMapper objectMapper,
            ExceptionClassifier classifier,
            ExceptionRepository exceptionRepository,
            AuditEventRepository auditEventRepository,
            KafkaTemplate<String, String> kafkaTemplate,
            DeadLetterPublisher deadLetterPublisher,
            KafkaProperties kafkaProperties) {
        this.objectMapper = objectMapper;
        this.classifier = classifier;
        this.exceptionRepository = exceptionRepository;
        this.auditEventRepository = auditEventRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.deadLetterPublisher = deadLetterPublisher;
        this.kafkaProperties = kafkaProperties;
    }

    public ClassificationOutcome processExceptionCreated(
            String rawPayload,
            String key,
            String topic,
            int partition,
            long offset) {

        EventEnvelope<ExceptionRecord> envelope;
        try {
            envelope = objectMapper.readValue(rawPayload, EXCEPTION_ENVELOPE_TYPE);
        } catch (JsonProcessingException ex) {
            deadLetterPublisher.publish(
                    topic, partition, offset, key, null,
                    DeadLetterErrorCategory.DESERIALIZATION_ERROR,
                    "Malformed ExceptionCreated JSON: " + ex.getOriginalMessage(),
                    List.of(),
                    rawPayload);
            return ClassificationOutcome.DEAD_LETTERED;
        }

        if (envelope == null || envelope.payload() == null || envelope.payload().getExceptionId() == null) {
            deadLetterPublisher.publish(
                    topic, partition, offset, key,
                    envelope == null ? null : envelope.correlationId(),
                    DeadLetterErrorCategory.VALIDATION_ERROR,
                    "ExceptionCreated envelope missing payload or exceptionId",
                    List.of(),
                    rawPayload);
            return ClassificationOutcome.DEAD_LETTERED;
        }

        ExceptionRecord inbound = envelope.payload();
        String docId = ExceptionRecord.TYPE + ":" + inbound.getExceptionId();

        Optional<ExceptionRecord> existingOpt = exceptionRepository.findById(docId);
        if (existingOpt.isPresent() && existingOpt.get().getClassifiedAt() != null) {
            log.info("Duplicate exception classification ignored [exceptionId={}, category={}]",
                    inbound.getExceptionId(), existingOpt.get().getCategory());
            return ClassificationOutcome.DUPLICATE_IGNORED;
        }

        ExceptionRecord target = existingOpt.orElse(inbound);
        ClassificationResult result = classifier.classify(target);

        target.setCategory(result.category());
        target.setRootCause(result.rootCause());
        target.setExplanation(result.explanation());
        target.setPriority(result.priority());
        target.setSeverity(result.severity());
        target.setRecommendedAction(result.recommendedAction());
        target.setClassifiedAt(result.classifiedAt());

        ExceptionRecord saved;
        try {
            saved = exceptionRepository.save(target);
        } catch (CouchDbConflictException ex) {
            log.info("Concurrent classification conflict for '{}' (HTTP 409); ignoring duplicate", target.getExceptionId());
            return ClassificationOutcome.DUPLICATE_IGNORED;
        }

        recordAudit(envelope, saved);
        publishClassified(envelope.correlationId(), saved);

        log.info("Classified exception [exceptionId={}, category={}, priority={}, correlationId={}]",
                saved.getExceptionId(), saved.getCategory(), saved.getPriority(), envelope.correlationId());
        return ClassificationOutcome.CLASSIFIED;
    }

    private void recordAudit(EventEnvelope<ExceptionRecord> inbound, ExceptionRecord classified) {
        AuditEvent audit = new AuditEvent();
        audit.setEventId("cls-" + inbound.eventId());
        audit.setEventType(EventType.EXCEPTION_CLASSIFIED);
        audit.setTradeId(classified.getTradeId());
        audit.setExceptionId(classified.getExceptionId());
        audit.setEntityId(classified.getExceptionId());
        audit.setEntityType(ExceptionRecord.TYPE);
        audit.setCorrelationId(inbound.correlationId());
        audit.setActor("system:exception-classification");
        audit.setOccurredAt(Instant.now());
        audit.setDetails(Map.of(
                "category", classified.getCategory().name(),
                "priority", classified.getPriority().name(),
                "severity", classified.getSeverity().name(),
                "reasonCode", classified.getReasonCode() == null ? "" : classified.getReasonCode()));
        try {
            auditEventRepository.save(audit);
        } catch (CouchDbConflictException ex) {
            log.debug("Audit event '{}' already exists; skipping duplicate", audit.getEventId());
        }
    }

    private void publishClassified(String correlationId, ExceptionRecord classified) {
        EventEnvelope<ExceptionRecord> outbound = EventEnvelope.of(
                "ExceptionClassified", correlationId, SCHEMA_VERSION, SOURCE, classified);
        try {
            kafkaTemplate.send(
                    kafkaProperties.getTopics().getExceptionClassified(),
                    classified.getTradeId(),
                    objectMapper.writeValueAsString(outbound));
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Failed to serialize ExceptionClassified for " + classified.getExceptionId(), ex);
        }
    }

    public enum ClassificationOutcome {
        CLASSIFIED,
        DUPLICATE_IGNORED,
        DEAD_LETTERED
    }
}
