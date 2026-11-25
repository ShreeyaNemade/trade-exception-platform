package com.tradeexception.platform.ingestion;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tradeexception.platform.audit.domain.AuditEvent;
import com.tradeexception.platform.audit.domain.EventType;
import com.tradeexception.platform.audit.repository.AuditEventRepository;
import com.tradeexception.platform.common.couchdb.CouchDbConflictException;
import com.tradeexception.platform.common.kafka.EventEnvelope;
import com.tradeexception.platform.common.kafka.KafkaProperties;
import com.tradeexception.platform.replay.DeadLetterPublisher;
import com.tradeexception.platform.replay.DeadLetterRecord.DeadLetterErrorCategory;
import com.tradeexception.platform.trade.domain.Trade;
import com.tradeexception.platform.trade.domain.TradeStatus;
import com.tradeexception.platform.trade.repository.TradeRepository;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Core trade ingestion workflow: deserializes raw Kafka events, validates the envelope and
 * {@link Trade} aggregate, enforces idempotency, persists to CouchDB, records an immutable
 * {@link AuditEvent}, and publishes {@code TradeAccepted} to {@code trade.accepted}.
 * <p>
 * Irrecoverable deserialization and validation failures are routed directly to
 * {@code deadletter.trade}. Recoverable infrastructure failures (such as CouchDB unavailability)
 * are allowed to propagate so Spring Kafka performs bounded retries before dead-lettering.
 */
@Service
public class TradeIngestionService {

    private static final Logger log = LoggerFactory.getLogger(TradeIngestionService.class);
    private static final TypeReference<EventEnvelope<Trade>> TRADE_ENVELOPE_TYPE = new TypeReference<>() {};
    private static final String SCHEMA_VERSION = "1.0";
    private static final String SOURCE = "trade-ingestion";

    private final ObjectMapper objectMapper;
    private final Validator validator;
    private final TradeRepository tradeRepository;
    private final AuditEventRepository auditEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final DeadLetterPublisher deadLetterPublisher;
    private final KafkaProperties kafkaProperties;

    public TradeIngestionService(
            ObjectMapper objectMapper,
            Validator validator,
            TradeRepository tradeRepository,
            AuditEventRepository auditEventRepository,
            KafkaTemplate<String, String> kafkaTemplate,
            DeadLetterPublisher deadLetterPublisher,
            KafkaProperties kafkaProperties) {
        this.objectMapper = objectMapper;
        this.validator = validator;
        this.tradeRepository = tradeRepository;
        this.auditEventRepository = auditEventRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.deadLetterPublisher = deadLetterPublisher;
        this.kafkaProperties = kafkaProperties;
    }

    public IngestionOutcome processRawMessage(
            String rawPayload,
            String key,
            String topic,
            int partition,
            long offset) {

        if (rawPayload == null || rawPayload.isBlank()) {
            deadLetterPublisher.publish(
                    topic, partition, offset, key, null,
                    DeadLetterErrorCategory.DESERIALIZATION_ERROR,
                    "Empty or null trade payload",
                    List.of(),
                    rawPayload);
            return IngestionOutcome.DEAD_LETTERED;
        }

        EventEnvelope<Trade> envelope;
        try {
            envelope = objectMapper.readValue(rawPayload, TRADE_ENVELOPE_TYPE);
        } catch (JsonProcessingException ex) {
            deadLetterPublisher.publish(
                    topic, partition, offset, key, null,
                    DeadLetterErrorCategory.DESERIALIZATION_ERROR,
                    "Malformed JSON payload: " + ex.getOriginalMessage(),
                    List.of(),
                    rawPayload);
            return IngestionOutcome.DEAD_LETTERED;
        }

        List<String> structuralErrors = validateStructuralRequirements(envelope);
        if (!structuralErrors.isEmpty()) {
            deadLetterPublisher.publish(
                    topic, partition, offset, key, envelope == null ? null : envelope.correlationId(),
                    DeadLetterErrorCategory.VALIDATION_ERROR,
                    "Trade envelope or mandatory core identifier validation failed",
                    structuralErrors,
                    rawPayload);
            return IngestionOutcome.DEAD_LETTERED;
        }

        Trade trade = envelope.payload();
        String tradeId = trade.getTradeId();

        // Idempotency check: skip already-persisted trades (duplicate delivery or consumer restart)
        if (tradeRepository.findByTradeId(tradeId).isPresent()) {
            log.info("Duplicate trade detected and ignored [tradeId={}, eventId={}, correlationId={}]",
                    tradeId, envelope.eventId(), envelope.correlationId());
            return IngestionOutcome.DUPLICATE_IGNORED;
        }

        trade.setStatus(TradeStatus.ACCEPTED);
        Trade savedTrade;
        try {
            savedTrade = tradeRepository.save(trade);
        } catch (CouchDbConflictException ex) {
            log.info("Concurrent duplicate write detected for trade {} (HTTP 409); ignoring duplicate", tradeId);
            return IngestionOutcome.DUPLICATE_IGNORED;
        }

        recordAuditEvent(envelope, savedTrade);
        publishAcceptedEvent(envelope, savedTrade);

        log.info("Accepted trade [tradeId={}, correlationId={}, rev={}]",
                savedTrade.getTradeId(), envelope.correlationId(), savedTrade.getRevision());
        return IngestionOutcome.ACCEPTED;
    }

    /**
     * Validates structural envelope fields and mandatory core trade identifiers at ingestion.
     * Business-rule validation (economic terms, counterparty references, dates, product rules)
     * is intentionally delegated downstream to Enrichment and the STP Validation Engine.
     */
    private List<String> validateStructuralRequirements(EventEnvelope<Trade> envelope) {
        List<String> errors = new ArrayList<>();
        if (envelope == null) {
            errors.add("envelope: must not be null");
            return errors;
        }
        for (String field : List.of("eventId", "eventType", "correlationId", "schemaVersion", "timestamp", "source", "payload")) {
            for (ConstraintViolation<EventEnvelope<Trade>> v : validator.validateProperty(envelope, field)) {
                errors.add(v.getPropertyPath() + ": " + v.getMessage());
            }
        }
        Trade trade = envelope.payload();
        if (trade != null) {
            if (trade.getTradeId() == null || trade.getTradeId().isBlank()) {
                errors.add("payload.tradeId: must not be blank");
            }
            if (trade.getSourceSystemTradeId() == null || trade.getSourceSystemTradeId().isBlank()) {
                errors.add("payload.sourceSystemTradeId: must not be blank");
            }
            if (trade.getProductType() == null) {
                errors.add("payload.productType: must not be null");
            }
            if (trade.getHeader() == null) {
                errors.add("payload.header: must not be null");
            } else {
                Set<ConstraintViolation<com.tradeexception.platform.trade.domain.TradeMessageHeader>> headerViolations =
                        validator.validate(trade.getHeader());
                for (var v : headerViolations) {
                    errors.add("payload.header." + v.getPropertyPath() + ": " + v.getMessage());
                }
            }
        }
        return errors.stream().sorted().toList();
    }

    private void recordAuditEvent(EventEnvelope<Trade> envelope, Trade savedTrade) {
        AuditEvent audit = new AuditEvent();
        audit.setEventId("ingest-" + envelope.eventId());
        audit.setEventType(EventType.TRADE_ACCEPTED);
        audit.setTradeId(savedTrade.getTradeId());
        audit.setCorrelationId(envelope.correlationId());
        audit.setActor("system:ingestion");
        audit.setOccurredAt(Instant.now());
        audit.setDetails(Map.of(
                "messageId", savedTrade.getHeader().messageId(),
                "sourceSystem", savedTrade.getHeader().sourceSystem(),
                "productType", savedTrade.getProductType().name()));
        try {
            auditEventRepository.save(audit);
        } catch (CouchDbConflictException ex) {
            log.debug("Audit event '{}' already exists; skipping duplicate audit write", audit.getEventId());
        }
    }

    private void publishAcceptedEvent(EventEnvelope<Trade> inboundEnvelope, Trade savedTrade) {
        EventEnvelope<Trade> accepted = EventEnvelope.of(
                "TradeAccepted",
                inboundEnvelope.correlationId(),
                SCHEMA_VERSION,
                SOURCE,
                savedTrade);
        String acceptedTopic = kafkaProperties.getTopics().getTradeAccepted();
        try {
            String json = objectMapper.writeValueAsString(accepted);
            kafkaTemplate.send(acceptedTopic, savedTrade.getTradeId(), json);
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Failed to serialize TradeAccepted event for " + savedTrade.getTradeId(), ex);
        }
    }

    public enum IngestionOutcome {
        ACCEPTED,
        DUPLICATE_IGNORED,
        DEAD_LETTERED
    }
}
