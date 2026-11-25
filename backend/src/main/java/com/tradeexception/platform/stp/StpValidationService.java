package com.tradeexception.platform.stp;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tradeexception.platform.audit.domain.AuditEvent;
import com.tradeexception.platform.audit.domain.EventType;
import com.tradeexception.platform.audit.repository.AuditEventRepository;
import com.tradeexception.platform.common.couchdb.CouchDbConflictException;
import com.tradeexception.platform.common.kafka.EventEnvelope;
import com.tradeexception.platform.common.kafka.KafkaProperties;
import com.tradeexception.platform.enrichment.domain.EnrichedTrade;
import com.tradeexception.platform.exception.domain.ExceptionCategory;
import com.tradeexception.platform.exception.domain.ExceptionPriority;
import com.tradeexception.platform.exception.domain.ExceptionRecord;
import com.tradeexception.platform.exception.domain.ExceptionStatus;
import com.tradeexception.platform.exception.repository.ExceptionRepository;
import com.tradeexception.platform.replay.DeadLetterPublisher;
import com.tradeexception.platform.replay.DeadLetterRecord.DeadLetterErrorCategory;
import com.tradeexception.platform.stp.domain.StpDecision;
import com.tradeexception.platform.stp.domain.StpValidationResult;
import com.tradeexception.platform.trade.domain.Trade;
import com.tradeexception.platform.trade.domain.TradeStatus;
import com.tradeexception.platform.trade.repository.TradeRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Consumes enriched trades from {@code trade.enriched}, evaluates deterministic STP rules,
 * persists initial {@link ExceptionRecord}s for rejected or manual-review trades, and routes
 * outcomes to {@code stp.validated}, {@code exception.created}, and {@code manual.review.required}.
 * <p>
 * Business rule failures are never routed to the technical dead-letter queue; only malformed
 * payloads or exhausted infrastructure retries go to {@code deadletter.trade}.
 */
@Service
public class StpValidationService {

    private static final Logger log = LoggerFactory.getLogger(StpValidationService.class);
    private static final TypeReference<EventEnvelope<EnrichedTrade>> ENRICHED_ENVELOPE_TYPE = new TypeReference<>() {};
    private static final String SCHEMA_VERSION = "1.0";
    private static final String SOURCE = "stp-validation";

    private final ObjectMapper objectMapper;
    private final StpRuleEngine ruleEngine;
    private final TradeRepository tradeRepository;
    private final ExceptionRepository exceptionRepository;
    private final AuditEventRepository auditEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final DeadLetterPublisher deadLetterPublisher;
    private final KafkaProperties kafkaProperties;

    public StpValidationService(
            ObjectMapper objectMapper,
            StpRuleEngine ruleEngine,
            TradeRepository tradeRepository,
            ExceptionRepository exceptionRepository,
            AuditEventRepository auditEventRepository,
            KafkaTemplate<String, String> kafkaTemplate,
            DeadLetterPublisher deadLetterPublisher,
            KafkaProperties kafkaProperties) {
        this.objectMapper = objectMapper;
        this.ruleEngine = ruleEngine;
        this.tradeRepository = tradeRepository;
        this.exceptionRepository = exceptionRepository;
        this.auditEventRepository = auditEventRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.deadLetterPublisher = deadLetterPublisher;
        this.kafkaProperties = kafkaProperties;
    }

    public ValidationOutcome processEnrichedMessage(
            String rawPayload,
            String key,
            String topic,
            int partition,
            long offset) {

        EventEnvelope<EnrichedTrade> envelope;
        try {
            envelope = objectMapper.readValue(rawPayload, ENRICHED_ENVELOPE_TYPE);
        } catch (JsonProcessingException ex) {
            deadLetterPublisher.publish(
                    topic, partition, offset, key, null,
                    DeadLetterErrorCategory.DESERIALIZATION_ERROR,
                    "Malformed TradeEnriched JSON: " + ex.getOriginalMessage(),
                    List.of(),
                    rawPayload);
            return ValidationOutcome.DEAD_LETTERED;
        }

        if (envelope == null || envelope.payload() == null || envelope.payload().trade() == null
                || envelope.payload().trade().getTradeId() == null) {
            deadLetterPublisher.publish(
                    topic, partition, offset, key,
                    envelope == null ? null : envelope.correlationId(),
                    DeadLetterErrorCategory.VALIDATION_ERROR,
                    "TradeEnriched envelope missing trade payload or tradeId",
                    List.of(),
                    rawPayload);
            return ValidationOutcome.DEAD_LETTERED;
        }

        EnrichedTrade enrichedTrade = envelope.payload();
        String tradeId = enrichedTrade.trade().getTradeId();

        // Idempotency: skip if trade already validated or moved into exception/confirmation state
        Optional<Trade> existing = tradeRepository.findByTradeId(tradeId);
        if (existing.isPresent() && isAlreadyValidatedOrBeyond(existing.get().getStatus())) {
            log.info("Duplicate STP validation skipped [tradeId={}, status={}]", tradeId, existing.get().getStatus());
            return ValidationOutcome.DUPLICATE_IGNORED;
        }

        StpValidationResult result = ruleEngine.validate(enrichedTrade);
        Trade tradeToPersist = existing.orElse(enrichedTrade.trade());

        if (result.decision() == StpDecision.STP_ELIGIBLE) {
            tradeToPersist.setStatus(TradeStatus.VALIDATED);
            if (!saveTradeIdempotently(tradeToPersist)) {
                return ValidationOutcome.DUPLICATE_IGNORED;
            }
            recordAudit(envelope, tradeId, null, EventType.STP_VALIDATED, result);
            publishJson(kafkaProperties.getTopics().getStpValidated(), tradeId,
                    EventEnvelope.of("StpValidated", envelope.correlationId(), SCHEMA_VERSION, SOURCE, result));
            log.info("Trade passed STP validation [tradeId={}, correlationId={}]", tradeId, envelope.correlationId());
            return ValidationOutcome.STP_ELIGIBLE;
        }

        // Business failure (STP_REJECTED or MANUAL_REVIEW_REQUIRED) -> initial ExceptionRecord
        ExceptionRecord exception = persistInitialExceptionIdempotently(result, envelope.correlationId());
        tradeToPersist.setStatus(TradeStatus.EXCEPTION);
        if (!saveTradeIdempotently(tradeToPersist)) {
            return ValidationOutcome.DUPLICATE_IGNORED;
        }

        EventType auditEventType = (result.decision() == StpDecision.MANUAL_REVIEW_REQUIRED)
                ? EventType.MANUAL_REVIEW_REQUIRED
                : EventType.EXCEPTION_CREATED;
        recordAudit(envelope, tradeId, exception.getExceptionId(), auditEventType, result);

        publishJson(kafkaProperties.getTopics().getExceptionCreated(), tradeId,
                EventEnvelope.of("ExceptionCreated", envelope.correlationId(), SCHEMA_VERSION, SOURCE, exception));

        if (result.decision() == StpDecision.MANUAL_REVIEW_REQUIRED) {
            publishJson(kafkaProperties.getTopics().getManualReviewRequired(), tradeId,
                    EventEnvelope.of("ManualReviewRequired", envelope.correlationId(), SCHEMA_VERSION, SOURCE, exception));
            log.info("Trade routed for manual review [tradeId={}, exceptionId={}, correlationId={}]",
                    tradeId, exception.getExceptionId(), envelope.correlationId());
            return ValidationOutcome.MANUAL_REVIEW_REQUIRED;
        }

        log.info("Trade rejected by STP validation [tradeId={}, exceptionId={}, correlationId={}]",
                tradeId, exception.getExceptionId(), envelope.correlationId());
        return ValidationOutcome.STP_REJECTED;
    }

    private ExceptionRecord persistInitialExceptionIdempotently(StpValidationResult result, String correlationId) {
        String exceptionId = "EXC-" + result.tradeId();
        String docId = ExceptionRecord.TYPE + ":" + exceptionId;

        Optional<ExceptionRecord> existing = exceptionRepository.findById(docId);
        if (existing.isPresent()) {
            log.info("Exception '{}' already exists; preserving existing record", exceptionId);
            return existing.get();
        }

        ExceptionRecord record = new ExceptionRecord();
        record.setExceptionId(exceptionId);
        record.setTradeId(result.tradeId());
        record.setCorrelationId(correlationId);
        record.setCategory(ExceptionCategory.UNCLASSIFIED); // Advanced classification belongs to Phase 5
        record.setPriority(result.decision() == StpDecision.STP_REJECTED
                ? ExceptionPriority.HIGH
                : ExceptionPriority.MEDIUM);
        record.setStatus(ExceptionStatus.OPEN);
        record.setReasonCode(result.reasonCodes().isEmpty()
                ? result.decision().name()
                : result.reasonCodes().getFirst());
        record.setDescription(result.explanation());
        record.setDetectedAt(result.validatedAt());

        try {
            return exceptionRepository.save(record);
        } catch (CouchDbConflictException ex) {
            log.info("Concurrent exception creation for '{}' (HTTP 409); returning existing record", exceptionId);
            return exceptionRepository.findById(docId).orElse(record);
        }
    }

    private boolean saveTradeIdempotently(Trade trade) {
        try {
            tradeRepository.save(trade);
            return true;
        } catch (CouchDbConflictException ex) {
            log.info("Concurrent trade status update for '{}' (HTTP 409); ignoring duplicate", trade.getTradeId());
            return false;
        }
    }

    private void recordAudit(
            EventEnvelope<EnrichedTrade> inbound,
            String tradeId,
            String exceptionId,
            EventType eventType,
            StpValidationResult result) {
        AuditEvent audit = new AuditEvent();
        audit.setEventId("stp-" + inbound.eventId());
        audit.setEventType(eventType);
        audit.setTradeId(tradeId);
        audit.setExceptionId(exceptionId);
        audit.setCorrelationId(inbound.correlationId());
        audit.setActor("system:stp-validation");
        audit.setOccurredAt(Instant.now());
        audit.setDetails(Map.of(
                "decision", result.decision().name(),
                "passedRules", String.valueOf(result.passedRules().size()),
                "failedRules", String.valueOf(result.failedRules().size())));
        try {
            auditEventRepository.save(audit);
        } catch (CouchDbConflictException ex) {
            log.debug("Audit event '{}' already exists; skipping duplicate", audit.getEventId());
        }
    }

    private void publishJson(String topic, String key, Object payload) {
        try {
            kafkaTemplate.send(topic, key, objectMapper.writeValueAsString(payload));
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Failed to serialize outbound event for topic " + topic, ex);
        }
    }

    private static boolean isAlreadyValidatedOrBeyond(TradeStatus status) {
        return status == TradeStatus.VALIDATED
                || status == TradeStatus.CONFIRMED
                || status == TradeStatus.EXCEPTION
                || status == TradeStatus.REJECTED;
    }

    public enum ValidationOutcome {
        STP_ELIGIBLE,
        STP_REJECTED,
        MANUAL_REVIEW_REQUIRED,
        DUPLICATE_IGNORED,
        DEAD_LETTERED
    }
}
