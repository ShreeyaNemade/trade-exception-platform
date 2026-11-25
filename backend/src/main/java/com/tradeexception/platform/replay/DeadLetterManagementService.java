package com.tradeexception.platform.replay;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tradeexception.platform.audit.domain.AuditEvent;
import com.tradeexception.platform.audit.domain.EventType;
import com.tradeexception.platform.audit.repository.AuditEventRepository;
import com.tradeexception.platform.common.api.PagedResponse;
import com.tradeexception.platform.common.couchdb.CouchDbConflictException;
import com.tradeexception.platform.common.couchdb.CouchDbException;
import com.tradeexception.platform.common.couchdb.CouchDbNotFoundException;
import com.tradeexception.platform.common.kafka.EventEnvelope;
import com.tradeexception.platform.replay.DeadLetterRecord.DeadLetterErrorCategory;
import com.tradeexception.platform.replay.api.DeadLetterDto;
import com.tradeexception.platform.replay.api.ReplayResultDto;
import com.tradeexception.platform.replay.domain.DeadLetterDocument;
import com.tradeexception.platform.replay.domain.ReplayAttempt;
import com.tradeexception.platform.replay.domain.ReplayStatus;
import com.tradeexception.platform.replay.repository.DeadLetterRepository;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Manages persistent DLQ records in CouchDB and controlled, audited message replay back to
 * original Kafka topics.
 * <p>
 * <strong>Atomicity note:</strong> Kafka publication and CouchDB status updates are not a single
 * atomic transaction. To prevent concurrent duplicate replays, the document is first transitioned
 * to {@code IN_PROGRESS} using CouchDB MVCC {@code _rev}. If Kafka succeeds and the final CouchDB
 * write fails, the record remains {@code IN_PROGRESS} while downstream consumer idempotency
 * prevents duplicate trade persistence.
 */
@Service
public class DeadLetterManagementService {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterManagementService.class);
    private static final TypeReference<EventEnvelope<DeadLetterRecord>> DLQ_ENVELOPE_TYPE = new TypeReference<>() {};

    private final ObjectMapper objectMapper;
    private final DeadLetterRepository deadLetterRepository;
    private final AuditEventRepository auditEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public DeadLetterManagementService(
            ObjectMapper objectMapper,
            DeadLetterRepository deadLetterRepository,
            AuditEventRepository auditEventRepository,
            KafkaTemplate<String, String> kafkaTemplate) {
        this.objectMapper = objectMapper;
        this.deadLetterRepository = deadLetterRepository;
        this.auditEventRepository = auditEventRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    /** Persists a dead-letter event from {@code deadletter.trade} idempotently. */
    public boolean persistFromKafka(String rawJson) {
        if (rawJson == null || rawJson.isBlank()) {
            return false;
        }
        EventEnvelope<DeadLetterRecord> envelope;
        try {
            envelope = objectMapper.readValue(rawJson, DLQ_ENVELOPE_TYPE);
        } catch (JsonProcessingException ex) {
            log.error("Unable to deserialize deadletter.trade message: {}", ex.getOriginalMessage());
            return false;
        }
        if (envelope == null || envelope.payload() == null) {
            return false;
        }

        DeadLetterRecord rec = envelope.payload();
        String dlqId = (rec.deadLetterId() != null && !rec.deadLetterId().isBlank())
                ? rec.deadLetterId()
                : DeadLetterPublisher.deterministicId(rec.originalTopic(), rec.originalPartition(), rec.originalOffset());

        if (deadLetterRepository.findByDeadLetterId(dlqId).isPresent()) {
            log.debug("Duplicate DLQ record '{}' ignored", dlqId);
            return false;
        }

        DeadLetterDocument doc = new DeadLetterDocument();
        doc.setDeadLetterId(dlqId);
        doc.setOriginalEventId(extractEventId(rec.rawPayload()));
        doc.setOriginalTopic(rec.originalTopic() == null ? "unknown" : rec.originalTopic());
        doc.setOriginalPartition(rec.originalPartition());
        doc.setOriginalOffset(rec.originalOffset());
        doc.setOriginalKey(rec.originalKey());
        doc.setCorrelationId(rec.correlationId() == null ? envelope.correlationId() : rec.correlationId());
        doc.setErrorCategory(rec.errorCategory());
        doc.setFailureType(rec.errorCategory() == DeadLetterErrorCategory.VALIDATION_ERROR ? "BUSINESS" : "TECHNICAL");
        doc.setErrorMessage(rec.errorMessage());
        doc.setValidationErrors(rec.validationErrors());
        doc.setRawPayload(rec.rawPayload());
        doc.setFailedAt(rec.failedAt() != null ? rec.failedAt() : Instant.now());
        doc.setReplayStatus(isReplayableCategory(rec.errorCategory()) ? ReplayStatus.PENDING : ReplayStatus.NOT_ELIGIBLE);
        doc.setReplayAttempts(0);
        doc.setMaxReplayAttempts(DeadLetterDocument.DEFAULT_MAX_REPLAY_ATTEMPTS);

        try {
            deadLetterRepository.save(doc);
        } catch (CouchDbConflictException ex) {
            log.debug("Concurrent DLQ persistence for '{}' (HTTP 409); ignoring duplicate", dlqId);
            return false;
        }

        recordAudit("dlq-create-" + dlqId, EventType.DLQ_RECORD_CREATED, doc, "system:dlq-persistence",
                Map.of("originalTopic", doc.getOriginalTopic(), "errorCategory", doc.getErrorCategory().name()));
        return true;
    }

    public PagedResponse<DeadLetterDto> list(
            DeadLetterErrorCategory errorCategory,
            String originalTopic,
            ReplayStatus replayStatus,
            Instant from,
            Instant to,
            int page,
            int size,
            String sortBy,
            String sortDir) {

        List<DeadLetterDto> items = deadLetterRepository.findAll().stream()
                .filter(d -> errorCategory == null || d.getErrorCategory() == errorCategory)
                .filter(d -> originalTopic == null || originalTopic.isBlank() || originalTopic.equalsIgnoreCase(d.getOriginalTopic()))
                .filter(d -> replayStatus == null || d.getReplayStatus() == replayStatus)
                .filter(d -> from == null || (d.getFailedAt() != null && !d.getFailedAt().isBefore(from)))
                .filter(d -> to == null || (d.getFailedAt() != null && !d.getFailedAt().isAfter(to)))
                .map(DeadLetterDto::from)
                .sorted(comparatorFor(sortBy, sortDir))
                .toList();

        return PagedResponse.of(items, page, size, sortBy, sortDir);
    }

    public DeadLetterDto getById(String deadLetterId) {
        return DeadLetterDto.from(loadOrThrow(deadLetterId));
    }

    public List<ReplayAttempt> getHistory(String deadLetterId) {
        return loadOrThrow(deadLetterId).getHistory();
    }

    /**
     * Performs controlled replay of an eligible dead-letter document back to its original Kafka topic
     * without modifying the original payload, event ID, or correlation ID.
     */
    public ReplayResultDto replay(String deadLetterId, String operator) {
        DeadLetterDocument doc = loadOrThrow(deadLetterId);
        validateEligibility(doc);

        String effectiveOperator = (operator == null || operator.isBlank()) ? "ops-user" : operator;
        int attemptNumber = doc.getReplayAttempts() + 1;
        Instant requestedAt = Instant.now();

        // Acquire optimistic lock in CouchDB before publishing to prevent concurrent duplicate replays
        doc.setReplayStatus(ReplayStatus.IN_PROGRESS);
        doc = deadLetterRepository.save(doc);

        recordAudit("dlq-req-" + deadLetterId + "-" + attemptNumber, EventType.DLQ_REPLAY_REQUESTED, doc, effectiveOperator,
                Map.of("attempt", String.valueOf(attemptNumber), "originalTopic", doc.getOriginalTopic()));

        try {
            String messageKey = (doc.getOriginalKey() != null && !doc.getOriginalKey().isBlank())
                    ? doc.getOriginalKey()
                    : doc.getDeadLetterId();
            ProducerRecord<String, String> record = new ProducerRecord<>(doc.getOriginalTopic(), messageKey, doc.getRawPayload());
            record.headers().add("x-replayed-from-dlq", doc.getDeadLetterId().getBytes(StandardCharsets.UTF_8));
            record.headers().add("x-replay-attempt", String.valueOf(attemptNumber).getBytes(StandardCharsets.UTF_8));
            if (doc.getCorrelationId() != null) {
                record.headers().add("x-correlation-id", doc.getCorrelationId().getBytes(StandardCharsets.UTF_8));
            }
            if (doc.getOriginalEventId() != null) {
                record.headers().add("x-original-event-id", doc.getOriginalEventId().getBytes(StandardCharsets.UTF_8));
            }

            kafkaTemplate.send(record).get(5, TimeUnit.SECONDS);

            Instant completedAt = Instant.now();
            doc.setReplayStatus(ReplayStatus.REPLAYED);
            doc.setReplayAttempts(attemptNumber);
            doc.setLastReplayedAt(completedAt);
            doc.setLastReplayedBy(effectiveOperator);
            doc.setHistory(appendHistory(doc.getHistory(), new ReplayAttempt(
                    attemptNumber, requestedAt, completedAt, effectiveOperator, "SUCCEEDED",
                    "Republished unmodified payload to " + doc.getOriginalTopic())));
            deadLetterRepository.save(doc);

            recordAudit("dlq-ok-" + deadLetterId + "-" + attemptNumber, EventType.DLQ_REPLAY_SUCCEEDED, doc, effectiveOperator,
                    Map.of("attempt", String.valueOf(attemptNumber), "originalTopic", doc.getOriginalTopic()));

            return new ReplayResultDto(
                    doc.getDeadLetterId(),
                    doc.getOriginalTopic(),
                    doc.getOriginalEventId(),
                    doc.getCorrelationId(),
                    doc.getReplayStatus(),
                    attemptNumber,
                    effectiveOperator,
                    completedAt,
                    "Message republished to " + doc.getOriginalTopic());
        } catch (Exception ex) {
            Instant failedAt = Instant.now();
            doc.setReplayStatus(ReplayStatus.FAILED);
            doc.setReplayAttempts(attemptNumber);
            doc.setLastReplayedAt(failedAt);
            doc.setLastReplayedBy(effectiveOperator);
            doc.setHistory(appendHistory(doc.getHistory(), new ReplayAttempt(
                    attemptNumber, requestedAt, failedAt, effectiveOperator, "FAILED",
                    "Kafka publish failed: " + ex.getMessage())));
            try {
                deadLetterRepository.save(doc);
            } catch (CouchDbException ignored) {
                log.error("Failed to persist FAILED replay state for {}", deadLetterId);
            }
            recordAudit("dlq-fail-" + deadLetterId + "-" + attemptNumber, EventType.DLQ_REPLAY_FAILED, doc, effectiveOperator,
                    Map.of("attempt", String.valueOf(attemptNumber), "error", String.valueOf(ex.getMessage())));
            throw new CouchDbException(HttpStatus.BAD_GATEWAY, "REPLAY_PUBLISH_FAILED",
                    "Failed to republish message '" + deadLetterId + "' to Kafka topic '" + doc.getOriginalTopic() + "'", ex);
        }
    }

    private static void validateEligibility(DeadLetterDocument doc) {
        if (doc.getReplayStatus() == ReplayStatus.REPLAYED) {
            throw new IllegalStateException("Dead-letter record '" + doc.getDeadLetterId() + "' has already been replayed");
        }
        if (doc.getReplayStatus() == ReplayStatus.IN_PROGRESS) {
            throw new IllegalStateException("Replay is already in progress for '" + doc.getDeadLetterId() + "'");
        }
        if (!isReplayableCategory(doc.getErrorCategory()) || doc.getReplayStatus() == ReplayStatus.NOT_ELIGIBLE) {
            throw new IllegalStateException("Malformed or invalid message (" + doc.getErrorCategory()
                    + ") cannot be replayed without payload correction");
        }
        if (doc.getReplayAttempts() >= doc.getMaxReplayAttempts()) {
            throw new IllegalStateException("Maximum replay attempts (" + doc.getMaxReplayAttempts()
                    + ") reached for '" + doc.getDeadLetterId() + "'");
        }
        if (doc.getRawPayload() == null || doc.getRawPayload().isBlank()) {
            throw new IllegalStateException("Cannot replay record '" + doc.getDeadLetterId() + "' with empty payload");
        }
    }

    private static boolean isReplayableCategory(DeadLetterErrorCategory category) {
        return category == DeadLetterErrorCategory.INFRASTRUCTURE_ERROR
                || category == DeadLetterErrorCategory.UNEXPECTED_ERROR;
    }

    private DeadLetterDocument loadOrThrow(String deadLetterId) {
        return deadLetterRepository.findByDeadLetterId(deadLetterId)
                .orElseThrow(() -> new CouchDbNotFoundException("dead-letters", DeadLetterDocument.documentId(deadLetterId), null));
    }

    private String extractEventId(String rawPayload) {
        if (rawPayload == null || rawPayload.isBlank()) {
            return null;
        }
        try {
            JsonNode node = objectMapper.readTree(rawPayload);
            return node.hasNonNull("eventId") ? node.get("eventId").asText() : null;
        } catch (JsonProcessingException ex) {
            return null;
        }
    }

    private static List<ReplayAttempt> appendHistory(List<ReplayAttempt> existing, ReplayAttempt next) {
        List<ReplayAttempt> updated = new ArrayList<>(existing == null ? List.of() : existing);
        updated.add(next);
        return updated;
    }

    private void recordAudit(String eventId, EventType type, DeadLetterDocument doc, String actor, Map<String, String> details) {
        AuditEvent audit = new AuditEvent();
        audit.setEventId(eventId);
        audit.setEventType(type);
        audit.setEntityId(doc.getDeadLetterId());
        audit.setEntityType(DeadLetterDocument.TYPE);
        audit.setCorrelationId(doc.getCorrelationId() == null ? "UNKNOWN-" + UUID.randomUUID() : doc.getCorrelationId());
        audit.setActor(actor);
        audit.setOccurredAt(Instant.now());
        audit.setDetails(details);
        try {
            auditEventRepository.save(audit);
        } catch (CouchDbException ex) {
            log.debug("Audit event '{}' skipped: {}", eventId, ex.getMessage());
        }
    }

    private static Comparator<DeadLetterDto> comparatorFor(String sortBy, String sortDir) {
        Comparator<DeadLetterDto> cmp = switch (sortBy == null ? "failedAt" : sortBy) {
            case "errorCategory" -> Comparator.comparing(d -> d.errorCategory() == null ? "" : d.errorCategory().name());
            case "replayStatus" -> Comparator.comparing(d -> d.replayStatus() == null ? "" : d.replayStatus().name());
            case "originalTopic" -> Comparator.comparing(d -> d.originalTopic() == null ? "" : d.originalTopic());
            default -> Comparator.comparing(DeadLetterDto::failedAt, Comparator.nullsLast(Comparator.naturalOrder()));
        };
        return "asc".equalsIgnoreCase(sortDir) ? cmp : cmp.reversed();
    }
}
