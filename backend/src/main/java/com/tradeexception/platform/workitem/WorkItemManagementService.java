package com.tradeexception.platform.workitem;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tradeexception.platform.audit.domain.AuditEvent;
import com.tradeexception.platform.audit.domain.EventType;
import com.tradeexception.platform.audit.repository.AuditEventRepository;
import com.tradeexception.platform.common.couchdb.CouchDbConflictException;
import com.tradeexception.platform.common.couchdb.CouchDbNotFoundException;
import com.tradeexception.platform.common.kafka.EventEnvelope;
import com.tradeexception.platform.common.kafka.KafkaProperties;
import com.tradeexception.platform.exception.domain.ExceptionPriority;
import com.tradeexception.platform.exception.domain.ExceptionRecord;
import com.tradeexception.platform.replay.DeadLetterPublisher;
import com.tradeexception.platform.replay.DeadLetterRecord.DeadLetterErrorCategory;
import com.tradeexception.platform.workitem.domain.OperationalTeam;
import com.tradeexception.platform.workitem.domain.WorkItem;
import com.tradeexception.platform.workitem.domain.WorkItemStatus;
import com.tradeexception.platform.workitem.repository.WorkItemRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Manages remediation work items: automatically creates deterministic work items from
 * {@code exception.classified} events, assigns synthetic operational teams, publishes
 * {@code workitem.linked}, and supports audited status transitions and assignment.
 */
@Service
public class WorkItemManagementService {

    private static final Logger log = LoggerFactory.getLogger(WorkItemManagementService.class);
    private static final TypeReference<EventEnvelope<ExceptionRecord>> EXCEPTION_ENVELOPE_TYPE = new TypeReference<>() {};
    private static final String SCHEMA_VERSION = "1.0";
    private static final String SOURCE = "workitem-management";

    private static final Map<WorkItemStatus, Set<WorkItemStatus>> ALLOWED_TRANSITIONS = Map.of(
            WorkItemStatus.OPEN, Set.of(WorkItemStatus.IN_PROGRESS, WorkItemStatus.PENDING, WorkItemStatus.RESOLVED, WorkItemStatus.CLOSED),
            WorkItemStatus.ASSIGNED, Set.of(WorkItemStatus.IN_PROGRESS, WorkItemStatus.PENDING, WorkItemStatus.RESOLVED, WorkItemStatus.CLOSED),
            WorkItemStatus.IN_PROGRESS, Set.of(WorkItemStatus.PENDING, WorkItemStatus.RESOLVED, WorkItemStatus.CLOSED),
            WorkItemStatus.PENDING, Set.of(WorkItemStatus.IN_PROGRESS, WorkItemStatus.RESOLVED, WorkItemStatus.CLOSED),
            WorkItemStatus.RESOLVED, Set.of(WorkItemStatus.CLOSED, WorkItemStatus.IN_PROGRESS),
            WorkItemStatus.CLOSED, Set.of(),
            WorkItemStatus.CANCELLED, Set.of()
    );

    private final ObjectMapper objectMapper;
    private final TeamRoutingStrategy teamRoutingStrategy;
    private final WorkItemRepository workItemRepository;
    private final AuditEventRepository auditEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final DeadLetterPublisher deadLetterPublisher;
    private final KafkaProperties kafkaProperties;

    public WorkItemManagementService(
            ObjectMapper objectMapper,
            TeamRoutingStrategy teamRoutingStrategy,
            WorkItemRepository workItemRepository,
            AuditEventRepository auditEventRepository,
            KafkaTemplate<String, String> kafkaTemplate,
            DeadLetterPublisher deadLetterPublisher,
            KafkaProperties kafkaProperties) {
        this.objectMapper = objectMapper;
        this.teamRoutingStrategy = teamRoutingStrategy;
        this.workItemRepository = workItemRepository;
        this.auditEventRepository = auditEventRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.deadLetterPublisher = deadLetterPublisher;
        this.kafkaProperties = kafkaProperties;
    }

    public WorkItemOutcome processClassifiedException(
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
                    "Malformed ExceptionClassified JSON: " + ex.getOriginalMessage(),
                    List.of(),
                    rawPayload);
            return WorkItemOutcome.DEAD_LETTERED;
        }

        if (envelope == null || envelope.payload() == null || envelope.payload().getExceptionId() == null) {
            deadLetterPublisher.publish(
                    topic, partition, offset, key,
                    envelope == null ? null : envelope.correlationId(),
                    DeadLetterErrorCategory.VALIDATION_ERROR,
                    "ExceptionClassified envelope missing payload or exceptionId",
                    List.of(),
                    rawPayload);
            return WorkItemOutcome.DEAD_LETTERED;
        }

        ExceptionRecord exception = envelope.payload();
        String workItemId = "WI-" + exception.getExceptionId();
        String docId = WorkItem.TYPE + ":" + workItemId;

        // Idempotency: never overwrite an existing work item
        if (workItemRepository.findById(docId).isPresent()) {
            log.info("Duplicate work item creation ignored [workItemId={}, exceptionId={}]",
                    workItemId, exception.getExceptionId());
            return WorkItemOutcome.DUPLICATE_IGNORED;
        }

        OperationalTeam team = teamRoutingStrategy.assignTeam(exception.getCategory());
        ExceptionPriority priority = exception.getPriority() == null ? ExceptionPriority.MEDIUM : exception.getPriority();

        WorkItem workItem = new WorkItem();
        workItem.setWorkItemId(workItemId);
        workItem.setExceptionId(exception.getExceptionId());
        workItem.setTradeId(exception.getTradeId());
        workItem.setCorrelationId(envelope.correlationId());
        workItem.setTitle("Remediate " + exception.getCategory() + " for trade " + exception.getTradeId());
        workItem.setDescription(exception.getExplanation() != null ? exception.getExplanation() : exception.getDescription());
        workItem.setPriority(priority);
        workItem.setStatus(WorkItemStatus.OPEN);
        workItem.setAssignedTeam(team);
        workItem.setDueAt(Instant.now().plus(slaForPriority(priority)));
        workItem.setRecommendedAction(exception.getRecommendedAction());

        WorkItem saved;
        try {
            saved = workItemRepository.save(workItem);
        } catch (CouchDbConflictException ex) {
            log.info("Concurrent work item creation for '{}' (HTTP 409); ignoring duplicate", workItemId);
            return WorkItemOutcome.DUPLICATE_IGNORED;
        }

        recordAudit("wi-create-" + envelope.eventId(), EventType.WORKITEM_CREATED, saved, "system:workitem-management",
                Map.of("assignedTeam", team.name(), "priority", priority.name(), "status", saved.getStatus().name()));
        publishWorkItemLinked(envelope.correlationId(), saved);

        log.info("Created work item [workItemId={}, team={}, priority={}, correlationId={}]",
                saved.getWorkItemId(), team, priority, envelope.correlationId());
        return WorkItemOutcome.CREATED;
    }

    /** Assigns a work item to a synthetic operational queue/user identifier and records an audit event. */
    public WorkItem assignWorkItem(String workItemId, String assignee, String actor) {
        return assignWorkItem(workItemId, null, assignee, actor);
    }

    /** Assigns a work item to a supported operational team (and optional assignee) and records an audit event. */
    public WorkItem assignWorkItem(String workItemId, OperationalTeam team, String assignee, String actor) {
        WorkItem item = loadWorkItem(workItemId);
        if (team != null) {
            item.setAssignedTeam(team);
        }
        if (assignee != null) {
            item.setAssignee(assignee.isBlank() ? null : assignee.trim());
        }
        WorkItem saved = workItemRepository.save(item);
        recordAudit("wi-assign-" + UUID.randomUUID(), EventType.WORKITEM_ASSIGNED, saved,
                (actor == null || actor.isBlank()) ? "ops-user" : actor,
                Map.of(
                        "assignedTeam", saved.getAssignedTeam() == null ? "" : saved.getAssignedTeam().name(),
                        "assignee", saved.getAssignee() == null ? "" : saved.getAssignee()));
        return saved;
    }

    /** Transitions a work item's status enforcing valid lifecycle rules and records an audit event. */
    public WorkItem updateStatus(String workItemId, WorkItemStatus targetStatus, String actor) {
        WorkItem item = loadWorkItem(workItemId);
        WorkItemStatus current = item.getStatus();
        if (current == targetStatus) {
            return item;
        }
        Set<WorkItemStatus> allowed = ALLOWED_TRANSITIONS.getOrDefault(current, Set.of());
        if (!allowed.contains(targetStatus)) {
            throw new IllegalStateException("Invalid work item status transition from " + current + " to " + targetStatus);
        }
        item.setStatus(targetStatus);
        WorkItem saved = workItemRepository.save(item);
        EventType eventType = (targetStatus == WorkItemStatus.RESOLVED)
                ? EventType.WORKITEM_RESOLVED
                : EventType.WORKITEM_UPDATED;
        recordAudit("wi-status-" + UUID.randomUUID(), eventType, saved, actor,
                Map.of("fromStatus", current.name(), "toStatus", targetStatus.name()));
        return saved;
    }

    private WorkItem loadWorkItem(String workItemId) {
        String docId = WorkItem.TYPE + ":" + workItemId;
        return workItemRepository.findById(docId)
                .orElseThrow(() -> new CouchDbNotFoundException("work-items", docId, null));
    }

    private static Duration slaForPriority(ExceptionPriority priority) {
        return switch (priority) {
            case CRITICAL -> Duration.ofHours(4);
            case HIGH -> Duration.ofHours(8);
            case MEDIUM -> Duration.ofHours(24);
            case LOW -> Duration.ofHours(48);
        };
    }

    private void recordAudit(String eventId, EventType type, WorkItem item, String actor, Map<String, String> details) {
        AuditEvent audit = new AuditEvent();
        audit.setEventId(eventId);
        audit.setEventType(type);
        audit.setTradeId(item.getTradeId());
        audit.setExceptionId(item.getExceptionId());
        audit.setEntityId(item.getWorkItemId());
        audit.setEntityType(WorkItem.TYPE);
        audit.setCorrelationId(item.getCorrelationId());
        audit.setActor(actor);
        audit.setOccurredAt(Instant.now());
        audit.setDetails(details);
        try {
            auditEventRepository.save(audit);
        } catch (CouchDbConflictException ex) {
            log.debug("Audit event '{}' already exists; skipping duplicate", eventId);
        }
    }

    private void publishWorkItemLinked(String correlationId, WorkItem saved) {
        EventEnvelope<WorkItem> outbound = EventEnvelope.of(
                "WorkItemLinked", correlationId, SCHEMA_VERSION, SOURCE, saved);
        try {
            kafkaTemplate.send(
                    kafkaProperties.getTopics().getWorkitemLinked(),
                    saved.getTradeId(),
                    objectMapper.writeValueAsString(outbound));
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Failed to serialize WorkItemLinked for " + saved.getWorkItemId(), ex);
        }
    }

    public enum WorkItemOutcome {
        CREATED,
        DUPLICATE_IGNORED,
        DEAD_LETTERED
    }
}
