package com.tradeexception.platform.audit.domain;

import com.tradeexception.platform.common.domain.BaseDocument;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** Immutable, append-only audit record. Document id: {@code audit:<eventId>}. */
public class AuditEvent extends BaseDocument {

    public static final String TYPE = "audit";
    public static final int SCHEMA_VERSION = 1;

    @NotBlank @Size(max = 64)
    private String eventId;

    @NotNull
    private EventType eventType;

    /** Optional: some events (e.g. reference data changes) are not trade-specific. */
    @Size(max = 64)
    private String tradeId;

    /** Optional: populated for exception and remediation lifecycle events. */
    @Size(max = 64)
    private String exceptionId;

    /** Generic target entity identifier (e.g. confirmationId, exceptionId, workItemId). */
    @Size(max = 64)
    private String entityId;

    /** Generic target entity type (e.g. trade, confirmation, exception, workitem). */
    @Size(max = 32)
    private String entityType;

    @NotBlank @Size(max = 64)
    private String correlationId;

    /** {@code system:<module>} or a user id. */
    @NotBlank @Size(max = 64)
    private String actor;

    @NotNull
    private Instant occurredAt;

    private Map<String, String> details;

    public AuditEvent() {
        super(SCHEMA_VERSION);
    }

    @Override
    public String getType() { return TYPE; }

    @Override
    protected String businessKey() { return eventId; }

    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }
    public EventType getEventType() { return eventType; }
    public void setEventType(EventType eventType) { this.eventType = eventType; }
    public String getTradeId() { return tradeId; }
    public void setTradeId(String tradeId) { this.tradeId = tradeId; }
    public String getExceptionId() { return exceptionId; }
    public void setExceptionId(String exceptionId) { this.exceptionId = exceptionId; }
    public String getEntityId() { return entityId; }
    public void setEntityId(String entityId) { this.entityId = entityId; }
    public String getEntityType() { return entityType; }
    public void setEntityType(String entityType) { this.entityType = entityType; }
    public String getCorrelationId() { return correlationId; }
    public void setCorrelationId(String correlationId) { this.correlationId = correlationId; }
    public String getActor() { return actor; }
    public void setActor(String actor) { this.actor = actor; }
    public Instant getOccurredAt() { return occurredAt; }
    public void setOccurredAt(Instant occurredAt) { this.occurredAt = occurredAt; }
    public Map<String, String> getDetails() { return details; }
    public void setDetails(Map<String, String> details) { this.details = details == null ? null : Collections.unmodifiableMap(new LinkedHashMap<>(details)); }
}
