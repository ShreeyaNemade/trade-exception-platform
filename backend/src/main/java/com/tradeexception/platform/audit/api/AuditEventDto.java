package com.tradeexception.platform.audit.api;

import com.tradeexception.platform.audit.domain.AuditEvent;
import com.tradeexception.platform.audit.domain.EventType;

import java.time.Instant;
import java.util.Map;

/** Read-only API representation of an immutable {@link AuditEvent}. */
public record AuditEventDto(
        String eventId,
        EventType eventType,
        String entityId,
        String entityType,
        String tradeId,
        String exceptionId,
        String correlationId,
        String actor,
        Instant occurredAt,
        Map<String, String> details) {

    public static AuditEventDto from(AuditEvent e) {
        return new AuditEventDto(
                e.getEventId(),
                e.getEventType(),
                e.getEntityId() != null ? e.getEntityId() : (e.getExceptionId() != null ? e.getExceptionId() : e.getTradeId()),
                e.getEntityType() != null ? e.getEntityType() : (e.getExceptionId() != null ? "exception" : "trade"),
                e.getTradeId(),
                e.getExceptionId(),
                e.getCorrelationId(),
                e.getActor(),
                e.getOccurredAt(),
                e.getDetails());
    }
}
