package com.tradeexception.platform.audit.api;

import com.tradeexception.platform.audit.domain.AuditEvent;
import com.tradeexception.platform.audit.domain.EventType;
import com.tradeexception.platform.audit.repository.AuditEventRepository;
import com.tradeexception.platform.common.api.PagedResponse;
import com.tradeexception.platform.common.couchdb.CouchDbNotFoundException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;

/** Read-only query service for immutable audit events. */
@Service
public class AuditQueryService {

    public static final int DEFAULT_PAGE_SIZE = 25;
    public static final int DEFAULT_MAX_PAGE_SIZE = 100;

    private final AuditEventRepository repository;
    private final int maxPageSize;

    public AuditQueryService(AuditEventRepository repository) {
        this(repository, DEFAULT_MAX_PAGE_SIZE);
    }

    @Autowired
    public AuditQueryService(
            AuditEventRepository repository,
            @Value("${app.audit.max-page-size:100}") int maxPageSize) {
        this.repository = repository;
        this.maxPageSize = Math.max(1, maxPageSize);
    }

    public PagedResponse<AuditEventDto> list(
            String entityId,
            String entityType,
            EventType eventType,
            String correlationId,
            Instant from,
            Instant to,
            int page,
            int size,
            String sortBy,
            String sortDir) {

        String normalizedSortBy = (sortBy == null || sortBy.isBlank()) ? "occurredAt" : sortBy;
        String normalizedSortDir = "asc".equalsIgnoreCase(sortDir) ? "asc" : "desc";
        int safePage = Math.max(0, page);
        int effectiveSize = size <= 0 ? DEFAULT_PAGE_SIZE : Math.min(size, maxPageSize);

        List<AuditEventDto> all = repository.findAll().stream()
                .map(AuditEventDto::from)
                .filter(e -> entityId == null || entityId.isBlank() || entityId.equalsIgnoreCase(e.entityId()) || entityId.equalsIgnoreCase(e.tradeId()))
                .filter(e -> entityType == null || entityType.isBlank() || entityType.equalsIgnoreCase(e.entityType()))
                .filter(e -> eventType == null || e.eventType() == eventType)
                .filter(e -> correlationId == null || correlationId.isBlank() || correlationId.equalsIgnoreCase(e.correlationId()))
                .filter(e -> from == null || (e.occurredAt() != null && !e.occurredAt().isBefore(from)))
                .filter(e -> to == null || (e.occurredAt() != null && !e.occurredAt().isAfter(to)))
                .sorted(comparatorFor(normalizedSortBy, normalizedSortDir))
                .toList();

        return PagedResponse.of(all, safePage, effectiveSize, maxPageSize, normalizedSortBy, normalizedSortDir);
    }

    public AuditEventDto getById(String eventId) {
        String docId = AuditEvent.TYPE + ":" + eventId;
        return repository.findById(docId)
                .map(AuditEventDto::from)
                .orElseThrow(() -> new CouchDbNotFoundException("audit-events", docId, null));
    }

    public List<AuditEventDto> listByTradeId(String tradeId) {
        return repository.findByTradeId(tradeId).stream()
                .map(AuditEventDto::from)
                .sorted(Comparator.comparing(AuditEventDto::occurredAt, Comparator.nullsLast(Comparator.naturalOrder()))
                        .thenComparing(AuditEventDto::eventId, Comparator.nullsLast(Comparator.naturalOrder())))
                .toList();
    }

    private static Comparator<AuditEventDto> comparatorFor(String sortBy, String sortDir) {
        Comparator<AuditEventDto> primary = switch (sortBy == null ? "occurredAt" : sortBy) {
            case "eventType" -> Comparator.comparing(e -> e.eventType() == null ? "" : e.eventType().name());
            case "actor" -> Comparator.comparing(e -> e.actor() == null ? "" : e.actor());
            default -> Comparator.comparing(AuditEventDto::occurredAt, Comparator.nullsLast(Comparator.naturalOrder()));
        };
        Comparator<AuditEventDto> deterministic = primary
                .thenComparing(AuditEventDto::eventId, Comparator.nullsLast(Comparator.naturalOrder()));
        return "asc".equalsIgnoreCase(sortDir) ? deterministic : deterministic.reversed();
    }
}
