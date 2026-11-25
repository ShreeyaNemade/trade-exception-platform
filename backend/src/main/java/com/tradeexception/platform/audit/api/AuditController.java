package com.tradeexception.platform.audit.api;

import com.tradeexception.platform.audit.domain.EventType;
import com.tradeexception.platform.common.api.ApiResponse;
import com.tradeexception.platform.common.api.PagedResponse;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;

/** Strictly read-only REST endpoints for the platform audit trail. */
@RestController
@RequestMapping("/api/v1/audit-events")
public class AuditController {

    private final AuditQueryService queryService;

    public AuditController(AuditQueryService queryService) {
        this.queryService = queryService;
    }

    @GetMapping
    public ApiResponse<PagedResponse<AuditEventDto>> list(
            @RequestParam(required = false) String entityId,
            @RequestParam(required = false) String entityType,
            @RequestParam(required = false) EventType eventType,
            @RequestParam(required = false) String correlationId,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant from,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant to,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "25") int size,
            @RequestParam(defaultValue = "occurredAt") String sortBy,
            @RequestParam(defaultValue = "desc") String sortDir) {
        return ApiResponse.ok(queryService.list(entityId, entityType, eventType, correlationId, from, to, page, size, sortBy, sortDir));
    }

    @GetMapping("/{eventId}")
    public ApiResponse<AuditEventDto> getById(@PathVariable String eventId) {
        return ApiResponse.ok(queryService.getById(eventId));
    }
}
