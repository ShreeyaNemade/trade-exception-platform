package com.tradeexception.platform.workitem.api;

import com.tradeexception.platform.common.api.ApiResponse;
import com.tradeexception.platform.common.api.PagedResponse;
import com.tradeexception.platform.exception.domain.ExceptionPriority;
import com.tradeexception.platform.workitem.WorkItemManagementService;
import com.tradeexception.platform.workitem.domain.OperationalTeam;
import com.tradeexception.platform.workitem.domain.WorkItemStatus;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.security.Principal;

/** Operational REST API for work item queries, team assignment, and status transitions. */
@RestController
@RequestMapping("/api/v1/work-items")
public class WorkItemController {

    private final WorkItemQueryService queryService;
    private final WorkItemManagementService managementService;

    public WorkItemController(WorkItemQueryService queryService, WorkItemManagementService managementService) {
        this.queryService = queryService;
        this.managementService = managementService;
    }

    @GetMapping
    public ApiResponse<PagedResponse<WorkItemDto>> list(
            @RequestParam(required = false) WorkItemStatus status,
            @RequestParam(required = false) ExceptionPriority priority,
            @RequestParam(required = false) OperationalTeam assignedTeam,
            @RequestParam(required = false) String tradeId,
            @RequestParam(required = false) String exceptionId,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size,
            @RequestParam(defaultValue = "dueAt") String sortBy,
            @RequestParam(defaultValue = "asc") String sortDir) {
        return ApiResponse.ok(queryService.list(
                status, priority, assignedTeam, tradeId, exceptionId, page, size, sortBy, sortDir));
    }

    @GetMapping("/{workItemId}")
    public ApiResponse<WorkItemDto> getById(@PathVariable String workItemId) {
        return ApiResponse.ok(queryService.getById(workItemId));
    }

    @PatchMapping("/{workItemId}/assign")
    public ApiResponse<WorkItemDto> assign(
            @PathVariable String workItemId,
            @Valid @RequestBody AssignWorkItemRequest request,
            Principal principal) {
        String actor = resolveActor(principal, request.actor());
        return ApiResponse.ok(WorkItemDto.from(
                managementService.assignWorkItem(workItemId, request.assignedTeam(), request.assignee(), actor)));
    }

    @PatchMapping("/{workItemId}/status")
    public ApiResponse<WorkItemDto> updateStatus(
            @PathVariable String workItemId,
            @Valid @RequestBody UpdateWorkItemStatusRequest request,
            Principal principal) {
        String actor = resolveActor(principal, request.actor());
        return ApiResponse.ok(WorkItemDto.from(
                managementService.updateStatus(workItemId, request.status(), actor)));
    }

    private static String resolveActor(Principal principal, String fallbackActor) {
        if (principal != null && principal.getName() != null && !principal.getName().isBlank()) {
            return principal.getName();
        }
        return (fallbackActor == null || fallbackActor.isBlank()) ? "ops-user" : fallbackActor;
    }
}
