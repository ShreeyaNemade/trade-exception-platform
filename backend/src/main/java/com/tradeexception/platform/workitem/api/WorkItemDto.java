package com.tradeexception.platform.workitem.api;

import com.tradeexception.platform.exception.domain.ExceptionPriority;
import com.tradeexception.platform.workitem.domain.OperationalTeam;
import com.tradeexception.platform.workitem.domain.WorkItem;
import com.tradeexception.platform.workitem.domain.WorkItemStatus;

import java.time.Instant;

/** API DTO for {@link WorkItem}. */
public record WorkItemDto(
        String workItemId,
        String exceptionId,
        String tradeId,
        String correlationId,
        String title,
        String description,
        ExceptionPriority priority,
        WorkItemStatus status,
        OperationalTeam assignedTeam,
        String assignee,
        Instant dueAt,
        String recommendedAction,
        Instant createdAt,
        Instant updatedAt) {

    public static WorkItemDto from(WorkItem w) {
        return new WorkItemDto(
                w.getWorkItemId(),
                w.getExceptionId(),
                w.getTradeId(),
                w.getCorrelationId(),
                w.getTitle(),
                w.getDescription(),
                w.getPriority(),
                w.getStatus(),
                w.getAssignedTeam(),
                w.getAssignee(),
                w.getDueAt(),
                w.getRecommendedAction(),
                w.getCreatedAt(),
                w.getUpdatedAt());
    }
}
