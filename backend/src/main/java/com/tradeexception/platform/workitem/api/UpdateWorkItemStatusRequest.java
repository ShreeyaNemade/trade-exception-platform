package com.tradeexception.platform.workitem.api;

import com.tradeexception.platform.workitem.domain.WorkItemStatus;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

/** Request payload for {@code PATCH /api/v1/work-items/{workItemId}/status}. */
public record UpdateWorkItemStatusRequest(
        @NotNull WorkItemStatus status,
        @Size(max = 64) String actor) {
}
