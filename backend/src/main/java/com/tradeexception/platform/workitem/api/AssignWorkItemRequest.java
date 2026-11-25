package com.tradeexception.platform.workitem.api;

import com.tradeexception.platform.workitem.domain.OperationalTeam;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

/** Request payload for {@code PATCH /api/v1/work-items/{workItemId}/assign}. */
public record AssignWorkItemRequest(
        @NotNull OperationalTeam assignedTeam,
        @Size(max = 64) String assignee,
        @Size(max = 64) String actor) {
}
