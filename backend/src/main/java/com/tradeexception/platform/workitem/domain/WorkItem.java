package com.tradeexception.platform.workitem.domain;

import com.tradeexception.platform.common.domain.BaseDocument;
import com.tradeexception.platform.exception.domain.ExceptionPriority;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.time.Instant;

/** Remediation task linked to an exception. Document id: {@code workitem:<workItemId>}. */
public class WorkItem extends BaseDocument {

    public static final String TYPE = "workitem";
    public static final int SCHEMA_VERSION = 1;

    @NotBlank @Size(max = 64)
    private String workItemId;

    @NotBlank @Size(max = 64)
    private String exceptionId;

    /** Denormalised from the exception so work items can be queried by trade without a lookup. */
    @NotBlank @Size(max = 64)
    private String tradeId;

    @NotBlank @Size(max = 64)
    private String correlationId;

    @Size(max = 256)
    private String title;

    @Size(max = 2000)
    private String description;

    @NotNull
    private WorkItemStatus status;

    @NotNull
    private ExceptionPriority priority;

    private OperationalTeam assignedTeam;

    @Size(max = 64)
    private String assignee;

    private Instant dueAt;

    @Size(max = 1000)
    private String recommendedAction;

    public WorkItem() {
        super(SCHEMA_VERSION);
    }

    @Override
    public String getType() { return TYPE; }

    @Override
    protected String businessKey() { return workItemId; }

    public String getWorkItemId() { return workItemId; }
    public void setWorkItemId(String workItemId) { this.workItemId = workItemId; }
    public String getExceptionId() { return exceptionId; }
    public void setExceptionId(String exceptionId) { this.exceptionId = exceptionId; }
    public String getTradeId() { return tradeId; }
    public void setTradeId(String tradeId) { this.tradeId = tradeId; }
    public String getCorrelationId() { return correlationId; }
    public void setCorrelationId(String correlationId) { this.correlationId = correlationId; }
    public String getTitle() { return title; }
    public void setTitle(String title) { this.title = title; }
    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }
    public WorkItemStatus getStatus() { return status; }
    public void setStatus(WorkItemStatus status) { this.status = status; }
    public ExceptionPriority getPriority() { return priority; }
    public void setPriority(ExceptionPriority priority) { this.priority = priority; }
    public OperationalTeam getAssignedTeam() { return assignedTeam; }
    public void setAssignedTeam(OperationalTeam assignedTeam) { this.assignedTeam = assignedTeam; }
    public String getAssignee() { return assignee; }
    public void setAssignee(String assignee) { this.assignee = assignee; }
    public Instant getDueAt() { return dueAt; }
    public void setDueAt(Instant dueAt) { this.dueAt = dueAt; }
    public String getRecommendedAction() { return recommendedAction; }
    public void setRecommendedAction(String recommendedAction) { this.recommendedAction = recommendedAction; }
}
