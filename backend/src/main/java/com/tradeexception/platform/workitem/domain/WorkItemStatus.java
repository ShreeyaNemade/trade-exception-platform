package com.tradeexception.platform.workitem.domain;

/** Lifecycle state of a remediation work item. */
public enum WorkItemStatus {
    OPEN,
    ASSIGNED,
    IN_PROGRESS,
    PENDING,
    RESOLVED,
    CLOSED,
    CANCELLED
}
