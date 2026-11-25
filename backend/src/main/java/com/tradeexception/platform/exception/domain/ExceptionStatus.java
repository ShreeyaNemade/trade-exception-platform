package com.tradeexception.platform.exception.domain;

/** Lifecycle state of an exception record. */
public enum ExceptionStatus {
    OPEN,
    IN_PROGRESS,
    RESOLVED,
    CLOSED
}
