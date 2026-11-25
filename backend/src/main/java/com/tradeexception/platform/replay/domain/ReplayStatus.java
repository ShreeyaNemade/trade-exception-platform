package com.tradeexception.platform.replay.domain;

/** Lifecycle state of a persisted dead-letter message. */
public enum ReplayStatus {
    PENDING,
    IN_PROGRESS,
    REPLAYED,
    FAILED,
    NOT_ELIGIBLE
}
