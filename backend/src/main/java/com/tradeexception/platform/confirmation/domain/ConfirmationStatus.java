package com.tradeexception.platform.confirmation.domain;

/** Lifecycle state of a trade confirmation. */
public enum ConfirmationStatus {
    PENDING,
    GENERATED,
    SENT,
    MATCHED,
    DISPUTED,
    CANCELLED
}
