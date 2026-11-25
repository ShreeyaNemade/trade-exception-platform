package com.tradeexception.platform.stp.domain;

/** Deterministic outcome produced by the STP validation engine. */
public enum StpDecision {
    STP_ELIGIBLE,
    STP_REJECTED,
    MANUAL_REVIEW_REQUIRED
}
