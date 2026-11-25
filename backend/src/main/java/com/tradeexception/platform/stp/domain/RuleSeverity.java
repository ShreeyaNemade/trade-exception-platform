package com.tradeexception.platform.stp.domain;

/** Impact of a failed STP validation rule on the overall trade decision. */
public enum RuleSeverity {
    REJECT,
    MANUAL_REVIEW
}
