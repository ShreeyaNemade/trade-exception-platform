package com.tradeexception.platform.exception.domain;

/** Root-cause category assigned to a trade exception. */
public enum ExceptionCategory {
    MISSING_TRADE_DATA,
    INVALID_COUNTERPARTY,
    UNSUPPORTED_PRODUCT,
    CURRENCY_MISMATCH,
    INVALID_ECONOMIC_TERMS,
    DATE_INCONSISTENCY,
    MISSING_REFERENCE_DATA,
    CONFIRMATION_TEMPLATE_MISSING,
    OTHER,
    // Retained for backward compatibility with earlier phase tests
    MISSING_DATA,
    INVALID_REFERENCE_DATA,
    COUNTERPARTY_UNKNOWN,
    ECONOMIC_MISMATCH,
    SETTLEMENT_INSTRUCTION,
    DUPLICATE_TRADE,
    VALIDATION_RULE_FAILURE,
    TECHNICAL_ERROR,
    UNCLASSIFIED
}
