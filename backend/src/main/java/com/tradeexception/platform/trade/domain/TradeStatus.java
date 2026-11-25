package com.tradeexception.platform.trade.domain;

/** Lifecycle state of a trade through the processing pipeline. */
public enum TradeStatus {
    RECEIVED,
    ACCEPTED,
    ENRICHED,
    VALIDATED,
    CONFIRMED,
    EXCEPTION,
    REJECTED
}
