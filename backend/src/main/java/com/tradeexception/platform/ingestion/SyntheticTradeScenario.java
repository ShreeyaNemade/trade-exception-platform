package com.tradeexception.platform.ingestion;

/** Synthetic trade scenarios supported by {@link SyntheticTradeProducer}. */
public enum SyntheticTradeScenario {
    VALID_FX_SPOT,
    VALID_FX_FORWARD,
    MISSING_MANDATORY_ECONOMIC_FIELD,
    INVALID_COUNTERPARTY_REFERENCE,
    DUPLICATE_MESSAGE,
    MALFORMED_PAYLOAD
}
