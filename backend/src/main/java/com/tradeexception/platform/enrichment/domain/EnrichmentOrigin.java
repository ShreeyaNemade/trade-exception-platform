package com.tradeexception.platform.enrichment.domain;

/** Indicates how an enriched field value was obtained. */
public enum EnrichmentOrigin {
    SUPPLIED,
    DERIVED,
    DEFAULTED
}
