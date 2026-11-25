package com.tradeexception.platform.enrichment.domain;

/** Structured failure captured when mandatory reference data is missing or inactive during enrichment. */
public record EnrichmentFailure(
        String code,
        String field,
        String message) {
}
