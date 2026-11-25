package com.tradeexception.platform.enrichment.domain;

import java.time.Instant;

/** Provenance metadata for a single enriched attribute. */
public record EnrichedField(
        String fieldName,
        String value,
        String source,
        Instant enrichedAt,
        EnrichmentOrigin origin) {
}
