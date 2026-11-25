package com.tradeexception.platform.enrichment.domain;

import com.tradeexception.platform.trade.domain.Trade;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Result of enriching an accepted {@link Trade} with synthetic reference data.
 * The original {@code trade} is preserved intact alongside field-level provenance and any
 * structured enrichment failures.
 */
public record EnrichedTrade(
        Trade trade,
        String correlationId,
        Instant enrichedAt,
        Map<String, EnrichedField> enrichedFields,
        boolean buyerFound,
        boolean buyerEligible,
        boolean sellerFound,
        boolean sellerEligible,
        boolean currencySupported,
        boolean settlementCurrencySupported,
        boolean productFound,
        boolean productStpEligible,
        boolean requireNonEconomicTerms,
        boolean instrumentFound,
        boolean confirmationTemplateAvailable,
        List<EnrichmentFailure> enrichmentFailures) {

    public boolean hasFailures() {
        return enrichmentFailures != null && !enrichmentFailures.isEmpty();
    }
}
