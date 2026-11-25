package com.tradeexception.platform.stp.rules;

import com.tradeexception.platform.enrichment.domain.EnrichedTrade;
import com.tradeexception.platform.stp.domain.RuleOutcome;
import org.springframework.stereotype.Component;

/** Verifies product configuration exists and determines whether the product is eligible for automated STP. */
@Component
public class ProductEligibilityRule implements StpValidationRule {

    @Override
    public String name() {
        return "PRODUCT_ELIGIBILITY";
    }

    @Override
    public RuleOutcome evaluate(EnrichedTrade enrichedTrade) {
        if (!enrichedTrade.productFound()) {
            return RuleOutcome.reject(name(), "PRODUCT_CONFIG_MISSING",
                    "Product configuration was not found in reference data");
        }
        if (!enrichedTrade.productStpEligible()) {
            return RuleOutcome.manualReview(name(), "PRODUCT_MANUAL_REVIEW_ONLY",
                    "Product type '" + enrichedTrade.trade().getProductType() + "' is not eligible for automated STP and requires manual review");
        }
        return RuleOutcome.pass(name());
    }
}
