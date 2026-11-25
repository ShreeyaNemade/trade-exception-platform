package com.tradeexception.platform.stp.rules;

import com.tradeexception.platform.enrichment.domain.EnrichedTrade;
import com.tradeexception.platform.stp.domain.RuleOutcome;
import org.springframework.stereotype.Component;

/** Verifies both buyer and seller exist in reference data and are STP-eligible. */
@Component
public class CounterpartyValidationRule implements StpValidationRule {

    @Override
    public String name() {
        return "COUNTERPARTY_VALIDATION";
    }

    @Override
    public RuleOutcome evaluate(EnrichedTrade enrichedTrade) {
        if (!enrichedTrade.buyerFound() || !enrichedTrade.sellerFound()) {
            return RuleOutcome.reject(name(), "COUNTERPARTY_NOT_FOUND",
                    "One or both trade counterparties were not found in active reference data");
        }
        if (!enrichedTrade.buyerEligible() || !enrichedTrade.sellerEligible()) {
            return RuleOutcome.manualReview(name(), "COUNTERPARTY_NOT_STP_ELIGIBLE",
                    "Counterparty exists in reference data but is restricted and requires manual review");
        }
        return RuleOutcome.pass(name());
    }
}
