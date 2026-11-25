package com.tradeexception.platform.stp.rules;

import com.tradeexception.platform.enrichment.domain.EnrichedTrade;
import com.tradeexception.platform.stp.domain.RuleOutcome;
import com.tradeexception.platform.trade.domain.NonEconomicTerms;
import org.springframework.stereotype.Component;

/** Verifies required non-economic legal terms when mandated by the product configuration. */
@Component
public class NonEconomicTermsRule implements StpValidationRule {

    @Override
    public String name() {
        return "NON_ECONOMIC_TERMS";
    }

    @Override
    public RuleOutcome evaluate(EnrichedTrade enrichedTrade) {
        if (!enrichedTrade.requireNonEconomicTerms()) {
            return RuleOutcome.pass(name());
        }
        NonEconomicTerms terms = enrichedTrade.trade() == null ? null : enrichedTrade.trade().getNonEconomicTerms();
        if (terms == null
                || isBlank(terms.documentationType())
                || isBlank(terms.governingLaw())
                || isBlank(terms.businessDayConvention())) {
            return RuleOutcome.manualReview(name(), "INCOMPLETE_NON_ECONOMIC_TERMS",
                    "Product requires documentationType, governingLaw, and businessDayConvention; manual review required");
        }
        return RuleOutcome.pass(name());
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
