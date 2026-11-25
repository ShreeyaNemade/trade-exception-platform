package com.tradeexception.platform.stp.rules;

import com.tradeexception.platform.enrichment.domain.EnrichedTrade;
import com.tradeexception.platform.stp.domain.RuleOutcome;
import com.tradeexception.platform.trade.domain.Trade;
import org.springframework.stereotype.Component;

/** Verifies tradeDate <= effectiveDate <= maturityDate consistency. */
@Component
public class DateConsistencyRule implements StpValidationRule {

    @Override
    public String name() {
        return "DATE_CONSISTENCY";
    }

    @Override
    public RuleOutcome evaluate(EnrichedTrade enrichedTrade) {
        Trade t = enrichedTrade.trade();
        if (t == null || t.getTradeDate() == null || t.getEffectiveDate() == null) {
            return RuleOutcome.reject(name(), "MISSING_TRADE_DATES",
                    "Trade date and effective date are mandatory");
        }
        if (!t.isDateSequenceValid()) {
            return RuleOutcome.reject(name(), "INVALID_DATE_SEQUENCE",
                    "Trade dates violate tradeDate <= effectiveDate <= maturityDate");
        }
        return RuleOutcome.pass(name());
    }
}
