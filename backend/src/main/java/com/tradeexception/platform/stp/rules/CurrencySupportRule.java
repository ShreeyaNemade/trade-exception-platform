package com.tradeexception.platform.stp.rules;

import com.tradeexception.platform.enrichment.domain.EnrichedTrade;
import com.tradeexception.platform.stp.domain.RuleOutcome;
import org.springframework.stereotype.Component;

/** Verifies notional and settlement currencies are supported in reference data. */
@Component
public class CurrencySupportRule implements StpValidationRule {

    @Override
    public String name() {
        return "CURRENCY_SUPPORT";
    }

    @Override
    public RuleOutcome evaluate(EnrichedTrade enrichedTrade) {
        if (!enrichedTrade.currencySupported() || !enrichedTrade.settlementCurrencySupported()) {
            return RuleOutcome.reject(name(), "CURRENCY_UNSUPPORTED",
                    "Notional or settlement currency is not supported for STP");
        }
        return RuleOutcome.pass(name());
    }
}
