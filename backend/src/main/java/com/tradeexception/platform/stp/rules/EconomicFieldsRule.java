package com.tradeexception.platform.stp.rules;

import com.tradeexception.platform.enrichment.domain.EnrichedTrade;
import com.tradeexception.platform.stp.domain.RuleOutcome;
import com.tradeexception.platform.trade.domain.Trade;
import com.tradeexception.platform.trade.domain.TradeEconomics;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

/** Verifies required economic terms and product-specific mandatory fields. */
@Component
public class EconomicFieldsRule implements StpValidationRule {

    @Override
    public String name() {
        return "ECONOMIC_FIELDS";
    }

    @Override
    public RuleOutcome evaluate(EnrichedTrade enrichedTrade) {
        Trade t = enrichedTrade.trade();
        if (t == null || t.getEconomics() == null || t.getProductType() == null) {
            return RuleOutcome.reject(name(), "MISSING_ECONOMIC_FIELDS",
                    "Trade economics or product type is missing");
        }
        TradeEconomics econ = t.getEconomics();
        if (econ.notionalAmount() == null || econ.notionalAmount().compareTo(BigDecimal.ZERO) <= 0
                || econ.notionalCurrency() == null || econ.notionalCurrency().isBlank()) {
            return RuleOutcome.reject(name(), "INVALID_ECONOMIC_TERMS",
                    "Notional amount must be positive and notional currency must be specified");
        }
        if (!t.isProductSpecificTermsValid()) {
            return RuleOutcome.reject(name(), "MISSING_PRODUCT_SPECIFIC_TERMS",
                    "Mandatory economic or maturity fields for product '" + t.getProductType() + "' are missing");
        }
        return RuleOutcome.pass(name());
    }
}
