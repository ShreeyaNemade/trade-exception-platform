package com.tradeexception.platform.stp.rules;

import com.tradeexception.platform.enrichment.domain.EnrichedTrade;
import com.tradeexception.platform.stp.domain.RuleOutcome;
import com.tradeexception.platform.trade.domain.Trade;
import org.springframework.stereotype.Component;

/** Verifies mandatory trade and message header identifiers are present. */
@Component
public class MandatoryIdentifiersRule implements StpValidationRule {

    @Override
    public String name() {
        return "MANDATORY_IDENTIFIERS";
    }

    @Override
    public RuleOutcome evaluate(EnrichedTrade enrichedTrade) {
        Trade t = enrichedTrade.trade();
        if (t == null || isBlank(t.getTradeId()) || isBlank(t.getSourceSystemTradeId())
                || t.getHeader() == null || isBlank(t.getHeader().messageId())
                || isBlank(t.getHeader().correlationId())) {
            return RuleOutcome.reject(name(), "MISSING_MANDATORY_IDENTIFIERS",
                    "Trade is missing required identifiers (tradeId, sourceSystemTradeId, messageId, or correlationId)");
        }
        return RuleOutcome.pass(name());
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
