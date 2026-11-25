package com.tradeexception.platform.stp.rules;

import com.tradeexception.platform.enrichment.domain.EnrichedTrade;
import com.tradeexception.platform.stp.domain.RuleOutcome;
import org.springframework.stereotype.Component;

/** Verifies synthetic instrument availability and confirmation template availability. */
@Component
public class InstrumentAndTemplateRule implements StpValidationRule {

    @Override
    public String name() {
        return "INSTRUMENT_AND_TEMPLATE";
    }

    @Override
    public RuleOutcome evaluate(EnrichedTrade enrichedTrade) {
        if (!enrichedTrade.instrumentFound()) {
            return RuleOutcome.reject(name(), "INSTRUMENT_NOT_FOUND",
                    "Derived instrument is not available in active reference data");
        }
        if (!enrichedTrade.confirmationTemplateAvailable()) {
            return RuleOutcome.manualReview(name(), "CONFIRMATION_TEMPLATE_UNAVAILABLE",
                    "No active confirmation template is available; manual confirmation drafting is required");
        }
        return RuleOutcome.pass(name());
    }
}
