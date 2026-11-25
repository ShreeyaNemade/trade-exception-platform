package com.tradeexception.platform.stp.rules;

import com.tradeexception.platform.enrichment.domain.EnrichedTrade;
import com.tradeexception.platform.stp.domain.RuleOutcome;

/** Single deterministic STP validation rule evaluated against an {@link EnrichedTrade}. */
public interface StpValidationRule {

    String name();

    RuleOutcome evaluate(EnrichedTrade enrichedTrade);
}
