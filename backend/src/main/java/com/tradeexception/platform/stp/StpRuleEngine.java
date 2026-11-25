package com.tradeexception.platform.stp;

import com.tradeexception.platform.enrichment.domain.EnrichedTrade;
import com.tradeexception.platform.stp.domain.RuleOutcome;
import com.tradeexception.platform.stp.domain.RuleSeverity;
import com.tradeexception.platform.stp.domain.StpDecision;
import com.tradeexception.platform.stp.domain.StpValidationResult;
import com.tradeexception.platform.stp.rules.StpValidationRule;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Deterministic STP validation rule engine. Evaluates all registered {@link StpValidationRule}
 * beans in name order and derives {@link StpDecision} without arbitrary risk scores.
 */
@Component
public class StpRuleEngine {

    private final List<StpValidationRule> rules;

    public StpRuleEngine(List<StpValidationRule> rules) {
        this.rules = rules.stream()
                .sorted(Comparator.comparing(StpValidationRule::name))
                .toList();
    }

    public StpValidationResult validate(EnrichedTrade enrichedTrade) {
        List<String> passed = new ArrayList<>();
        List<String> failed = new ArrayList<>();
        List<String> reasonCodes = new ArrayList<>();
        List<String> failureMessages = new ArrayList<>();
        boolean hasReject = false;
        boolean hasManualReview = false;

        for (StpValidationRule rule : rules) {
            RuleOutcome outcome = rule.evaluate(enrichedTrade);
            if (outcome.passed()) {
                passed.add(outcome.ruleName());
            } else {
                failed.add(outcome.ruleName());
                if (outcome.reasonCode() != null) {
                    reasonCodes.add(outcome.reasonCode());
                }
                failureMessages.add(outcome.ruleName() + ": " + outcome.message());
                if (outcome.failureSeverity() == RuleSeverity.REJECT) {
                    hasReject = true;
                } else if (outcome.failureSeverity() == RuleSeverity.MANUAL_REVIEW) {
                    hasManualReview = true;
                }
            }
        }

        StpDecision decision = hasReject
                ? StpDecision.STP_REJECTED
                : (hasManualReview ? StpDecision.MANUAL_REVIEW_REQUIRED : StpDecision.STP_ELIGIBLE);

        String explanation = switch (decision) {
            case STP_ELIGIBLE -> "All " + passed.size() + " STP validation rules passed.";
            case STP_REJECTED -> "Trade rejected by STP validation (" + failed.size() + " rule(s) failed): "
                    + failureMessages.stream().collect(Collectors.joining("; "));
            case MANUAL_REVIEW_REQUIRED -> "Manual review required (" + failed.size() + " rule(s) flagged): "
                    + failureMessages.stream().collect(Collectors.joining("; "));
        };

        String tradeId = enrichedTrade.trade() == null ? null : enrichedTrade.trade().getTradeId();
        return new StpValidationResult(
                tradeId,
                enrichedTrade.correlationId(),
                decision,
                Instant.now(),
                List.copyOf(passed),
                List.copyOf(failed),
                List.copyOf(reasonCodes),
                explanation);
    }
}
