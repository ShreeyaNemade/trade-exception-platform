package com.tradeexception.platform.stp.domain;

/** Result of evaluating a single deterministic STP validation rule. */
public record RuleOutcome(
        String ruleName,
        boolean passed,
        String reasonCode,
        String message,
        RuleSeverity failureSeverity) {

    public static RuleOutcome pass(String ruleName) {
        return new RuleOutcome(ruleName, true, null, "Passed", null);
    }

    public static RuleOutcome reject(String ruleName, String reasonCode, String message) {
        return new RuleOutcome(ruleName, false, reasonCode, message, RuleSeverity.REJECT);
    }

    public static RuleOutcome manualReview(String ruleName, String reasonCode, String message) {
        return new RuleOutcome(ruleName, false, reasonCode, message, RuleSeverity.MANUAL_REVIEW);
    }
}
