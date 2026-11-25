package com.tradeexception.platform.stp.domain;

import java.time.Instant;
import java.util.List;

/** Complete, explainable STP validation decision for an enriched trade. */
public record StpValidationResult(
        String tradeId,
        String correlationId,
        StpDecision decision,
        Instant validatedAt,
        List<String> passedRules,
        List<String> failedRules,
        List<String> reasonCodes,
        String explanation) {
}
