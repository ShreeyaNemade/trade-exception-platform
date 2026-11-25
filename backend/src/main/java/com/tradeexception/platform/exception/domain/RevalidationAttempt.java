package com.tradeexception.platform.exception.domain;

import java.time.Instant;
import java.util.List;

/** Immutable record of a single remediation and STP revalidation attempt on an exception. */
public record RevalidationAttempt(
        int attemptNumber,
        Instant submittedAt,
        String operator,
        String outcome,
        List<String> failedRules,
        List<String> reasonCodes,
        String explanation) {
}
