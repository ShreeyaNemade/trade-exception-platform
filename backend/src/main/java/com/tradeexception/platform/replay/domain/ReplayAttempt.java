package com.tradeexception.platform.replay.domain;

import java.time.Instant;

/** Immutable record of a single replay attempt for a dead-letter document. */
public record ReplayAttempt(
        int attemptNumber,
        Instant requestedAt,
        Instant completedAt,
        String operator,
        String outcome,
        String message) {
}
