package com.tradeexception.platform.replay.api;

import com.tradeexception.platform.replay.domain.ReplayStatus;

import java.time.Instant;

/** Response payload returned after a controlled DLQ replay request. */
public record ReplayResultDto(
        String deadLetterId,
        String originalTopic,
        String originalEventId,
        String correlationId,
        ReplayStatus replayStatus,
        int attemptNumber,
        String replayedBy,
        Instant replayedAt,
        String message) {
}
