package com.tradeexception.platform.replay.api;

import com.tradeexception.platform.replay.DeadLetterRecord.DeadLetterErrorCategory;
import com.tradeexception.platform.replay.domain.DeadLetterDocument;
import com.tradeexception.platform.replay.domain.ReplayStatus;

import java.time.Instant;
import java.util.List;

/** Sanitized API DTO for {@link DeadLetterDocument}. */
public record DeadLetterDto(
        String deadLetterId,
        String originalEventId,
        String originalTopic,
        Integer originalPartition,
        Long originalOffset,
        String originalKey,
        String correlationId,
        DeadLetterErrorCategory errorCategory,
        String failureType,
        String errorMessage,
        List<String> validationErrors,
        String rawPayload,
        Instant failedAt,
        ReplayStatus replayStatus,
        int replayAttempts,
        int maxReplayAttempts,
        Instant lastReplayedAt,
        String lastReplayedBy) {

    public static DeadLetterDto from(DeadLetterDocument d) {
        return new DeadLetterDto(
                d.getDeadLetterId(),
                d.getOriginalEventId(),
                d.getOriginalTopic(),
                d.getOriginalPartition(),
                d.getOriginalOffset(),
                d.getOriginalKey(),
                d.getCorrelationId(),
                d.getErrorCategory(),
                d.getFailureType(),
                d.getErrorMessage(),
                d.getValidationErrors(),
                d.getRawPayload(),
                d.getFailedAt(),
                d.getReplayStatus(),
                d.getReplayAttempts(),
                d.getMaxReplayAttempts(),
                d.getLastReplayedAt(),
                d.getLastReplayedBy());
    }
}
