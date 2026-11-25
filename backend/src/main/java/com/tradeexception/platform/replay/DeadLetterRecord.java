package com.tradeexception.platform.replay;

import java.time.Instant;
import java.util.List;

/** Diagnostic payload published to {@code deadletter.trade} for irrecoverable messages. */
public record DeadLetterRecord(
        String deadLetterId,
        String originalTopic,
        Integer originalPartition,
        Long originalOffset,
        String originalKey,
        String correlationId,
        DeadLetterErrorCategory errorCategory,
        String errorMessage,
        List<String> validationErrors,
        String rawPayload,
        Instant failedAt) {

    public enum DeadLetterErrorCategory {
        DESERIALIZATION_ERROR,
        VALIDATION_ERROR,
        INFRASTRUCTURE_ERROR,
        UNEXPECTED_ERROR
    }
}
