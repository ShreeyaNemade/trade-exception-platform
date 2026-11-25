package com.tradeexception.platform.exception.api;

import com.tradeexception.platform.exception.domain.ExceptionCategory;
import com.tradeexception.platform.exception.domain.ExceptionPriority;
import com.tradeexception.platform.exception.domain.ExceptionRecord;
import com.tradeexception.platform.exception.domain.ExceptionSeverity;
import com.tradeexception.platform.exception.domain.ExceptionStatus;
import com.tradeexception.platform.exception.domain.RevalidationAttempt;

import java.time.Instant;
import java.util.List;

/** API DTO for {@link ExceptionRecord} including classification, resolution, and revalidation history. */
public record ExceptionDto(
        String exceptionId,
        String tradeId,
        String correlationId,
        ExceptionCategory category,
        ExceptionPriority priority,
        ExceptionSeverity severity,
        ExceptionStatus status,
        boolean classified,
        boolean manualReviewRequired,
        String reasonCode,
        String rootCause,
        String explanation,
        String description,
        String recommendedAction,
        Instant detectedAt,
        Instant classifiedAt,
        String resolutionReason,
        Instant resolvedAt,
        String resolvedBy,
        Instant lastRevalidatedAt,
        String lastRevalidationOutcome,
        List<RevalidationAttempt> revalidationHistory) {

    public static ExceptionDto from(ExceptionRecord e) {
        boolean isManualReview = (e.getDescription() != null && e.getDescription().startsWith("Manual review required"))
                || "COUNTERPARTY_NOT_STP_ELIGIBLE".equals(e.getReasonCode())
                || "PRODUCT_MANUAL_REVIEW_ONLY".equals(e.getReasonCode())
                || "CONFIRMATION_TEMPLATE_UNAVAILABLE".equals(e.getReasonCode())
                || "INCOMPLETE_NON_ECONOMIC_TERMS".equals(e.getReasonCode());
        return new ExceptionDto(
                e.getExceptionId(),
                e.getTradeId(),
                e.getCorrelationId(),
                e.getCategory(),
                e.getPriority(),
                e.getSeverity(),
                e.getStatus(),
                e.getClassifiedAt() != null,
                isManualReview,
                e.getReasonCode(),
                e.getRootCause(),
                e.getExplanation(),
                e.getDescription(),
                e.getRecommendedAction(),
                e.getDetectedAt(),
                e.getClassifiedAt(),
                e.getResolutionReason(),
                e.getResolvedAt(),
                e.getResolvedBy(),
                e.getLastRevalidatedAt(),
                e.getLastRevalidationOutcome(),
                e.getRevalidationHistory() == null ? List.of() : e.getRevalidationHistory());
    }
}
