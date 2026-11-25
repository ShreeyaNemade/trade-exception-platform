package com.tradeexception.platform.exception.domain;

import com.tradeexception.platform.common.domain.BaseDocument;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A trade that could not be processed straight-through. Document id: {@code exception:<exceptionId>}.
 * Named {@code ExceptionRecord} to avoid confusion with {@link java.lang.Exception}.
 */
public class ExceptionRecord extends BaseDocument {

    public static final String TYPE = "exception";
    public static final int SCHEMA_VERSION = 1;

    @NotBlank @Size(max = 64)
    private String exceptionId;

    @NotBlank @Size(max = 64)
    private String tradeId;

    @NotBlank @Size(max = 64)
    private String correlationId;

    /** {@code UNCLASSIFIED} until the classification module assigns a root cause. */
    @NotNull
    private ExceptionCategory category;

    @NotNull
    private ExceptionPriority priority;

    @NotNull
    private ExceptionStatus status;

    /** Machine-readable reason, e.g. {@code MISSING_COUNTERPARTY}. */
    @NotBlank @Size(max = 64)
    private String reasonCode;

    @Size(max = 2000)
    private String description;

    @NotNull
    private Instant detectedAt;

    /** Populated by Exception Classification (Phase 5). */
    @Size(max = 256)
    private String rootCause;

    @Size(max = 2000)
    private String explanation;

    private ExceptionSeverity severity;

    @Size(max = 1000)
    private String recommendedAction;

    private Instant classifiedAt;

    /** Populated only after successful STP revalidation (never by WorkItem resolution alone). */
    @Size(max = 500)
    private String resolutionReason;

    private Instant resolvedAt;

    @Size(max = 64)
    private String resolvedBy;

    private Instant lastRevalidatedAt;

    @Size(max = 32)
    private String lastRevalidationOutcome;

    private List<RevalidationAttempt> revalidationHistory = List.of();

    public ExceptionRecord() {
        super(SCHEMA_VERSION);
    }

    @Override
    public String getType() { return TYPE; }

    @Override
    protected String businessKey() { return exceptionId; }

    public String getExceptionId() { return exceptionId; }
    public void setExceptionId(String exceptionId) { this.exceptionId = exceptionId; }
    public String getTradeId() { return tradeId; }
    public void setTradeId(String tradeId) { this.tradeId = tradeId; }
    public String getCorrelationId() { return correlationId; }
    public void setCorrelationId(String correlationId) { this.correlationId = correlationId; }
    public ExceptionCategory getCategory() { return category; }
    public void setCategory(ExceptionCategory category) { this.category = category; }
    public ExceptionPriority getPriority() { return priority; }
    public void setPriority(ExceptionPriority priority) { this.priority = priority; }
    public ExceptionStatus getStatus() { return status; }
    public void setStatus(ExceptionStatus status) { this.status = status; }
    public String getReasonCode() { return reasonCode; }
    public void setReasonCode(String reasonCode) { this.reasonCode = reasonCode; }
    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }
    public Instant getDetectedAt() { return detectedAt; }
    public void setDetectedAt(Instant detectedAt) { this.detectedAt = detectedAt; }
    public String getRootCause() { return rootCause; }
    public void setRootCause(String rootCause) { this.rootCause = rootCause; }
    public String getExplanation() { return explanation; }
    public void setExplanation(String explanation) { this.explanation = explanation; }
    public ExceptionSeverity getSeverity() { return severity; }
    public void setSeverity(ExceptionSeverity severity) { this.severity = severity; }
    public String getRecommendedAction() { return recommendedAction; }
    public void setRecommendedAction(String recommendedAction) { this.recommendedAction = recommendedAction; }
    public Instant getClassifiedAt() { return classifiedAt; }
    public void setClassifiedAt(Instant classifiedAt) { this.classifiedAt = classifiedAt; }
    public String getResolutionReason() { return resolutionReason; }
    public void setResolutionReason(String resolutionReason) { this.resolutionReason = resolutionReason; }
    public Instant getResolvedAt() { return resolvedAt; }
    public void setResolvedAt(Instant resolvedAt) { this.resolvedAt = resolvedAt; }
    public String getResolvedBy() { return resolvedBy; }
    public void setResolvedBy(String resolvedBy) { this.resolvedBy = resolvedBy; }
    public Instant getLastRevalidatedAt() { return lastRevalidatedAt; }
    public void setLastRevalidatedAt(Instant lastRevalidatedAt) { this.lastRevalidatedAt = lastRevalidatedAt; }
    public String getLastRevalidationOutcome() { return lastRevalidationOutcome; }
    public void setLastRevalidationOutcome(String lastRevalidationOutcome) { this.lastRevalidationOutcome = lastRevalidationOutcome; }
    public List<RevalidationAttempt> getRevalidationHistory() { return revalidationHistory; }
    public void setRevalidationHistory(List<RevalidationAttempt> revalidationHistory) {
        this.revalidationHistory = revalidationHistory == null
                ? List.of()
                : Collections.unmodifiableList(new ArrayList<>(revalidationHistory));
    }
}
