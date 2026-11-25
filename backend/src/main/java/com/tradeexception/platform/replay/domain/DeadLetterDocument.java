package com.tradeexception.platform.replay.domain;

import com.tradeexception.platform.common.domain.BaseDocument;
import com.tradeexception.platform.replay.DeadLetterRecord.DeadLetterErrorCategory;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Persistent CouchDB representation of a failed Kafka message ({@code dlq:<deadLetterId>}).
 * Preserves the unmodified original payload and tracks replay attempts and history.
 */
public class DeadLetterDocument extends BaseDocument {

    public static final String TYPE = "dlq";
    public static final int SCHEMA_VERSION = 1;
    public static final int DEFAULT_MAX_REPLAY_ATTEMPTS = 3;

    @NotBlank @Size(max = 128)
    private String deadLetterId;

    @Size(max = 64)
    private String originalEventId;

    @NotBlank @Size(max = 128)
    private String originalTopic;

    private Integer originalPartition;
    private Long originalOffset;
    private String originalKey;

    @NotBlank @Size(max = 128)
    private String correlationId;

    @NotNull
    private DeadLetterErrorCategory errorCategory;

    /** {@code TECHNICAL} vs {@code BUSINESS} classification. */
    @NotBlank @Size(max = 32)
    private String failureType;

    @Size(max = 2000)
    private String errorMessage;

    private List<String> validationErrors;

    /** Unmodified original payload preserved for diagnostics and safe replay. */
    private String rawPayload;

    @NotNull
    private Instant failedAt;

    @NotNull
    private ReplayStatus replayStatus;

    private int replayAttempts;
    private int maxReplayAttempts = DEFAULT_MAX_REPLAY_ATTEMPTS;
    private Instant lastReplayedAt;
    private String lastReplayedBy;
    private List<ReplayAttempt> history = List.of();

    public DeadLetterDocument() {
        super(SCHEMA_VERSION);
    }

    @Override
    public String getType() { return TYPE; }

    @Override
    protected String businessKey() { return deadLetterId; }

    public static String documentId(String deadLetterId) {
        return TYPE + ":" + deadLetterId;
    }

    public String getDeadLetterId() { return deadLetterId; }
    public void setDeadLetterId(String deadLetterId) { this.deadLetterId = deadLetterId; }
    public String getOriginalEventId() { return originalEventId; }
    public void setOriginalEventId(String originalEventId) { this.originalEventId = originalEventId; }
    public String getOriginalTopic() { return originalTopic; }
    public void setOriginalTopic(String originalTopic) { this.originalTopic = originalTopic; }
    public Integer getOriginalPartition() { return originalPartition; }
    public void setOriginalPartition(Integer originalPartition) { this.originalPartition = originalPartition; }
    public Long getOriginalOffset() { return originalOffset; }
    public void setOriginalOffset(Long originalOffset) { this.originalOffset = originalOffset; }
    public String getOriginalKey() { return originalKey; }
    public void setOriginalKey(String originalKey) { this.originalKey = originalKey; }
    public String getCorrelationId() { return correlationId; }
    public void setCorrelationId(String correlationId) { this.correlationId = correlationId; }
    public DeadLetterErrorCategory getErrorCategory() { return errorCategory; }
    public void setErrorCategory(DeadLetterErrorCategory errorCategory) { this.errorCategory = errorCategory; }
    public String getFailureType() { return failureType; }
    public void setFailureType(String failureType) { this.failureType = failureType; }
    public String getErrorMessage() { return errorMessage; }
    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
    public List<String> getValidationErrors() { return validationErrors; }
    public void setValidationErrors(List<String> validationErrors) {
        this.validationErrors = validationErrors == null ? null : Collections.unmodifiableList(new ArrayList<>(validationErrors));
    }
    public String getRawPayload() { return rawPayload; }
    public void setRawPayload(String rawPayload) { this.rawPayload = rawPayload; }
    public Instant getFailedAt() { return failedAt; }
    public void setFailedAt(Instant failedAt) { this.failedAt = failedAt; }
    public ReplayStatus getReplayStatus() { return replayStatus; }
    public void setReplayStatus(ReplayStatus replayStatus) { this.replayStatus = replayStatus; }
    public int getReplayAttempts() { return replayAttempts; }
    public void setReplayAttempts(int replayAttempts) { this.replayAttempts = replayAttempts; }
    public int getMaxReplayAttempts() { return maxReplayAttempts; }
    public void setMaxReplayAttempts(int maxReplayAttempts) { this.maxReplayAttempts = maxReplayAttempts; }
    public Instant getLastReplayedAt() { return lastReplayedAt; }
    public void setLastReplayedAt(Instant lastReplayedAt) { this.lastReplayedAt = lastReplayedAt; }
    public String getLastReplayedBy() { return lastReplayedBy; }
    public void setLastReplayedBy(String lastReplayedBy) { this.lastReplayedBy = lastReplayedBy; }
    public List<ReplayAttempt> getHistory() { return history; }
    public void setHistory(List<ReplayAttempt> history) {
        this.history = history == null ? List.of() : Collections.unmodifiableList(new ArrayList<>(history));
    }
}
