package com.tradeexception.platform.common.kafka;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** Environment-driven Kafka topic, consumer group, and retry properties ({@code app.kafka.*}). */
@ConfigurationProperties(prefix = "app.kafka")
public class KafkaProperties {

    private String consumerGroupId = "trade-ingestion-group";
    private String enrichmentConsumerGroupId = "trade-enrichment-group";
    private String stpConsumerGroupId = "stp-validation-group";
    private String confirmationConsumerGroupId = "confirmation-generation-group";
    private String classificationConsumerGroupId = "exception-classification-group";
    private String workitemConsumerGroupId = "workitem-management-group";
    private String dlqConsumerGroupId = "dlq-persistence-group";
    private int partitions = 3;
    private short replicas = 1;
    private Topics topics = new Topics();
    private Retry retry = new Retry();

    public String getConsumerGroupId() { return consumerGroupId; }
    public void setConsumerGroupId(String consumerGroupId) { this.consumerGroupId = consumerGroupId; }
    public String getEnrichmentConsumerGroupId() { return enrichmentConsumerGroupId; }
    public void setEnrichmentConsumerGroupId(String enrichmentConsumerGroupId) { this.enrichmentConsumerGroupId = enrichmentConsumerGroupId; }
    public String getStpConsumerGroupId() { return stpConsumerGroupId; }
    public void setStpConsumerGroupId(String stpConsumerGroupId) { this.stpConsumerGroupId = stpConsumerGroupId; }
    public String getConfirmationConsumerGroupId() { return confirmationConsumerGroupId; }
    public void setConfirmationConsumerGroupId(String confirmationConsumerGroupId) { this.confirmationConsumerGroupId = confirmationConsumerGroupId; }
    public String getClassificationConsumerGroupId() { return classificationConsumerGroupId; }
    public void setClassificationConsumerGroupId(String classificationConsumerGroupId) { this.classificationConsumerGroupId = classificationConsumerGroupId; }
    public String getWorkitemConsumerGroupId() { return workitemConsumerGroupId; }
    public void setWorkitemConsumerGroupId(String workitemConsumerGroupId) { this.workitemConsumerGroupId = workitemConsumerGroupId; }
    public String getDlqConsumerGroupId() { return dlqConsumerGroupId; }
    public void setDlqConsumerGroupId(String dlqConsumerGroupId) { this.dlqConsumerGroupId = dlqConsumerGroupId; }
    public int getPartitions() { return partitions; }
    public void setPartitions(int partitions) { this.partitions = partitions; }
    public short getReplicas() { return replicas; }
    public void setReplicas(short replicas) { this.replicas = replicas; }
    public Topics getTopics() { return topics; }
    public void setTopics(Topics topics) { this.topics = topics; }
    public Retry getRetry() { return retry; }
    public void setRetry(Retry retry) { this.retry = retry; }

    public static class Topics {
        private String tradeRaw = "trade.raw";
        private String tradeAccepted = "trade.accepted";
        private String tradeEnriched = "trade.enriched";
        private String stpValidated = "stp.validated";
        private String confirmationGenerated = "confirmation.generated";
        private String exceptionCreated = "exception.created";
        private String exceptionClassified = "exception.classified";
        private String workitemLinked = "workitem.linked";
        private String manualReviewRequired = "manual.review.required";
        private String deadletterTrade = "deadletter.trade";

        public String getTradeRaw() { return tradeRaw; }
        public void setTradeRaw(String tradeRaw) { this.tradeRaw = tradeRaw; }
        public String getTradeAccepted() { return tradeAccepted; }
        public void setTradeAccepted(String tradeAccepted) { this.tradeAccepted = tradeAccepted; }
        public String getTradeEnriched() { return tradeEnriched; }
        public void setTradeEnriched(String tradeEnriched) { this.tradeEnriched = tradeEnriched; }
        public String getStpValidated() { return stpValidated; }
        public void setStpValidated(String stpValidated) { this.stpValidated = stpValidated; }
        public String getConfirmationGenerated() { return confirmationGenerated; }
        public void setConfirmationGenerated(String confirmationGenerated) { this.confirmationGenerated = confirmationGenerated; }
        public String getExceptionCreated() { return exceptionCreated; }
        public void setExceptionCreated(String exceptionCreated) { this.exceptionCreated = exceptionCreated; }
        public String getExceptionClassified() { return exceptionClassified; }
        public void setExceptionClassified(String exceptionClassified) { this.exceptionClassified = exceptionClassified; }
        public String getWorkitemLinked() { return workitemLinked; }
        public void setWorkitemLinked(String workitemLinked) { this.workitemLinked = workitemLinked; }
        public String getManualReviewRequired() { return manualReviewRequired; }
        public void setManualReviewRequired(String manualReviewRequired) { this.manualReviewRequired = manualReviewRequired; }
        public String getDeadletterTrade() { return deadletterTrade; }
        public void setDeadletterTrade(String deadletterTrade) { this.deadletterTrade = deadletterTrade; }
    }

    public static class Retry {
        /** Total delivery attempts including the initial try (must be >= 1). */
        private long maxAttempts = 3;
        private long backoffMs = 500;

        public long getMaxAttempts() { return maxAttempts; }
        public void setMaxAttempts(long maxAttempts) { this.maxAttempts = maxAttempts; }
        public long getBackoffMs() { return backoffMs; }
        public void setBackoffMs(long backoffMs) { this.backoffMs = backoffMs; }
    }
}
