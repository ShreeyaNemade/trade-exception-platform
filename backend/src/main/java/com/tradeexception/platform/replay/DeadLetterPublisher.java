package com.tradeexception.platform.replay;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tradeexception.platform.common.kafka.EventEnvelope;
import com.tradeexception.platform.common.kafka.KafkaProperties;
import com.tradeexception.platform.replay.DeadLetterRecord.DeadLetterErrorCategory;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** Publishes failed trade messages with diagnostic metadata to {@code deadletter.trade}. */
@Component
public class DeadLetterPublisher {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterPublisher.class);
    private static final String SCHEMA_VERSION = "1.0";
    private static final String SOURCE = "trade-ingestion-dlq";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final KafkaProperties kafkaProperties;

    public DeadLetterPublisher(
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper,
            KafkaProperties kafkaProperties) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.kafkaProperties = kafkaProperties;
    }

    public DeadLetterRecord publish(
            String originalTopic,
            Integer originalPartition,
            Long originalOffset,
            String originalKey,
            String correlationId,
            DeadLetterErrorCategory errorCategory,
            String errorMessage,
            List<String> validationErrors,
            String rawPayload) {

        String deadLetterId = deterministicId(originalTopic, originalPartition, originalOffset);
        String effectiveCorrelationId = (correlationId == null || correlationId.isBlank())
                ? "UNKNOWN-" + deadLetterId
                : correlationId;

        DeadLetterRecord record = new DeadLetterRecord(
                deadLetterId,
                originalTopic,
                originalPartition,
                originalOffset,
                originalKey,
                effectiveCorrelationId,
                errorCategory,
                errorMessage,
                validationErrors == null ? List.of() : List.copyOf(validationErrors),
                rawPayload,
                Instant.now());

        EventEnvelope<DeadLetterRecord> envelope = EventEnvelope.of(
                "TradeDeadLettered",
                effectiveCorrelationId,
                SCHEMA_VERSION,
                SOURCE,
                record);

        String dlqTopic = kafkaProperties.getTopics().getDeadletterTrade();
        String messageKey = (originalKey != null && !originalKey.isBlank()) ? originalKey : deadLetterId;

        try {
            String json = objectMapper.writeValueAsString(envelope);
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(dlqTopic, messageKey, json);
            producerRecord.headers().add("x-error-category", errorCategory.name().getBytes(StandardCharsets.UTF_8));
            producerRecord.headers().add("x-correlation-id", effectiveCorrelationId.getBytes(StandardCharsets.UTF_8));
            if (originalTopic != null) {
                producerRecord.headers().add("x-original-topic", originalTopic.getBytes(StandardCharsets.UTF_8));
            }
            kafkaTemplate.send(producerRecord);
            log.warn("Routed failed message to {} [category={}, key={}, correlationId={}]",
                    dlqTopic, errorCategory, messageKey, effectiveCorrelationId);
            return record;
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Failed to serialize DeadLetterRecord " + deadLetterId, ex);
        }
    }

    /** Generates a deterministic DLQ id from topic/partition/offset when available. */
    public static String deterministicId(String topic, Integer partition, Long offset) {
        if (topic != null && !topic.isBlank() && partition != null && offset != null) {
            return "DLQ-" + topic.replace('.', '-') + "-p" + partition + "-o" + offset;
        }
        return "DLQ-" + UUID.randomUUID();
    }
}
