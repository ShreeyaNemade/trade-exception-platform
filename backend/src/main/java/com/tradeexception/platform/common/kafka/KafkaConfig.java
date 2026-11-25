package com.tradeexception.platform.common.kafka;

import com.tradeexception.platform.common.couchdb.CouchDbConnectionException;
import com.tradeexception.platform.replay.DeadLetterPublisher;
import com.tradeexception.platform.replay.DeadLetterRecord.DeadLetterErrorCategory;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Spring Kafka producer, consumer, topic, retry, and dead-letter configuration.
 * <p>
 * Payloads are transported as JSON strings and serialized/deserialized explicitly with Jackson so
 * malformed payloads are captured cleanly and routed to {@code deadletter.trade} without poisoning
 * consumer partitions.
 */
@Configuration
@EnableKafka
@EnableConfigurationProperties(KafkaProperties.class)
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        config.put(ProducerConfig.RETRIES_CONFIG, 3);
        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    public ConsumerFactory<String, String> consumerFactory(KafkaProperties kafkaProperties) {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaProperties.getConsumerGroupId());
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return new DefaultKafkaConsumerFactory<>(config);
    }

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(
            DeadLetterPublisher deadLetterPublisher,
            KafkaProperties kafkaProperties) {

        long retries = Math.max(0, kafkaProperties.getRetry().getMaxAttempts() - 1);
        FixedBackOff backOff = new FixedBackOff(kafkaProperties.getRetry().getBackoffMs(), retries);

        ConsumerRecordRecoverer recoverer = (ConsumerRecord<?, ?> rec, Exception ex) -> {
            Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
            DeadLetterErrorCategory category = (cause instanceof CouchDbConnectionException)
                    ? DeadLetterErrorCategory.INFRASTRUCTURE_ERROR
                    : DeadLetterErrorCategory.UNEXPECTED_ERROR;

            deadLetterPublisher.publish(
                    rec.topic(),
                    rec.partition(),
                    rec.offset(),
                    rec.key() == null ? null : rec.key().toString(),
                    null,
                    category,
                    "Retries exhausted: " + cause.getMessage(),
                    List.of(),
                    rec.value() == null ? null : rec.value().toString());
        };

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);
        errorHandler.addNotRetryableExceptions(IllegalArgumentException.class);
        return errorHandler;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory,
            DefaultErrorHandler kafkaErrorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(kafkaErrorHandler);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        return factory;
    }

    @Bean
    public NewTopic tradeRawTopic(KafkaProperties props) {
        return TopicBuilder.name(props.getTopics().getTradeRaw())
                .partitions(props.getPartitions())
                .replicas(props.getReplicas())
                .build();
    }

    @Bean
    public NewTopic tradeAcceptedTopic(KafkaProperties props) {
        return TopicBuilder.name(props.getTopics().getTradeAccepted())
                .partitions(props.getPartitions())
                .replicas(props.getReplicas())
                .build();
    }

    @Bean
    public NewTopic tradeEnrichedTopic(KafkaProperties props) {
        return TopicBuilder.name(props.getTopics().getTradeEnriched())
                .partitions(props.getPartitions())
                .replicas(props.getReplicas())
                .build();
    }

    @Bean
    public NewTopic stpValidatedTopic(KafkaProperties props) {
        return TopicBuilder.name(props.getTopics().getStpValidated())
                .partitions(props.getPartitions())
                .replicas(props.getReplicas())
                .build();
    }

    @Bean
    public NewTopic confirmationGeneratedTopic(KafkaProperties props) {
        return TopicBuilder.name(props.getTopics().getConfirmationGenerated())
                .partitions(props.getPartitions())
                .replicas(props.getReplicas())
                .build();
    }

    @Bean
    public NewTopic exceptionCreatedTopic(KafkaProperties props) {
        return TopicBuilder.name(props.getTopics().getExceptionCreated())
                .partitions(props.getPartitions())
                .replicas(props.getReplicas())
                .build();
    }

    @Bean
    public NewTopic exceptionClassifiedTopic(KafkaProperties props) {
        return TopicBuilder.name(props.getTopics().getExceptionClassified())
                .partitions(props.getPartitions())
                .replicas(props.getReplicas())
                .build();
    }

    @Bean
    public NewTopic workitemLinkedTopic(KafkaProperties props) {
        return TopicBuilder.name(props.getTopics().getWorkitemLinked())
                .partitions(props.getPartitions())
                .replicas(props.getReplicas())
                .build();
    }

    @Bean
    public NewTopic manualReviewRequiredTopic(KafkaProperties props) {
        return TopicBuilder.name(props.getTopics().getManualReviewRequired())
                .partitions(props.getPartitions())
                .replicas(props.getReplicas())
                .build();
    }

    @Bean
    public NewTopic deadletterTradeTopic(KafkaProperties props) {
        return TopicBuilder.name(props.getTopics().getDeadletterTrade())
                .partitions(props.getPartitions())
                .replicas(props.getReplicas())
                .build();
    }
}
