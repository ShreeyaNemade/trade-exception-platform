package com.tradeexception.platform.ingestion;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tradeexception.platform.audit.domain.AuditEvent;
import com.tradeexception.platform.audit.repository.AuditEventRepository;
import com.tradeexception.platform.common.couchdb.CouchDbConnectionException;
import com.tradeexception.platform.common.kafka.EventEnvelope;
import com.tradeexception.platform.replay.DeadLetterRecord;
import com.tradeexception.platform.replay.DeadLetterRecord.DeadLetterErrorCategory;
import com.tradeexception.platform.replay.repository.DeadLetterRepository;
import com.tradeexception.platform.trade.domain.Trade;
import com.tradeexception.platform.trade.domain.TradeStatus;
import com.tradeexception.platform.trade.repository.TradeRepository;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * End-to-end Kafka integration test using Embedded Kafka (no Docker required).
 * CouchDB repositories are mocked because a live CouchDB container is not required here.
 */
@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "app.couchdb.auto-initialize=false",
        "app.reference-data.auto-seed=false",
        "app.kafka.partitions=1",
        "app.kafka.replicas=1",
        "app.kafka.retry.max-attempts=2",
        "app.kafka.retry.backoff-ms=50"
})
@EmbeddedKafka(partitions = 1, topics = {
        "trade.raw", "trade.accepted", "trade.enriched", "stp.validated",
        "confirmation.generated", "exception.created", "exception.classified",
        "workitem.linked", "manual.review.required", "deadletter.trade"
})
@DirtiesContext
class TradeIngestionEmbeddedKafkaTest {

    @Autowired private SyntheticTradeProducer producer;
    @Autowired private EmbeddedKafkaBroker embeddedKafka;
    @Autowired private ObjectMapper objectMapper;

    @MockBean private TradeRepository tradeRepository;
    @MockBean private AuditEventRepository auditEventRepository;
    @MockBean private DeadLetterRepository deadLetterRepository;

    private Consumer<String, String> testConsumer;

    @AfterEach
    void tearDown() {
        if (testConsumer != null) {
            testConsumer.close();
            testConsumer = null;
        }
    }

    @Test
    void validTradeFlowsFromTradeRawToTradeAccepted() throws Exception {
        testConsumer = createConsumer("trade.accepted");
        when(tradeRepository.findByTradeId(any())).thenReturn(Optional.empty());
        when(tradeRepository.save(any(Trade.class))).thenAnswer(inv -> {
            Trade t = inv.getArgument(0);
            t.setRevision("1-embedded");
            return t;
        });

        SyntheticTradeProducer.PublishResult published =
                producer.publishScenario(SyntheticTradeScenario.VALID_FX_SPOT);

        ConsumerRecord<String, String> acceptedRecord = pollForKey(testConsumer, published.tradeId());
        assertThat(acceptedRecord).isNotNull();

        EventEnvelope<Trade> accepted = objectMapper.readValue(
                acceptedRecord.value(), new TypeReference<>() {});
        assertThat(accepted.eventType()).isEqualTo("TradeAccepted");
        assertThat(accepted.correlationId()).isEqualTo(published.correlationId());
        assertThat(accepted.payload().getTradeId()).isEqualTo(published.tradeId());
        assertThat(accepted.payload().getStatus()).isEqualTo(TradeStatus.ACCEPTED);

        verify(auditEventRepository, timeout(5000)).save(any(AuditEvent.class));
    }

    @Test
    void malformedPayloadIsRoutedToDeadLetterTopic() throws Exception {
        testConsumer = createConsumer("deadletter.trade");

        SyntheticTradeProducer.PublishResult published =
                producer.publishScenario(SyntheticTradeScenario.MALFORMED_PAYLOAD);

        ConsumerRecord<String, String> dlqRecord = pollForKey(testConsumer, published.eventId());
        assertThat(dlqRecord).isNotNull();

        EventEnvelope<DeadLetterRecord> dlqEnvelope = objectMapper.readValue(
                dlqRecord.value(), new TypeReference<>() {});
        assertThat(dlqEnvelope.eventType()).isEqualTo("TradeDeadLettered");
        assertThat(dlqEnvelope.payload().errorCategory()).isEqualTo(DeadLetterErrorCategory.DESERIALIZATION_ERROR);
        assertThat(dlqEnvelope.payload().originalTopic()).isEqualTo("trade.raw");
    }

    @Test
    void transientInfrastructureErrorRetriesThenRoutesToDeadLetter() throws Exception {
        testConsumer = createConsumer("deadletter.trade");
        EventEnvelope<Trade> envelope = producer.buildEnvelopeForScenario(SyntheticTradeScenario.VALID_FX_FORWARD);
        String tradeId = envelope.payload().getTradeId();

        when(tradeRepository.findByTradeId(tradeId))
                .thenThrow(new CouchDbConnectionException("Simulated CouchDB outage", null));

        producer.publishTradeEvent(envelope);

        ConsumerRecord<String, String> dlqRecord = pollForKey(testConsumer, tradeId);
        assertThat(dlqRecord).isNotNull();

        EventEnvelope<DeadLetterRecord> dlqEnvelope = objectMapper.readValue(
                dlqRecord.value(), new TypeReference<>() {});
        assertThat(dlqEnvelope.payload().errorCategory()).isEqualTo(DeadLetterErrorCategory.INFRASTRUCTURE_ERROR);
        // max-attempts=2 -> initial attempt + 1 retry
        verify(tradeRepository, times(2)).findByTradeId(tradeId);
    }

    private Consumer<String, String> createConsumer(String topic) {
        Map<String, Object> props = KafkaTestUtils.consumerProps(
                "test-verifier-" + UUID.randomUUID(), "true", embeddedKafka);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        DefaultKafkaConsumerFactory<String, String> cf =
                new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), new StringDeserializer());
        Consumer<String, String> consumer = cf.createConsumer();
        consumer.subscribe(List.of(topic));
        return consumer;
    }

    private ConsumerRecord<String, String> pollForKey(Consumer<String, String> consumer, String expectedKey) {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(250));
            for (ConsumerRecord<String, String> rec : records) {
                if (expectedKey.equals(rec.key())) {
                    return rec;
                }
            }
        }
        return null;
    }
}
