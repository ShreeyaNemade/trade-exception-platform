package com.tradeexception.platform.stp;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tradeexception.platform.audit.repository.AuditEventRepository;
import com.tradeexception.platform.common.kafka.EventEnvelope;
import com.tradeexception.platform.confirmation.repository.ConfirmationRepository;
import com.tradeexception.platform.enrichment.ReferenceDataSeeder;
import com.tradeexception.platform.replay.repository.DeadLetterRepository;
import com.tradeexception.platform.workitem.repository.WorkItemRepository;
import com.tradeexception.platform.enrichment.domain.ReferenceData;
import com.tradeexception.platform.enrichment.domain.ReferenceDataCategory;
import com.tradeexception.platform.enrichment.repository.ReferenceDataRepository;
import com.tradeexception.platform.exception.domain.ExceptionRecord;
import com.tradeexception.platform.exception.repository.ExceptionRepository;
import com.tradeexception.platform.ingestion.SyntheticTradeProducer;
import com.tradeexception.platform.ingestion.SyntheticTradeScenario;
import com.tradeexception.platform.stp.domain.StpDecision;
import com.tradeexception.platform.stp.domain.StpValidationResult;
import com.tradeexception.platform.trade.domain.PartyRole;
import com.tradeexception.platform.trade.domain.Trade;
import com.tradeexception.platform.trade.domain.TradeParty;
import com.tradeexception.platform.trade.domain.TradeStatus;
import com.tradeexception.platform.trade.repository.TradeRepository;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

/**
 * Embedded Kafka integration test verifying the Phase 4 event pipeline:
 * {@code trade.accepted -> trade.enriched -> stp.validated / exception.created / manual.review.required}.
 */
@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "app.couchdb.auto-initialize=false",
        "app.reference-data.auto-seed=false",
        "app.kafka.partitions=1",
        "app.kafka.replicas=1"
})
@EmbeddedKafka(partitions = 1, topics = {
        "trade.raw", "trade.accepted", "trade.enriched", "stp.validated",
        "confirmation.generated", "exception.created", "exception.classified",
        "workitem.linked", "manual.review.required", "deadletter.trade"
})
@DirtiesContext
class EnrichmentAndStpEmbeddedKafkaTest {

    @Autowired private KafkaTemplate<String, String> kafkaTemplate;
    @Autowired private SyntheticTradeProducer producer;
    @Autowired private EmbeddedKafkaBroker embeddedKafka;
    @Autowired private ObjectMapper objectMapper;

    @MockBean private ReferenceDataRepository referenceDataRepository;
    @MockBean private TradeRepository tradeRepository;
    @MockBean private ConfirmationRepository confirmationRepository;
    @MockBean private ExceptionRepository exceptionRepository;
    @MockBean private WorkItemRepository workItemRepository;
    @MockBean private AuditEventRepository auditEventRepository;
    @MockBean private DeadLetterRepository deadLetterRepository;

    private final Map<String, Trade> tradeStore = new ConcurrentHashMap<>();
    private Consumer<String, String> testConsumer;

    @BeforeEach
    void setUpMocks() {
        Map<String, ReferenceData> catalog = ReferenceDataSeeder.defaultCatalog().stream()
                .collect(Collectors.toMap(ReferenceData::getId, r -> r));

        when(referenceDataRepository.findByCategoryAndCode(any(), anyString())).thenAnswer(inv -> {
            ReferenceDataCategory cat = inv.getArgument(0);
            String code = inv.getArgument(1);
            return Optional.ofNullable(catalog.get("refdata:" + cat.name() + ":" + code));
        });
        when(tradeRepository.findByTradeId(anyString())).thenAnswer(inv -> Optional.ofNullable(tradeStore.get(inv.getArgument(0))));
        when(tradeRepository.save(any(Trade.class))).thenAnswer(inv -> {
            Trade t = inv.getArgument(0);
            tradeStore.put(t.getTradeId(), t);
            return t;
        });
        when(exceptionRepository.findById(anyString())).thenReturn(Optional.empty());
        when(exceptionRepository.save(any(ExceptionRecord.class))).thenAnswer(inv -> inv.getArgument(0));
    }

    @AfterEach
    void tearDown() {
        if (testConsumer != null) {
            testConsumer.close();
            testConsumer = null;
        }
    }

    @Test
    void eligibleFxSpotFlowsFromTradeAcceptedThroughEnrichmentToStpValidated() throws Exception {
        testConsumer = createConsumer("stp.validated");
        EventEnvelope<Trade> accepted = producer.buildEnvelopeForScenario(SyntheticTradeScenario.VALID_FX_SPOT);
        Trade trade = accepted.payload();
        trade.setStatus(TradeStatus.ACCEPTED);
        tradeStore.put(trade.getTradeId(), trade);

        kafkaTemplate.send("trade.accepted", trade.getTradeId(), objectMapper.writeValueAsString(accepted));

        ConsumerRecord<String, String> validatedRecord = pollForKey(testConsumer, trade.getTradeId());
        assertThat(validatedRecord).isNotNull();

        EventEnvelope<StpValidationResult> resultEnv = objectMapper.readValue(
                validatedRecord.value(), new TypeReference<>() {});
        assertThat(resultEnv.correlationId()).isEqualTo(accepted.correlationId());
        assertThat(resultEnv.payload().decision()).isEqualTo(StpDecision.STP_ELIGIBLE);
        assertThat(tradeStore.get(trade.getTradeId()).getStatus()).isEqualTo(TradeStatus.VALIDATED);
    }

    @Test
    void restrictedCounterpartyFlowsToExceptionCreatedAndManualReviewRequired() throws Exception {
        testConsumer = createConsumer("manual.review.required");
        EventEnvelope<Trade> accepted = producer.buildEnvelopeForScenario(SyntheticTradeScenario.VALID_FX_SPOT);
        Trade trade = accepted.payload();
        trade.setStatus(TradeStatus.ACCEPTED);
        trade.setParties(List.of(
                new TradeParty("SYNTHBANK00000000001", "Synthetic Bank Alpha", PartyRole.BUYER),
                new TradeParty("SYNTHFUND00000000003", "Synthetic Fund Gamma", PartyRole.SELLER)));
        tradeStore.put(trade.getTradeId(), trade);

        kafkaTemplate.send("trade.accepted", trade.getTradeId(), objectMapper.writeValueAsString(accepted));

        ConsumerRecord<String, String> reviewRecord = pollForKey(testConsumer, trade.getTradeId());
        assertThat(reviewRecord).isNotNull();

        EventEnvelope<ExceptionRecord> exEnv = objectMapper.readValue(
                reviewRecord.value(), new TypeReference<>() {});
        assertThat(exEnv.correlationId()).isEqualTo(accepted.correlationId());
        assertThat(exEnv.payload().getReasonCode()).isEqualTo("COUNTERPARTY_NOT_STP_ELIGIBLE");
        assertThat(tradeStore.get(trade.getTradeId()).getStatus()).isEqualTo(TradeStatus.EXCEPTION);
    }

    private Consumer<String, String> createConsumer(String topic) {
        Map<String, Object> props = KafkaTestUtils.consumerProps(
                "stp-verifier-" + UUID.randomUUID(), "true", embeddedKafka);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        DefaultKafkaConsumerFactory<String, String> cf =
                new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), new StringDeserializer());
        Consumer<String, String> consumer = cf.createConsumer();
        consumer.subscribe(List.of(topic));
        return consumer;
    }

    private ConsumerRecord<String, String> pollForKey(Consumer<String, String> consumer, String expectedKey) {
        long deadline = System.currentTimeMillis() + 12_000;
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
