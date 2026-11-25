package com.tradeexception.platform.workitem;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tradeexception.platform.audit.repository.AuditEventRepository;
import com.tradeexception.platform.common.kafka.EventEnvelope;
import com.tradeexception.platform.confirmation.domain.Confirmation;
import com.tradeexception.platform.confirmation.domain.ConfirmationStatus;
import com.tradeexception.platform.confirmation.repository.ConfirmationRepository;
import com.tradeexception.platform.enrichment.ReferenceDataSeeder;
import com.tradeexception.platform.enrichment.domain.ReferenceData;
import com.tradeexception.platform.enrichment.domain.ReferenceDataCategory;
import com.tradeexception.platform.enrichment.repository.ReferenceDataRepository;
import com.tradeexception.platform.exception.domain.ExceptionCategory;
import com.tradeexception.platform.exception.domain.ExceptionPriority;
import com.tradeexception.platform.exception.domain.ExceptionRecord;
import com.tradeexception.platform.exception.domain.ExceptionStatus;
import com.tradeexception.platform.exception.repository.ExceptionRepository;
import com.tradeexception.platform.ingestion.SyntheticTradeProducer;
import com.tradeexception.platform.ingestion.SyntheticTradeScenario;
import com.tradeexception.platform.replay.repository.DeadLetterRepository;
import com.tradeexception.platform.stp.domain.StpDecision;
import com.tradeexception.platform.stp.domain.StpValidationResult;
import com.tradeexception.platform.trade.domain.Trade;
import com.tradeexception.platform.trade.domain.TradeStatus;
import com.tradeexception.platform.trade.repository.TradeRepository;
import com.tradeexception.platform.workitem.domain.OperationalTeam;
import com.tradeexception.platform.workitem.domain.WorkItem;
import com.tradeexception.platform.workitem.repository.WorkItemRepository;
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
import java.time.Instant;
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
 * Embedded Kafka integration test verifying Phase 5 event flows:
 * 1) {@code stp.validated -> confirmation.generated}
 * 2) {@code exception.created -> exception.classified -> workitem.linked}
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
class Phase5WorkflowEmbeddedKafkaTest {

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
    private final Map<String, Confirmation> confirmationStore = new ConcurrentHashMap<>();
    private final Map<String, ExceptionRecord> exceptionStore = new ConcurrentHashMap<>();
    private final Map<String, WorkItem> workItemStore = new ConcurrentHashMap<>();
    private Consumer<String, String> testConsumer;

    @BeforeEach
    void setUpMocks() {
        Map<String, ReferenceData> catalog = ReferenceDataSeeder.defaultCatalog().stream()
                .collect(Collectors.toMap(ReferenceData::getId, r -> r));

        when(referenceDataRepository.findByCategoryAndCode(any(), anyString())).thenAnswer(inv ->
                Optional.ofNullable(catalog.get("refdata:" + ((ReferenceDataCategory) inv.getArgument(0)).name() + ":" + inv.getArgument(1))));
        when(tradeRepository.findByTradeId(anyString())).thenAnswer(inv -> Optional.ofNullable(tradeStore.get(inv.getArgument(0))));
        when(tradeRepository.save(any(Trade.class))).thenAnswer(inv -> {
            Trade t = inv.getArgument(0);
            tradeStore.put(t.getTradeId(), t);
            return t;
        });
        when(confirmationRepository.findById(anyString())).thenAnswer(inv -> Optional.ofNullable(confirmationStore.get(inv.getArgument(0))));
        when(confirmationRepository.save(any(Confirmation.class))).thenAnswer(inv -> {
            Confirmation c = inv.getArgument(0);
            confirmationStore.put(c.getId(), c);
            return c;
        });
        when(exceptionRepository.findById(anyString())).thenAnswer(inv -> Optional.ofNullable(exceptionStore.get(inv.getArgument(0))));
        when(exceptionRepository.save(any(ExceptionRecord.class))).thenAnswer(inv -> {
            ExceptionRecord e = inv.getArgument(0);
            exceptionStore.put(e.getId(), e);
            return e;
        });
        when(workItemRepository.findById(anyString())).thenAnswer(inv -> Optional.ofNullable(workItemStore.get(inv.getArgument(0))));
        when(workItemRepository.save(any(WorkItem.class))).thenAnswer(inv -> {
            WorkItem w = inv.getArgument(0);
            workItemStore.put(w.getId(), w);
            return w;
        });
    }

    @AfterEach
    void tearDown() {
        if (testConsumer != null) {
            testConsumer.close();
            testConsumer = null;
        }
    }

    @Test
    void stpValidatedTradeGeneratesConfirmationAndPublishesConfirmationGenerated() throws Exception {
        testConsumer = createConsumer("confirmation.generated");
        Trade trade = producer.buildEnvelopeForScenario(SyntheticTradeScenario.VALID_FX_SPOT).payload();
        trade.setStatus(TradeStatus.VALIDATED);
        tradeStore.put(trade.getTradeId(), trade);
        String corrId = trade.getHeader().correlationId();

        StpValidationResult result = new StpValidationResult(
                trade.getTradeId(), corrId, StpDecision.STP_ELIGIBLE, Instant.now(),
                List.of("ALL"), List.of(), List.of(), "All passed");
        EventEnvelope<StpValidationResult> env = EventEnvelope.of("StpValidated", corrId, "1.0", "stp-validation", result);

        kafkaTemplate.send("stp.validated", trade.getTradeId(), objectMapper.writeValueAsString(env));

        ConsumerRecord<String, String> cnfRecord = pollForKey(testConsumer, trade.getTradeId());
        assertThat(cnfRecord).isNotNull();

        EventEnvelope<Confirmation> cnfEnv = objectMapper.readValue(cnfRecord.value(), new TypeReference<>() {});
        assertThat(cnfEnv.correlationId()).isEqualTo(corrId);
        assertThat(cnfEnv.payload().getStatus()).isEqualTo(ConfirmationStatus.GENERATED);
        assertThat(cnfEnv.payload().getRenderedContent()).contains(trade.getTradeId());
        assertThat(tradeStore.get(trade.getTradeId()).getStatus()).isEqualTo(TradeStatus.CONFIRMED);
    }

    @Test
    void exceptionCreatedFlowsThroughClassificationToWorkItemLinked() throws Exception {
        testConsumer = createConsumer("workitem.linked");
        String tradeId = "TRD-P5-EXC";
        String corrId = "CORR-P5-EXC";

        ExceptionRecord initial = new ExceptionRecord();
        initial.setExceptionId("EXC-" + tradeId);
        initial.setTradeId(tradeId);
        initial.setCorrelationId(corrId);
        initial.setCategory(ExceptionCategory.UNCLASSIFIED);
        initial.setPriority(ExceptionPriority.HIGH);
        initial.setStatus(ExceptionStatus.OPEN);
        initial.setReasonCode("COUNTERPARTY_NOT_FOUND");
        initial.setDescription("Counterparty missing");
        initial.setDetectedAt(Instant.now());
        exceptionStore.put(initial.getId(), initial);

        EventEnvelope<ExceptionRecord> env = EventEnvelope.of("ExceptionCreated", corrId, "1.0", "stp-validation", initial);
        kafkaTemplate.send("exception.created", tradeId, objectMapper.writeValueAsString(env));

        ConsumerRecord<String, String> wiRecord = pollForKey(testConsumer, tradeId);
        assertThat(wiRecord).isNotNull();

        EventEnvelope<WorkItem> wiEnv = objectMapper.readValue(wiRecord.value(), new TypeReference<>() {});
        assertThat(wiEnv.correlationId()).isEqualTo(corrId);
        assertThat(wiEnv.payload().getAssignedTeam()).isEqualTo(OperationalTeam.COUNTERPARTY_OPERATIONS);
        assertThat(wiEnv.payload().getPriority()).isEqualTo(ExceptionPriority.HIGH);
        assertThat(exceptionStore.get(initial.getId()).getCategory()).isEqualTo(ExceptionCategory.INVALID_COUNTERPARTY);
    }

    private Consumer<String, String> createConsumer(String topic) {
        Map<String, Object> props = KafkaTestUtils.consumerProps(
                "p5-verifier-" + UUID.randomUUID(), "true", embeddedKafka);
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
