package com.tradeexception.platform;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tradeexception.platform.audit.domain.AuditEvent;
import com.tradeexception.platform.audit.repository.AuditEventRepository;
import com.tradeexception.platform.confirmation.domain.Confirmation;
import com.tradeexception.platform.confirmation.repository.ConfirmationRepository;
import com.tradeexception.platform.enrichment.ReferenceDataSeeder;
import com.tradeexception.platform.enrichment.domain.ReferenceData;
import com.tradeexception.platform.enrichment.domain.ReferenceDataCategory;
import com.tradeexception.platform.enrichment.repository.ReferenceDataRepository;
import com.tradeexception.platform.exception.domain.ExceptionRecord;
import com.tradeexception.platform.exception.repository.ExceptionRepository;
import com.tradeexception.platform.replay.DeadLetterRecord.DeadLetterErrorCategory;
import com.tradeexception.platform.replay.domain.DeadLetterDocument;
import com.tradeexception.platform.replay.domain.ReplayStatus;
import com.tradeexception.platform.replay.repository.DeadLetterRepository;
import com.tradeexception.platform.trade.domain.Trade;
import com.tradeexception.platform.trade.repository.TradeRepository;
import com.tradeexception.platform.workitem.domain.WorkItem;
import com.tradeexception.platform.workitem.repository.WorkItemRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.patch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Full-stack backend end-to-end integration test over MockMvc (with real Spring Security) and
 * Embedded Kafka, backed by an in-memory CouchDB repository store.
 */
@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "app.couchdb.auto-initialize=false",
        "app.reference-data.auto-seed=false",
        "app.kafka.partitions=1",
        "app.kafka.replicas=1",
        "app.security.token-secret=e2e-embedded-test-secret-only",
        "app.security.operations.username=operations",
        "app.security.operations.password=e2e-ops-pass",
        "app.security.viewer.username=viewer",
        "app.security.viewer.password=e2e-viewer-pass"
})
@AutoConfigureMockMvc
@EmbeddedKafka(partitions = 1, topics = {
        "trade.raw", "trade.accepted", "trade.enriched", "stp.validated",
        "confirmation.generated", "exception.created", "exception.classified",
        "workitem.linked", "manual.review.required", "deadletter.trade"
})
@DirtiesContext
class EndToEndLifecycleEmbeddedKafkaTest {

    @Autowired private MockMvc mockMvc;
    @Autowired private ObjectMapper objectMapper;

    @MockBean private ReferenceDataRepository referenceDataRepository;
    @MockBean private TradeRepository tradeRepository;
    @MockBean private ConfirmationRepository confirmationRepository;
    @MockBean private ExceptionRepository exceptionRepository;
    @MockBean private WorkItemRepository workItemRepository;
    @MockBean private AuditEventRepository auditEventRepository;
    @MockBean private DeadLetterRepository deadLetterRepository;

    private final Map<String, Trade> trades = new ConcurrentHashMap<>();
    private final Map<String, Confirmation> confirmations = new ConcurrentHashMap<>();
    private final Map<String, ExceptionRecord> exceptions = new ConcurrentHashMap<>();
    private final Map<String, WorkItem> workItems = new ConcurrentHashMap<>();
    private final Map<String, AuditEvent> audits = new ConcurrentHashMap<>();
    private final Map<String, DeadLetterDocument> dlqs = new ConcurrentHashMap<>();

    @BeforeEach
    void wireInMemoryStores() {
        Map<String, ReferenceData> catalog = ReferenceDataSeeder.defaultCatalog().stream()
                .collect(Collectors.toMap(ReferenceData::getId, r -> r));

        when(referenceDataRepository.findByCategoryAndCode(any(), anyString())).thenAnswer(inv ->
                Optional.ofNullable(catalog.get("refdata:" + ((ReferenceDataCategory) inv.getArgument(0)).name() + ":" + inv.getArgument(1))));

        when(tradeRepository.findByTradeId(anyString())).thenAnswer(inv -> Optional.ofNullable(trades.get(inv.getArgument(0))));
        when(tradeRepository.findAll()).thenAnswer(inv -> new ArrayList<>(trades.values()));
        when(tradeRepository.save(any(Trade.class))).thenAnswer(inv -> {
            Trade t = inv.getArgument(0);
            t.setRevision("1-mem");
            trades.put(t.getTradeId(), t);
            return t;
        });

        when(confirmationRepository.findById(anyString())).thenAnswer(inv -> Optional.ofNullable(confirmations.get(inv.getArgument(0))));
        when(confirmationRepository.findByTradeId(anyString())).thenAnswer(inv -> confirmations.values().stream()
                .filter(c -> inv.getArgument(0).equals(c.getTradeId())).toList());
        when(confirmationRepository.findAll()).thenAnswer(inv -> new ArrayList<>(confirmations.values()));
        when(confirmationRepository.save(any(Confirmation.class))).thenAnswer(inv -> {
            Confirmation c = inv.getArgument(0);
            confirmations.put(c.getId(), c);
            return c;
        });

        when(exceptionRepository.findById(anyString())).thenAnswer(inv -> Optional.ofNullable(exceptions.get(inv.getArgument(0))));
        when(exceptionRepository.findAll()).thenAnswer(inv -> new ArrayList<>(exceptions.values()));
        when(exceptionRepository.save(any(ExceptionRecord.class))).thenAnswer(inv -> {
            ExceptionRecord e = inv.getArgument(0);
            exceptions.put(e.getId(), e);
            return e;
        });

        when(workItemRepository.findById(anyString())).thenAnswer(inv -> Optional.ofNullable(workItems.get(inv.getArgument(0))));
        when(workItemRepository.findAll()).thenAnswer(inv -> new ArrayList<>(workItems.values()));
        when(workItemRepository.save(any(WorkItem.class))).thenAnswer(inv -> {
            WorkItem w = inv.getArgument(0);
            workItems.put(w.getId(), w);
            return w;
        });

        when(auditEventRepository.findAll()).thenAnswer(inv -> new ArrayList<>(audits.values()));
        when(auditEventRepository.findByTradeId(anyString())).thenAnswer(inv -> audits.values().stream()
                .filter(a -> inv.getArgument(0).equals(a.getTradeId())).toList());
        when(auditEventRepository.save(any(AuditEvent.class))).thenAnswer(inv -> {
            AuditEvent a = inv.getArgument(0);
            audits.put(a.getId(), a);
            return a;
        });

        when(deadLetterRepository.findByDeadLetterId(anyString())).thenAnswer(inv -> Optional.ofNullable(dlqs.get(inv.getArgument(0))));
        when(deadLetterRepository.findAll()).thenAnswer(inv -> new ArrayList<>(dlqs.values()));
        when(deadLetterRepository.save(any(DeadLetterDocument.class))).thenAnswer(inv -> {
            DeadLetterDocument d = inv.getArgument(0);
            dlqs.put(d.getDeadLetterId(), d);
            return d;
        });
    }

    @Test
    void fullAuthenticatedLifecycleFromLoginThroughConfirmationDlqAndReplay() throws Exception {
        // 1. Authenticate as operations & viewer
        String opsToken = login("operations", "e2e-ops-pass");
        String viewerToken = login("viewer", "e2e-viewer-pass");

        // Viewer forbidden from triggering synthetic trade
        mockMvc.perform(post("/api/v1/synthetic-trades/VALID_FX_SPOT")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + viewerToken))
                .andExpect(status().isForbidden());

        // 2. Trigger valid FX Spot via Kafka
        MvcResult triggerRes = mockMvc.perform(post("/api/v1/synthetic-trades/VALID_FX_SPOT")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + opsToken))
                .andExpect(status().isAccepted())
                .andReturn();
        String tradeId = objectMapper.readTree(triggerRes.getResponse().getContentAsString())
                .at("/data/tradeId").asText();

        // Wait for full Kafka pipeline: trade.raw -> trade.accepted -> trade.enriched -> stp.validated -> confirmation.generated
        awaitCondition(() -> confirmations.containsKey("confirmation:CNF-" + tradeId), 12_000);

        // 3. Verify via REST APIs (trade, confirmation HTML, audit timeline, dashboard summary)
        mockMvc.perform(get("/api/v1/trades/" + tradeId)
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + viewerToken))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.status").value("CONFIRMED"));

        mockMvc.perform(get("/api/v1/confirmations/CNF-" + tradeId + "/document")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + viewerToken))
                .andExpect(status().isOk());

        mockMvc.perform(get("/api/v1/trades/" + tradeId + "/events")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + viewerToken))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.length()").value(4)); // ACCEPTED, ENRICHED, STP_VALIDATED, CONFIRMATION_GENERATED

        mockMvc.perform(get("/api/v1/dashboard/summary")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + viewerToken))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.confirmationsGenerated").value(1));

        // 3b. Trigger MISSING_MANDATORY_ECONOMIC_FIELD -> EXCEPTION -> INVALID_ECONOMIC_TERMS -> PRODUCT_CONTROL work item
        MvcResult econRes = mockMvc.perform(post("/api/v1/synthetic-trades/MISSING_MANDATORY_ECONOMIC_FIELD")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + opsToken))
                .andExpect(status().isAccepted())
                .andReturn();
        String econTradeId = objectMapper.readTree(econRes.getResponse().getContentAsString())
                .at("/data/tradeId").asText();
        awaitCondition(() -> workItems.containsKey("workitem:WI-EXC-" + econTradeId), 12_000);
        assertThat(trades.get(econTradeId).getStatus()).isEqualTo(com.tradeexception.platform.trade.domain.TradeStatus.EXCEPTION);
        assertThat(exceptions.get("exception:EXC-" + econTradeId).getCategory())
                .isEqualTo(com.tradeexception.platform.exception.domain.ExceptionCategory.INVALID_ECONOMIC_TERMS);
        assertThat(workItems.get("workitem:WI-EXC-" + econTradeId).getAssignedTeam())
                .isEqualTo(com.tradeexception.platform.workitem.domain.OperationalTeam.PRODUCT_CONTROL);

        // 3b-ii. Assign & transition work item (verify authenticated actor is recorded, not spoofed)
        String econExId = "EXC-" + econTradeId;
        String econWiId = "WI-" + econExId;
        mockMvc.perform(patch("/api/v1/work-items/" + econWiId + "/assign")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + opsToken)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"assignedTeam\":\"PRODUCT_CONTROL\",\"assignee\":\"e2e-analyst\",\"actor\":\"spoofed\"}"))
                .andExpect(status().isOk());
        mockMvc.perform(patch("/api/v1/work-items/" + econWiId + "/status")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + opsToken)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"status\":\"IN_PROGRESS\",\"actor\":\"spoofed\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.status").value("IN_PROGRESS"));

        // 3b-iii. Workflow 3: Incomplete remediation fails safely, valid remediation resolves & confirms, duplicate returns 409
        mockMvc.perform(post("/api/v1/exceptions/" + econExId + "/remediate")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + opsToken)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"notionalAmount\":1500000,\"notionalCurrency\":\"USD\",\"settlementCurrency\":\"EUR\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.validationResult.decision").value("STP_REJECTED"))
                .andExpect(jsonPath("$.data.exception.status").value("OPEN"))
                .andExpect(jsonPath("$.data.trade.status").value("EXCEPTION"));

        mockMvc.perform(post("/api/v1/exceptions/" + econExId + "/remediate")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + opsToken)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"notionalAmount\":1500000,\"notionalCurrency\":\"USD\",\"price\":1.0845,\"settlementCurrency\":\"EUR\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.validationResult.decision").value("STP_ELIGIBLE"))
                .andExpect(jsonPath("$.data.exception.status").value("RESOLVED"))
                .andExpect(jsonPath("$.data.exception.resolvedBy").value("operations"));

        // Downstream Kafka stp.validated -> confirmation generation -> Trade CONFIRMED
        awaitCondition(() -> confirmations.containsKey("confirmation:CNF-" + econTradeId), 12_000);
        assertThat(trades.get(econTradeId).getStatus())
                .isEqualTo(com.tradeexception.platform.trade.domain.TradeStatus.CONFIRMED);
        assertThat(workItems.get("workitem:" + econWiId).getStatus())
                .isEqualTo(com.tradeexception.platform.workitem.domain.WorkItemStatus.RESOLVED);

        // Duplicate remediation rejected with 409 and produces no duplicate confirmation
        mockMvc.perform(post("/api/v1/exceptions/" + econExId + "/remediate")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + opsToken)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"notionalAmount\":1500000,\"notionalCurrency\":\"USD\",\"price\":1.0845}"))
                .andExpect(status().isConflict());
        assertThat(confirmations.values().stream().filter(c -> econTradeId.equals(c.getTradeId())).count()).isEqualTo(1);

        // Workflow 5: Audit integrity — verify authenticated actor (no spoofed actor) and complete event trail
        List<AuditEvent> econAudits = audits.values().stream()
                .filter(a -> econTradeId.equals(a.getTradeId()))
                .toList();
        assertThat(econAudits).isNotEmpty();
        assertThat(econAudits).noneMatch(a -> "spoofed".equals(a.getActor()));
        assertThat(econAudits).anyMatch(a -> "operations".equals(a.getActor())
                && a.getEventType() == com.tradeexception.platform.audit.domain.EventType.EXCEPTION_RESOLVED);
        assertThat(econAudits).anyMatch(a -> a.getEventType() == com.tradeexception.platform.audit.domain.EventType.CONFIRMATION_GENERATED);

        // 3c. Trigger INVALID_COUNTERPARTY_REFERENCE -> EXCEPTION -> INVALID_COUNTERPARTY -> COUNTERPARTY_OPERATIONS work item
        MvcResult cptyRes = mockMvc.perform(post("/api/v1/synthetic-trades/INVALID_COUNTERPARTY_REFERENCE")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + opsToken))
                .andExpect(status().isAccepted())
                .andReturn();
        String cptyTradeId = objectMapper.readTree(cptyRes.getResponse().getContentAsString())
                .at("/data/tradeId").asText();
        awaitCondition(() -> workItems.containsKey("workitem:WI-EXC-" + cptyTradeId), 12_000);
        assertThat(exceptions.get("exception:EXC-" + cptyTradeId).getCategory())
                .isEqualTo(com.tradeexception.platform.exception.domain.ExceptionCategory.INVALID_COUNTERPARTY);
        assertThat(workItems.get("workitem:WI-EXC-" + cptyTradeId).getAssignedTeam())
                .isEqualTo(com.tradeexception.platform.workitem.domain.OperationalTeam.COUNTERPARTY_OPERATIONS);

        // 3d. Trigger DUPLICATE_MESSAGE -> only 1 trade & 1 confirmation created
        MvcResult dupRes = mockMvc.perform(post("/api/v1/synthetic-trades/DUPLICATE_MESSAGE")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + opsToken))
                .andExpect(status().isAccepted())
                .andReturn();
        String dupTradeId = objectMapper.readTree(dupRes.getResponse().getContentAsString())
                .at("/data/tradeId").asText();
        awaitCondition(() -> confirmations.containsKey("confirmation:CNF-" + dupTradeId), 12_000);
        assertThat(confirmations.values().stream().filter(c -> dupTradeId.equals(c.getTradeId())).count()).isEqualTo(1);

        // 4. Trigger MALFORMED_PAYLOAD -> DLQ persistence -> verify replay rejected (409)
        mockMvc.perform(post("/api/v1/synthetic-trades/MALFORMED_PAYLOAD")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + opsToken))
                .andExpect(status().isAccepted());

        awaitCondition(() -> dlqs.values().stream()
                .anyMatch(d -> d.getErrorCategory() == DeadLetterErrorCategory.DESERIALIZATION_ERROR), 10_000);

        String badDlqId = dlqs.values().stream()
                .filter(d -> d.getErrorCategory() == DeadLetterErrorCategory.DESERIALIZATION_ERROR)
                .findFirst().orElseThrow().getDeadLetterId();

        mockMvc.perform(post("/api/v1/dlq/" + badDlqId + "/replay")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + opsToken))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.error.code").value("INVALID_STATE_TRANSITION"));

        // 5. Seed an eligible TECHNICAL DLQ record, replay it once (200), verify second replay blocked (409)
        DeadLetterDocument eligible = new DeadLetterDocument();
        eligible.setDeadLetterId("DLQ-E2E-TECH-1");
        eligible.setOriginalEventId("EVT-E2E-1");
        eligible.setOriginalTopic("trade.raw");
        eligible.setOriginalKey("TRD-REPLAY");
        eligible.setCorrelationId("CORR-REPLAY");
        eligible.setErrorCategory(DeadLetterErrorCategory.INFRASTRUCTURE_ERROR);
        eligible.setFailureType("TECHNICAL");
        eligible.setRawPayload("{\"eventId\":\"EVT-E2E-1\"}");
        eligible.setFailedAt(Instant.now());
        eligible.setReplayStatus(ReplayStatus.PENDING);
        dlqs.put(eligible.getDeadLetterId(), eligible);

        mockMvc.perform(post("/api/v1/dlq/DLQ-E2E-TECH-1/replay")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + opsToken))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.replayStatus").value("REPLAYED"));

        mockMvc.perform(post("/api/v1/dlq/DLQ-E2E-TECH-1/replay")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + opsToken))
                .andExpect(status().isConflict());
    }

    private String login(String user, String pass) throws Exception {
        MvcResult res = mockMvc.perform(post("/api/v1/auth/login")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"username\":\"" + user + "\",\"password\":\"" + pass + "\"}"))
                .andExpect(status().isOk())
                .andReturn();
        JsonNode root = objectMapper.readTree(res.getResponse().getContentAsString());
        String token = root.at("/data/accessToken").asText();
        assertThat(token).isNotBlank();
        return token;
    }

    private static void awaitCondition(java.util.function.BooleanSupplier condition, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(150);
        }
        assertThat(condition.getAsBoolean()).as("Expected async Kafka workflow condition within " + timeoutMs + "ms").isTrue();
    }
}
