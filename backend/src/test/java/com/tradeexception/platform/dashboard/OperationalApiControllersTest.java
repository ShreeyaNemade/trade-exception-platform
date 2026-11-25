package com.tradeexception.platform.dashboard;

import com.tradeexception.platform.audit.api.AuditController;
import com.tradeexception.platform.audit.api.AuditQueryService;
import com.tradeexception.platform.audit.domain.AuditEvent;
import com.tradeexception.platform.audit.domain.EventType;
import com.tradeexception.platform.audit.repository.AuditEventRepository;
import com.tradeexception.platform.common.exception.GlobalExceptionHandler;
import com.tradeexception.platform.confirmation.api.ConfirmationController;
import com.tradeexception.platform.confirmation.api.ConfirmationQueryService;
import com.tradeexception.platform.confirmation.domain.Confirmation;
import com.tradeexception.platform.confirmation.domain.ConfirmationStatus;
import com.tradeexception.platform.confirmation.repository.ConfirmationRepository;
import com.tradeexception.platform.exception.ExceptionRemediationService;
import com.tradeexception.platform.exception.api.ExceptionController;
import com.tradeexception.platform.exception.api.ExceptionQueryService;
import com.tradeexception.platform.exception.domain.ExceptionCategory;
import com.tradeexception.platform.exception.domain.ExceptionPriority;
import com.tradeexception.platform.exception.domain.ExceptionRecord;
import com.tradeexception.platform.exception.domain.ExceptionStatus;
import com.tradeexception.platform.exception.repository.ExceptionRepository;
import com.tradeexception.platform.ingestion.SyntheticTradeProducer;
import com.tradeexception.platform.ingestion.SyntheticTradeProducer.PublishResult;
import com.tradeexception.platform.ingestion.SyntheticTradeScenario;
import com.tradeexception.platform.ingestion.api.SyntheticTradeController;
import com.tradeexception.platform.trade.api.TradeController;
import com.tradeexception.platform.trade.api.TradeQueryService;
import com.tradeexception.platform.trade.domain.ProductType;
import com.tradeexception.platform.trade.domain.Trade;
import com.tradeexception.platform.trade.domain.TradeStatus;
import com.tradeexception.platform.trade.repository.TradeRepository;
import com.tradeexception.platform.workitem.WorkItemManagementService;
import com.tradeexception.platform.workitem.api.WorkItemController;
import com.tradeexception.platform.workitem.api.WorkItemQueryService;
import com.tradeexception.platform.workitem.domain.OperationalTeam;
import com.tradeexception.platform.workitem.domain.WorkItem;
import com.tradeexception.platform.workitem.domain.WorkItemStatus;
import com.tradeexception.platform.workitem.repository.WorkItemRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.patch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@ExtendWith(MockitoExtension.class)
class OperationalApiControllersTest {

    @Mock private TradeRepository tradeRepository;
    @Mock private ConfirmationRepository confirmationRepository;
    @Mock private ExceptionRepository exceptionRepository;
    @Mock private WorkItemRepository workItemRepository;
    @Mock private AuditEventRepository auditEventRepository;
    @Mock private WorkItemManagementService workItemManagementService;
    @Mock private ExceptionRemediationService exceptionRemediationService;
    @Mock private SyntheticTradeProducer syntheticTradeProducer;

    private MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        AuditQueryService auditQuery = new AuditQueryService(auditEventRepository);
        TradeQueryService tradeQuery = new TradeQueryService(tradeRepository, auditQuery);
        ConfirmationQueryService cnfQuery = new ConfirmationQueryService(confirmationRepository);
        ExceptionQueryService exQuery = new ExceptionQueryService(exceptionRepository);
        WorkItemQueryService wiQuery = new WorkItemQueryService(workItemRepository);
        DashboardSummaryService dashService = new DashboardSummaryService(
                tradeRepository, confirmationRepository, exceptionRepository, workItemRepository);

        mockMvc = MockMvcBuilders.standaloneSetup(
                        new TradeController(tradeQuery),
                        new ConfirmationController(cnfQuery),
                        new ExceptionController(exQuery, exceptionRemediationService),
                        new WorkItemController(wiQuery, workItemManagementService),
                        new AuditController(auditQuery),
                        new DashboardController(dashService),
                        new SyntheticTradeController(syntheticTradeProducer))
                .setControllerAdvice(new GlobalExceptionHandler())
                .build();
    }

    @Test
    void listsTradesWithFilteringAndReturns404ForUnknownTrade() throws Exception {
        Trade t1 = new Trade();
        t1.setTradeId("TRD-1");
        t1.setProductType(ProductType.FX_SPOT);
        t1.setStatus(TradeStatus.CONFIRMED);
        t1.setCreatedAt(Instant.now());

        Trade t2 = new Trade();
        t2.setTradeId("TRD-2");
        t2.setProductType(ProductType.FX_FORWARD);
        t2.setStatus(TradeStatus.EXCEPTION);
        t2.setCreatedAt(Instant.now());

        when(tradeRepository.findAll()).thenReturn(List.of(t1, t2));
        when(tradeRepository.findByTradeId("TRD-UNKNOWN")).thenReturn(Optional.empty());

        mockMvc.perform(get("/api/v1/trades").param("status", "CONFIRMED").param("page", "0").param("size", "10"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data.totalElements").value(1))
                .andExpect(jsonPath("$.data.items[0].tradeId").value("TRD-1"));

        mockMvc.perform(get("/api/v1/trades/TRD-UNKNOWN"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.error.code").value("DOCUMENT_NOT_FOUND"));
    }

    @Test
    void returnsPersistedConfirmationHtmlDocumentWithoutRegenerating() throws Exception {
        Confirmation c = new Confirmation();
        c.setConfirmationId("CNF-TRD-1");
        c.setTradeId("TRD-1");
        c.setCorrelationId("CORR-1");
        c.setStatus(ConfirmationStatus.GENERATED);
        c.setRenderedContent("<article>Synthetic Confirmation</article>");

        when(confirmationRepository.findById("confirmation:CNF-TRD-1")).thenReturn(Optional.of(c));

        mockMvc.perform(get("/api/v1/confirmations/CNF-TRD-1/document"))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(MediaType.TEXT_HTML))
                .andExpect(content().string("<article>Synthetic Confirmation</article>"));
    }

    @Test
    void filtersExceptionsAndHandlesWorkItemAssignmentAndInvalidTransition() throws Exception {
        ExceptionRecord ex = new ExceptionRecord();
        ex.setExceptionId("EXC-1");
        ex.setTradeId("TRD-2");
        ex.setCategory(ExceptionCategory.INVALID_COUNTERPARTY);
        ex.setPriority(ExceptionPriority.HIGH);
        ex.setStatus(ExceptionStatus.OPEN);
        ex.setClassifiedAt(Instant.now());
        when(exceptionRepository.findAll()).thenReturn(List.of(ex));

        mockMvc.perform(get("/api/v1/exceptions").param("category", "INVALID_COUNTERPARTY").param("classified", "true"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.items[0].exceptionId").value("EXC-1"));

        WorkItem updated = new WorkItem();
        updated.setWorkItemId("WI-1");
        updated.setAssignedTeam(OperationalTeam.COUNTERPARTY_OPERATIONS);
        updated.setStatus(WorkItemStatus.IN_PROGRESS);
        when(workItemManagementService.assignWorkItem(eq("WI-1"), eq(OperationalTeam.COUNTERPARTY_OPERATIONS), eq("ops-1"), any()))
                .thenReturn(updated);

        mockMvc.perform(patch("/api/v1/work-items/WI-1/assign")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"assignedTeam\":\"COUNTERPARTY_OPERATIONS\",\"assignee\":\"ops-1\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.assignedTeam").value("COUNTERPARTY_OPERATIONS"));

        when(workItemManagementService.updateStatus(eq("WI-1"), eq(WorkItemStatus.OPEN), any()))
                .thenThrow(new IllegalStateException("Invalid transition"));

        mockMvc.perform(patch("/api/v1/work-items/WI-1/status")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"status\":\"OPEN\"}"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.error.code").value("INVALID_STATE_TRANSITION"));
    }

    @Test
    void triggersSyntheticTradeScenarioAndRejectsInvalidScenario() throws Exception {
        when(syntheticTradeProducer.publishScenario(SyntheticTradeScenario.VALID_FX_SPOT))
                .thenReturn(new PublishResult("EVT-1", "TRD-1", "CORR-1", "trade.raw", SyntheticTradeScenario.VALID_FX_SPOT, 1));

        mockMvc.perform(post("/api/v1/synthetic-trades/VALID_FX_SPOT"))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.data.tradeId").value("TRD-1"))
                .andExpect(jsonPath("$.data.topic").value("trade.raw"));

        mockMvc.perform(post("/api/v1/synthetic-trades/ARBITRARY_INVALID"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error.code").value("INVALID_PARAMETER"));
    }

    @Test
    void listsReadOnlyAuditEvents() throws Exception {
        AuditEvent e = new AuditEvent();
        e.setEventId("AUD-1");
        e.setEventType(EventType.TRADE_ACCEPTED);
        e.setTradeId("TRD-1");
        e.setCorrelationId("CORR-1");
        e.setActor("system:ingestion");
        e.setOccurredAt(Instant.now());
        when(auditEventRepository.findAll()).thenReturn(List.of(e));

        mockMvc.perform(get("/api/v1/audit-events").param("correlationId", "CORR-1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.items[0].eventId").value("AUD-1"));
    }
}
