package com.tradeexception.platform.security;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tradeexception.platform.audit.api.AuditController;
import com.tradeexception.platform.audit.api.AuditQueryService;
import com.tradeexception.platform.audit.repository.AuditEventRepository;
import com.tradeexception.platform.common.api.PagedResponse;
import com.tradeexception.platform.confirmation.api.ConfirmationController;
import com.tradeexception.platform.confirmation.api.ConfirmationQueryService;
import com.tradeexception.platform.dashboard.DashboardController;
import com.tradeexception.platform.dashboard.DashboardSummaryDto;
import com.tradeexception.platform.dashboard.DashboardSummaryService;
import com.tradeexception.platform.exception.ExceptionRemediationService;
import com.tradeexception.platform.exception.api.ExceptionController;
import com.tradeexception.platform.exception.api.ExceptionQueryService;
import com.tradeexception.platform.exception.api.RemediationResultDto;
import com.tradeexception.platform.health.HealthController;
import com.tradeexception.platform.ingestion.SyntheticTradeProducer;
import com.tradeexception.platform.ingestion.SyntheticTradeProducer.PublishResult;
import com.tradeexception.platform.ingestion.SyntheticTradeScenario;
import com.tradeexception.platform.ingestion.api.SyntheticTradeController;
import com.tradeexception.platform.replay.DeadLetterManagementService;
import com.tradeexception.platform.replay.api.DeadLetterController;
import com.tradeexception.platform.replay.api.ReplayResultDto;
import com.tradeexception.platform.replay.domain.ReplayStatus;
import com.tradeexception.platform.trade.api.TradeController;
import com.tradeexception.platform.trade.api.TradeQueryService;
import com.tradeexception.platform.workitem.WorkItemManagementService;
import com.tradeexception.platform.workitem.api.WorkItemController;
import com.tradeexception.platform.workitem.api.WorkItemQueryService;
import com.tradeexception.platform.workitem.domain.OperationalTeam;
import com.tradeexception.platform.workitem.domain.WorkItem;
import com.tradeexception.platform.workitem.domain.WorkItemStatus;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.user;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.patch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(controllers = {
        HealthController.class,
        AuthController.class,
        DashboardController.class,
        TradeController.class,
        ConfirmationController.class,
        ExceptionController.class,
        WorkItemController.class,
        AuditController.class,
        SyntheticTradeController.class,
        DeadLetterController.class
})
@Import({SecurityConfig.class, HmacTokenService.class})
@TestPropertySource(properties = {
        "app.security.token-secret=test-signing-secret-for-unit-tests-only",
        "app.security.admin.username=admin",
        "app.security.admin.password=admin-test-pass",
        "app.security.operations.username=operations",
        "app.security.operations.password=ops-test-pass",
        "app.security.viewer.username=viewer",
        "app.security.viewer.password=viewer-test-pass"
})
class SecurityAuthorizationTest {

    private static final String TEST_SECRET = "test-signing-secret-for-unit-tests-only";

    @Autowired private MockMvc mockMvc;
    @Autowired private HmacTokenService tokenService;
    @Autowired private ObjectMapper objectMapper;

    @MockBean private DashboardSummaryService dashboardSummaryService;
    @MockBean private TradeQueryService tradeQueryService;
    @MockBean private ConfirmationQueryService confirmationQueryService;
    @MockBean private ExceptionQueryService exceptionQueryService;
    @MockBean private ExceptionRemediationService exceptionRemediationService;
    @MockBean private WorkItemQueryService workItemQueryService;
    @MockBean private WorkItemManagementService workItemManagementService;
    @MockBean private AuditQueryService auditQueryService;
    @MockBean private SyntheticTradeProducer syntheticTradeProducer;
    @MockBean private DeadLetterManagementService deadLetterManagementService;
    @MockBean private AuditEventRepository auditEventRepository;

    @Test
    void healthIsPublicAndAllProtectedEndpointsRequireAuthentication() throws Exception {
        mockMvc.perform(get("/api/v1/health")).andExpect(status().isOk());

        mockMvc.perform(get("/api/v1/dashboard/summary"))
                .andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.error.code").value("UNAUTHORIZED"));
        mockMvc.perform(get("/api/v1/trades")).andExpect(status().isUnauthorized());
        mockMvc.perform(get("/api/v1/confirmations")).andExpect(status().isUnauthorized());
        mockMvc.perform(get("/api/v1/exceptions")).andExpect(status().isUnauthorized());
        mockMvc.perform(get("/api/v1/work-items")).andExpect(status().isUnauthorized());
        mockMvc.perform(get("/api/v1/audit-events")).andExpect(status().isUnauthorized());
        mockMvc.perform(get("/api/v1/dlq")).andExpect(status().isUnauthorized());
        mockMvc.perform(post("/api/v1/dlq/DLQ-1/replay")).andExpect(status().isUnauthorized());
        mockMvc.perform(post("/api/v1/exceptions/EXC-1/remediate")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"notionalAmount\":1000,\"notionalCurrency\":\"USD\",\"price\":1.08}"))
                .andExpect(status().isUnauthorized());
        mockMvc.perform(patch("/api/v1/work-items/WI-1/status")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"status\":\"IN_PROGRESS\"}"))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void viewerCanReadAllOperationalEndpointsButIsForbiddenFromAllWriteAndAdminEndpoints() throws Exception {
        stubReadServices();

        mockMvc.perform(get("/api/v1/dashboard/summary").with(user("viewer").roles("VIEWER"))).andExpect(status().isOk());
        mockMvc.perform(get("/api/v1/trades").with(user("viewer").roles("VIEWER"))).andExpect(status().isOk());
        mockMvc.perform(get("/api/v1/confirmations").with(user("viewer").roles("VIEWER"))).andExpect(status().isOk());
        mockMvc.perform(get("/api/v1/exceptions").with(user("viewer").roles("VIEWER"))).andExpect(status().isOk());
        mockMvc.perform(get("/api/v1/work-items").with(user("viewer").roles("VIEWER"))).andExpect(status().isOk());
        mockMvc.perform(get("/api/v1/audit-events").with(user("viewer").roles("VIEWER"))).andExpect(status().isOk());
        mockMvc.perform(get("/api/v1/dlq").with(user("viewer").roles("VIEWER"))).andExpect(status().isOk());

        // Write operations must all return 403 FORBIDDEN for VIEWER
        mockMvc.perform(post("/api/v1/synthetic-trades/VALID_FX_SPOT").with(user("viewer").roles("VIEWER")))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.error.code").value("FORBIDDEN"));
        mockMvc.perform(post("/api/v1/dlq/DLQ-1/replay").with(user("viewer").roles("VIEWER")))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.error.code").value("FORBIDDEN"));
        mockMvc.perform(post("/api/v1/exceptions/EXC-1/remediate")
                        .with(user("viewer").roles("VIEWER"))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"notionalAmount\":1000000,\"notionalCurrency\":\"USD\",\"price\":1.08}"))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.error.code").value("FORBIDDEN"));
        mockMvc.perform(patch("/api/v1/work-items/WI-1/assign")
                        .with(user("viewer").roles("VIEWER"))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"assignedTeam\":\"TRADE_OPERATIONS\",\"assignee\":\"viewer\"}"))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.error.code").value("FORBIDDEN"));
        mockMvc.perform(patch("/api/v1/work-items/WI-1/status")
                        .with(user("viewer").roles("VIEWER"))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"status\":\"IN_PROGRESS\"}"))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.error.code").value("FORBIDDEN"));
        // Admin-only fallback route must return 403 for VIEWER
        mockMvc.perform(post("/api/v1/admin/internal-action").with(user("viewer").roles("VIEWER")))
                .andExpect(status().isForbidden());
    }

    @Test
    void operationsCanPerformOperationalWritesCannotSpoofActorAndCannotAccessAdminOnlyRoutes() throws Exception {
        mockMvc.perform(post("/api/v1/auth/login")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"username\":\"operations\",\"password\":\"ops-test-pass\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.tokenType").value("Bearer"))
                .andExpect(jsonPath("$.data.username").value("operations"));

        String opsBearer = tokenService.issue("operations", List.of("OPERATIONS")).accessToken();

        when(syntheticTradeProducer.publishScenario(SyntheticTradeScenario.VALID_FX_SPOT))
                .thenReturn(new PublishResult("EVT-1", "TRD-1", "CORR-1", "trade.raw", SyntheticTradeScenario.VALID_FX_SPOT, 1));
        when(deadLetterManagementService.replay(eq("DLQ-1"), eq("operations")))
                .thenReturn(new ReplayResultDto("DLQ-1", "trade.raw", "EVT-1", "CORR-1",
                        ReplayStatus.REPLAYED, 1, "operations", Instant.now(), "Replayed"));
        when(exceptionRemediationService.remediate(eq("EXC-1"), any(), eq("operations")))
                .thenReturn(new RemediationResultDto(null, null, null));
        WorkItem wi = new WorkItem();
        wi.setWorkItemId("WI-1");
        wi.setStatus(WorkItemStatus.IN_PROGRESS);
        wi.setAssignedTeam(OperationalTeam.TRADE_OPERATIONS);
        when(workItemManagementService.updateStatus(eq("WI-1"), eq(WorkItemStatus.IN_PROGRESS), eq("operations")))
                .thenReturn(wi);
        when(workItemManagementService.assignWorkItem(eq("WI-1"), eq(OperationalTeam.TRADE_OPERATIONS), eq("ops-1"), eq("operations")))
                .thenReturn(wi);

        mockMvc.perform(post("/api/v1/synthetic-trades/VALID_FX_SPOT")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + opsBearer))
                .andExpect(status().isAccepted());
        mockMvc.perform(post("/api/v1/dlq/DLQ-1/replay")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + opsBearer))
                .andExpect(status().isOk());
        mockMvc.perform(post("/api/v1/exceptions/EXC-1/remediate")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + opsBearer)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"notionalAmount\":1000000,\"notionalCurrency\":\"USD\",\"price\":1.08}"))
                .andExpect(status().isOk());

        // Attempt to spoof actor="admin" in body -> controller MUST enforce authenticated Principal ("operations")
        mockMvc.perform(patch("/api/v1/work-items/WI-1/status")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + opsBearer)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"status\":\"IN_PROGRESS\",\"actor\":\"admin\"}"))
                .andExpect(status().isOk());
        verify(workItemManagementService).updateStatus("WI-1", WorkItemStatus.IN_PROGRESS, "operations");

        mockMvc.perform(patch("/api/v1/work-items/WI-1/assign")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + opsBearer)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"assignedTeam\":\"TRADE_OPERATIONS\",\"assignee\":\"ops-1\",\"actor\":\"admin\"}"))
                .andExpect(status().isOk());
        verify(workItemManagementService).assignWorkItem("WI-1", OperationalTeam.TRADE_OPERATIONS, "ops-1", "operations");

        // Admin-only fallback route must return 403 for OPERATIONS
        mockMvc.perform(post("/api/v1/admin/internal-action")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + opsBearer))
                .andExpect(status().isForbidden());
    }

    @Test
    void adminHasAccessToAllOperationalEndpointsAndAdminFallbackPolicy() throws Exception {
        stubReadServices();
        String adminBearer = tokenService.issue("admin", List.of("ADMIN")).accessToken();

        mockMvc.perform(get("/api/v1/dashboard/summary")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + adminBearer))
                .andExpect(status().isOk());
        mockMvc.perform(get("/api/v1/audit-events")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + adminBearer))
                .andExpect(status().isOk());

        // Admin passes authorization on unmapped admin path (404 Not Found rather than 403 Forbidden)
        mockMvc.perform(post("/api/v1/admin/internal-action")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + adminBearer))
                .andExpect(status().isNotFound());
    }

    @Test
    void invalidTamperedAndExpiredBearerTokensAreRejectedWith401() throws Exception {
        // 1. Malformed token
        mockMvc.perform(get("/api/v1/dashboard/summary")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer not-a-valid-token"))
                .andExpect(status().isUnauthorized());

        // 2. Tampered role escalation: take valid VIEWER signature, swap payload to ADMIN
        String viewerToken = tokenService.issue("viewer", List.of("VIEWER")).accessToken();
        String signature = viewerToken.split("\\.")[1];
        HmacTokenService.TokenPayload forgedPayload = new HmacTokenService.TokenPayload(
                "viewer", List.of("ADMIN"), Instant.now().plusSeconds(3600).getEpochSecond());
        String forgedEncoded = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(objectMapper.writeValueAsBytes(forgedPayload));
        mockMvc.perform(post("/api/v1/dlq/DLQ-1/replay")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + forgedEncoded + "." + signature))
                .andExpect(status().isUnauthorized());

        // 3. Validly signed but expired token
        HmacTokenService.TokenPayload expiredPayload = new HmacTokenService.TokenPayload(
                "admin", List.of("ADMIN"), Instant.now().minusSeconds(60).getEpochSecond());
        String expiredEncoded = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(objectMapper.writeValueAsBytes(expiredPayload));
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(TEST_SECRET.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
        String expiredSig = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(mac.doFinal(expiredEncoded.getBytes(StandardCharsets.UTF_8)));
        mockMvc.perform(get("/api/v1/dashboard/summary")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + expiredEncoded + "." + expiredSig))
                .andExpect(status().isUnauthorized());
    }

    private void stubReadServices() {
        when(dashboardSummaryService.getSummary()).thenReturn(new DashboardSummaryDto(
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0.0, Map.of(), List.of(), Map.of(), Map.of()));
        when(tradeQueryService.list(any(), any(), any(), anyInt(), anyInt(), any(), any()))
                .thenReturn(PagedResponse.of(List.of(), 0, 20, "createdAt", "desc"));
        when(confirmationQueryService.list(any(), any(), anyInt(), anyInt(), any(), any()))
                .thenReturn(PagedResponse.of(List.of(), 0, 20, "generatedAt", "desc"));
        when(exceptionQueryService.list(any(), any(), any(), any(), any(), any(), any(), any(), anyInt(), anyInt(), any(), any()))
                .thenReturn(PagedResponse.of(List.of(), 0, 20, "detectedAt", "desc"));
        when(workItemQueryService.list(any(), any(), any(), any(), any(), anyInt(), anyInt(), any(), any()))
                .thenReturn(PagedResponse.of(List.of(), 0, 20, "dueAt", "asc"));
        when(auditQueryService.list(any(), any(), any(), any(), any(), any(), anyInt(), anyInt(), any(), any()))
                .thenReturn(PagedResponse.of(List.of(), 0, 25, "occurredAt", "desc"));
        when(deadLetterManagementService.list(any(), any(), any(), any(), any(), anyInt(), anyInt(), any(), any()))
                .thenReturn(PagedResponse.of(List.of(), 0, 20, "failedAt", "desc"));
    }
}
