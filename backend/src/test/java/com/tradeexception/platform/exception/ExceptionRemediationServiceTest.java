package com.tradeexception.platform.exception;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.tradeexception.platform.audit.domain.AuditEvent;
import com.tradeexception.platform.audit.domain.EventType;
import com.tradeexception.platform.audit.repository.AuditEventRepository;
import com.tradeexception.platform.classification.ExceptionClassifier;
import com.tradeexception.platform.common.kafka.KafkaProperties;
import com.tradeexception.platform.enrichment.ReferenceDataSeeder;
import com.tradeexception.platform.enrichment.TradeEnrichmentService;
import com.tradeexception.platform.enrichment.domain.ReferenceData;
import com.tradeexception.platform.enrichment.domain.ReferenceDataCategory;
import com.tradeexception.platform.enrichment.repository.ReferenceDataRepository;
import com.tradeexception.platform.exception.api.RemediateExceptionRequest;
import com.tradeexception.platform.exception.api.RemediationResultDto;
import com.tradeexception.platform.exception.domain.ExceptionCategory;
import com.tradeexception.platform.exception.domain.ExceptionPriority;
import com.tradeexception.platform.exception.domain.ExceptionRecord;
import com.tradeexception.platform.exception.domain.ExceptionStatus;
import com.tradeexception.platform.exception.repository.ExceptionRepository;
import com.tradeexception.platform.replay.DeadLetterPublisher;
import com.tradeexception.platform.stp.StpRuleEngine;
import com.tradeexception.platform.stp.domain.StpDecision;
import com.tradeexception.platform.stp.rules.CounterpartyValidationRule;
import com.tradeexception.platform.stp.rules.CurrencySupportRule;
import com.tradeexception.platform.stp.rules.DateConsistencyRule;
import com.tradeexception.platform.stp.rules.EconomicFieldsRule;
import com.tradeexception.platform.stp.rules.InstrumentAndTemplateRule;
import com.tradeexception.platform.stp.rules.MandatoryIdentifiersRule;
import com.tradeexception.platform.stp.rules.NonEconomicTermsRule;
import com.tradeexception.platform.stp.rules.ProductEligibilityRule;
import com.tradeexception.platform.trade.domain.PartyRole;
import com.tradeexception.platform.trade.domain.ProductType;
import com.tradeexception.platform.trade.domain.Trade;
import com.tradeexception.platform.trade.domain.TradeEconomics;
import com.tradeexception.platform.trade.domain.TradeMessageHeader;
import com.tradeexception.platform.trade.domain.TradeParty;
import com.tradeexception.platform.trade.domain.TradeStatus;
import com.tradeexception.platform.trade.repository.TradeRepository;
import com.tradeexception.platform.workitem.TeamRoutingStrategy;
import com.tradeexception.platform.workitem.WorkItemManagementService;
import com.tradeexception.platform.workitem.domain.WorkItem;
import com.tradeexception.platform.workitem.domain.WorkItemStatus;
import com.tradeexception.platform.workitem.repository.WorkItemRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ExceptionRemediationServiceTest {

    @Mock private ExceptionRepository exceptionRepository;
    @Mock private TradeRepository tradeRepository;
    @Mock private WorkItemRepository workItemRepository;
    @Mock private AuditEventRepository auditEventRepository;
    @Mock private ReferenceDataRepository referenceDataRepository;
    @Mock private KafkaTemplate<String, String> kafkaTemplate;
    @Mock private DeadLetterPublisher deadLetterPublisher;

    private ExceptionRemediationService remediationService;
    private WorkItemManagementService workItemService;

    @BeforeEach
    void setUp() {
        ObjectMapper mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        KafkaProperties props = new KafkaProperties();

        Map<String, ReferenceData> catalog = ReferenceDataSeeder.defaultCatalog().stream()
                .collect(Collectors.toMap(ReferenceData::getId, r -> r));
        lenient().when(referenceDataRepository.findByCategoryAndCode(any(), anyString())).thenAnswer(inv ->
                Optional.ofNullable(catalog.get("refdata:" + ((ReferenceDataCategory) inv.getArgument(0)).name() + ":" + inv.getArgument(1))));

        TradeEnrichmentService enrichmentService = new TradeEnrichmentService(
                mapper, referenceDataRepository, tradeRepository, auditEventRepository,
                kafkaTemplate, deadLetterPublisher, props);
        StpRuleEngine ruleEngine = new StpRuleEngine(List.of(
                new MandatoryIdentifiersRule(),
                new CounterpartyValidationRule(),
                new ProductEligibilityRule(),
                new CurrencySupportRule(),
                new EconomicFieldsRule(),
                new DateConsistencyRule(),
                new InstrumentAndTemplateRule(),
                new NonEconomicTermsRule()));
        workItemService = new WorkItemManagementService(
                mapper, new TeamRoutingStrategy(), workItemRepository, auditEventRepository,
                kafkaTemplate, deadLetterPublisher, props);

        remediationService = new ExceptionRemediationService(
                exceptionRepository, tradeRepository, workItemRepository, auditEventRepository,
                enrichmentService, ruleEngine, new ExceptionClassifier(), workItemService,
                kafkaTemplate, mapper, props);
    }

    private Trade fxSpotInException(BigDecimal notional, String notionalCcy, BigDecimal price) {
        Trade t = new Trade();
        t.setHeader(new TradeMessageHeader("MSG-1", "CORR-1", "1.0", "SYNTH-OMS", Instant.now()));
        t.setTradeId("TRD-REM-1");
        t.setSourceSystemTradeId("OMS-1");
        t.setProductType(ProductType.FX_SPOT);
        t.setTradeDate(LocalDate.of(2026, 11, 24));
        t.setEffectiveDate(LocalDate.of(2026, 11, 26));
        t.setParties(List.of(
                new TradeParty("SYNTHBANK00000000001", "Synthetic Bank Alpha", PartyRole.BUYER),
                new TradeParty("SYNTHCORP00000000002", "Synthetic Corp Beta", PartyRole.SELLER)));
        t.setEconomics(new TradeEconomics(notional, notionalCcy, price, null, null, "EUR"));
        t.setStatus(TradeStatus.EXCEPTION);
        return t;
    }

    private ExceptionRecord openException(String tradeId) {
        ExceptionRecord ex = new ExceptionRecord();
        ex.setExceptionId("EXC-" + tradeId);
        ex.setTradeId(tradeId);
        ex.setCorrelationId("CORR-1");
        ex.setCategory(ExceptionCategory.INVALID_ECONOMIC_TERMS);
        ex.setPriority(ExceptionPriority.CRITICAL);
        ex.setStatus(ExceptionStatus.OPEN);
        ex.setReasonCode("INVALID_ECONOMIC_TERMS");
        ex.setDetectedAt(Instant.now());
        return ex;
    }

    @Test
    void correctingMissingNotionalAndPricePassesRevalidationResolvesExceptionAndPublishesStpValidated() {
        Trade trade = fxSpotInException(null, "USD", null);
        ExceptionRecord ex = openException(trade.getTradeId());
        WorkItem wi = new WorkItem();
        wi.setWorkItemId("WI-" + ex.getExceptionId());
        wi.setExceptionId(ex.getExceptionId());
        wi.setTradeId(trade.getTradeId());
        wi.setCorrelationId("CORR-1");
        wi.setStatus(WorkItemStatus.OPEN);
        wi.setPriority(ExceptionPriority.CRITICAL);

        when(exceptionRepository.findById(ex.getId())).thenReturn(Optional.of(ex));
        when(tradeRepository.findByTradeId(trade.getTradeId())).thenReturn(Optional.of(trade));
        when(workItemRepository.findById(wi.getId())).thenReturn(Optional.of(wi));
        when(tradeRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));
        when(exceptionRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));
        when(workItemRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        RemediationResultDto res = remediationService.remediate(
                ex.getExceptionId(),
                new RemediateExceptionRequest(new BigDecimal("1500000.00"), "USD", new BigDecimal("1.0845"), null, null, "EUR", "Added missing notional and price"),
                "operations");

        assertThat(res.validationResult().decision()).isEqualTo(StpDecision.STP_ELIGIBLE);
        assertThat(res.trade().status()).isEqualTo(TradeStatus.VALIDATED);
        assertThat(res.exception().status()).isEqualTo(ExceptionStatus.RESOLVED);
        assertThat(res.exception().resolvedBy()).isEqualTo("operations");
        assertThat(res.exception().resolutionReason()).isEqualTo("Added missing notional and price");
        assertThat(res.exception().revalidationHistory()).hasSize(1);
        assertThat(wi.getStatus()).isEqualTo(WorkItemStatus.RESOLVED);

        verify(kafkaTemplate, times(1)).send(eq("stp.validated"), eq(trade.getTradeId()), anyString());

        ArgumentCaptor<AuditEvent> audits = ArgumentCaptor.forClass(AuditEvent.class);
        verify(auditEventRepository, times(4)).save(audits.capture());
        assertThat(audits.getAllValues()).extracting(AuditEvent::getEventType)
                .contains(EventType.EXCEPTION_REMEDIATION_SUBMITTED, EventType.WORKITEM_RESOLVED,
                        EventType.EXCEPTION_REVALIDATION_SUCCEEDED, EventType.EXCEPTION_RESOLVED);
    }

    @Test
    void correctingMissingCurrencyPassesRevalidation() {
        Trade trade = fxSpotInException(new BigDecimal("500000.00"), "XXX", new BigDecimal("1.08"));
        ExceptionRecord ex = openException(trade.getTradeId());

        when(exceptionRepository.findById(ex.getId())).thenReturn(Optional.of(ex));
        when(tradeRepository.findByTradeId(trade.getTradeId())).thenReturn(Optional.of(trade));
        when(workItemRepository.findById(anyString())).thenReturn(Optional.empty());
        when(tradeRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));
        when(exceptionRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        RemediationResultDto res = remediationService.remediate(
                ex.getExceptionId(),
                new RemediateExceptionRequest(null, "USD", null, null, null, null, "Fixed currency"),
                "admin");

        assertThat(res.validationResult().decision()).isEqualTo(StpDecision.STP_ELIGIBLE);
        assertThat(res.exception().status()).isEqualTo(ExceptionStatus.RESOLVED);
    }

    @Test
    void failedRevalidationKeepsTradeInExceptionAndExceptionOpenWithPreservedHistory() {
        // Still missing price for FX_SPOT -> fails EconomicFieldsRule
        Trade trade = fxSpotInException(null, "USD", null);
        ExceptionRecord ex = openException(trade.getTradeId());

        when(exceptionRepository.findById(ex.getId())).thenReturn(Optional.of(ex));
        when(tradeRepository.findByTradeId(trade.getTradeId())).thenReturn(Optional.of(trade));
        when(tradeRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));
        when(exceptionRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        RemediationResultDto res = remediationService.remediate(
                ex.getExceptionId(),
                new RemediateExceptionRequest(new BigDecimal("1000000.00"), "USD", null, null, null, null, "Partial fix"),
                "operations");

        assertThat(res.validationResult().decision()).isEqualTo(StpDecision.STP_REJECTED);
        assertThat(res.trade().status()).isEqualTo(TradeStatus.EXCEPTION);
        assertThat(res.exception().status()).isEqualTo(ExceptionStatus.OPEN);
        assertThat(res.exception().resolvedAt()).isNull();
        assertThat(res.exception().revalidationHistory()).hasSize(1);

        verify(kafkaTemplate, never()).send(eq("stp.validated"), anyString(), anyString());

        ArgumentCaptor<AuditEvent> audits = ArgumentCaptor.forClass(AuditEvent.class);
        verify(auditEventRepository, times(2)).save(audits.capture());
        assertThat(audits.getAllValues()).extracting(AuditEvent::getEventType)
                .containsExactly(EventType.EXCEPTION_REMEDIATION_SUBMITTED, EventType.EXCEPTION_REVALIDATION_FAILED);
    }

    @Test
    void duplicateRevalidationOnResolvedExceptionIsRejectedAndWorkItemResolutionAloneDoesNotResolveException() {
        ExceptionRecord open = openException("TRD-WI-ONLY");
        WorkItem wi = new WorkItem();
        wi.setWorkItemId("WI-EXC-TRD-WI-ONLY");
        wi.setExceptionId(open.getExceptionId());
        wi.setTradeId("TRD-WI-ONLY");
        wi.setCorrelationId("CORR-1");
        wi.setStatus(WorkItemStatus.OPEN);
        when(workItemRepository.findById(wi.getId())).thenReturn(Optional.of(wi));
        when(workItemRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        // Resolving WorkItem alone must NOT touch ExceptionRecord
        workItemService.updateStatus(wi.getWorkItemId(), WorkItemStatus.RESOLVED, "operations");
        assertThat(wi.getStatus()).isEqualTo(WorkItemStatus.RESOLVED);
        assertThat(open.getStatus()).isEqualTo(ExceptionStatus.OPEN);
        verify(exceptionRepository, never()).save(any());

        // Duplicate revalidation on already-RESOLVED exception throws IllegalStateException (HTTP 409)
        open.setStatus(ExceptionStatus.RESOLVED);
        when(exceptionRepository.findById(open.getId())).thenReturn(Optional.of(open));
        assertThatThrownBy(() -> remediationService.remediate(
                open.getExceptionId(),
                new RemediateExceptionRequest(new BigDecimal("100"), "USD", new BigDecimal("1.0"), null, null, null, null),
                "operations"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already RESOLVED");
    }
}
