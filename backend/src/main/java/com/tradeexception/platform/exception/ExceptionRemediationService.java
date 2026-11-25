package com.tradeexception.platform.exception;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tradeexception.platform.audit.domain.AuditEvent;
import com.tradeexception.platform.audit.domain.EventType;
import com.tradeexception.platform.audit.repository.AuditEventRepository;
import com.tradeexception.platform.classification.ExceptionClassifier;
import com.tradeexception.platform.classification.ExceptionClassifier.ClassificationResult;
import com.tradeexception.platform.common.couchdb.CouchDbException;
import com.tradeexception.platform.common.couchdb.CouchDbNotFoundException;
import com.tradeexception.platform.common.kafka.EventEnvelope;
import com.tradeexception.platform.common.kafka.KafkaProperties;
import com.tradeexception.platform.enrichment.TradeEnrichmentService;
import com.tradeexception.platform.enrichment.domain.EnrichedTrade;
import com.tradeexception.platform.exception.api.ExceptionDto;
import com.tradeexception.platform.exception.api.RemediateExceptionRequest;
import com.tradeexception.platform.exception.api.RemediationResultDto;
import com.tradeexception.platform.exception.domain.ExceptionRecord;
import com.tradeexception.platform.exception.domain.ExceptionStatus;
import com.tradeexception.platform.exception.domain.RevalidationAttempt;
import com.tradeexception.platform.exception.repository.ExceptionRepository;
import com.tradeexception.platform.stp.StpRuleEngine;
import com.tradeexception.platform.stp.domain.StpDecision;
import com.tradeexception.platform.stp.domain.StpValidationResult;
import com.tradeexception.platform.trade.api.TradeDto;
import com.tradeexception.platform.trade.domain.Trade;
import com.tradeexception.platform.trade.domain.TradeEconomics;
import com.tradeexception.platform.trade.domain.TradeStatus;
import com.tradeexception.platform.trade.repository.TradeRepository;
import com.tradeexception.platform.workitem.WorkItemManagementService;
import com.tradeexception.platform.workitem.domain.WorkItem;
import com.tradeexception.platform.workitem.domain.WorkItemStatus;
import com.tradeexception.platform.workitem.repository.WorkItemRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Orchestrates authorized trade remediation and STP revalidation for an open exception.
 * <p>
 * Reuses the existing {@link TradeEnrichmentService}, {@link StpRuleEngine},
 * {@link ExceptionClassifier}, {@link WorkItemManagementService}, and Kafka {@code stp.validated}
 * confirmation pipeline — never bypassing STP rules or resolving exceptions manually.
 */
@Service
public class ExceptionRemediationService {

    private static final Logger log = LoggerFactory.getLogger(ExceptionRemediationService.class);
    private static final String SCHEMA_VERSION = "1.0";
    private static final String SOURCE = "exception-remediation";

    private final ExceptionRepository exceptionRepository;
    private final TradeRepository tradeRepository;
    private final WorkItemRepository workItemRepository;
    private final AuditEventRepository auditEventRepository;
    private final TradeEnrichmentService enrichmentService;
    private final StpRuleEngine stpRuleEngine;
    private final ExceptionClassifier exceptionClassifier;
    private final WorkItemManagementService workItemManagementService;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final KafkaProperties kafkaProperties;

    public ExceptionRemediationService(
            ExceptionRepository exceptionRepository,
            TradeRepository tradeRepository,
            WorkItemRepository workItemRepository,
            AuditEventRepository auditEventRepository,
            TradeEnrichmentService enrichmentService,
            StpRuleEngine stpRuleEngine,
            ExceptionClassifier exceptionClassifier,
            WorkItemManagementService workItemManagementService,
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper,
            KafkaProperties kafkaProperties) {
        this.exceptionRepository = exceptionRepository;
        this.tradeRepository = tradeRepository;
        this.workItemRepository = workItemRepository;
        this.auditEventRepository = auditEventRepository;
        this.enrichmentService = enrichmentService;
        this.stpRuleEngine = stpRuleEngine;
        this.exceptionClassifier = exceptionClassifier;
        this.workItemManagementService = workItemManagementService;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.kafkaProperties = kafkaProperties;
    }

    public RemediationResultDto remediate(String exceptionId, RemediateExceptionRequest request, String operator) {
        String effectiveOperator = (operator == null || operator.isBlank()) ? "ops-user" : operator;
        String exDocId = ExceptionRecord.TYPE + ":" + exceptionId;

        ExceptionRecord exception = exceptionRepository.findById(exDocId)
                .orElseThrow(() -> new CouchDbNotFoundException("exceptions", exDocId, null));

        if (exception.getStatus() == ExceptionStatus.RESOLVED || exception.getStatus() == ExceptionStatus.CLOSED) {
            throw new IllegalStateException("Exception '" + exceptionId + "' is already " + exception.getStatus());
        }

        String tradeId = exception.getTradeId();
        Trade trade = tradeRepository.findByTradeId(tradeId)
                .orElseThrow(() -> new CouchDbNotFoundException("trades", Trade.documentId(tradeId), null));

        if (trade.getStatus() == TradeStatus.CONFIRMED || trade.getStatus() == TradeStatus.VALIDATED) {
            throw new IllegalStateException("Trade '" + trade.getTradeId() + "' is already " + trade.getStatus());
        }

        // Apply corrected economic fields while preserving unspecified terms
        TradeEconomics current = trade.getEconomics();
        BigDecimal notional = request.notionalAmount() != null
                ? request.notionalAmount()
                : (current == null ? null : current.notionalAmount());
        String notionalCcy = request.notionalCurrency() != null
                ? request.notionalCurrency()
                : (current == null ? null : current.notionalCurrency());
        BigDecimal price = request.price() != null
                ? request.price()
                : (current == null ? null : current.price());
        BigDecimal fixedRate = request.fixedRate() != null
                ? request.fixedRate()
                : (current == null ? null : current.fixedRate());
        String floatingIndex = request.floatingRateIndex() != null
                ? request.floatingRateIndex()
                : (current == null ? null : current.floatingRateIndex());
        String settleCcy = request.settlementCurrency() != null
                ? request.settlementCurrency()
                : (current == null ? null : current.settlementCurrency());

        trade.setEconomics(new TradeEconomics(notional, notionalCcy, price, fixedRate, floatingIndex, settleCcy));
        trade = tradeRepository.save(trade);

        recordAudit(EventType.EXCEPTION_REMEDIATION_SUBMITTED, exception, effectiveOperator,
                Map.of("tradeId", trade.getTradeId(), "note", request.remediationNote() == null ? "" : request.remediationNote()));

        // Re-run existing Enrichment + STP Validation Engine (no parallel validation engine)
        EnrichedTrade enriched = enrichmentService.enrich(trade, exception.getCorrelationId());
        StpValidationResult result = stpRuleEngine.validate(enriched);

        int attemptNo = (exception.getRevalidationHistory() == null ? 0 : exception.getRevalidationHistory().size()) + 1;
        RevalidationAttempt attempt = new RevalidationAttempt(
                attemptNo,
                result.validatedAt(),
                effectiveOperator,
                result.decision().name(),
                result.failedRules(),
                result.reasonCodes(),
                result.explanation());

        List<RevalidationAttempt> updatedHistory = new ArrayList<>(
                exception.getRevalidationHistory() == null ? List.of() : exception.getRevalidationHistory());
        updatedHistory.add(attempt);
        exception.setRevalidationHistory(updatedHistory);
        exception.setLastRevalidatedAt(result.validatedAt());
        exception.setLastRevalidationOutcome(result.decision().name());

        if (result.decision() == StpDecision.STP_ELIGIBLE) {
            Instant resolvedAt = Instant.now();
            trade.setStatus(TradeStatus.VALIDATED);
            trade = tradeRepository.save(trade);

            String reason = (request.remediationNote() != null && !request.remediationNote().isBlank())
                    ? request.remediationNote().trim()
                    : "Trade economics corrected and passed STP revalidation";
            exception.setStatus(ExceptionStatus.RESOLVED);
            exception.setResolvedAt(resolvedAt);
            exception.setResolvedBy(effectiveOperator);
            exception.setResolutionReason(reason);
            exception = exceptionRepository.save(exception);

            resolveLinkedWorkItemIfOpen(exception.getExceptionId(), effectiveOperator);

            recordAudit(EventType.EXCEPTION_REVALIDATION_SUCCEEDED, exception, effectiveOperator,
                    Map.of("attempt", String.valueOf(attemptNo), "decision", result.decision().name()));
            recordAudit(EventType.EXCEPTION_RESOLVED, exception, effectiveOperator,
                    Map.of("resolutionReason", reason));

            // Publish to stp.validated so ConfirmationGenerationConsumer generates the confirmation idempotently
            publishJson(kafkaProperties.getTopics().getStpValidated(), trade.getTradeId(),
                    EventEnvelope.of("StpValidated", exception.getCorrelationId(), SCHEMA_VERSION, SOURCE, result));

            log.info("Exception '{}' resolved after STP revalidation [tradeId={}, operator={}]",
                    exceptionId, trade.getTradeId(), effectiveOperator);
            return new RemediationResultDto(ExceptionDto.from(exception), TradeDto.from(trade), result);
        }

        // Revalidation failed: keep trade in EXCEPTION and exception OPEN, preserving history
        trade.setStatus(TradeStatus.EXCEPTION);
        trade = tradeRepository.save(trade);

        exception.setStatus(ExceptionStatus.OPEN);
        if (!result.reasonCodes().isEmpty()) {
            exception.setReasonCode(result.reasonCodes().getFirst());
        }
        exception.setDescription(result.explanation());
        ClassificationResult reclassified = exceptionClassifier.classify(exception);
        exception.setCategory(reclassified.category());
        exception.setRootCause(reclassified.rootCause());
        exception.setExplanation(reclassified.explanation());
        exception.setPriority(reclassified.priority());
        exception.setSeverity(reclassified.severity());
        exception.setRecommendedAction(reclassified.recommendedAction());
        exception.setClassifiedAt(reclassified.classifiedAt());
        exception = exceptionRepository.save(exception);

        recordAudit(EventType.EXCEPTION_REVALIDATION_FAILED, exception, effectiveOperator,
                Map.of("attempt", String.valueOf(attemptNo), "decision", result.decision().name(),
                        "reasonCodes", String.join(",", result.reasonCodes())));

        publishJson(kafkaProperties.getTopics().getExceptionClassified(), trade.getTradeId(),
                EventEnvelope.of("ExceptionClassified", exception.getCorrelationId(), SCHEMA_VERSION, SOURCE, exception));

        log.info("STP revalidation failed for exception '{}' [tradeId={}, decision={}, reasons={}]",
                exceptionId, trade.getTradeId(), result.decision(), result.reasonCodes());
        return new RemediationResultDto(ExceptionDto.from(exception), TradeDto.from(trade), result);
    }

    private void resolveLinkedWorkItemIfOpen(String exceptionId, String operator) {
        String wiId = "WI-" + exceptionId;
        Optional<WorkItem> wiOpt = workItemRepository.findById(WorkItem.TYPE + ":" + wiId);
        if (wiOpt.isPresent()) {
            WorkItemStatus status = wiOpt.get().getStatus();
            if (status != WorkItemStatus.RESOLVED && status != WorkItemStatus.CLOSED && status != WorkItemStatus.CANCELLED) {
                workItemManagementService.updateStatus(wiId, WorkItemStatus.RESOLVED, operator);
            }
        }
    }

    private void recordAudit(EventType eventType, ExceptionRecord exception, String actor, Map<String, String> details) {
        AuditEvent audit = new AuditEvent();
        audit.setEventId("rem-" + UUID.randomUUID());
        audit.setEventType(eventType);
        audit.setTradeId(exception.getTradeId());
        audit.setExceptionId(exception.getExceptionId());
        audit.setEntityId(exception.getExceptionId());
        audit.setEntityType(ExceptionRecord.TYPE);
        audit.setCorrelationId(exception.getCorrelationId());
        audit.setActor(actor);
        audit.setOccurredAt(Instant.now());
        audit.setDetails(details);
        try {
            auditEventRepository.save(audit);
        } catch (CouchDbException ex) {
            log.debug("Audit write skipped for {}: {}", eventType, ex.getMessage());
        }
    }

    private void publishJson(String topic, String key, Object payload) {
        try {
            kafkaTemplate.send(topic, key, objectMapper.writeValueAsString(payload));
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Failed to serialize remediation event for topic " + topic, ex);
        }
    }
}
