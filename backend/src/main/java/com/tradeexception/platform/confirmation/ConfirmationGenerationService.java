package com.tradeexception.platform.confirmation;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tradeexception.platform.audit.domain.AuditEvent;
import com.tradeexception.platform.audit.domain.EventType;
import com.tradeexception.platform.audit.repository.AuditEventRepository;
import com.tradeexception.platform.common.couchdb.CouchDbConflictException;
import com.tradeexception.platform.common.kafka.EventEnvelope;
import com.tradeexception.platform.common.kafka.KafkaProperties;
import com.tradeexception.platform.confirmation.domain.Confirmation;
import com.tradeexception.platform.confirmation.domain.ConfirmationStatus;
import com.tradeexception.platform.confirmation.repository.ConfirmationRepository;
import com.tradeexception.platform.enrichment.domain.ReferenceData;
import com.tradeexception.platform.enrichment.domain.ReferenceDataCategory;
import com.tradeexception.platform.enrichment.repository.ReferenceDataRepository;
import com.tradeexception.platform.exception.domain.ExceptionCategory;
import com.tradeexception.platform.exception.domain.ExceptionPriority;
import com.tradeexception.platform.exception.domain.ExceptionRecord;
import com.tradeexception.platform.exception.domain.ExceptionStatus;
import com.tradeexception.platform.exception.repository.ExceptionRepository;
import com.tradeexception.platform.replay.DeadLetterPublisher;
import com.tradeexception.platform.replay.DeadLetterRecord.DeadLetterErrorCategory;
import com.tradeexception.platform.stp.domain.StpDecision;
import com.tradeexception.platform.stp.domain.StpValidationResult;
import com.tradeexception.platform.trade.domain.Trade;
import com.tradeexception.platform.trade.domain.TradeParty;
import com.tradeexception.platform.trade.domain.TradeStatus;
import com.tradeexception.platform.trade.repository.TradeRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Generates structured trade confirmations for {@code STP_ELIGIBLE} trades arriving on
 * {@code stp.validated}, persists them in CouchDB, updates trade status to {@code CONFIRMED},
 * records an immutable audit event, and publishes {@code confirmation.generated}.
 */
@Service
public class ConfirmationGenerationService {

    private static final Logger log = LoggerFactory.getLogger(ConfirmationGenerationService.class);
    private static final TypeReference<EventEnvelope<StpValidationResult>> STP_ENVELOPE_TYPE = new TypeReference<>() {};
    private static final String SCHEMA_VERSION = "1.0";
    private static final String SOURCE = "confirmation-generation";
    private static final String DEFAULT_TEMPLATE_VERSION = "1.0";

    private final ObjectMapper objectMapper;
    private final TradeRepository tradeRepository;
    private final ReferenceDataRepository referenceDataRepository;
    private final ConfirmationRepository confirmationRepository;
    private final ExceptionRepository exceptionRepository;
    private final AuditEventRepository auditEventRepository;
    private final ConfirmationTemplateRenderer templateRenderer;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final DeadLetterPublisher deadLetterPublisher;
    private final KafkaProperties kafkaProperties;

    public ConfirmationGenerationService(
            ObjectMapper objectMapper,
            TradeRepository tradeRepository,
            ReferenceDataRepository referenceDataRepository,
            ConfirmationRepository confirmationRepository,
            ExceptionRepository exceptionRepository,
            AuditEventRepository auditEventRepository,
            ConfirmationTemplateRenderer templateRenderer,
            KafkaTemplate<String, String> kafkaTemplate,
            DeadLetterPublisher deadLetterPublisher,
            KafkaProperties kafkaProperties) {
        this.objectMapper = objectMapper;
        this.tradeRepository = tradeRepository;
        this.referenceDataRepository = referenceDataRepository;
        this.confirmationRepository = confirmationRepository;
        this.exceptionRepository = exceptionRepository;
        this.auditEventRepository = auditEventRepository;
        this.templateRenderer = templateRenderer;
        this.kafkaTemplate = kafkaTemplate;
        this.deadLetterPublisher = deadLetterPublisher;
        this.kafkaProperties = kafkaProperties;
    }

    public ConfirmationOutcome processValidatedMessage(
            String rawPayload,
            String key,
            String topic,
            int partition,
            long offset) {

        EventEnvelope<StpValidationResult> envelope;
        try {
            envelope = objectMapper.readValue(rawPayload, STP_ENVELOPE_TYPE);
        } catch (JsonProcessingException ex) {
            deadLetterPublisher.publish(
                    topic, partition, offset, key, null,
                    DeadLetterErrorCategory.DESERIALIZATION_ERROR,
                    "Malformed StpValidated JSON: " + ex.getOriginalMessage(),
                    List.of(),
                    rawPayload);
            return ConfirmationOutcome.DEAD_LETTERED;
        }

        if (envelope == null || envelope.payload() == null || envelope.payload().tradeId() == null) {
            deadLetterPublisher.publish(
                    topic, partition, offset, key,
                    envelope == null ? null : envelope.correlationId(),
                    DeadLetterErrorCategory.VALIDATION_ERROR,
                    "StpValidated envelope missing payload or tradeId",
                    List.of(),
                    rawPayload);
            return ConfirmationOutcome.DEAD_LETTERED;
        }

        StpValidationResult result = envelope.payload();
        if (result.decision() != StpDecision.STP_ELIGIBLE) {
            log.info("Skipping confirmation generation for non-eligible decision [tradeId={}, decision={}]",
                    result.tradeId(), result.decision());
            return ConfirmationOutcome.SKIPPED_NOT_ELIGIBLE;
        }

        String tradeId = result.tradeId();
        String confirmationId = "CNF-" + tradeId;
        String docId = Confirmation.TYPE + ":" + confirmationId;

        // Idempotency: never overwrite an existing confirmation
        if (confirmationRepository.findById(docId).isPresent()) {
            log.info("Duplicate confirmation request ignored [confirmationId={}, tradeId={}]", confirmationId, tradeId);
            return ConfirmationOutcome.DUPLICATE_IGNORED;
        }

        Optional<Trade> tradeOpt = tradeRepository.findByTradeId(tradeId);
        if (tradeOpt.isEmpty()) {
            deadLetterPublisher.publish(
                    topic, partition, offset, key, envelope.correlationId(),
                    DeadLetterErrorCategory.VALIDATION_ERROR,
                    "Trade '" + tradeId + "' not found in CouchDB for confirmation generation",
                    List.of(),
                    rawPayload);
            return ConfirmationOutcome.MISSING_TRADE;
        }
        Trade trade = tradeOpt.get();

        // Look up product config & confirmation template
        Optional<ReferenceData> productRef = referenceDataRepository
                .findByCategoryAndCode(ReferenceDataCategory.PRODUCT, trade.getProductType().name())
                .filter(ReferenceData::isActive);
        String templateId = productRef
                .map(ReferenceData::getAttributes)
                .map(attrs -> attrs.get("confirmationTemplateId"))
                .orElse(null);

        Optional<ReferenceData> templateRef = (templateId == null || templateId.isBlank())
                ? Optional.empty()
                : referenceDataRepository.findByCategoryAndCode(ReferenceDataCategory.CONFIRMATION_TEMPLATE, templateId)
                        .filter(ReferenceData::isActive)
                        .filter(r -> r.getAttributes() != null && "true".equalsIgnoreCase(r.getAttributes().get("available")));

        if (templateRef.isEmpty()) {
            routeMissingTemplateException(trade, envelope.correlationId(), templateId);
            return ConfirmationOutcome.TEMPLATE_UNAVAILABLE;
        }

        String templateVersion = Optional.ofNullable(templateRef.get().getAttributes())
                .map(attrs -> attrs.getOrDefault("version", DEFAULT_TEMPLATE_VERSION))
                .orElse(DEFAULT_TEMPLATE_VERSION);

        List<TradeParty> enrichedParties = resolveCounterparties(trade);

        Confirmation confirmation = new Confirmation();
        confirmation.setConfirmationId(confirmationId);
        confirmation.setTradeId(tradeId);
        confirmation.setCorrelationId(envelope.correlationId());
        confirmation.setProductType(trade.getProductType());
        confirmation.setTradeDate(trade.getTradeDate());
        confirmation.setEffectiveDate(trade.getEffectiveDate());
        confirmation.setMaturityDate(trade.getMaturityDate());
        confirmation.setCounterparties(enrichedParties);
        if (trade.getEconomics() != null) {
            confirmation.setNotionalAmount(trade.getEconomics().notionalAmount());
            confirmation.setCurrency(trade.getEconomics().notionalCurrency());
            confirmation.setEconomicTerms(trade.getEconomics());
        }
        confirmation.setNonEconomicTerms(trade.getNonEconomicTerms());
        confirmation.setStatus(ConfirmationStatus.GENERATED);
        confirmation.setGeneratedAt(Instant.now());
        confirmation.setTemplateId(templateId);
        confirmation.setTemplateVersion(templateVersion);
        confirmation.setRenderedContent(templateRenderer.renderHtml(confirmation));

        Confirmation saved;
        try {
            saved = confirmationRepository.save(confirmation);
        } catch (CouchDbConflictException ex) {
            log.info("Concurrent confirmation generation for '{}' (HTTP 409); ignoring duplicate", confirmationId);
            return ConfirmationOutcome.DUPLICATE_IGNORED;
        }

        trade.setStatus(TradeStatus.CONFIRMED);
        try {
            tradeRepository.save(trade);
        } catch (CouchDbConflictException ex) {
            log.warn("Revision conflict updating trade '{}' to CONFIRMED after confirmation save", tradeId);
        }

        recordAudit(envelope, saved);
        publishConfirmationGenerated(envelope.correlationId(), saved);

        log.info("Generated confirmation [confirmationId={}, tradeId={}, templateId={}, correlationId={}]",
                saved.getConfirmationId(), tradeId, templateId, envelope.correlationId());
        return ConfirmationOutcome.GENERATED;
    }

    private List<TradeParty> resolveCounterparties(Trade trade) {
        if (trade.getParties() == null) {
            return List.of();
        }
        return trade.getParties().stream()
                .map(p -> referenceDataRepository.findByCategoryAndCode(ReferenceDataCategory.COUNTERPARTY, p.partyId())
                        .filter(ReferenceData::isActive)
                        .map(ref -> new TradeParty(p.partyId(), ref.getDescription(), p.role()))
                        .orElse(p))
                .toList();
    }

    private void routeMissingTemplateException(Trade trade, String correlationId, String templateId) {
        String exceptionId = "EXC-CNF-" + trade.getTradeId();
        String docId = ExceptionRecord.TYPE + ":" + exceptionId;

        ExceptionRecord record = exceptionRepository.findById(docId).orElseGet(() -> {
            ExceptionRecord ex = new ExceptionRecord();
            ex.setExceptionId(exceptionId);
            ex.setTradeId(trade.getTradeId());
            ex.setCorrelationId(correlationId);
            ex.setCategory(ExceptionCategory.UNCLASSIFIED);
            ex.setPriority(ExceptionPriority.MEDIUM);
            ex.setStatus(ExceptionStatus.OPEN);
            ex.setReasonCode("CONFIRMATION_TEMPLATE_MISSING");
            ex.setDescription("Confirmation template '" + templateId + "' is missing or unavailable for product " + trade.getProductType());
            ex.setDetectedAt(Instant.now());
            try {
                return exceptionRepository.save(ex);
            } catch (CouchDbConflictException conflict) {
                return exceptionRepository.findById(docId).orElse(ex);
            }
        });

        trade.setStatus(TradeStatus.EXCEPTION);
        try {
            tradeRepository.save(trade);
        } catch (CouchDbConflictException ignored) {
            // Trade status already updated concurrently
        }

        EventEnvelope<ExceptionRecord> exEvent = EventEnvelope.of(
                "ExceptionCreated", correlationId, SCHEMA_VERSION, SOURCE, record);
        try {
            kafkaTemplate.send(
                    kafkaProperties.getTopics().getExceptionCreated(),
                    trade.getTradeId(),
                    objectMapper.writeValueAsString(exEvent));
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Failed to serialize missing-template exception", ex);
        }
    }

    private void recordAudit(EventEnvelope<StpValidationResult> inbound, Confirmation confirmation) {
        AuditEvent audit = new AuditEvent();
        audit.setEventId("cnf-" + inbound.eventId());
        audit.setEventType(EventType.CONFIRMATION_GENERATED);
        audit.setTradeId(confirmation.getTradeId());
        audit.setEntityId(confirmation.getConfirmationId());
        audit.setEntityType(Confirmation.TYPE);
        audit.setCorrelationId(inbound.correlationId());
        audit.setActor("system:confirmation-generation");
        audit.setOccurredAt(Instant.now());
        audit.setDetails(Map.of(
                "confirmationId", confirmation.getConfirmationId(),
                "templateId", confirmation.getTemplateId(),
                "templateVersion", confirmation.getTemplateVersion()));
        try {
            auditEventRepository.save(audit);
        } catch (CouchDbConflictException ex) {
            log.debug("Audit event '{}' already exists; skipping duplicate", audit.getEventId());
        }
    }

    private void publishConfirmationGenerated(String correlationId, Confirmation saved) {
        EventEnvelope<Confirmation> outbound = EventEnvelope.of(
                "ConfirmationGenerated", correlationId, SCHEMA_VERSION, SOURCE, saved);
        try {
            kafkaTemplate.send(
                    kafkaProperties.getTopics().getConfirmationGenerated(),
                    saved.getTradeId(),
                    objectMapper.writeValueAsString(outbound));
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Failed to serialize ConfirmationGenerated for " + saved.getConfirmationId(), ex);
        }
    }

    public enum ConfirmationOutcome {
        GENERATED,
        DUPLICATE_IGNORED,
        SKIPPED_NOT_ELIGIBLE,
        MISSING_TRADE,
        TEMPLATE_UNAVAILABLE,
        DEAD_LETTERED
    }
}
