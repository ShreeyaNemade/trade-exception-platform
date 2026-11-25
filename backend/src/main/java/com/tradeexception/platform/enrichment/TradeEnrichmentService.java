package com.tradeexception.platform.enrichment;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tradeexception.platform.audit.domain.AuditEvent;
import com.tradeexception.platform.audit.domain.EventType;
import com.tradeexception.platform.audit.repository.AuditEventRepository;
import com.tradeexception.platform.common.couchdb.CouchDbConflictException;
import com.tradeexception.platform.common.kafka.EventEnvelope;
import com.tradeexception.platform.common.kafka.KafkaProperties;
import com.tradeexception.platform.enrichment.domain.EnrichedField;
import com.tradeexception.platform.enrichment.domain.EnrichedTrade;
import com.tradeexception.platform.enrichment.domain.EnrichmentFailure;
import com.tradeexception.platform.enrichment.domain.EnrichmentOrigin;
import com.tradeexception.platform.enrichment.domain.ReferenceData;
import com.tradeexception.platform.enrichment.domain.ReferenceDataCategory;
import com.tradeexception.platform.enrichment.repository.ReferenceDataRepository;
import com.tradeexception.platform.replay.DeadLetterPublisher;
import com.tradeexception.platform.replay.DeadLetterRecord.DeadLetterErrorCategory;
import com.tradeexception.platform.trade.domain.PartyRole;
import com.tradeexception.platform.trade.domain.ProductType;
import com.tradeexception.platform.trade.domain.Trade;
import com.tradeexception.platform.trade.domain.TradeParty;
import com.tradeexception.platform.trade.domain.TradeStatus;
import com.tradeexception.platform.trade.repository.TradeRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Enriches accepted trades with counterparty, instrument, currency, product, and confirmation
 * template reference data, recording field-level provenance without inventing missing financial data.
 */
@Service
public class TradeEnrichmentService {

    private static final Logger log = LoggerFactory.getLogger(TradeEnrichmentService.class);
    private static final TypeReference<EventEnvelope<Trade>> TRADE_ENVELOPE_TYPE = new TypeReference<>() {};
    private static final String SCHEMA_VERSION = "1.0";
    private static final String SOURCE = "trade-enrichment";

    private final ObjectMapper objectMapper;
    private final ReferenceDataRepository referenceDataRepository;
    private final TradeRepository tradeRepository;
    private final AuditEventRepository auditEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final DeadLetterPublisher deadLetterPublisher;
    private final KafkaProperties kafkaProperties;

    public TradeEnrichmentService(
            ObjectMapper objectMapper,
            ReferenceDataRepository referenceDataRepository,
            TradeRepository tradeRepository,
            AuditEventRepository auditEventRepository,
            KafkaTemplate<String, String> kafkaTemplate,
            DeadLetterPublisher deadLetterPublisher,
            KafkaProperties kafkaProperties) {
        this.objectMapper = objectMapper;
        this.referenceDataRepository = referenceDataRepository;
        this.tradeRepository = tradeRepository;
        this.auditEventRepository = auditEventRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.deadLetterPublisher = deadLetterPublisher;
        this.kafkaProperties = kafkaProperties;
    }

    public EnrichmentOutcome processAcceptedMessage(
            String rawPayload,
            String key,
            String topic,
            int partition,
            long offset) {

        EventEnvelope<Trade> envelope;
        try {
            envelope = objectMapper.readValue(rawPayload, TRADE_ENVELOPE_TYPE);
        } catch (JsonProcessingException ex) {
            deadLetterPublisher.publish(
                    topic, partition, offset, key, null,
                    DeadLetterErrorCategory.DESERIALIZATION_ERROR,
                    "Malformed TradeAccepted JSON: " + ex.getOriginalMessage(),
                    List.of(),
                    rawPayload);
            return EnrichmentOutcome.DEAD_LETTERED;
        }

        if (envelope == null || envelope.payload() == null || envelope.payload().getTradeId() == null) {
            deadLetterPublisher.publish(
                    topic, partition, offset, key,
                    envelope == null ? null : envelope.correlationId(),
                    DeadLetterErrorCategory.VALIDATION_ERROR,
                    "TradeAccepted envelope missing trade payload or tradeId",
                    List.of(),
                    rawPayload);
            return EnrichmentOutcome.DEAD_LETTERED;
        }

        Trade trade = envelope.payload();
        String tradeId = trade.getTradeId();

        // Idempotency: skip if trade already progressed to ENRICHED or beyond
        Optional<Trade> existing = tradeRepository.findByTradeId(tradeId);
        if (existing.isPresent() && isAlreadyEnrichedOrBeyond(existing.get().getStatus())) {
            log.info("Duplicate enrichment skipped [tradeId={}, status={}]", tradeId, existing.get().getStatus());
            return EnrichmentOutcome.DUPLICATE_IGNORED;
        }

        Trade tradeToPersist = existing.orElse(trade);
        EnrichedTrade enrichedTrade = enrich(tradeToPersist, envelope.correlationId());

        tradeToPersist.setStatus(TradeStatus.ENRICHED);
        try {
            tradeRepository.save(tradeToPersist);
        } catch (CouchDbConflictException ex) {
            log.info("Concurrent update during enrichment for trade {} (HTTP 409); ignoring duplicate", tradeId);
            return EnrichmentOutcome.DUPLICATE_IGNORED;
        }

        recordAuditEvent(envelope, enrichedTrade);
        publishEnrichedEvent(envelope, enrichedTrade);

        log.info("Enriched trade [tradeId={}, correlationId={}, failures={}]",
                tradeId, envelope.correlationId(), enrichedTrade.enrichmentFailures().size());
        return EnrichmentOutcome.ENRICHED;
    }

    /** Enriches a trade against {@link ReferenceDataRepository} without mutating original economic terms. */
    public EnrichedTrade enrich(Trade trade, String correlationId) {
        Instant now = Instant.now();
        Map<String, EnrichedField> fields = new LinkedHashMap<>();
        List<EnrichmentFailure> failures = new ArrayList<>();

        // 1. Counterparties (BUYER & SELLER)
        TradeParty buyer = findParty(trade, PartyRole.BUYER);
        TradeParty seller = findParty(trade, PartyRole.SELLER);

        boolean buyerFound = false;
        boolean buyerEligible = false;
        if (buyer != null) {
            Optional<ReferenceData> buyerRef = lookupActive(ReferenceDataCategory.COUNTERPARTY, buyer.partyId());
            if (buyerRef.isPresent()) {
                buyerFound = true;
                buyerEligible = isAttrTrue(buyerRef.get(), "eligible");
                String src = "reference-data:" + buyerRef.get().getId();
                fields.put("buyer.legalName", new EnrichedField("buyer.legalName", buyerRef.get().getDescription(), src, now, EnrichmentOrigin.SUPPLIED));
                fields.put("buyer.eligible", new EnrichedField("buyer.eligible", String.valueOf(buyerEligible), src, now, EnrichmentOrigin.SUPPLIED));
                if (attr(buyerRef.get(), "jurisdiction") != null) {
                    fields.put("buyer.jurisdiction", new EnrichedField("buyer.jurisdiction", attr(buyerRef.get(), "jurisdiction"), src, now, EnrichmentOrigin.SUPPLIED));
                }
            } else {
                failures.add(new EnrichmentFailure("COUNTERPARTY_NOT_FOUND", "parties.buyer",
                        "Buyer counterparty '" + buyer.partyId() + "' not found in reference data"));
            }
        }

        boolean sellerFound = false;
        boolean sellerEligible = false;
        if (seller != null) {
            Optional<ReferenceData> sellerRef = lookupActive(ReferenceDataCategory.COUNTERPARTY, seller.partyId());
            if (sellerRef.isPresent()) {
                sellerFound = true;
                sellerEligible = isAttrTrue(sellerRef.get(), "eligible");
                String src = "reference-data:" + sellerRef.get().getId();
                fields.put("seller.legalName", new EnrichedField("seller.legalName", sellerRef.get().getDescription(), src, now, EnrichmentOrigin.SUPPLIED));
                fields.put("seller.eligible", new EnrichedField("seller.eligible", String.valueOf(sellerEligible), src, now, EnrichmentOrigin.SUPPLIED));
                if (attr(sellerRef.get(), "jurisdiction") != null) {
                    fields.put("seller.jurisdiction", new EnrichedField("seller.jurisdiction", attr(sellerRef.get(), "jurisdiction"), src, now, EnrichmentOrigin.SUPPLIED));
                }
            } else {
                failures.add(new EnrichmentFailure("COUNTERPARTY_NOT_FOUND", "parties.seller",
                        "Seller counterparty '" + seller.partyId() + "' not found in reference data"));
            }
        }

        // 2. Currencies
        boolean currencySupported = false;
        boolean settlementCurrencySupported = true;
        if (trade.getEconomics() != null) {
            String notionalCcy = trade.getEconomics().notionalCurrency();
            Optional<ReferenceData> ccyRef = lookupActive(ReferenceDataCategory.CURRENCY, notionalCcy);
            if (ccyRef.isPresent() && isAttrTrue(ccyRef.get(), "stpSupported")) {
                currencySupported = true;
                String src = "reference-data:" + ccyRef.get().getId();
                fields.put("currency.notional.supported", new EnrichedField("currency.notional.supported", "true", src, now, EnrichmentOrigin.SUPPLIED));
            } else {
                failures.add(new EnrichmentFailure("CURRENCY_UNSUPPORTED", "economics.notionalCurrency",
                        "Notional currency '" + notionalCcy + "' is not supported in reference data"));
            }

            String settleCcy = trade.getEconomics().settlementCurrency();
            if (settleCcy != null && !settleCcy.isBlank()) {
                Optional<ReferenceData> settleRef = lookupActive(ReferenceDataCategory.CURRENCY, settleCcy);
                if (settleRef.isPresent() && isAttrTrue(settleRef.get(), "stpSupported")) {
                    String src = "reference-data:" + settleRef.get().getId();
                    fields.put("currency.settlement.supported", new EnrichedField("currency.settlement.supported", "true", src, now, EnrichmentOrigin.SUPPLIED));
                } else {
                    settlementCurrencySupported = false;
                    failures.add(new EnrichmentFailure("CURRENCY_UNSUPPORTED", "economics.settlementCurrency",
                            "Settlement currency '" + settleCcy + "' is not supported in reference data"));
                }
            } else {
                // Defaulted to notional currency for settlement check when not explicitly provided
                settlementCurrencySupported = currencySupported;
                fields.put("currency.settlement.effective", new EnrichedField(
                        "currency.settlement.effective", String.valueOf(notionalCcy),
                        "trade:economics.notionalCurrency", now, EnrichmentOrigin.DEFAULTED));
            }
        }

        // 3. Product configuration
        boolean productFound = false;
        boolean productStpEligible = false;
        boolean requireNonEconomicTerms = false;
        String templateId = null;
        if (trade.getProductType() != null) {
            Optional<ReferenceData> productRef = lookupActive(ReferenceDataCategory.PRODUCT, trade.getProductType().name());
            if (productRef.isPresent()) {
                productFound = true;
                productStpEligible = isAttrTrue(productRef.get(), "stpEligible");
                requireNonEconomicTerms = isAttrTrue(productRef.get(), "requireNonEconomicTerms");
                templateId = attr(productRef.get(), "confirmationTemplateId");
                String src = "reference-data:" + productRef.get().getId();
                fields.put("product.stpEligible", new EnrichedField("product.stpEligible", String.valueOf(productStpEligible), src, now, EnrichmentOrigin.SUPPLIED));
                fields.put("product.requireNonEconomicTerms", new EnrichedField("product.requireNonEconomicTerms", String.valueOf(requireNonEconomicTerms), src, now, EnrichmentOrigin.SUPPLIED));
                if (templateId != null) {
                    fields.put("product.confirmationTemplateId", new EnrichedField("product.confirmationTemplateId", templateId, src, now, EnrichmentOrigin.SUPPLIED));
                }
            } else {
                failures.add(new EnrichmentFailure("PRODUCT_CONFIG_MISSING", "productType",
                        "Product configuration for '" + trade.getProductType() + "' not found in reference data"));
            }
        }

        // 4. Instrument lookup (derived key from productType + currencies/index)
        boolean instrumentFound = false;
        String instrumentCode = deriveInstrumentCode(trade);
        if (instrumentCode != null) {
            Optional<ReferenceData> instrumentRef = lookupActive(ReferenceDataCategory.INSTRUMENT, instrumentCode);
            if (instrumentRef.isPresent()) {
                instrumentFound = true;
                String src = "reference-data:" + instrumentRef.get().getId();
                fields.put("instrument.code", new EnrichedField("instrument.code", instrumentCode, src, now, EnrichmentOrigin.DERIVED));
                if (attr(instrumentRef.get(), "assetClass") != null) {
                    fields.put("instrument.assetClass", new EnrichedField("instrument.assetClass", attr(instrumentRef.get(), "assetClass"), src, now, EnrichmentOrigin.DERIVED));
                }
            } else {
                failures.add(new EnrichmentFailure("INSTRUMENT_NOT_FOUND", "instrument",
                        "Instrument '" + instrumentCode + "' not found in reference data"));
            }
        }

        // 5. Confirmation template availability
        boolean templateAvailable = false;
        if (templateId != null && !templateId.isBlank()) {
            Optional<ReferenceData> tmplRef = lookupActive(ReferenceDataCategory.CONFIRMATION_TEMPLATE, templateId);
            if (tmplRef.isPresent() && isAttrTrue(tmplRef.get(), "available")) {
                templateAvailable = true;
                String src = "reference-data:" + tmplRef.get().getId();
                fields.put("confirmation.templateAvailable", new EnrichedField("confirmation.templateAvailable", "true", src, now, EnrichmentOrigin.DERIVED));
            } else {
                failures.add(new EnrichmentFailure("CONFIRMATION_TEMPLATE_UNAVAILABLE", "confirmationTemplate",
                        "Confirmation template '" + templateId + "' is unavailable"));
            }
        }

        return new EnrichedTrade(
                trade,
                correlationId,
                now,
                Map.copyOf(fields),
                buyerFound,
                buyerEligible,
                sellerFound,
                sellerEligible,
                currencySupported,
                settlementCurrencySupported,
                productFound,
                productStpEligible,
                requireNonEconomicTerms,
                instrumentFound,
                templateAvailable,
                List.copyOf(failures));
    }

    /** Derives the canonical synthetic instrument code from trade product and economics. */
    public static String deriveInstrumentCode(Trade trade) {
        if (trade == null || trade.getProductType() == null || trade.getEconomics() == null) {
            return null;
        }
        ProductType pt = trade.getProductType();
        String ccy = trade.getEconomics().notionalCurrency();
        String settle = trade.getEconomics().settlementCurrency();
        return switch (pt) {
            case FX_SPOT, FX_FORWARD -> pt.name() + ":" + ccy + "/" + (settle != null ? settle : ccy);
            case INTEREST_RATE_SWAP -> {
                String index = trade.getEconomics().floatingRateIndex();
                yield pt.name() + ":" + ccy + (index != null && !index.isBlank() ? "/" + index : "");
            }
            case BOND, EQUITY_SWAP, CREDIT_DEFAULT_SWAP -> pt.name() + ":" + ccy;
        };
    }

    private Optional<ReferenceData> lookupActive(ReferenceDataCategory category, String code) {
        if (code == null || code.isBlank()) {
            return Optional.empty();
        }
        return referenceDataRepository.findByCategoryAndCode(category, code)
                .filter(ReferenceData::isActive);
    }

    private static TradeParty findParty(Trade trade, PartyRole role) {
        if (trade.getParties() == null) {
            return null;
        }
        return trade.getParties().stream()
                .filter(p -> p != null && p.role() == role)
                .findFirst()
                .orElse(null);
    }

    private static String attr(ReferenceData data, String key) {
        return data.getAttributes() == null ? null : data.getAttributes().get(key);
    }

    private static boolean isAttrTrue(ReferenceData data, String key) {
        return "true".equalsIgnoreCase(attr(data, key));
    }

    private static boolean isAlreadyEnrichedOrBeyond(TradeStatus status) {
        return status == TradeStatus.ENRICHED
                || status == TradeStatus.VALIDATED
                || status == TradeStatus.CONFIRMED
                || status == TradeStatus.EXCEPTION
                || status == TradeStatus.REJECTED;
    }

    private void recordAuditEvent(EventEnvelope<Trade> inbound, EnrichedTrade enriched) {
        AuditEvent audit = new AuditEvent();
        audit.setEventId("enrich-" + inbound.eventId());
        audit.setEventType(EventType.TRADE_ENRICHED);
        audit.setTradeId(enriched.trade().getTradeId());
        audit.setCorrelationId(inbound.correlationId());
        audit.setActor("system:enrichment");
        audit.setOccurredAt(Instant.now());
        audit.setDetails(Map.of(
                "enrichedFieldCount", String.valueOf(enriched.enrichedFields().size()),
                "failureCount", String.valueOf(enriched.enrichmentFailures().size())));
        try {
            auditEventRepository.save(audit);
        } catch (CouchDbConflictException ex) {
            log.debug("Audit event '{}' already exists; skipping duplicate", audit.getEventId());
        }
    }

    private void publishEnrichedEvent(EventEnvelope<Trade> inbound, EnrichedTrade enriched) {
        EventEnvelope<EnrichedTrade> outbound = EventEnvelope.of(
                "TradeEnriched",
                inbound.correlationId(),
                SCHEMA_VERSION,
                SOURCE,
                enriched);
        String topic = kafkaProperties.getTopics().getTradeEnriched();
        try {
            String json = objectMapper.writeValueAsString(outbound);
            kafkaTemplate.send(topic, enriched.trade().getTradeId(), json);
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Failed to serialize TradeEnriched for " + enriched.trade().getTradeId(), ex);
        }
    }

    public enum EnrichmentOutcome {
        ENRICHED,
        DUPLICATE_IGNORED,
        DEAD_LETTERED
    }
}
