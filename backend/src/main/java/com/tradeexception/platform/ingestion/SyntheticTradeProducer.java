package com.tradeexception.platform.ingestion;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tradeexception.platform.common.kafka.EventEnvelope;
import com.tradeexception.platform.common.kafka.KafkaProperties;
import com.tradeexception.platform.trade.domain.NonEconomicTerms;
import com.tradeexception.platform.trade.domain.PartyRole;
import com.tradeexception.platform.trade.domain.ProductType;
import com.tradeexception.platform.trade.domain.Trade;
import com.tradeexception.platform.trade.domain.TradeEconomics;
import com.tradeexception.platform.trade.domain.TradeMessageHeader;
import com.tradeexception.platform.trade.domain.TradeParty;
import com.tradeexception.platform.trade.domain.TradeStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;

/**
 * Generates synthetic educational trade events and publishes them to {@code trade.raw}.
 * Supports both valid trades and negative/edge-case scenarios for pipeline testing.
 */
@Component
public class SyntheticTradeProducer {

    private static final Logger log = LoggerFactory.getLogger(SyntheticTradeProducer.class);
    private static final String SCHEMA_VERSION = "1.0";
    private static final String SOURCE = "synthetic-trade-producer";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final KafkaProperties kafkaProperties;

    public SyntheticTradeProducer(
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper,
            KafkaProperties kafkaProperties) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.kafkaProperties = kafkaProperties;
    }

    /** Generates and publishes a synthetic trade scenario to {@code trade.raw}. */
    public PublishResult publishScenario(SyntheticTradeScenario scenario) {
        String topic = kafkaProperties.getTopics().getTradeRaw();
        if (scenario == SyntheticTradeScenario.MALFORMED_PAYLOAD) {
            String key = "MALFORMED-" + shortId();
            String brokenJson = "{\"eventId\":\"BAD\",\"payload\":{\"tradeId\":\"TRD-BROKEN\", broken_json";
            publishRawPayload(key, brokenJson);
            return new PublishResult(key, null, null, topic, scenario, 1);
        }

        EventEnvelope<Trade> envelope = buildEnvelopeForScenario(scenario);
        if (scenario == SyntheticTradeScenario.DUPLICATE_MESSAGE) {
            publishTradeEvent(envelope);
            publishTradeEvent(envelope);
            return new PublishResult(
                    envelope.eventId(),
                    envelope.payload().getTradeId(),
                    envelope.correlationId(),
                    topic,
                    scenario,
                    2);
        }

        publishTradeEvent(envelope);
        return new PublishResult(
                envelope.eventId(),
                envelope.payload().getTradeId(),
                envelope.correlationId(),
                topic,
                scenario,
                1);
    }

    /** Builds a realistic synthetic {@link EventEnvelope} for the given scenario without publishing. */
    public EventEnvelope<Trade> buildEnvelopeForScenario(SyntheticTradeScenario scenario) {
        String correlationId = "CORR-" + shortId();
        Trade trade = buildTradeForScenario(scenario, correlationId);
        return EventEnvelope.of("TradeReceived", correlationId, SCHEMA_VERSION, SOURCE, trade);
    }

    public void publishTradeEvent(EventEnvelope<Trade> envelope) {
        String topic = kafkaProperties.getTopics().getTradeRaw();
        String key = (envelope.payload() != null && envelope.payload().getTradeId() != null
                && !envelope.payload().getTradeId().isBlank())
                ? envelope.payload().getTradeId()
                : envelope.eventId();
        try {
            String json = objectMapper.writeValueAsString(envelope);
            kafkaTemplate.send(topic, key, json);
            log.info("Published synthetic event to {} [key={}, eventId={}, correlationId={}]",
                    topic, key, envelope.eventId(), envelope.correlationId());
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Failed to serialize synthetic trade event " + envelope.eventId(), ex);
        }
    }

    public void publishRawPayload(String key, String rawPayload) {
        String topic = kafkaProperties.getTopics().getTradeRaw();
        kafkaTemplate.send(topic, key, rawPayload);
        log.info("Published raw payload to {} [key={}]", topic, key);
    }

    private Trade buildTradeForScenario(SyntheticTradeScenario scenario, String correlationId) {
        String suffix = shortId();
        LocalDate today = LocalDate.now();
        Trade trade = new Trade();
        trade.setHeader(new TradeMessageHeader(
                "MSG-" + suffix,
                correlationId,
                SCHEMA_VERSION,
                "SYNTH-OMS",
                Instant.now()));
        trade.setTradeId("TRD-" + suffix);
        trade.setSourceSystemTradeId("OMS-" + suffix);
        trade.setTradeDate(today);
        trade.setEffectiveDate(today.plusDays(2));
        trade.setStatus(TradeStatus.RECEIVED);
        trade.setParties(List.of(
                new TradeParty("SYNTHBANK00000000001", "Synthetic Bank Alpha", PartyRole.BUYER),
                new TradeParty("SYNTHCORP00000000002", "Synthetic Corp Beta", PartyRole.SELLER)));
        trade.setNonEconomicTerms(new NonEconomicTerms(
                "ISDA_2002", "ENGLISH", "MODIFIED_FOLLOWING", "SYNTHBANK00000000001"));

        return switch (scenario) {
            case VALID_FX_SPOT, DUPLICATE_MESSAGE -> {
                trade.setProductType(ProductType.FX_SPOT);
                trade.setMaturityDate(null);
                trade.setEconomics(new TradeEconomics(
                        new BigDecimal("1500000.00"), "USD", new BigDecimal("1.0845"), null, null, "EUR"));
                yield trade;
            }
            case VALID_FX_FORWARD -> {
                trade.setProductType(ProductType.FX_FORWARD);
                trade.setMaturityDate(today.plusMonths(3));
                trade.setEconomics(new TradeEconomics(
                        new BigDecimal("2500000.00"), "GBP", new BigDecimal("1.2710"), null, null, "USD"));
                yield trade;
            }
            case MISSING_MANDATORY_ECONOMIC_FIELD -> {
                trade.setProductType(ProductType.FX_SPOT);
                trade.setMaturityDate(null);
                // Missing notionalAmount and price
                trade.setEconomics(new TradeEconomics(
                        null, "USD", null, null, null, "EUR"));
                yield trade;
            }
            case INVALID_COUNTERPARTY_REFERENCE -> {
                trade.setProductType(ProductType.FX_SPOT);
                trade.setMaturityDate(null);
                trade.setParties(List.of(
                        new TradeParty("SYNTHBANK00000000001", "Synthetic Bank Alpha", PartyRole.BUYER),
                        new TradeParty("", "Unknown Counterparty", PartyRole.SELLER)));
                trade.setEconomics(new TradeEconomics(
                        new BigDecimal("750000.00"), "USD", new BigDecimal("1.0820"), null, null, "EUR"));
                yield trade;
            }
            case MALFORMED_PAYLOAD -> throw new IllegalStateException("Handled before Trade construction");
        };
    }

    private static String shortId() {
        return UUID.randomUUID().toString().substring(0, 8).toUpperCase();
    }

    public record PublishResult(
            String eventId,
            String tradeId,
            String correlationId,
            String topic,
            SyntheticTradeScenario scenario,
            int messagesPublished) {
    }
}
