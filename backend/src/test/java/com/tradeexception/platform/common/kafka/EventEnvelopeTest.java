package com.tradeexception.platform.common.kafka;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.tradeexception.platform.trade.domain.NonEconomicTerms;
import com.tradeexception.platform.trade.domain.PartyRole;
import com.tradeexception.platform.trade.domain.ProductType;
import com.tradeexception.platform.trade.domain.Trade;
import com.tradeexception.platform.trade.domain.TradeEconomics;
import com.tradeexception.platform.trade.domain.TradeMessageHeader;
import com.tradeexception.platform.trade.domain.TradeParty;
import com.tradeexception.platform.trade.domain.TradeStatus;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class EventEnvelopeTest {

    private final ObjectMapper mapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    private final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();

    @Test
    void serializesAndDeserializesTradeEnvelope() throws Exception {
        Trade trade = new Trade();
        trade.setHeader(new TradeMessageHeader("MSG-1", "CORR-1", "1.0", "SYNTH-OMS", Instant.parse("2026-11-24T09:00:00Z")));
        trade.setTradeId("TRD-1");
        trade.setSourceSystemTradeId("OMS-1");
        trade.setProductType(ProductType.FX_SPOT);
        trade.setTradeDate(LocalDate.of(2026, 11, 24));
        trade.setEffectiveDate(LocalDate.of(2026, 11, 26));
        trade.setParties(List.of(
                new TradeParty("SYNTHBANK00000000001", "Bank A", PartyRole.BUYER),
                new TradeParty("SYNTHCORP00000000002", "Corp B", PartyRole.SELLER)));
        trade.setEconomics(new TradeEconomics(new BigDecimal("100000.00"), "USD", new BigDecimal("1.08"), null, null, "EUR"));
        trade.setNonEconomicTerms(new NonEconomicTerms("ISDA_2002", "ENGLISH", "MODIFIED_FOLLOWING", "SYNTHBANK00000000001"));
        trade.setStatus(TradeStatus.RECEIVED);

        EventEnvelope<Trade> envelope = EventEnvelope.of("TradeReceived", "CORR-1", "1.0", "synthetic-trade-producer", trade);
        assertThat(validator.validate(envelope)).isEmpty();

        String json = mapper.writeValueAsString(envelope);
        EventEnvelope<Trade> restored = mapper.readValue(json, new TypeReference<>() {});

        assertThat(restored.eventId()).isEqualTo(envelope.eventId());
        assertThat(restored.eventType()).isEqualTo("TradeReceived");
        assertThat(restored.correlationId()).isEqualTo("CORR-1");
        assertThat(restored.schemaVersion()).isEqualTo("1.0");
        assertThat(restored.source()).isEqualTo("synthetic-trade-producer");
        assertThat(restored.payload().getTradeId()).isEqualTo("TRD-1");
        assertThat(restored.payload().getEconomics().notionalAmount()).isEqualByComparingTo("100000.00");
    }

    @Test
    void rejectsInvalidEnvelopeMetadata() {
        EventEnvelope<Trade> invalid = new EventEnvelope<>("", "", "", "v1", null, "", null);
        Set<ConstraintViolation<EventEnvelope<Trade>>> violations = validator.validate(invalid);

        assertThat(violations)
                .extracting(v -> v.getPropertyPath().toString())
                .contains("eventId", "eventType", "correlationId", "schemaVersion", "timestamp", "source", "payload");
    }
}
