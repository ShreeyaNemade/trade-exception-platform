package com.tradeexception.platform.trade.domain;

import com.tradeexception.platform.audit.domain.AuditEvent;
import com.tradeexception.platform.audit.domain.EventType;
import com.tradeexception.platform.confirmation.domain.Confirmation;
import com.tradeexception.platform.confirmation.domain.ConfirmationStatus;
import com.tradeexception.platform.enrichment.domain.ReferenceData;
import com.tradeexception.platform.enrichment.domain.ReferenceDataCategory;
import com.tradeexception.platform.exception.domain.ExceptionCategory;
import com.tradeexception.platform.exception.domain.ExceptionPriority;
import com.tradeexception.platform.exception.domain.ExceptionRecord;
import com.tradeexception.platform.exception.domain.ExceptionStatus;
import com.tradeexception.platform.workitem.domain.WorkItem;
import com.tradeexception.platform.workitem.domain.WorkItemStatus;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class DomainValidationTest {

    private static Validator validator;

    @BeforeAll
    static void setUpValidator() {
        validator = Validation.buildDefaultValidatorFactory().getValidator();
    }

    static Trade validSwapTrade() {
        Trade trade = new Trade();
        trade.setHeader(new TradeMessageHeader(
                "MSG-001", "CORR-001", "1.0", "SYNTH-OMS", Instant.parse("2026-11-24T09:00:00Z")));
        trade.setTradeId("TRD-001");
        trade.setSourceSystemTradeId("OMS-1001");
        trade.setProductType(ProductType.INTEREST_RATE_SWAP);
        trade.setTradeDate(LocalDate.of(2026, 11, 24));
        trade.setEffectiveDate(LocalDate.of(2026, 11, 26));
        trade.setMaturityDate(LocalDate.of(2031, 11, 26));
        trade.setParties(List.of(
                new TradeParty("SYNTHBANK00000000001", "Synthetic Bank A", PartyRole.BUYER),
                new TradeParty("SYNTHCORP00000000002", "Synthetic Corp B", PartyRole.SELLER)));
        trade.setEconomics(new TradeEconomics(
                new BigDecimal("10000000.00"), "USD", null, new BigDecimal("0.0425"), "SOFR", "USD"));
        trade.setNonEconomicTerms(new NonEconomicTerms(
                "ISDA_2002", "NEW_YORK", "MODIFIED_FOLLOWING", "SYNTHBANK00000000001"));
        trade.setStatus(TradeStatus.RECEIVED);
        return trade;
    }

    @Test
    void validInterestRateSwapPassesValidation() {
        Trade trade = validSwapTrade();
        assertThat(validator.validate(trade)).isEmpty();
        assertThat(trade.getId()).isEqualTo("trade:TRD-001");
    }

    @Test
    void missingRequiredIdentifiersAndHeaderFieldsFailValidation() {
        Trade trade = validSwapTrade();
        trade.setTradeId(" ");
        trade.setHeader(new TradeMessageHeader("", "", "bad", "", null));

        Set<ConstraintViolation<Trade>> violations = validator.validate(trade);
        assertThat(violations)
                .extracting(v -> v.getPropertyPath().toString())
                .contains("tradeId", "header.messageId", "header.correlationId", "header.schemaVersion", "header.sentAt");
    }

    @Test
    void invalidEconomicsFailValidation() {
        Trade trade = validSwapTrade();
        trade.setEconomics(new TradeEconomics(
                new BigDecimal("-500.00"), "us", new BigDecimal("-1"), new BigDecimal("0.04"), null, "123"));

        Set<ConstraintViolation<Trade>> violations = validator.validate(trade);
        assertThat(violations)
                .extracting(v -> v.getPropertyPath().toString())
                .contains("economics.notionalAmount", "economics.notionalCurrency", "economics.price", "economics.settlementCurrency");
    }

    @Test
    void invalidDateSequenceAndPartyRolesFailValidation() {
        Trade trade = validSwapTrade();
        trade.setEffectiveDate(LocalDate.of(2026, 11, 20)); // before tradeDate
        trade.setParties(List.of(
                new TradeParty("SYNTHBANK00000000001", "Bank A", PartyRole.BUYER),
                new TradeParty("SYNTHBANK00000000002", "Bank B", PartyRole.BUYER)));

        Set<ConstraintViolation<Trade>> violations = validator.validate(trade);
        assertThat(violations)
                .extracting(ConstraintViolation::getMessage)
                .contains(
                        "dates must satisfy tradeDate <= effectiveDate <= maturityDate",
                        "parties must contain one BUYER and one SELLER");
    }

    @Test
    void productSpecificMandatoryFieldsAreEnforced() {
        Trade irsWithoutRate = validSwapTrade();
        irsWithoutRate.setEconomics(new TradeEconomics(
                new BigDecimal("500000.00"), "EUR", null, null, null, null));
        assertThat(validator.validate(irsWithoutRate))
                .extracting(ConstraintViolation::getMessage)
                .contains("product-specific mandatory fields are missing");

        Trade fxSpotWithoutPrice = validSwapTrade();
        fxSpotWithoutPrice.setProductType(ProductType.FX_SPOT);
        fxSpotWithoutPrice.setMaturityDate(null);
        fxSpotWithoutPrice.setEconomics(new TradeEconomics(
                new BigDecimal("250000.00"), "USD", null, null, null, "EUR"));
        assertThat(validator.validate(fxSpotWithoutPrice))
                .extracting(ConstraintViolation::getMessage)
                .contains("product-specific mandatory fields are missing");

        fxSpotWithoutPrice.setEconomics(new TradeEconomics(
                new BigDecimal("250000.00"), "USD", new BigDecimal("1.0850"), null, null, "EUR"));
        assertThat(validator.validate(fxSpotWithoutPrice)).isEmpty();
    }

    @Test
    void otherDomainAggregatesValidateAndGenerateDeterministicIds() {
        Confirmation confirmation = new Confirmation();
        confirmation.setConfirmationId("CNF-1");
        confirmation.setTradeId("TRD-001");
        confirmation.setCorrelationId("CORR-001");
        confirmation.setStatus(ConfirmationStatus.GENERATED);
        assertThat(validator.validate(confirmation)).isEmpty();
        assertThat(confirmation.getId()).isEqualTo("confirmation:CNF-1");

        ExceptionRecord exception = new ExceptionRecord();
        exception.setExceptionId("EXC-1");
        exception.setTradeId("TRD-001");
        exception.setCorrelationId("CORR-001");
        exception.setCategory(ExceptionCategory.COUNTERPARTY_UNKNOWN);
        exception.setPriority(ExceptionPriority.HIGH);
        exception.setStatus(ExceptionStatus.OPEN);
        exception.setReasonCode("UNKNOWN_LEI");
        exception.setDetectedAt(Instant.now());
        assertThat(validator.validate(exception)).isEmpty();
        assertThat(exception.getId()).isEqualTo("exception:EXC-1");

        WorkItem workItem = new WorkItem();
        workItem.setWorkItemId("WI-1");
        workItem.setExceptionId("EXC-1");
        workItem.setTradeId("TRD-001");
        workItem.setCorrelationId("CORR-001");
        workItem.setStatus(WorkItemStatus.OPEN);
        workItem.setPriority(ExceptionPriority.HIGH);
        assertThat(validator.validate(workItem)).isEmpty();
        assertThat(workItem.getId()).isEqualTo("workitem:WI-1");

        AuditEvent audit = new AuditEvent();
        audit.setEventId("AUD-1");
        audit.setEventType(EventType.TRADE_RECEIVED);
        audit.setTradeId("TRD-001");
        audit.setCorrelationId("CORR-001");
        audit.setActor("system:ingestion");
        audit.setOccurredAt(Instant.now());
        assertThat(validator.validate(audit)).isEmpty();
        assertThat(audit.getId()).isEqualTo("audit:AUD-1");

        ReferenceData ref = new ReferenceData();
        ref.setCategory(ReferenceDataCategory.CURRENCY);
        ref.setCode("USD");
        ref.setDescription("US Dollar");
        assertThat(validator.validate(ref)).isEmpty();
        assertThat(ref.getId()).isEqualTo("refdata:CURRENCY:USD");
    }
}
