package com.tradeexception.platform.stp;

import com.tradeexception.platform.enrichment.domain.EnrichedTrade;
import com.tradeexception.platform.stp.domain.StpDecision;
import com.tradeexception.platform.stp.domain.StpValidationResult;
import com.tradeexception.platform.stp.rules.CounterpartyValidationRule;
import com.tradeexception.platform.stp.rules.CurrencySupportRule;
import com.tradeexception.platform.stp.rules.DateConsistencyRule;
import com.tradeexception.platform.stp.rules.EconomicFieldsRule;
import com.tradeexception.platform.stp.rules.InstrumentAndTemplateRule;
import com.tradeexception.platform.stp.rules.MandatoryIdentifiersRule;
import com.tradeexception.platform.stp.rules.NonEconomicTermsRule;
import com.tradeexception.platform.stp.rules.ProductEligibilityRule;
import com.tradeexception.platform.trade.domain.NonEconomicTerms;
import com.tradeexception.platform.trade.domain.PartyRole;
import com.tradeexception.platform.trade.domain.ProductType;
import com.tradeexception.platform.trade.domain.Trade;
import com.tradeexception.platform.trade.domain.TradeEconomics;
import com.tradeexception.platform.trade.domain.TradeMessageHeader;
import com.tradeexception.platform.trade.domain.TradeParty;
import com.tradeexception.platform.trade.domain.TradeStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class StpRuleEngineTest {

    private StpRuleEngine engine;

    @BeforeEach
    void setUp() {
        engine = new StpRuleEngine(List.of(
                new MandatoryIdentifiersRule(),
                new CounterpartyValidationRule(),
                new ProductEligibilityRule(),
                new CurrencySupportRule(),
                new EconomicFieldsRule(),
                new DateConsistencyRule(),
                new InstrumentAndTemplateRule(),
                new NonEconomicTermsRule()));
    }

    private static Trade validFxForward() {
        Trade t = new Trade();
        t.setHeader(new TradeMessageHeader("MSG-1", "CORR-1", "1.0", "SYNTH-OMS", Instant.now()));
        t.setTradeId("TRD-100");
        t.setSourceSystemTradeId("OMS-100");
        t.setProductType(ProductType.FX_FORWARD);
        t.setTradeDate(LocalDate.of(2026, 11, 24));
        t.setEffectiveDate(LocalDate.of(2026, 11, 26));
        t.setMaturityDate(LocalDate.of(2027, 2, 26));
        t.setParties(List.of(
                new TradeParty("SYNTHBANK00000000001", "Bank A", PartyRole.BUYER),
                new TradeParty("SYNTHCORP00000000002", "Corp B", PartyRole.SELLER)));
        t.setEconomics(new TradeEconomics(new BigDecimal("2000000.00"), "GBP", new BigDecimal("1.27"), null, null, "USD"));
        t.setNonEconomicTerms(new NonEconomicTerms("ISDA_2002", "ENGLISH", "MODIFIED_FOLLOWING", "SYNTHBANK00000000001"));
        t.setStatus(TradeStatus.ENRICHED);
        return t;
    }

    @Test
    void returnsStpEligibleWhenAllRulesPass() {
        EnrichedTrade enriched = new EnrichedTrade(
                validFxForward(), "CORR-1", Instant.now(), Map.of(),
                true, true, true, true, true, true, true, true, true, true, true, List.of());

        StpValidationResult result = engine.validate(enriched);

        assertThat(result.decision()).isEqualTo(StpDecision.STP_ELIGIBLE);
        assertThat(result.passedRules()).hasSize(8);
        assertThat(result.failedRules()).isEmpty();
        assertThat(result.reasonCodes()).isEmpty();
    }

    @Test
    void returnsStpRejectedOnMissingCounterpartyOrUnsupportedCurrency() {
        EnrichedTrade enriched = new EnrichedTrade(
                validFxForward(), "CORR-1", Instant.now(), Map.of(),
                true, true, false, false, false, true, true, true, true, false, true, List.of());

        StpValidationResult result = engine.validate(enriched);

        assertThat(result.decision()).isEqualTo(StpDecision.STP_REJECTED);
        assertThat(result.reasonCodes())
                .contains("COUNTERPARTY_NOT_FOUND", "CURRENCY_UNSUPPORTED", "INSTRUMENT_NOT_FOUND");
        assertThat(result.explanation()).contains("rejected");
    }

    @Test
    void returnsManualReviewWhenRestrictedCounterpartyOrMissingTemplateOrNonEconomicTerms() {
        Trade trade = validFxForward();
        trade.setNonEconomicTerms(null);
        EnrichedTrade enriched = new EnrichedTrade(
                trade, "CORR-1", Instant.now(), Map.of(),
                true, true, true, false, true, true, true, false, true, true, false, List.of());

        StpValidationResult result = engine.validate(enriched);

        assertThat(result.decision()).isEqualTo(StpDecision.MANUAL_REVIEW_REQUIRED);
        assertThat(result.reasonCodes()).contains(
                "COUNTERPARTY_NOT_STP_ELIGIBLE",
                "PRODUCT_MANUAL_REVIEW_ONLY",
                "CONFIRMATION_TEMPLATE_UNAVAILABLE",
                "INCOMPLETE_NON_ECONOMIC_TERMS");
        assertThat(result.explanation()).contains("Manual review required");
    }
}
