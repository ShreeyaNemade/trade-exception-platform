package com.tradeexception.platform.confirmation.api;

import com.tradeexception.platform.confirmation.domain.Confirmation;
import com.tradeexception.platform.confirmation.domain.ConfirmationStatus;
import com.tradeexception.platform.trade.domain.NonEconomicTerms;
import com.tradeexception.platform.trade.domain.ProductType;
import com.tradeexception.platform.trade.domain.TradeEconomics;
import com.tradeexception.platform.trade.domain.TradeParty;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;

/** API DTO for {@link Confirmation}. */
public record ConfirmationDto(
        String confirmationId,
        String tradeId,
        String correlationId,
        ProductType productType,
        LocalDate tradeDate,
        LocalDate effectiveDate,
        LocalDate maturityDate,
        List<TradeParty> counterparties,
        BigDecimal notionalAmount,
        String currency,
        TradeEconomics economicTerms,
        NonEconomicTerms nonEconomicTerms,
        ConfirmationStatus status,
        Instant generatedAt,
        String templateId,
        String templateVersion,
        boolean hasRenderedDocument) {

    public static ConfirmationDto from(Confirmation c) {
        return new ConfirmationDto(
                c.getConfirmationId(),
                c.getTradeId(),
                c.getCorrelationId(),
                c.getProductType(),
                c.getTradeDate(),
                c.getEffectiveDate(),
                c.getMaturityDate(),
                c.getCounterparties(),
                c.getNotionalAmount(),
                c.getCurrency(),
                c.getEconomicTerms(),
                c.getNonEconomicTerms(),
                c.getStatus(),
                c.getGeneratedAt(),
                c.getTemplateId(),
                c.getTemplateVersion(),
                c.getRenderedContent() != null && !c.getRenderedContent().isBlank());
    }
}
