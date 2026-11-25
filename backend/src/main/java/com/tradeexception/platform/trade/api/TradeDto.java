package com.tradeexception.platform.trade.api;

import com.tradeexception.platform.trade.domain.NonEconomicTerms;
import com.tradeexception.platform.trade.domain.ProductType;
import com.tradeexception.platform.trade.domain.Trade;
import com.tradeexception.platform.trade.domain.TradeEconomics;
import com.tradeexception.platform.trade.domain.TradeMessageHeader;
import com.tradeexception.platform.trade.domain.TradeParty;
import com.tradeexception.platform.trade.domain.TradeStatus;

import java.time.Instant;
import java.time.LocalDate;
import java.util.List;

/** API DTO for {@link Trade} (excludes internal CouchDB revision metadata). */
public record TradeDto(
        String tradeId,
        String sourceSystemTradeId,
        String correlationId,
        TradeMessageHeader header,
        ProductType productType,
        LocalDate tradeDate,
        LocalDate effectiveDate,
        LocalDate maturityDate,
        List<TradeParty> parties,
        TradeEconomics economics,
        NonEconomicTerms nonEconomicTerms,
        TradeStatus status,
        Instant createdAt,
        Instant updatedAt) {

    public static TradeDto from(Trade t) {
        return new TradeDto(
                t.getTradeId(),
                t.getSourceSystemTradeId(),
                t.getHeader() == null ? null : t.getHeader().correlationId(),
                t.getHeader(),
                t.getProductType(),
                t.getTradeDate(),
                t.getEffectiveDate(),
                t.getMaturityDate(),
                t.getParties(),
                t.getEconomics(),
                t.getNonEconomicTerms(),
                t.getStatus(),
                t.getCreatedAt(),
                t.getUpdatedAt());
    }
}
