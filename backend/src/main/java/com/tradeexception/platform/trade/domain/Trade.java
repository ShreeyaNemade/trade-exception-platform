package com.tradeexception.platform.trade.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.tradeexception.platform.common.domain.BaseDocument;
import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Trade aggregate. Document id: {@code trade:<tradeId>}. */
public class Trade extends BaseDocument {

    public static final String TYPE = "trade";
    public static final int SCHEMA_VERSION = 1;

    @NotNull @Valid
    private TradeMessageHeader header;

    @NotBlank @Size(max = 64)
    private String tradeId;

    @NotBlank @Size(max = 64)
    private String sourceSystemTradeId;

    @NotNull
    private ProductType productType;

    @NotNull
    private LocalDate tradeDate;

    @NotNull
    private LocalDate effectiveDate;

    /** Optional: spot products have no maturity. */
    private LocalDate maturityDate;

    @NotNull @Size(min = 2, max = 2, message = "must contain exactly two parties")
    private List<@NotNull @Valid TradeParty> parties;

    @NotNull @Valid
    private TradeEconomics economics;

    @Valid
    private NonEconomicTerms nonEconomicTerms;

    @NotNull
    private TradeStatus status;

    public Trade() {
        super(SCHEMA_VERSION);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    protected String businessKey() {
        return tradeId;
    }

    public static String documentId(String tradeId) {
        return TYPE + ":" + tradeId;
    }

    @JsonIgnore
    @AssertTrue(message = "dates must satisfy tradeDate <= effectiveDate <= maturityDate")
    public boolean isDateSequenceValid() {
        if (tradeDate == null || effectiveDate == null) {
            return true; // reported by @NotNull
        }
        return !effectiveDate.isBefore(tradeDate)
                && (maturityDate == null || !maturityDate.isBefore(effectiveDate));
    }

    @JsonIgnore
    @AssertTrue(message = "parties must contain one BUYER and one SELLER")
    public boolean isPartyRolesValid() {
        if (parties == null || parties.size() != 2 || parties.contains(null)) {
            return true; // reported by @Size / @NotNull
        }
        return parties.stream().anyMatch(p -> p.role() == PartyRole.BUYER)
                && parties.stream().anyMatch(p -> p.role() == PartyRole.SELLER);
    }

    @JsonIgnore
    @AssertTrue(message = "product-specific mandatory fields are missing")
    public boolean isProductSpecificTermsValid() {
        if (productType == null || economics == null) {
            return true; // reported by @NotNull
        }
        return switch (productType) {
            case INTEREST_RATE_SWAP -> maturityDate != null
                    && (economics.fixedRate() != null
                    || (economics.floatingRateIndex() != null && !economics.floatingRateIndex().isBlank()));
            case FX_SPOT -> economics.price() != null;
            case FX_FORWARD, BOND -> maturityDate != null && economics.price() != null;
            case EQUITY_SWAP, CREDIT_DEFAULT_SWAP -> maturityDate != null;
        };
    }

    public TradeMessageHeader getHeader() { return header; }
    public void setHeader(TradeMessageHeader header) { this.header = header; }
    public String getTradeId() { return tradeId; }
    public void setTradeId(String tradeId) { this.tradeId = tradeId; }
    public String getSourceSystemTradeId() { return sourceSystemTradeId; }
    public void setSourceSystemTradeId(String sourceSystemTradeId) { this.sourceSystemTradeId = sourceSystemTradeId; }
    public ProductType getProductType() { return productType; }
    public void setProductType(ProductType productType) { this.productType = productType; }
    public LocalDate getTradeDate() { return tradeDate; }
    public void setTradeDate(LocalDate tradeDate) { this.tradeDate = tradeDate; }
    public LocalDate getEffectiveDate() { return effectiveDate; }
    public void setEffectiveDate(LocalDate effectiveDate) { this.effectiveDate = effectiveDate; }
    public LocalDate getMaturityDate() { return maturityDate; }
    public void setMaturityDate(LocalDate maturityDate) { this.maturityDate = maturityDate; }
    public List<TradeParty> getParties() { return parties; }
    public void setParties(List<TradeParty> parties) { this.parties = parties == null ? null : Collections.unmodifiableList(new ArrayList<>(parties)); }
    public TradeEconomics getEconomics() { return economics; }
    public void setEconomics(TradeEconomics economics) { this.economics = economics; }
    public NonEconomicTerms getNonEconomicTerms() { return nonEconomicTerms; }
    public void setNonEconomicTerms(NonEconomicTerms nonEconomicTerms) { this.nonEconomicTerms = nonEconomicTerms; }
    public TradeStatus getStatus() { return status; }
    public void setStatus(TradeStatus status) { this.status = status; }
}
