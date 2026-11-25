package com.tradeexception.platform.confirmation.domain;

import com.tradeexception.platform.common.domain.BaseDocument;
import com.tradeexception.platform.trade.domain.NonEconomicTerms;
import com.tradeexception.platform.trade.domain.ProductType;
import com.tradeexception.platform.trade.domain.TradeEconomics;
import com.tradeexception.platform.trade.domain.TradeParty;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Confirmation generated for an STP-validated trade. Document id: {@code confirmation:<confirmationId>}.
 * Synthetic educational representation — not legally binding.
 */
public class Confirmation extends BaseDocument {

    public static final String TYPE = "confirmation";
    public static final int SCHEMA_VERSION = 1;

    @NotBlank @Size(max = 64)
    private String confirmationId;

    @NotBlank @Size(max = 64)
    private String tradeId;

    @NotBlank @Size(max = 64)
    private String correlationId;

    private ProductType productType;
    private LocalDate tradeDate;
    private LocalDate effectiveDate;
    private LocalDate maturityDate;
    private List<TradeParty> counterparties;
    private BigDecimal notionalAmount;
    private String currency;
    private TradeEconomics economicTerms;
    private NonEconomicTerms nonEconomicTerms;

    @NotNull
    private ConfirmationStatus status;

    private Instant generatedAt;
    private String templateId;
    private String templateVersion;
    private String renderedContent;

    public Confirmation() {
        super(SCHEMA_VERSION);
    }

    @Override
    public String getType() { return TYPE; }

    @Override
    protected String businessKey() { return confirmationId; }

    public String getConfirmationId() { return confirmationId; }
    public void setConfirmationId(String confirmationId) { this.confirmationId = confirmationId; }
    public String getTradeId() { return tradeId; }
    public void setTradeId(String tradeId) { this.tradeId = tradeId; }
    public String getCorrelationId() { return correlationId; }
    public void setCorrelationId(String correlationId) { this.correlationId = correlationId; }
    public ProductType getProductType() { return productType; }
    public void setProductType(ProductType productType) { this.productType = productType; }
    public LocalDate getTradeDate() { return tradeDate; }
    public void setTradeDate(LocalDate tradeDate) { this.tradeDate = tradeDate; }
    public LocalDate getEffectiveDate() { return effectiveDate; }
    public void setEffectiveDate(LocalDate effectiveDate) { this.effectiveDate = effectiveDate; }
    public LocalDate getMaturityDate() { return maturityDate; }
    public void setMaturityDate(LocalDate maturityDate) { this.maturityDate = maturityDate; }
    public List<TradeParty> getCounterparties() { return counterparties; }
    public void setCounterparties(List<TradeParty> counterparties) {
        this.counterparties = counterparties == null ? null : Collections.unmodifiableList(new ArrayList<>(counterparties));
    }
    public BigDecimal getNotionalAmount() { return notionalAmount; }
    public void setNotionalAmount(BigDecimal notionalAmount) { this.notionalAmount = notionalAmount; }
    public String getCurrency() { return currency; }
    public void setCurrency(String currency) { this.currency = currency; }
    public TradeEconomics getEconomicTerms() { return economicTerms; }
    public void setEconomicTerms(TradeEconomics economicTerms) { this.economicTerms = economicTerms; }
    public NonEconomicTerms getNonEconomicTerms() { return nonEconomicTerms; }
    public void setNonEconomicTerms(NonEconomicTerms nonEconomicTerms) { this.nonEconomicTerms = nonEconomicTerms; }
    public ConfirmationStatus getStatus() { return status; }
    public void setStatus(ConfirmationStatus status) { this.status = status; }
    public Instant getGeneratedAt() { return generatedAt; }
    public void setGeneratedAt(Instant generatedAt) { this.generatedAt = generatedAt; }
    public String getTemplateId() { return templateId; }
    public void setTemplateId(String templateId) { this.templateId = templateId; }
    public String getTemplateVersion() { return templateVersion; }
    public void setTemplateVersion(String templateVersion) { this.templateVersion = templateVersion; }
    public String getRenderedContent() { return renderedContent; }
    public void setRenderedContent(String renderedContent) { this.renderedContent = renderedContent; }
}
