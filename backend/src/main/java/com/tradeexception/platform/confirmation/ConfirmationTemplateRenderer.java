package com.tradeexception.platform.confirmation;

import com.tradeexception.platform.confirmation.domain.Confirmation;
import com.tradeexception.platform.trade.domain.TradeParty;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Renders a readable, structured HTML confirmation document for operational display.
 * Clearly marked as synthetic educational output — not a legally binding confirmation.
 */
@Component
public class ConfirmationTemplateRenderer {

    public String renderHtml(Confirmation c) {
        String partiesHtml = renderParties(c.getCounterparties());
        String maturity = c.getMaturityDate() == null ? "N/A (Spot)" : c.getMaturityDate().toString();
        String price = (c.getEconomicTerms() != null && c.getEconomicTerms().price() != null)
                ? c.getEconomicTerms().price().toPlainString() : "N/A";
        String fixedRate = (c.getEconomicTerms() != null && c.getEconomicTerms().fixedRate() != null)
                ? c.getEconomicTerms().fixedRate().toPlainString() : "N/A";
        String docType = (c.getNonEconomicTerms() != null && c.getNonEconomicTerms().documentationType() != null)
                ? escape(c.getNonEconomicTerms().documentationType()) : "N/A";
        String law = (c.getNonEconomicTerms() != null && c.getNonEconomicTerms().governingLaw() != null)
                ? escape(c.getNonEconomicTerms().governingLaw()) : "N/A";

        return """
                <article class="trade-confirmation" data-template="%s" data-version="%s">
                  <header>
                    <h1>Trade Confirmation (%s)</h1>
                    <p class="notice">Synthetic Educational Confirmation — Not Legally Binding</p>
                  </header>
                  <section>
                    <h2>Identifiers</h2>
                    <dl>
                      <dt>Confirmation ID</dt><dd>%s</dd>
                      <dt>Trade ID</dt><dd>%s</dd>
                      <dt>Correlation ID</dt><dd>%s</dd>
                      <dt>Generated At</dt><dd>%s</dd>
                    </dl>
                  </section>
                  <section>
                    <h2>Counterparties</h2>
                    <ul>%s</ul>
                  </section>
                  <section>
                    <h2>Dates &amp; Economics</h2>
                    <dl>
                      <dt>Product Type</dt><dd>%s</dd>
                      <dt>Trade Date</dt><dd>%s</dd>
                      <dt>Effective Date</dt><dd>%s</dd>
                      <dt>Maturity Date</dt><dd>%s</dd>
                      <dt>Notional</dt><dd>%s %s</dd>
                      <dt>Price</dt><dd>%s</dd>
                      <dt>Fixed Rate</dt><dd>%s</dd>
                    </dl>
                  </section>
                  <section>
                    <h2>Non-Economic Terms</h2>
                    <dl>
                      <dt>Documentation</dt><dd>%s</dd>
                      <dt>Governing Law</dt><dd>%s</dd>
                    </dl>
                  </section>
                </article>
                """.formatted(
                escape(c.getTemplateId()),
                escape(c.getTemplateVersion()),
                escape(String.valueOf(c.getProductType())),
                escape(c.getConfirmationId()),
                escape(c.getTradeId()),
                escape(c.getCorrelationId()),
                c.getGeneratedAt() == null ? "" : c.getGeneratedAt().toString(),
                partiesHtml,
                escape(String.valueOf(c.getProductType())),
                c.getTradeDate() == null ? "" : c.getTradeDate().toString(),
                c.getEffectiveDate() == null ? "" : c.getEffectiveDate().toString(),
                maturity,
                c.getNotionalAmount() == null ? "" : c.getNotionalAmount().toPlainString(),
                escape(c.getCurrency()),
                price,
                fixedRate,
                docType,
                law);
    }

    private static String renderParties(List<TradeParty> parties) {
        if (parties == null || parties.isEmpty()) {
            return "<li>None</li>";
        }
        return parties.stream()
                .map(p -> "<li><strong>" + escape(String.valueOf(p.role())) + ":</strong> "
                        + escape(p.partyName()) + " (" + escape(p.partyId()) + ")</li>")
                .collect(Collectors.joining());
    }

    private static String escape(String value) {
        if (value == null) {
            return "";
        }
        return value.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("\"", "&quot;");
    }
}
