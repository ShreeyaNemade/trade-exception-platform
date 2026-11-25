package com.tradeexception.platform.enrichment;

import com.tradeexception.platform.common.couchdb.CouchDbConflictException;
import com.tradeexception.platform.common.couchdb.CouchDbException;
import com.tradeexception.platform.enrichment.domain.ReferenceData;
import com.tradeexception.platform.enrichment.domain.ReferenceDataCategory;
import com.tradeexception.platform.enrichment.repository.ReferenceDataRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * Idempotently seeds synthetic educational reference data into CouchDB ({@code reference-data}).
 * Existing records are never overwritten so manual operational updates are preserved.
 */
@Component
@Order(20)
@EnableConfigurationProperties(ReferenceDataProperties.class)
public class ReferenceDataSeeder implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(ReferenceDataSeeder.class);

    private final ReferenceDataRepository repository;
    private final ReferenceDataProperties properties;

    public ReferenceDataSeeder(ReferenceDataRepository repository, ReferenceDataProperties properties) {
        this.repository = repository;
        this.properties = properties;
    }

    @Override
    public void run(ApplicationArguments args) {
        if (!properties.isAutoSeed()) {
            log.info("Reference data auto-seeding is disabled");
            return;
        }
        try {
            int inserted = seedAll();
            log.info("Reference data seeding complete (newly inserted: {})", inserted);
        } catch (CouchDbException ex) {
            if (properties.isFailFast()) {
                throw ex;
            }
            log.warn("Reference data seeding skipped because CouchDB is unavailable: {}", ex.getMessage());
        }
    }

    /** Seeds all synthetic reference records that do not yet exist. Returns count of newly inserted records. */
    public int seedAll() {
        int inserted = 0;
        for (ReferenceData item : defaultCatalog()) {
            if (repository.findByCategoryAndCode(item.getCategory(), item.getCode()).isPresent()) {
                continue; // Preserve existing / manually updated records
            }
            try {
                repository.save(item);
                inserted++;
            } catch (CouchDbConflictException ex) {
                log.debug("Reference data '{}' already exists (concurrent seed)", item.getId());
            }
        }
        return inserted;
    }

    /** Returns the synthetic educational reference data catalog. */
    public static List<ReferenceData> defaultCatalog() {
        return List.of(
                // Supported currencies
                ref(ReferenceDataCategory.CURRENCY, "USD", "US Dollar", Map.of("decimals", "2", "stpSupported", "true")),
                ref(ReferenceDataCategory.CURRENCY, "EUR", "Euro", Map.of("decimals", "2", "stpSupported", "true")),
                ref(ReferenceDataCategory.CURRENCY, "GBP", "British Pound Sterling", Map.of("decimals", "2", "stpSupported", "true")),
                ref(ReferenceDataCategory.CURRENCY, "JPY", "Japanese Yen", Map.of("decimals", "0", "stpSupported", "true")),
                ref(ReferenceDataCategory.CURRENCY, "CHF", "Swiss Franc", Map.of("decimals", "2", "stpSupported", "true")),

                // Synthetic counterparties
                ref(ReferenceDataCategory.COUNTERPARTY, "SYNTHBANK00000000001", "Synthetic Bank Alpha",
                        Map.of("eligible", "true", "jurisdiction", "US", "leiStatus", "ACTIVE")),
                ref(ReferenceDataCategory.COUNTERPARTY, "SYNTHCORP00000000002", "Synthetic Corp Beta",
                        Map.of("eligible", "true", "jurisdiction", "UK", "leiStatus", "ACTIVE")),
                ref(ReferenceDataCategory.COUNTERPARTY, "SYNTHFUND00000000003", "Synthetic Fund Gamma",
                        Map.of("eligible", "false", "jurisdiction", "EU", "leiStatus", "RESTRICTED")),

                // Synthetic instruments
                ref(ReferenceDataCategory.INSTRUMENT, "FX_SPOT:USD/EUR", "FX Spot USD/EUR",
                        Map.of("assetClass", "FX", "settlementDays", "2")),
                ref(ReferenceDataCategory.INSTRUMENT, "FX_SPOT:GBP/USD", "FX Spot GBP/USD",
                        Map.of("assetClass", "FX", "settlementDays", "2")),
                ref(ReferenceDataCategory.INSTRUMENT, "FX_SPOT:USD/JPY", "FX Spot USD/JPY",
                        Map.of("assetClass", "FX", "settlementDays", "2")),
                ref(ReferenceDataCategory.INSTRUMENT, "FX_FORWARD:GBP/USD", "FX Forward GBP/USD",
                        Map.of("assetClass", "FX", "settlementDays", "2")),
                ref(ReferenceDataCategory.INSTRUMENT, "FX_FORWARD:USD/EUR", "FX Forward USD/EUR",
                        Map.of("assetClass", "FX", "settlementDays", "2")),
                ref(ReferenceDataCategory.INSTRUMENT, "INTEREST_RATE_SWAP:USD/SOFR", "USD SOFR Interest Rate Swap",
                        Map.of("assetClass", "RATES", "settlementDays", "2")),
                ref(ReferenceDataCategory.INSTRUMENT, "BOND:USD", "Synthetic USD Fixed Rate Bond",
                        Map.of("assetClass", "FIXED_INCOME", "settlementDays", "2")),

                // Product configurations & STP eligibility
                ref(ReferenceDataCategory.PRODUCT, "FX_SPOT", "Foreign Exchange Spot",
                        Map.of("stpEligible", "true", "confirmationTemplateId", "TMPL_FX_SPOT_V1", "requireNonEconomicTerms", "false")),
                ref(ReferenceDataCategory.PRODUCT, "FX_FORWARD", "Foreign Exchange Forward",
                        Map.of("stpEligible", "true", "confirmationTemplateId", "TMPL_FX_FORWARD_V1", "requireNonEconomicTerms", "true")),
                ref(ReferenceDataCategory.PRODUCT, "INTEREST_RATE_SWAP", "Interest Rate Swap",
                        Map.of("stpEligible", "true", "confirmationTemplateId", "TMPL_IRS_V1", "requireNonEconomicTerms", "true")),
                ref(ReferenceDataCategory.PRODUCT, "BOND", "Fixed Income Bond",
                        Map.of("stpEligible", "true", "confirmationTemplateId", "TMPL_BOND_V1", "requireNonEconomicTerms", "false")),
                ref(ReferenceDataCategory.PRODUCT, "EQUITY_SWAP", "Equity Swap",
                        Map.of("stpEligible", "false", "confirmationTemplateId", "TMPL_EQ_SWAP_V1", "requireNonEconomicTerms", "true")),
                ref(ReferenceDataCategory.PRODUCT, "CREDIT_DEFAULT_SWAP", "Credit Default Swap",
                        Map.of("stpEligible", "false", "confirmationTemplateId", "TMPL_CDS_V1", "requireNonEconomicTerms", "true")),

                // Confirmation template availability
                ref(ReferenceDataCategory.CONFIRMATION_TEMPLATE, "TMPL_FX_SPOT_V1", "FX Spot FpML Confirmation Template",
                        Map.of("available", "true", "format", "FPML_JSON")),
                ref(ReferenceDataCategory.CONFIRMATION_TEMPLATE, "TMPL_FX_FORWARD_V1", "FX Forward FpML Confirmation Template",
                        Map.of("available", "true", "format", "FPML_JSON")),
                ref(ReferenceDataCategory.CONFIRMATION_TEMPLATE, "TMPL_IRS_V1", "Interest Rate Swap FpML Confirmation Template",
                        Map.of("available", "true", "format", "FPML_JSON")),
                ref(ReferenceDataCategory.CONFIRMATION_TEMPLATE, "TMPL_BOND_V1", "Bond Confirmation Template",
                        Map.of("available", "true", "format", "FPML_JSON")),
                ref(ReferenceDataCategory.CONFIRMATION_TEMPLATE, "TMPL_CDS_V1", "CDS Confirmation Template (Pending Legal Sign-off)",
                        Map.of("available", "false", "format", "FPML_JSON"))
        );
    }

    private static ReferenceData ref(ReferenceDataCategory category, String code, String description, Map<String, String> attributes) {
        ReferenceData data = new ReferenceData();
        data.setCategory(category);
        data.setCode(code);
        data.setDescription(description);
        data.setAttributes(attributes);
        data.setActive(true);
        return data;
    }
}
