package com.tradeexception.platform.classification;

import com.tradeexception.platform.exception.domain.ExceptionCategory;
import com.tradeexception.platform.exception.domain.ExceptionPriority;
import com.tradeexception.platform.exception.domain.ExceptionRecord;
import com.tradeexception.platform.exception.domain.ExceptionSeverity;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Locale;

/**
 * Deterministic rule-based exception classifier. Maps STP reason codes and exception metadata
 * to category, root cause, priority, severity, explanation, and recommended remediation action.
 * No machine learning or arbitrary numeric risk scores are used.
 */
@Component
public class ExceptionClassifier {

    public ClassificationResult classify(ExceptionRecord record) {
        String code = record == null || record.getReasonCode() == null
                ? ""
                : record.getReasonCode().trim().toUpperCase(Locale.ROOT);
        Instant now = Instant.now();

        return switch (code) {
            case "MISSING_MANDATORY_IDENTIFIERS", "INCOMPLETE_NON_ECONOMIC_TERMS", "MISSING_DATA" ->
                    new ClassificationResult(
                            ExceptionCategory.MISSING_TRADE_DATA,
                            "Mandatory trade identifiers or required legal/non-economic fields are absent.",
                            buildExplanation("Missing Trade Data", code, record),
                            code.equals("MISSING_MANDATORY_IDENTIFIERS") ? ExceptionPriority.HIGH : ExceptionPriority.MEDIUM,
                            code.equals("MISSING_MANDATORY_IDENTIFIERS") ? ExceptionSeverity.HIGH : ExceptionSeverity.MEDIUM,
                            "Verify source booking payload, request missing trade fields from upstream OMS, and re-submit.",
                            now);

            case "COUNTERPARTY_NOT_FOUND", "COUNTERPARTY_NOT_STP_ELIGIBLE", "UNKNOWN_LEI" ->
                    new ClassificationResult(
                            ExceptionCategory.INVALID_COUNTERPARTY,
                            code.equals("COUNTERPARTY_NOT_STP_ELIGIBLE")
                                    ? "Counterparty is restricted and ineligible for automated STP."
                                    : "Counterparty identifier does not exist in active reference data.",
                            buildExplanation("Invalid or Restricted Counterparty", code, record),
                            code.equals("COUNTERPARTY_NOT_FOUND") ? ExceptionPriority.HIGH : ExceptionPriority.MEDIUM,
                            code.equals("COUNTERPARTY_NOT_FOUND") ? ExceptionSeverity.HIGH : ExceptionSeverity.MEDIUM,
                            "Confirm counterparty LEI onboarding and STP eligibility with Counterparty Operations.",
                            now);

            case "PRODUCT_CONFIG_MISSING", "PRODUCT_MANUAL_REVIEW_ONLY" ->
                    new ClassificationResult(
                            ExceptionCategory.UNSUPPORTED_PRODUCT,
                            code.equals("PRODUCT_MANUAL_REVIEW_ONLY")
                                    ? "Product type is configured for manual confirmation review only."
                                    : "Product configuration is missing from active reference data.",
                            buildExplanation("Unsupported or Manual-Only Product", code, record),
                            ExceptionPriority.MEDIUM,
                            ExceptionSeverity.MEDIUM,
                            "Review product eligibility configuration with Product Control and process manually if approved.",
                            now);

            case "CURRENCY_UNSUPPORTED" ->
                    new ClassificationResult(
                            ExceptionCategory.CURRENCY_MISMATCH,
                            "Notional or settlement currency is not supported for automated STP settlement.",
                            buildExplanation("Unsupported Currency / Currency Mismatch", code, record),
                            ExceptionPriority.HIGH,
                            ExceptionSeverity.HIGH,
                            "Verify trade and settlement currencies against supported currency reference data.",
                            now);

            case "MISSING_ECONOMIC_FIELDS", "INVALID_ECONOMIC_TERMS", "MISSING_PRODUCT_SPECIFIC_TERMS", "ECONOMIC_MISMATCH" ->
                    new ClassificationResult(
                            ExceptionCategory.INVALID_ECONOMIC_TERMS,
                            "Core economic terms (notional, price, rate, or product-specific fields) failed validation.",
                            buildExplanation("Invalid Economic Terms", code, record),
                            ExceptionPriority.CRITICAL,
                            ExceptionSeverity.CRITICAL,
                            "Escalate immediately to Product Control to reconcile economic terms with front-office booking.",
                            now);

            case "MISSING_TRADE_DATES", "INVALID_DATE_SEQUENCE" ->
                    new ClassificationResult(
                            ExceptionCategory.DATE_INCONSISTENCY,
                            "Trade, effective, or maturity dates are missing or violate chronological sequence.",
                            buildExplanation("Date Inconsistency", code, record),
                            ExceptionPriority.HIGH,
                            ExceptionSeverity.HIGH,
                            "Confirm tradeDate <= effectiveDate <= maturityDate with booking desk and correct trade dates.",
                            now);

            case "INSTRUMENT_NOT_FOUND", "INVALID_REFERENCE_DATA" ->
                    new ClassificationResult(
                            ExceptionCategory.MISSING_REFERENCE_DATA,
                            "Derived financial instrument or required static reference data is missing.",
                            buildExplanation("Missing Reference Data", code, record),
                            ExceptionPriority.HIGH,
                            ExceptionSeverity.HIGH,
                            "Onboard missing instrument in reference-data store and trigger replay once active.",
                            now);

            case "CONFIRMATION_TEMPLATE_UNAVAILABLE", "CONFIRMATION_TEMPLATE_MISSING" ->
                    new ClassificationResult(
                            ExceptionCategory.CONFIRMATION_TEMPLATE_MISSING,
                            "No active confirmation template is available for this product.",
                            buildExplanation("Confirmation Template Missing", code, record),
                            ExceptionPriority.MEDIUM,
                            ExceptionSeverity.MEDIUM,
                            "Draft manual confirmation with Confirmation Operations and activate template if recurring.",
                            now);

            default ->
                    new ClassificationResult(
                            ExceptionCategory.OTHER,
                            code.isBlank()
                                    ? "Exception arrived without a recognized reason code."
                                    : "Unmapped validation reason code '" + code + "'.",
                            buildExplanation("Unmapped Exception", code.isBlank() ? "NONE" : code, record),
                            ExceptionPriority.MEDIUM,
                            ExceptionSeverity.MEDIUM,
                            "Inspect trade payload and validation history with Trade Operations to determine root cause.",
                            now);
        };
    }

    private static String buildExplanation(String label, String code, ExceptionRecord record) {
        String original = (record != null && record.getDescription() != null && !record.getDescription().isBlank())
                ? " Details: " + record.getDescription()
                : "";
        return label + " (reasonCode=" + code + ")." + original;
    }

    public record ClassificationResult(
            ExceptionCategory category,
            String rootCause,
            String explanation,
            ExceptionPriority priority,
            ExceptionSeverity severity,
            String recommendedAction,
            Instant classifiedAt) {
    }
}
