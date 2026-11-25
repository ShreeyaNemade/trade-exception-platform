package com.tradeexception.platform.exception.api;

import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;
import jakarta.validation.constraints.Size;

import java.math.BigDecimal;

/** Request payload for {@code POST /api/v1/exceptions/{exceptionId}/remediate}. */
public record RemediateExceptionRequest(
        @Positive @Digits(integer = 15, fraction = 2) BigDecimal notionalAmount,
        @Pattern(regexp = "[A-Z]{3}", message = "must be a 3-letter uppercase currency code") String notionalCurrency,
        @PositiveOrZero BigDecimal price,
        BigDecimal fixedRate,
        @Size(max = 32) String floatingRateIndex,
        @Pattern(regexp = "[A-Z]{3}", message = "must be a 3-letter uppercase currency code") String settlementCurrency,
        @Size(max = 500) String remediationNote) {
}
