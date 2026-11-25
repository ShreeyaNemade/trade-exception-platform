package com.tradeexception.platform.trade.domain;

import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;
import jakarta.validation.constraints.Size;

import java.math.BigDecimal;

/**
 * Economic terms — the fields that determine cash flows and must match on confirmation.
 *
 * @param notionalAmount     required, positive notional
 * @param notionalCurrency   required ISO-4217 style code
 * @param price              optional price (cash/FX products)
 * @param fixedRate          optional fixed rate (swaps); may be negative
 * @param floatingRateIndex  optional floating index name (swaps), synthetic
 * @param settlementCurrency optional settlement currency when different from notional
 */
public record TradeEconomics(
        @NotNull @Positive @Digits(integer = 15, fraction = 2) BigDecimal notionalAmount,
        @NotBlank @Pattern(regexp = "[A-Z]{3}", message = "must be a 3-letter currency code") String notionalCurrency,
        @PositiveOrZero BigDecimal price,
        BigDecimal fixedRate,
        @Size(max = 32) String floatingRateIndex,
        @Pattern(regexp = "[A-Z]{3}", message = "must be a 3-letter currency code") String settlementCurrency) {
}
