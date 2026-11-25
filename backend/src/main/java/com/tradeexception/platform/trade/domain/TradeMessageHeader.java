package com.tradeexception.platform.trade.domain;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

import java.time.Instant;

/**
 * Message envelope metadata (FpML-style header).
 *
 * @param messageId     unique id of the inbound message (deduplication)
 * @param correlationId id that follows the trade through every downstream event
 * @param schemaVersion version of the inbound message contract, e.g. {@code 1.0}
 * @param sourceSystem  synthetic upstream booking system that sent the message
 * @param sentAt        time the source system sent the message
 */
public record TradeMessageHeader(
        @NotBlank @Size(max = 64) String messageId,
        @NotBlank @Size(max = 64) String correlationId,
        @NotBlank @Pattern(regexp = "\\d+\\.\\d+", message = "must look like MAJOR.MINOR") String schemaVersion,
        @NotBlank @Size(max = 32) String sourceSystem,
        @NotNull Instant sentAt) {
}
