package com.tradeexception.platform.common.kafka;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

import java.time.Instant;
import java.util.UUID;

/**
 * Standard JSON envelope for all Kafka events across the platform.
 *
 * @param eventId       unique identifier for this event (used for idempotency and tracing)
 * @param eventType     PascalCase past-tense event name, e.g. {@code TradeReceived}, {@code TradeAccepted}
 * @param correlationId end-to-end identifier carried unchanged across downstream events
 * @param schemaVersion contract version in {@code MAJOR.MINOR} format
 * @param timestamp     UTC instant when the event was created
 * @param source        originating module or synthetic system
 * @param payload       domain payload
 */
public record EventEnvelope<T>(
        @NotBlank @Size(max = 64) String eventId,
        @NotBlank @Size(max = 64) String eventType,
        @NotBlank @Size(max = 64) String correlationId,
        @NotBlank @Pattern(regexp = "\\d+\\.\\d+", message = "must look like MAJOR.MINOR") String schemaVersion,
        @NotNull Instant timestamp,
        @NotBlank @Size(max = 64) String source,
        @NotNull @Valid T payload) {

    public static <T> EventEnvelope<T> of(
            String eventType,
            String correlationId,
            String schemaVersion,
            String source,
            T payload) {
        return new EventEnvelope<>(
                UUID.randomUUID().toString(),
                eventType,
                correlationId,
                schemaVersion,
                Instant.now(),
                source,
                payload);
    }
}
