package com.tradeexception.platform.security;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Optional;

/**
 * Lightweight HMAC-SHA256 bearer token issuer and verifier using standard JDK cryptography.
 * Uses {@code APP_AUTH_SECRET} when configured, or an ephemeral random key per JVM instance.
 */
@Component
public class HmacTokenService {

    private static final Logger log = LoggerFactory.getLogger(HmacTokenService.class);
    private static final String HMAC_ALG = "HmacSHA256";

    private final byte[] secretKey;
    private final long ttlSeconds;
    private final ObjectMapper objectMapper;

    public HmacTokenService(SecurityProperties properties, ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        this.ttlSeconds = Math.max(60, properties.getTokenTtlSeconds());
        String configured = properties.getTokenSecret();
        if (configured != null && !configured.isBlank()) {
            this.secretKey = configured.getBytes(StandardCharsets.UTF_8);
        } else {
            byte[] random = new byte[32];
            new SecureRandom().nextBytes(random);
            this.secretKey = random;
            log.warn("APP_AUTH_SECRET not set; using ephemeral per-instance signing key");
        }
    }

    public IssuedToken issue(String username, List<String> roles) {
        Instant expiresAt = Instant.now().plusSeconds(ttlSeconds);
        TokenPayload payload = new TokenPayload(username, List.copyOf(roles), expiresAt.getEpochSecond());
        try {
            String encodedPayload = Base64.getUrlEncoder().withoutPadding()
                    .encodeToString(objectMapper.writeValueAsBytes(payload));
            String signature = sign(encodedPayload);
            return new IssuedToken(encodedPayload + "." + signature, "Bearer", username, payload.roles(), expiresAt);
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to issue authentication token", ex);
        }
    }

    public Optional<TokenPayload> verify(String token) {
        if (token == null || token.isBlank()) {
            return Optional.empty();
        }
        String[] parts = token.split("\\.");
        if (parts.length != 2) {
            return Optional.empty();
        }
        try {
            String expectedSig = sign(parts[0]);
            if (!MessageDigest.isEqual(
                    expectedSig.getBytes(StandardCharsets.UTF_8),
                    parts[1].getBytes(StandardCharsets.UTF_8))) {
                return Optional.empty();
            }
            byte[] json = Base64.getUrlDecoder().decode(parts[0]);
            TokenPayload payload = objectMapper.readValue(json, TokenPayload.class);
            if (payload.exp() <= Instant.now().getEpochSecond()) {
                return Optional.empty();
            }
            return Optional.of(payload);
        } catch (Exception ex) {
            return Optional.empty();
        }
    }

    private String sign(String data) throws Exception {
        Mac mac = Mac.getInstance(HMAC_ALG);
        mac.init(new SecretKeySpec(secretKey, HMAC_ALG));
        return Base64.getUrlEncoder().withoutPadding()
                .encodeToString(mac.doFinal(data.getBytes(StandardCharsets.UTF_8)));
    }

    public record TokenPayload(String sub, List<String> roles, long exp) {}
    public record IssuedToken(String accessToken, String tokenType, String username, List<String> roles, Instant expiresAt) {}
}
