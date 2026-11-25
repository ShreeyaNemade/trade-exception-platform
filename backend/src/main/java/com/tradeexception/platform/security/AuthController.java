package com.tradeexception.platform.security;

import com.tradeexception.platform.audit.domain.AuditEvent;
import com.tradeexception.platform.audit.domain.EventType;
import com.tradeexception.platform.audit.repository.AuditEventRepository;
import com.tradeexception.platform.common.api.ApiResponse;
import com.tradeexception.platform.common.couchdb.CouchDbException;
import com.tradeexception.platform.security.HmacTokenService.IssuedToken;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Issues signed HMAC-SHA256 bearer tokens for configured synthetic roles. */
@RestController
@RequestMapping("/api/v1/auth")
public class AuthController {

    private static final Logger log = LoggerFactory.getLogger(AuthController.class);

    private final AuthenticationManager authenticationManager;
    private final HmacTokenService tokenService;
    private final AuditEventRepository auditEventRepository;

    public AuthController(
            AuthenticationManager authenticationManager,
            HmacTokenService tokenService,
            AuditEventRepository auditEventRepository) {
        this.authenticationManager = authenticationManager;
        this.tokenService = tokenService;
        this.auditEventRepository = auditEventRepository;
    }

    @PostMapping("/login")
    public ResponseEntity<ApiResponse<IssuedToken>> login(@Valid @RequestBody LoginRequest request) {
        try {
            Authentication auth = authenticationManager.authenticate(
                    new UsernamePasswordAuthenticationToken(request.username(), request.password()));
            List<String> roles = auth.getAuthorities().stream()
                    .map(GrantedAuthority::getAuthority)
                    .map(a -> a.startsWith("ROLE_") ? a.substring(5) : a)
                    .toList();
            IssuedToken token = tokenService.issue(auth.getName(), roles);
            recordAuthAudit(auth.getName(), roles);
            return ResponseEntity.ok(ApiResponse.ok(token));
        } catch (BadCredentialsException ex) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED)
                    .body(ApiResponse.failure("INVALID_CREDENTIALS", "Invalid username or password", List.of()));
        }
    }

    private void recordAuthAudit(String username, List<String> roles) {
        AuditEvent audit = new AuditEvent();
        audit.setEventId("auth-" + UUID.randomUUID());
        audit.setEventType(EventType.AUTHENTICATION_SUCCESS);
        audit.setEntityId(username);
        audit.setEntityType("user");
        audit.setCorrelationId("AUTH-" + UUID.randomUUID());
        audit.setActor(username);
        audit.setOccurredAt(Instant.now());
        audit.setDetails(Map.of("roles", String.join(",", roles)));
        try {
            auditEventRepository.save(audit);
        } catch (CouchDbException ex) {
            log.debug("Auth audit skipped because CouchDB is unavailable: {}", ex.getMessage());
        }
    }

    public record LoginRequest(@NotBlank String username, @NotBlank String password) {}
}
