package com.tradeexception.platform.security;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tradeexception.platform.common.api.ApiResponse;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.authentication.configuration.AuthenticationConfiguration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;

import java.util.ArrayList;
import java.util.List;

/**
 * Spring Security configuration providing stateless Bearer + HTTP Basic authentication and
 * role-based authorization ({@code ADMIN}, {@code OPERATIONS}, {@code VIEWER}).
 */
@Configuration
@EnableWebSecurity
@EnableConfigurationProperties(SecurityProperties.class)
public class SecurityConfig {

    @Bean
    public PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder();
    }

    @Bean
    public UserDetailsService userDetailsService(SecurityProperties props, PasswordEncoder encoder) {
        List<UserDetails> users = new ArrayList<>();
        registerIfConfigured(users, props.getAdmin(), "ADMIN", encoder);
        registerIfConfigured(users, props.getOperations(), "OPERATIONS", encoder);
        registerIfConfigured(users, props.getViewer(), "VIEWER", encoder);
        return new InMemoryUserDetailsManager(users);
    }

    private static void registerIfConfigured(
            List<UserDetails> users, SecurityProperties.UserSpec spec, String role, PasswordEncoder encoder) {
        if (spec != null && spec.getUsername() != null && !spec.getUsername().isBlank()
                && spec.getPassword() != null && !spec.getPassword().isBlank()) {
            users.add(User.withUsername(spec.getUsername())
                    .password(encoder.encode(spec.getPassword()))
                    .roles(role)
                    .build());
        }
    }

    @Bean
    public AuthenticationManager authenticationManager(AuthenticationConfiguration config) throws Exception {
        return config.getAuthenticationManager();
    }

    @Bean
    public SecurityFilterChain securityFilterChain(
            HttpSecurity http,
            HmacTokenService tokenService,
            ObjectMapper objectMapper) throws Exception {

        return http
                .csrf(csrf -> csrf.disable())
                .cors(Customizer.withDefaults())
                .sessionManagement(sm -> sm.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .httpBasic(Customizer.withDefaults())
                .addFilterBefore(new BearerTokenAuthFilter(tokenService), UsernamePasswordAuthenticationFilter.class)
                .exceptionHandling(eh -> eh
                        .authenticationEntryPoint((req, res, ex) -> {
                            res.setStatus(HttpStatus.UNAUTHORIZED.value());
                            res.setContentType(MediaType.APPLICATION_JSON_VALUE);
                            objectMapper.writeValue(res.getOutputStream(),
                                    ApiResponse.failure("UNAUTHORIZED", "Authentication is required", List.of()));
                        })
                        .accessDeniedHandler((req, res, ex) -> {
                            res.setStatus(HttpStatus.FORBIDDEN.value());
                            res.setContentType(MediaType.APPLICATION_JSON_VALUE);
                            objectMapper.writeValue(res.getOutputStream(),
                                    ApiResponse.failure("FORBIDDEN", "Insufficient role permissions for this operation", List.of()));
                        }))
                .authorizeHttpRequests(auth -> auth
                        .requestMatchers(HttpMethod.OPTIONS, "/**").permitAll()
                        .requestMatchers(HttpMethod.GET, "/api/v1/health", "/actuator/health", "/actuator/info").permitAll()
                        .requestMatchers(HttpMethod.POST, "/api/v1/auth/login").permitAll()
                        .requestMatchers(HttpMethod.POST, "/api/v1/synthetic-trades/**").hasAnyRole("ADMIN", "OPERATIONS")
                        .requestMatchers(HttpMethod.PATCH, "/api/v1/work-items/**").hasAnyRole("ADMIN", "OPERATIONS")
                        .requestMatchers(HttpMethod.POST, "/api/v1/exceptions/*/remediate").hasAnyRole("ADMIN", "OPERATIONS")
                        .requestMatchers(HttpMethod.POST, "/api/v1/dlq/*/replay").hasAnyRole("ADMIN", "OPERATIONS")
                        .requestMatchers(HttpMethod.GET, "/api/v1/**").hasAnyRole("ADMIN", "OPERATIONS", "VIEWER")
                        .anyRequest().hasRole("ADMIN"))
                .build();
    }
}
