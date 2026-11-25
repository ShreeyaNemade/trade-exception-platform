package com.tradeexception.platform.common.couchdb;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;

/** Configures the HTTP {@link RestClient} used for CouchDB communication. */
@Configuration
@EnableConfigurationProperties(CouchDbProperties.class)
public class CouchDbConfig {

    @Bean("couchDbRestClient")
    public RestClient couchDbRestClient(RestClient.Builder builder, CouchDbProperties properties) {
        HttpClient httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(properties.getConnectTimeoutMs()))
                .build();

        JdkClientHttpRequestFactory requestFactory = new JdkClientHttpRequestFactory(httpClient);
        requestFactory.setReadTimeout(Duration.ofMillis(properties.getReadTimeoutMs()));

        RestClient.Builder configured = builder
                .requestFactory(requestFactory);
        return applyDefaults(configured, properties).build();
    }

    static RestClient.Builder applyDefaults(RestClient.Builder builder, CouchDbProperties properties) {
        builder.baseUrl(properties.getUrl())
                .defaultHeader(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE);

        if (properties.getUsername() != null && !properties.getUsername().isBlank()) {
            String raw = properties.getUsername() + ":" + (properties.getPassword() == null ? "" : properties.getPassword());
            String encoded = Base64.getEncoder().encodeToString(raw.getBytes(StandardCharsets.UTF_8));
            builder.defaultHeader(HttpHeaders.AUTHORIZATION, "Basic " + encoded);
        }
        return builder;
    }
}
