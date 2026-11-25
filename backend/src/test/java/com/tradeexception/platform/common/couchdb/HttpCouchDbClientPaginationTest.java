package com.tradeexception.platform.common.couchdb;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.sun.net.httpserver.HttpServer;
import com.tradeexception.platform.audit.domain.AuditEvent;
import com.tradeexception.platform.audit.domain.EventType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.web.client.RestClient;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class HttpCouchDbClientPaginationTest {

    private HttpServer server;
    private HttpCouchDbClient client;
    private ObjectMapper mapper;
    private AtomicInteger findRequests;

    @BeforeEach
    void setUp() throws IOException {
        mapper = new ObjectMapper().registerModule(new JavaTimeModule());
        findRequests = new AtomicInteger();

        // Build 245 synthetic audit events to force 2 Mango bookmark batches (200 + 45)
        List<AuditEvent> allDocs = new ArrayList<>();
        for (int i = 1; i <= 245; i++) {
            AuditEvent ev = new AuditEvent();
            ev.setEventId(String.format("EVT-%03d", i));
            ev.setEventType(EventType.TRADE_ACCEPTED);
            ev.setEntityType("trade");
            ev.setEntityId("TRD-" + i);
            ev.setCorrelationId("CORR-" + i);
            ev.setActor("system");
            ev.setOccurredAt(Instant.parse("2026-11-24T10:00:00Z").plusSeconds(i));
            allDocs.add(ev);
        }

        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/audit-events/_find", exchange -> {
            findRequests.incrementAndGet();
            JsonNode req = mapper.readTree(exchange.getRequestBody());
            int limit = req.has("limit") ? req.get("limit").asInt() : 25;
            String bookmark = req.has("bookmark") ? req.get("bookmark").asText() : null;

            int offset = (bookmark == null) ? 0 : Integer.parseInt(bookmark);
            int end = Math.min(offset + limit, allDocs.size());
            List<AuditEvent> slice = allDocs.subList(offset, end);

            Map<String, Object> responsePayload = Map.of(
                    "docs", slice,
                    "bookmark", String.valueOf(end)
            );
            byte[] bytes = mapper.writeValueAsBytes(responsePayload);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        });
        server.start();

        int port = server.getAddress().getPort();
        RestClient restClient = RestClient.builder()
                .baseUrl("http://localhost:" + port)
                .build();
        client = new HttpCouchDbClient(restClient, mapper);
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void findBySelectorIteratesAcrossMangoBookmarksWithoutTruncatingAt25() {
        List<AuditEvent> results = client.findBySelector(
                "audit-events",
                Map.of("type", Map.of("$exists", true)),
                AuditEvent.class);

        assertThat(results).hasSize(245);
        assertThat(findRequests.get()).isEqualTo(2);
        assertThat(results.get(0).getEventId()).isEqualTo("EVT-001");
        assertThat(results.get(244).getEventId()).isEqualTo("EVT-245");
    }
}
