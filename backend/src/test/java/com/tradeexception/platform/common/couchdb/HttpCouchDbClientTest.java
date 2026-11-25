package com.tradeexception.platform.common.couchdb;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.tradeexception.platform.confirmation.domain.Confirmation;
import com.tradeexception.platform.confirmation.domain.ConfirmationStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

class HttpCouchDbClientTest {

    private MockRestServiceServer server;
    private HttpCouchDbClient client;

    @BeforeEach
    void setUp() {
        RestClient.Builder builder = RestClient.builder().baseUrl("http://couchdb.test:5984");
        server = MockRestServiceServer.bindTo(builder).build();
        ObjectMapper mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        client = new HttpCouchDbClient(builder.build(), mapper);
    }

    @Test
    void databaseExistsReturnsTrueOn200AndFalseOn404() {
        server.expect(requestTo("http://couchdb.test:5984/trades"))
                .andExpect(method(HttpMethod.HEAD))
                .andRespond(withSuccess());
        server.expect(requestTo("http://couchdb.test:5984/missing"))
                .andExpect(method(HttpMethod.HEAD))
                .andRespond(withStatus(HttpStatus.NOT_FOUND));

        assertThat(client.databaseExists("trades")).isTrue();
        assertThat(client.databaseExists("missing")).isFalse();
        server.verify();
    }

    @Test
    void createDatabaseIfAbsentHandles201And412Idempotently() {
        server.expect(requestTo("http://couchdb.test:5984/trades"))
                .andExpect(method(HttpMethod.PUT))
                .andRespond(withStatus(HttpStatus.CREATED).body("{\"ok\":true}").contentType(MediaType.APPLICATION_JSON));
        server.expect(requestTo("http://couchdb.test:5984/trades"))
                .andExpect(method(HttpMethod.PUT))
                .andRespond(withStatus(HttpStatus.PRECONDITION_FAILED));

        assertThat(client.createDatabaseIfAbsent("trades")).isTrue();
        assertThat(client.createDatabaseIfAbsent("trades")).isFalse();
        server.verify();
    }

    @Test
    void saveUpdatesRevisionAndMapsConflictTo409Exception() {
        Confirmation doc = new Confirmation();
        doc.setConfirmationId("CNF-1");
        doc.setTradeId("TRD-1");
        doc.setCorrelationId("CORR-1");
        doc.setStatus(ConfirmationStatus.GENERATED);

        server.expect(requestTo("http://couchdb.test:5984/confirmations/confirmation%3ACNF-1"))
                .andExpect(method(HttpMethod.PUT))
                .andRespond(withStatus(HttpStatus.CREATED)
                        .contentType(MediaType.APPLICATION_JSON)
                        .body("{\"ok\":true,\"id\":\"confirmation:CNF-1\",\"rev\":\"1-abc\"}"));
        server.expect(requestTo("http://couchdb.test:5984/confirmations/confirmation%3ACNF-1"))
                .andExpect(method(HttpMethod.PUT))
                .andRespond(withStatus(HttpStatus.CONFLICT));

        Confirmation saved = client.save("confirmations", doc);
        assertThat(saved.getRevision()).isEqualTo("1-abc");

        assertThatThrownBy(() -> client.save("confirmations", saved))
                .isInstanceOf(CouchDbConflictException.class)
                .hasMessageContaining("confirmation:CNF-1");
        server.verify();
    }

    @Test
    void findByIdReturnsEmptyOn404AndDocumentOn200() {
        server.expect(requestTo("http://couchdb.test:5984/confirmations/confirmation%3AMISSING"))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withStatus(HttpStatus.NOT_FOUND));
        server.expect(requestTo("http://couchdb.test:5984/confirmations/confirmation%3ACNF-1"))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess("""
                        {"_id":"confirmation:CNF-1","_rev":"1-abc","type":"confirmation",
                         "confirmationId":"CNF-1","tradeId":"TRD-1","correlationId":"CORR-1","status":"SENT"}
                        """, MediaType.APPLICATION_JSON));

        assertThat(client.findById("confirmations", "confirmation:MISSING", Confirmation.class)).isEmpty();
        Optional<Confirmation> found = client.findById("confirmations", "confirmation:CNF-1", Confirmation.class);
        assertThat(found).isPresent();
        assertThat(found.get().getStatus()).isEqualTo(ConfirmationStatus.SENT);
        server.verify();
    }

    @Test
    void deletePassesRevisionAndMapsNotFound() {
        server.expect(requestTo("http://couchdb.test:5984/confirmations/confirmation%3ACNF-1?rev=1-abc"))
                .andExpect(method(HttpMethod.DELETE))
                .andRespond(withSuccess("{\"ok\":true}", MediaType.APPLICATION_JSON));
        server.expect(requestTo("http://couchdb.test:5984/confirmations/confirmation%3ACNF-2?rev=1-zzz"))
                .andExpect(method(HttpMethod.DELETE))
                .andRespond(withStatus(HttpStatus.NOT_FOUND));

        client.delete("confirmations", "confirmation:CNF-1", "1-abc");
        assertThatThrownBy(() -> client.delete("confirmations", "confirmation:CNF-2", "1-zzz"))
                .isInstanceOf(CouchDbNotFoundException.class);
        server.verify();
    }

    @Test
    void findBySelectorDeserializesMangoDocs() {
        server.expect(requestTo("http://couchdb.test:5984/confirmations/_find"))
                .andExpect(method(HttpMethod.POST))
                .andRespond(withSuccess("""
                        {"docs":[{"_id":"confirmation:CNF-1","_rev":"1-a","type":"confirmation",
                          "confirmationId":"CNF-1","tradeId":"TRD-1","correlationId":"CORR-1","status":"GENERATED"}]}
                        """, MediaType.APPLICATION_JSON));

        List<Confirmation> results = client.findBySelector("confirmations", Map.of("tradeId", "TRD-1"), Confirmation.class);
        assertThat(results).hasSize(1);
        assertThat(results.getFirst().getConfirmationId()).isEqualTo("CNF-1");
        server.verify();
    }

    @Test
    void connectionFailureWrapsInCouchDbConnectionException() {
        server.expect(requestTo("http://couchdb.test:5984/trades"))
                .andExpect(method(HttpMethod.HEAD))
                .andRespond(req -> {
                    throw new ResourceAccessException("Connection refused");
                });

        assertThatThrownBy(() -> client.databaseExists("trades"))
                .isInstanceOf(CouchDbConnectionException.class);
    }
}
