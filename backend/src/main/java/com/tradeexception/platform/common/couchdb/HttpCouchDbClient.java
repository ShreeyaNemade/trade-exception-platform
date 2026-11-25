package com.tradeexception.platform.common.couchdb;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tradeexception.platform.common.domain.BaseDocument;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientResponseException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Spring {@link RestClient}-based implementation of {@link CouchDbClient}. */
@Component
public class HttpCouchDbClient implements CouchDbClient {

    private final RestClient restClient;
    private final ObjectMapper objectMapper;

    public HttpCouchDbClient(@Qualifier("couchDbRestClient") RestClient restClient, ObjectMapper objectMapper) {
        this.restClient = restClient;
        this.objectMapper = objectMapper;
    }

    @Override
    public boolean databaseExists(String database) {
        try {
            restClient.head()
                    .uri("/{db}", database)
                    .retrieve()
                    .toBodilessEntity();
            return true;
        } catch (HttpClientErrorException.NotFound ex) {
            return false;
        } catch (ResourceAccessException ex) {
            throw new CouchDbConnectionException("Unable to reach CouchDB while checking database '" + database + "'", ex);
        } catch (RestClientResponseException ex) {
            throw mapHttpError(database, null, null, ex);
        }
    }

    @Override
    public boolean createDatabaseIfAbsent(String database) {
        try {
            restClient.put()
                    .uri("/{db}", database)
                    .retrieve()
                    .toBodilessEntity();
            return true;
        } catch (HttpClientErrorException ex) {
            if (ex.getStatusCode().value() == HttpStatus.PRECONDITION_FAILED.value()) {
                return false; // 412 file_exists: already created
            }
            throw mapHttpError(database, null, null, ex);
        } catch (ResourceAccessException ex) {
            throw new CouchDbConnectionException("Unable to reach CouchDB while creating database '" + database + "'", ex);
        } catch (RestClientResponseException ex) {
            throw mapHttpError(database, null, null, ex);
        }
    }

    @Override
    public <T> Optional<T> findById(String database, String id, Class<T> type) {
        try {
            T doc = restClient.get()
                    .uri("/{db}/{id}", database, id)
                    .retrieve()
                    .body(type);
            return Optional.ofNullable(doc);
        } catch (HttpClientErrorException.NotFound ex) {
            return Optional.empty();
        } catch (ResourceAccessException ex) {
            throw new CouchDbConnectionException("Unable to reach CouchDB while reading '" + id + "'", ex);
        } catch (RestClientResponseException ex) {
            throw mapHttpError(database, id, null, ex);
        }
    }

    @Override
    public <T extends BaseDocument> T save(String database, T document) {
        String id = document.getId();
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("Document id must not be null or blank before saving");
        }
        try {
            WriteResponse response = restClient.put()
                    .uri("/{db}/{id}", database, id)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(document)
                    .retrieve()
                    .body(WriteResponse.class);
            if (response == null || !response.ok()) {
                throw new CouchDbException(HttpStatus.BAD_GATEWAY, "COUCHDB_WRITE_FAILED",
                        "CouchDB did not confirm write for document '" + id + "'");
            }
            document.setRevision(response.rev());
            return document;
        } catch (ResourceAccessException ex) {
            throw new CouchDbConnectionException("Unable to reach CouchDB while saving '" + id + "'", ex);
        } catch (RestClientResponseException ex) {
            throw mapHttpError(database, id, document.getRevision(), ex);
        }
    }

    @Override
    public void delete(String database, String id, String revision) {
        if (revision == null || revision.isBlank()) {
            throw new IllegalArgumentException("Document revision (_rev) is required to delete a CouchDB document");
        }
        try {
            restClient.delete()
                    .uri(uriBuilder -> uriBuilder.path("/{db}/{id}").queryParam("rev", revision).build(database, id))
                    .retrieve()
                    .toBodilessEntity();
        } catch (ResourceAccessException ex) {
            throw new CouchDbConnectionException("Unable to reach CouchDB while deleting '" + id + "'", ex);
        } catch (RestClientResponseException ex) {
            throw mapHttpError(database, id, revision, ex);
        }
    }

    private static final int MANGO_BATCH_SIZE = 200;

    @Override
    public <T> List<T> findBySelector(String database, Map<String, Object> selector, Class<T> type) {
        try {
            List<T> results = new ArrayList<>();
            String bookmark = null;
            while (true) {
                Map<String, Object> requestBody = new java.util.LinkedHashMap<>();
                requestBody.put("selector", selector);
                requestBody.put("limit", MANGO_BATCH_SIZE);
                if (bookmark != null && !bookmark.isBlank()) {
                    requestBody.put("bookmark", bookmark);
                }

                JsonNode root = restClient.post()
                        .uri("/{db}/_find", database)
                        .contentType(MediaType.APPLICATION_JSON)
                        .body(requestBody)
                        .retrieve()
                        .body(JsonNode.class);
                if (root == null || !root.has("docs") || !root.get("docs").isArray()) {
                    break;
                }
                JsonNode docs = root.get("docs");
                for (JsonNode node : docs) {
                    results.add(objectMapper.treeToValue(node, type));
                }

                String nextBookmark = root.hasNonNull("bookmark") ? root.get("bookmark").asText() : null;
                if (docs.size() < MANGO_BATCH_SIZE
                        || nextBookmark == null
                        || nextBookmark.isBlank()
                        || nextBookmark.equals(bookmark)) {
                    break;
                }
                bookmark = nextBookmark;
            }
            return results;
        } catch (JsonProcessingException ex) {
            throw new CouchDbException(HttpStatus.INTERNAL_SERVER_ERROR, "COUCHDB_DESERIALIZATION_ERROR",
                    "Failed to deserialize CouchDB query results for database '" + database + "'", ex);
        } catch (ResourceAccessException ex) {
            throw new CouchDbConnectionException("Unable to reach CouchDB while querying '" + database + "'", ex);
        } catch (RestClientResponseException ex) {
            throw mapHttpError(database, null, null, ex);
        }
    }

    @Override
    public void ensureIndex(String database, String indexName, List<String> fields) {
        Map<String, Object> payload = Map.of(
                "index", Map.of("fields", fields),
                "name", indexName,
                "type", "json"
        );
        try {
            restClient.post()
                    .uri("/{db}/_index", database)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(payload)
                    .retrieve()
                    .toBodilessEntity();
        } catch (ResourceAccessException ex) {
            throw new CouchDbConnectionException("Unable to reach CouchDB while creating index '" + indexName + "'", ex);
        } catch (RestClientResponseException ex) {
            throw mapHttpError(database, indexName, null, ex);
        }
    }

    private CouchDbException mapHttpError(String database, String documentId, String revision, RestClientResponseException ex) {
        int status = ex.getStatusCode().value();
        if (status == HttpStatus.CONFLICT.value()) {
            return new CouchDbConflictException(database, documentId, revision, ex);
        }
        if (status == HttpStatus.NOT_FOUND.value()) {
            return new CouchDbNotFoundException(database, documentId, ex);
        }
        if (status == HttpStatus.UNAUTHORIZED.value() || status == HttpStatus.FORBIDDEN.value()) {
            return new CouchDbException(HttpStatus.BAD_GATEWAY, "COUCHDB_AUTH_ERROR",
                    "CouchDB authentication or authorization failed for database '" + database + "'", ex);
        }
        return new CouchDbException(HttpStatus.BAD_GATEWAY, "COUCHDB_ERROR",
                "CouchDB returned HTTP " + status + " for database '" + database + "'", ex);
    }

    private record WriteResponse(boolean ok, String id, String rev) {
    }
}
