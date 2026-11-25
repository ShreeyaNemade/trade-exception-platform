package com.tradeexception.platform.common.couchdb;

import com.tradeexception.platform.common.domain.BaseDocument;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Low-level client abstraction over CouchDB's HTTP document API. */
public interface CouchDbClient {

    boolean databaseExists(String database);

    /**
     * Creates the database if it does not already exist.
     *
     * @return {@code true} if newly created, {@code false} if it already existed
     */
    boolean createDatabaseIfAbsent(String database);

    <T> Optional<T> findById(String database, String id, Class<T> type);

    /**
     * Inserts or updates a document using its deterministic {@code _id} and MVCC {@code _rev}.
     * Updates the document's {@code revision} with the new {@code _rev} returned by CouchDB.
     */
    <T extends BaseDocument> T save(String database, T document);

    void delete(String database, String id, String revision);

    /** Executes a Mango {@code _find} query using the given selector map. */
    <T> List<T> findBySelector(String database, Map<String, Object> selector, Class<T> type);

    /** Ensures a JSON Mango index exists on the given fields (idempotent in CouchDB). */
    void ensureIndex(String database, String indexName, List<String> fields);
}
