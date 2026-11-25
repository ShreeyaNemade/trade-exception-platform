package com.tradeexception.platform.common.couchdb;

import com.tradeexception.platform.common.domain.BaseDocument;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Reusable CouchDB repository base class handling timestamps and common CRUD operations. */
public abstract class AbstractCouchDbRepository<T extends BaseDocument> implements DocumentRepository<T> {

    protected final CouchDbClient client;
    protected final String database;
    protected final Class<T> documentType;

    protected AbstractCouchDbRepository(CouchDbClient client, String database, Class<T> documentType) {
        this.client = client;
        this.database = database;
        this.documentType = documentType;
    }

    @Override
    public T save(T document) {
        if (document == null) {
            throw new IllegalArgumentException("Document must not be null");
        }
        Instant now = Instant.now();
        if (document.getCreatedAt() == null) {
            document.setCreatedAt(now);
        }
        document.setUpdatedAt(now);
        return client.save(database, document);
    }

    @Override
    public Optional<T> findById(String id) {
        if (id == null || id.isBlank()) {
            return Optional.empty();
        }
        return client.findById(database, id, documentType);
    }

    @Override
    public List<T> findAll() {
        return findBySelector(Map.of("type", Map.of("$exists", true)));
    }

    @Override
    public boolean existsById(String id) {
        return findById(id).isPresent();
    }

    @Override
    public void delete(String id, String revision) {
        client.delete(database, id, revision);
    }

    protected List<T> findBySelector(Map<String, Object> selector) {
        return client.findBySelector(database, selector, documentType);
    }
}
