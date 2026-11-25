package com.tradeexception.platform.common.couchdb;

import com.tradeexception.platform.common.domain.BaseDocument;

import java.util.List;
import java.util.Optional;

/** Persistence contract shared by CouchDB-backed module repositories. */
public interface DocumentRepository<T extends BaseDocument> {

    /** Persists a new document or updates an existing revision; returns the document with its new {@code _rev}. */
    T save(T document);

    Optional<T> findById(String id);

    List<T> findAll();

    boolean existsById(String id);

    /** Deletes a specific revision of a document. */
    void delete(String id, String revision);
}
