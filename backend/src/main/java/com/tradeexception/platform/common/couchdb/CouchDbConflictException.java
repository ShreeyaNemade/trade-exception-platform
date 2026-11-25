package com.tradeexception.platform.common.couchdb;

import org.springframework.http.HttpStatus;

/** Raised on CouchDB HTTP 409 (optimistic concurrency / revision mismatch). */
public class CouchDbConflictException extends CouchDbException {

    private final String documentId;
    private final String revision;

    public CouchDbConflictException(String database, String documentId, String revision, Throwable cause) {
        super(HttpStatus.CONFLICT, "COUCHDB_CONFLICT",
                "Document revision conflict in database '" + database + "' for id '" + documentId + "'", cause);
        this.documentId = documentId;
        this.revision = revision;
    }

    public String getDocumentId() {
        return documentId;
    }

    public String getRevision() {
        return revision;
    }
}
