package com.tradeexception.platform.common.couchdb;

import org.springframework.http.HttpStatus;

/** Raised when a requested CouchDB database or document does not exist (HTTP 404). */
public class CouchDbNotFoundException extends CouchDbException {

    private final String database;
    private final String documentId;

    public CouchDbNotFoundException(String database, String documentId, Throwable cause) {
        super(HttpStatus.NOT_FOUND, "DOCUMENT_NOT_FOUND",
                "Resource not found in database '" + database + "'" + (documentId == null ? "" : " for id '" + documentId + "'"),
                cause);
        this.database = database;
        this.documentId = documentId;
    }

    public String getDatabase() {
        return database;
    }

    public String getDocumentId() {
        return documentId;
    }
}
