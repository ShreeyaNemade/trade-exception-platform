package com.tradeexception.platform.common.couchdb;

import org.springframework.http.HttpStatus;

/** Raised when CouchDB cannot be reached or times out. */
public class CouchDbConnectionException extends CouchDbException {

    public CouchDbConnectionException(String message, Throwable cause) {
        super(HttpStatus.SERVICE_UNAVAILABLE, "COUCHDB_UNAVAILABLE", message, cause);
    }
}
