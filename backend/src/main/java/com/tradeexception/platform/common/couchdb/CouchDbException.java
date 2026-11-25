package com.tradeexception.platform.common.couchdb;

import com.tradeexception.platform.common.exception.PlatformException;
import org.springframework.http.HttpStatus;

/** Base exception for CouchDB persistence failures. */
public class CouchDbException extends PlatformException {

    public CouchDbException(HttpStatus status, String code, String message) {
        super(status, code, message);
    }

    public CouchDbException(HttpStatus status, String code, String message, Throwable cause) {
        super(status, code, message);
        initCause(cause);
    }
}
