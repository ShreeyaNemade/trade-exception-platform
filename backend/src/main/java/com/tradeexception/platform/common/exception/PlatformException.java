package com.tradeexception.platform.common.exception;

import org.springframework.http.HttpStatus;

/** Base type for business exceptions that map to a well-defined HTTP status. */
public class PlatformException extends RuntimeException {

    private final HttpStatus status;
    private final String code;

    public PlatformException(HttpStatus status, String code, String message) {
        super(message);
        this.status = status;
        this.code = code;
    }

    public HttpStatus getStatus() {
        return status;
    }

    public String getCode() {
        return code;
    }
}
