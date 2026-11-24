package com.tradeexception.platform.common.api;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Instant;
import java.util.List;

/** Consistent envelope for every API response. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ApiResponse<T>(boolean success, T data, ApiError error, Instant timestamp) {

    public static <T> ApiResponse<T> ok(T data) {
        return new ApiResponse<>(true, data, null, Instant.now());
    }

    public static <T> ApiResponse<T> failure(String code, String message, List<FieldIssue> details) {
        return new ApiResponse<>(false, null, new ApiError(code, message, details), Instant.now());
    }

    public record ApiError(String code, String message, List<FieldIssue> details) {
    }

    public record FieldIssue(String field, String message) {
    }
}
