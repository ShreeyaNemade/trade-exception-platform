package com.tradeexception.platform.exception.api;

import com.tradeexception.platform.common.api.ApiResponse;
import com.tradeexception.platform.common.api.PagedResponse;
import com.tradeexception.platform.exception.ExceptionRemediationService;
import com.tradeexception.platform.exception.domain.ExceptionCategory;
import com.tradeexception.platform.exception.domain.ExceptionPriority;
import com.tradeexception.platform.exception.domain.ExceptionSeverity;
import com.tradeexception.platform.exception.domain.ExceptionStatus;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.security.Principal;

/** Operational REST API for trade exceptions, classification details, and authorized remediation. */
@RestController
@RequestMapping("/api/v1/exceptions")
public class ExceptionController {

    private final ExceptionQueryService queryService;
    private final ExceptionRemediationService remediationService;

    public ExceptionController(
            ExceptionQueryService queryService,
            ExceptionRemediationService remediationService) {
        this.queryService = queryService;
        this.remediationService = remediationService;
    }

    @GetMapping
    public ApiResponse<PagedResponse<ExceptionDto>> list(
            @RequestParam(required = false) ExceptionCategory category,
            @RequestParam(required = false) ExceptionPriority priority,
            @RequestParam(required = false) ExceptionSeverity severity,
            @RequestParam(required = false) Boolean classified,
            @RequestParam(required = false) Boolean manualReviewOnly,
            @RequestParam(required = false) ExceptionStatus status,
            @RequestParam(required = false) String tradeId,
            @RequestParam(required = false) String search,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size,
            @RequestParam(defaultValue = "detectedAt") String sortBy,
            @RequestParam(defaultValue = "desc") String sortDir) {
        return ApiResponse.ok(queryService.list(
                category, priority, severity, classified, manualReviewOnly, status, tradeId, search, page, size, sortBy, sortDir));
    }

    @GetMapping("/{exceptionId}")
    public ApiResponse<ExceptionDto> getById(@PathVariable String exceptionId) {
        return ApiResponse.ok(queryService.getById(exceptionId));
    }

    @PostMapping("/{exceptionId}/remediate")
    public ApiResponse<RemediationResultDto> remediate(
            @PathVariable String exceptionId,
            @Valid @RequestBody RemediateExceptionRequest request,
            Principal principal) {
        String operator = principal == null ? "ops-user" : principal.getName();
        return ApiResponse.ok(remediationService.remediate(exceptionId, request, operator));
    }
}
