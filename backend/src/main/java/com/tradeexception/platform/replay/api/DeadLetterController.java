package com.tradeexception.platform.replay.api;

import com.tradeexception.platform.common.api.ApiResponse;
import com.tradeexception.platform.common.api.PagedResponse;
import com.tradeexception.platform.replay.DeadLetterManagementService;
import com.tradeexception.platform.replay.DeadLetterRecord.DeadLetterErrorCategory;
import com.tradeexception.platform.replay.domain.ReplayAttempt;
import com.tradeexception.platform.replay.domain.ReplayStatus;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.security.Principal;
import java.time.Instant;
import java.util.List;

/** Operational REST API for DLQ inspection, replay history, and controlled message replay. */
@RestController
@RequestMapping("/api/v1/dlq")
public class DeadLetterController {

    private final DeadLetterManagementService managementService;

    public DeadLetterController(DeadLetterManagementService managementService) {
        this.managementService = managementService;
    }

    @GetMapping
    public ApiResponse<PagedResponse<DeadLetterDto>> list(
            @RequestParam(required = false) DeadLetterErrorCategory errorCategory,
            @RequestParam(required = false) String originalTopic,
            @RequestParam(required = false) ReplayStatus replayStatus,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant from,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant to,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size,
            @RequestParam(defaultValue = "failedAt") String sortBy,
            @RequestParam(defaultValue = "desc") String sortDir) {
        return ApiResponse.ok(managementService.list(
                errorCategory, originalTopic, replayStatus, from, to, page, size, sortBy, sortDir));
    }

    @GetMapping("/{deadLetterId}")
    public ApiResponse<DeadLetterDto> getById(@PathVariable String deadLetterId) {
        return ApiResponse.ok(managementService.getById(deadLetterId));
    }

    @GetMapping("/{deadLetterId}/history")
    public ApiResponse<List<ReplayAttempt>> getHistory(@PathVariable String deadLetterId) {
        return ApiResponse.ok(managementService.getHistory(deadLetterId));
    }

    @PostMapping("/{deadLetterId}/replay")
    public ApiResponse<ReplayResultDto> replay(@PathVariable String deadLetterId, Principal principal) {
        String operator = principal == null ? "ops-user" : principal.getName();
        return ApiResponse.ok(managementService.replay(deadLetterId, operator));
    }
}
