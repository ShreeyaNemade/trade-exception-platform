package com.tradeexception.platform.dashboard;

import com.tradeexception.platform.common.api.ApiResponse;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** Operational dashboard KPI and chart distribution endpoint. */
@RestController
@RequestMapping("/api/v1/dashboard")
public class DashboardController {

    private final DashboardSummaryService summaryService;

    public DashboardController(DashboardSummaryService summaryService) {
        this.summaryService = summaryService;
    }

    @GetMapping("/summary")
    public ApiResponse<DashboardSummaryDto> summary() {
        return ApiResponse.ok(summaryService.getSummary());
    }
}
