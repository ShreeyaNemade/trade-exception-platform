package com.tradeexception.platform.trade.api;

import com.tradeexception.platform.audit.api.AuditEventDto;
import com.tradeexception.platform.common.api.ApiResponse;
import com.tradeexception.platform.common.api.PagedResponse;
import com.tradeexception.platform.trade.domain.ProductType;
import com.tradeexception.platform.trade.domain.TradeStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/** Read-only operational REST API for trades and trade event timelines. */
@RestController
@RequestMapping("/api/v1/trades")
public class TradeController {

    private final TradeQueryService queryService;

    public TradeController(TradeQueryService queryService) {
        this.queryService = queryService;
    }

    @GetMapping
    public ApiResponse<PagedResponse<TradeDto>> list(
            @RequestParam(required = false) TradeStatus status,
            @RequestParam(required = false) ProductType productType,
            @RequestParam(required = false) String search,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size,
            @RequestParam(defaultValue = "createdAt") String sortBy,
            @RequestParam(defaultValue = "desc") String sortDir) {
        return ApiResponse.ok(queryService.list(status, productType, search, page, size, sortBy, sortDir));
    }

    @GetMapping("/{tradeId}")
    public ApiResponse<TradeDto> getById(@PathVariable String tradeId) {
        return ApiResponse.ok(queryService.getById(tradeId));
    }

    @GetMapping("/{tradeId}/events")
    public ApiResponse<List<AuditEventDto>> getEvents(@PathVariable String tradeId) {
        return ApiResponse.ok(queryService.getEvents(tradeId));
    }
}
