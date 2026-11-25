package com.tradeexception.platform.confirmation.api;

import com.tradeexception.platform.common.api.ApiResponse;
import com.tradeexception.platform.common.api.PagedResponse;
import com.tradeexception.platform.confirmation.domain.ConfirmationStatus;
import com.tradeexception.platform.trade.domain.ProductType;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/** Read-only REST API for trade confirmations and rendered confirmation HTML documents. */
@RestController
@RequestMapping("/api/v1/confirmations")
public class ConfirmationController {

    private final ConfirmationQueryService queryService;

    public ConfirmationController(ConfirmationQueryService queryService) {
        this.queryService = queryService;
    }

    @GetMapping
    public ApiResponse<PagedResponse<ConfirmationDto>> list(
            @RequestParam(required = false) ConfirmationStatus status,
            @RequestParam(required = false) ProductType productType,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size,
            @RequestParam(defaultValue = "generatedAt") String sortBy,
            @RequestParam(defaultValue = "desc") String sortDir) {
        return ApiResponse.ok(queryService.list(status, productType, page, size, sortBy, sortDir));
    }

    @GetMapping("/{confirmationId}")
    public ApiResponse<ConfirmationDto> getById(@PathVariable String confirmationId) {
        return ApiResponse.ok(queryService.getById(confirmationId));
    }

    @GetMapping("/trade/{tradeId}")
    public ApiResponse<List<ConfirmationDto>> getByTradeId(@PathVariable String tradeId) {
        return ApiResponse.ok(queryService.getByTradeId(tradeId));
    }

    @GetMapping(value = "/{confirmationId}/document", produces = MediaType.TEXT_HTML_VALUE)
    public ResponseEntity<String> getDocument(@PathVariable String confirmationId) {
        return ResponseEntity.ok()
                .contentType(MediaType.TEXT_HTML)
                .body(queryService.getRenderedDocument(confirmationId));
    }
}
