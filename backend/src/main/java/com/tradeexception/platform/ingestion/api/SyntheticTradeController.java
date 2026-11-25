package com.tradeexception.platform.ingestion.api;

import com.tradeexception.platform.common.api.ApiResponse;
import com.tradeexception.platform.ingestion.SyntheticTradeProducer;
import com.tradeexception.platform.ingestion.SyntheticTradeProducer.PublishResult;
import com.tradeexception.platform.ingestion.SyntheticTradeScenario;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Triggers controlled synthetic trade scenarios via Kafka ({@code trade.raw}).
 * Never writes directly to CouchDB and restricts input to {@link SyntheticTradeScenario}.
 */
@RestController
@RequestMapping("/api/v1/synthetic-trades")
public class SyntheticTradeController {

    private final SyntheticTradeProducer producer;

    public SyntheticTradeController(SyntheticTradeProducer producer) {
        this.producer = producer;
    }

    @PostMapping("/{scenario}")
    public ResponseEntity<ApiResponse<PublishResult>> trigger(@PathVariable SyntheticTradeScenario scenario) {
        PublishResult result = producer.publishScenario(scenario);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(ApiResponse.ok(result));
    }
}
