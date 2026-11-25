package com.tradeexception.platform.enrichment;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Kafka listener consuming accepted trades from {@code trade.accepted} for enrichment. */
@Component
public class TradeEnrichmentConsumer {

    private final TradeEnrichmentService enrichmentService;

    public TradeEnrichmentConsumer(TradeEnrichmentService enrichmentService) {
        this.enrichmentService = enrichmentService;
    }

    @KafkaListener(
            topics = "${app.kafka.topics.trade-accepted:trade.accepted}",
            groupId = "${app.kafka.enrichment-consumer-group-id:trade-enrichment-group}",
            containerFactory = "kafkaListenerContainerFactory")
    public void onMessage(ConsumerRecord<String, String> record) {
        enrichmentService.processAcceptedMessage(
                record.value(),
                record.key(),
                record.topic(),
                record.partition(),
                record.offset());
    }
}
