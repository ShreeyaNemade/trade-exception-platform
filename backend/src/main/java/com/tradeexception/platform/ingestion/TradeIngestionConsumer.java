package com.tradeexception.platform.ingestion;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Kafka listener consuming raw synthetic trade messages from {@code trade.raw}. */
@Component
public class TradeIngestionConsumer {

    private final TradeIngestionService ingestionService;

    public TradeIngestionConsumer(TradeIngestionService ingestionService) {
        this.ingestionService = ingestionService;
    }

    @KafkaListener(
            topics = "${app.kafka.topics.trade-raw:trade.raw}",
            groupId = "${app.kafka.consumer-group-id:trade-ingestion-group}",
            containerFactory = "kafkaListenerContainerFactory")
    public void onMessage(ConsumerRecord<String, String> record) {
        ingestionService.processRawMessage(
                record.value(),
                record.key(),
                record.topic(),
                record.partition(),
                record.offset());
    }
}
