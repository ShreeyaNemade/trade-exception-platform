package com.tradeexception.platform.stp;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Kafka listener consuming enriched trades from {@code trade.enriched} for STP validation. */
@Component
public class StpValidationConsumer {

    private final StpValidationService validationService;

    public StpValidationConsumer(StpValidationService validationService) {
        this.validationService = validationService;
    }

    @KafkaListener(
            topics = "${app.kafka.topics.trade-enriched:trade.enriched}",
            groupId = "${app.kafka.stp-consumer-group-id:stp-validation-group}",
            containerFactory = "kafkaListenerContainerFactory")
    public void onMessage(ConsumerRecord<String, String> record) {
        validationService.processEnrichedMessage(
                record.value(),
                record.key(),
                record.topic(),
                record.partition(),
                record.offset());
    }
}
