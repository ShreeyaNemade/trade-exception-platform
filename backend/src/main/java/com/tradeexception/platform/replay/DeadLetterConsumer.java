package com.tradeexception.platform.replay;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Persists messages arriving on {@code deadletter.trade} into CouchDB {@code dead-letters}. */
@Component
public class DeadLetterConsumer {

    private final DeadLetterManagementService managementService;

    public DeadLetterConsumer(DeadLetterManagementService managementService) {
        this.managementService = managementService;
    }

    @KafkaListener(
            topics = "${app.kafka.topics.deadletter-trade:deadletter.trade}",
            groupId = "${app.kafka.dlq-consumer-group-id:dlq-persistence-group}",
            containerFactory = "kafkaListenerContainerFactory")
    public void onMessage(ConsumerRecord<String, String> record) {
        managementService.persistFromKafka(record.value());
    }
}
