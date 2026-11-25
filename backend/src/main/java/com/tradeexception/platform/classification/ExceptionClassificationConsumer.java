package com.tradeexception.platform.classification;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Kafka listener consuming initial exceptions from {@code exception.created} for classification. */
@Component
public class ExceptionClassificationConsumer {

    private final ExceptionClassificationService classificationService;

    public ExceptionClassificationConsumer(ExceptionClassificationService classificationService) {
        this.classificationService = classificationService;
    }

    @KafkaListener(
            topics = "${app.kafka.topics.exception-created:exception.created}",
            groupId = "${app.kafka.classification-consumer-group-id:exception-classification-group}",
            containerFactory = "kafkaListenerContainerFactory")
    public void onMessage(ConsumerRecord<String, String> record) {
        classificationService.processExceptionCreated(
                record.value(),
                record.key(),
                record.topic(),
                record.partition(),
                record.offset());
    }
}
