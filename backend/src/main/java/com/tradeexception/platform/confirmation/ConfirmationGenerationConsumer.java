package com.tradeexception.platform.confirmation;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Kafka listener consuming validated trades from {@code stp.validated} for confirmation generation. */
@Component
public class ConfirmationGenerationConsumer {

    private final ConfirmationGenerationService confirmationService;

    public ConfirmationGenerationConsumer(ConfirmationGenerationService confirmationService) {
        this.confirmationService = confirmationService;
    }

    @KafkaListener(
            topics = "${app.kafka.topics.stp-validated:stp.validated}",
            groupId = "${app.kafka.confirmation-consumer-group-id:confirmation-generation-group}",
            containerFactory = "kafkaListenerContainerFactory")
    public void onMessage(ConsumerRecord<String, String> record) {
        confirmationService.processValidatedMessage(
                record.value(),
                record.key(),
                record.topic(),
                record.partition(),
                record.offset());
    }
}
