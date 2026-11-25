package com.tradeexception.platform.workitem;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Kafka listener consuming classified exceptions from {@code exception.classified} to create work items. */
@Component
public class WorkItemManagementConsumer {

    private final WorkItemManagementService workItemService;

    public WorkItemManagementConsumer(WorkItemManagementService workItemService) {
        this.workItemService = workItemService;
    }

    @KafkaListener(
            topics = "${app.kafka.topics.exception-classified:exception.classified}",
            groupId = "${app.kafka.workitem-consumer-group-id:workitem-management-group}",
            containerFactory = "kafkaListenerContainerFactory")
    public void onMessage(ConsumerRecord<String, String> record) {
        workItemService.processClassifiedException(
                record.value(),
                record.key(),
                record.topic(),
                record.partition(),
                record.offset());
    }
}
