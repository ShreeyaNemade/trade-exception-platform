package com.tradeexception.platform.workitem;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.tradeexception.platform.audit.domain.AuditEvent;
import com.tradeexception.platform.audit.domain.EventType;
import com.tradeexception.platform.audit.repository.AuditEventRepository;
import com.tradeexception.platform.common.kafka.EventEnvelope;
import com.tradeexception.platform.common.kafka.KafkaProperties;
import com.tradeexception.platform.exception.domain.ExceptionCategory;
import com.tradeexception.platform.exception.domain.ExceptionPriority;
import com.tradeexception.platform.exception.domain.ExceptionRecord;
import com.tradeexception.platform.exception.domain.ExceptionStatus;
import com.tradeexception.platform.replay.DeadLetterPublisher;
import com.tradeexception.platform.workitem.WorkItemManagementService.WorkItemOutcome;
import com.tradeexception.platform.workitem.domain.OperationalTeam;
import com.tradeexception.platform.workitem.domain.WorkItem;
import com.tradeexception.platform.workitem.domain.WorkItemStatus;
import com.tradeexception.platform.workitem.repository.WorkItemRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Instant;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class WorkItemManagementServiceTest {

    @Mock private WorkItemRepository workItemRepository;
    @Mock private AuditEventRepository auditEventRepository;
    @Mock private KafkaTemplate<String, String> kafkaTemplate;
    @Mock private DeadLetterPublisher deadLetterPublisher;

    private ObjectMapper mapper;
    private WorkItemManagementService service;

    @BeforeEach
    void setUp() {
        mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        service = new WorkItemManagementService(
                mapper, new TeamRoutingStrategy(), workItemRepository, auditEventRepository,
                kafkaTemplate, deadLetterPublisher, new KafkaProperties());
    }

    @Test
    void createsWorkItemRoutesTeamPropagatesPriorityAndPublishesWorkItemLinked() throws Exception {
        ExceptionRecord ex = new ExceptionRecord();
        ex.setExceptionId("EXC-TRD-9");
        ex.setTradeId("TRD-9");
        ex.setCorrelationId("CORR-9");
        ex.setCategory(ExceptionCategory.INVALID_COUNTERPARTY);
        ex.setPriority(ExceptionPriority.HIGH);
        ex.setStatus(ExceptionStatus.OPEN);
        ex.setReasonCode("COUNTERPARTY_NOT_FOUND");
        ex.setExplanation("Counterparty missing");
        ex.setRecommendedAction("Verify LEI onboarding");
        ex.setDetectedAt(Instant.now());

        EventEnvelope<ExceptionRecord> env = EventEnvelope.of("ExceptionClassified", "CORR-9", "1.0", "exception-classification", ex);
        when(workItemRepository.findById("workitem:WI-EXC-TRD-9")).thenReturn(Optional.empty());
        when(workItemRepository.save(any(WorkItem.class))).thenAnswer(inv -> inv.getArgument(0));

        WorkItemOutcome out = service.processClassifiedException(
                mapper.writeValueAsString(env), "TRD-9", "exception.classified", 0, 1L);

        assertThat(out).isEqualTo(WorkItemOutcome.CREATED);

        ArgumentCaptor<WorkItem> wiCaptor = ArgumentCaptor.forClass(WorkItem.class);
        verify(workItemRepository).save(wiCaptor.capture());
        WorkItem saved = wiCaptor.getValue();
        assertThat(saved.getWorkItemId()).isEqualTo("WI-EXC-TRD-9");
        assertThat(saved.getAssignedTeam()).isEqualTo(OperationalTeam.COUNTERPARTY_OPERATIONS);
        assertThat(saved.getPriority()).isEqualTo(ExceptionPriority.HIGH);
        assertThat(saved.getStatus()).isEqualTo(WorkItemStatus.OPEN);
        assertThat(saved.getDueAt()).isAfter(Instant.now());

        verify(kafkaTemplate).send(eq("workitem.linked"), eq("TRD-9"), anyString());
    }

    @Test
    void preventsDuplicateWorkItemCreation() throws Exception {
        ExceptionRecord ex = new ExceptionRecord();
        ex.setExceptionId("EXC-DUP");
        ex.setTradeId("TRD-DUP");
        ex.setCorrelationId("CORR-DUP");
        ex.setCategory(ExceptionCategory.UNSUPPORTED_PRODUCT);
        ex.setPriority(ExceptionPriority.MEDIUM);
        ex.setStatus(ExceptionStatus.OPEN);
        ex.setReasonCode("PRODUCT_MANUAL_REVIEW_ONLY");
        ex.setDetectedAt(Instant.now());

        WorkItem existing = new WorkItem();
        existing.setWorkItemId("WI-EXC-DUP");
        when(workItemRepository.findById("workitem:WI-EXC-DUP")).thenReturn(Optional.of(existing));

        EventEnvelope<ExceptionRecord> env = EventEnvelope.of("ExceptionClassified", "CORR-DUP", "1.0", "exception-classification", ex);
        assertThat(service.processClassifiedException(mapper.writeValueAsString(env), "TRD-DUP", "exception.classified", 0, 2L))
                .isEqualTo(WorkItemOutcome.DUPLICATE_IGNORED);
        verify(workItemRepository, never()).save(any());
    }

    @Test
    void enforcesStatusTransitionsAndRecordsLifecycleAudits() {
        WorkItem item = new WorkItem();
        item.setWorkItemId("WI-1");
        item.setExceptionId("EXC-1");
        item.setTradeId("TRD-1");
        item.setCorrelationId("CORR-1");
        item.setPriority(ExceptionPriority.HIGH);
        item.setStatus(WorkItemStatus.OPEN);

        when(workItemRepository.findById("workitem:WI-1")).thenReturn(Optional.of(item));
        when(workItemRepository.save(any(WorkItem.class))).thenAnswer(inv -> inv.getArgument(0));

        service.assignWorkItem("WI-1", "ops-queue-1", "ops-supervisor");
        service.updateStatus("WI-1", WorkItemStatus.IN_PROGRESS, "ops-queue-1");
        service.updateStatus("WI-1", WorkItemStatus.RESOLVED, "ops-queue-1");
        service.updateStatus("WI-1", WorkItemStatus.CLOSED, "ops-supervisor");

        assertThat(item.getStatus()).isEqualTo(WorkItemStatus.CLOSED);
        assertThatThrownBy(() -> service.updateStatus("WI-1", WorkItemStatus.OPEN, "ops-supervisor"))
                .isInstanceOf(IllegalStateException.class);

        ArgumentCaptor<AuditEvent> audits = ArgumentCaptor.forClass(AuditEvent.class);
        verify(auditEventRepository, times(4)).save(audits.capture());
        assertThat(audits.getAllValues())
                .extracting(AuditEvent::getEventType)
                .containsExactly(
                        EventType.WORKITEM_ASSIGNED,
                        EventType.WORKITEM_UPDATED,
                        EventType.WORKITEM_RESOLVED,
                        EventType.WORKITEM_UPDATED);
    }
}
