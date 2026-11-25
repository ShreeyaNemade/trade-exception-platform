package com.tradeexception.platform.workitem.api;

import com.tradeexception.platform.common.api.PagedResponse;
import com.tradeexception.platform.common.couchdb.CouchDbNotFoundException;
import com.tradeexception.platform.exception.domain.ExceptionPriority;
import com.tradeexception.platform.workitem.domain.OperationalTeam;
import com.tradeexception.platform.workitem.domain.WorkItem;
import com.tradeexception.platform.workitem.domain.WorkItemStatus;
import com.tradeexception.platform.workitem.repository.WorkItemRepository;
import org.springframework.stereotype.Service;

import java.util.Comparator;
import java.util.List;

/** Read-only query service for work items. */
@Service
public class WorkItemQueryService {

    private final WorkItemRepository repository;

    public WorkItemQueryService(WorkItemRepository repository) {
        this.repository = repository;
    }

    public PagedResponse<WorkItemDto> list(
            WorkItemStatus status,
            ExceptionPriority priority,
            OperationalTeam assignedTeam,
            String tradeId,
            String exceptionId,
            int page,
            int size,
            String sortBy,
            String sortDir) {

        List<WorkItemDto> items = repository.findAll().stream()
                .filter(w -> status == null || w.getStatus() == status)
                .filter(w -> priority == null || w.getPriority() == priority)
                .filter(w -> assignedTeam == null || w.getAssignedTeam() == assignedTeam)
                .filter(w -> tradeId == null || tradeId.isBlank() || tradeId.equalsIgnoreCase(w.getTradeId()))
                .filter(w -> exceptionId == null || exceptionId.isBlank() || exceptionId.equalsIgnoreCase(w.getExceptionId()))
                .map(WorkItemDto::from)
                .sorted(comparatorFor(sortBy, sortDir))
                .toList();

        return PagedResponse.of(items, page, size, sortBy, sortDir);
    }

    public WorkItemDto getById(String workItemId) {
        String docId = WorkItem.TYPE + ":" + workItemId;
        return repository.findById(docId)
                .map(WorkItemDto::from)
                .orElseThrow(() -> new CouchDbNotFoundException("work-items", docId, null));
    }

    private static Comparator<WorkItemDto> comparatorFor(String sortBy, String sortDir) {
        Comparator<WorkItemDto> cmp = switch (sortBy == null ? "dueAt" : sortBy) {
            case "priority" -> Comparator.comparing(w -> w.priority() == null ? 0 : w.priority().ordinal());
            case "status" -> Comparator.comparing(w -> w.status() == null ? "" : w.status().name());
            case "createdAt" -> Comparator.comparing(WorkItemDto::createdAt, Comparator.nullsLast(Comparator.naturalOrder()));
            default -> Comparator.comparing(WorkItemDto::dueAt, Comparator.nullsLast(Comparator.naturalOrder()));
        };
        return "asc".equalsIgnoreCase(sortDir) ? cmp : cmp.reversed();
    }
}
