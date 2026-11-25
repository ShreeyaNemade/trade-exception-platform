package com.tradeexception.platform.exception.api;

import com.tradeexception.platform.common.api.PagedResponse;
import com.tradeexception.platform.common.couchdb.CouchDbNotFoundException;
import com.tradeexception.platform.exception.domain.ExceptionCategory;
import com.tradeexception.platform.exception.domain.ExceptionPriority;
import com.tradeexception.platform.exception.domain.ExceptionRecord;
import com.tradeexception.platform.exception.domain.ExceptionSeverity;
import com.tradeexception.platform.exception.domain.ExceptionStatus;
import com.tradeexception.platform.exception.repository.ExceptionRepository;
import org.springframework.stereotype.Service;

import java.util.Comparator;
import java.util.List;
import java.util.Locale;

/** Query service for exception listing, filtering, and detail retrieval. */
@Service
public class ExceptionQueryService {

    private final ExceptionRepository repository;

    public ExceptionQueryService(ExceptionRepository repository) {
        this.repository = repository;
    }

    public PagedResponse<ExceptionDto> list(
            ExceptionCategory category,
            ExceptionPriority priority,
            ExceptionSeverity severity,
            Boolean classified,
            Boolean manualReviewOnly,
            ExceptionStatus status,
            String tradeId,
            String search,
            int page,
            int size,
            String sortBy,
            String sortDir) {

        String needle = (search == null || search.isBlank()) ? null : search.trim().toLowerCase(Locale.ROOT);
        List<ExceptionDto> items = repository.findAll().stream()
                .map(ExceptionDto::from)
                .filter(e -> category == null || e.category() == category)
                .filter(e -> priority == null || e.priority() == priority)
                .filter(e -> severity == null || e.severity() == severity)
                .filter(e -> classified == null || e.classified() == classified)
                .filter(e -> manualReviewOnly == null || !manualReviewOnly || e.manualReviewRequired())
                .filter(e -> status == null || e.status() == status)
                .filter(e -> tradeId == null || tradeId.isBlank() || tradeId.equalsIgnoreCase(e.tradeId()))
                .filter(e -> needle == null || matches(e, needle))
                .sorted(comparatorFor(sortBy, sortDir))
                .toList();

        return PagedResponse.of(items, page, size, sortBy, sortDir);
    }

    public ExceptionDto getById(String exceptionId) {
        String docId = ExceptionRecord.TYPE + ":" + exceptionId;
        return repository.findById(docId)
                .map(ExceptionDto::from)
                .orElseThrow(() -> new CouchDbNotFoundException("exceptions", docId, null));
    }

    private static boolean matches(ExceptionDto e, String needle) {
        return contains(e.exceptionId(), needle)
                || contains(e.tradeId(), needle)
                || contains(e.reasonCode(), needle)
                || contains(e.rootCause(), needle)
                || contains(e.explanation(), needle);
    }

    private static boolean contains(String v, String needle) {
        return v != null && v.toLowerCase(Locale.ROOT).contains(needle);
    }

    private static Comparator<ExceptionDto> comparatorFor(String sortBy, String sortDir) {
        Comparator<ExceptionDto> cmp = switch (sortBy == null ? "detectedAt" : sortBy) {
            case "priority" -> Comparator.comparing(e -> e.priority() == null ? 0 : e.priority().ordinal());
            case "severity" -> Comparator.comparing(e -> e.severity() == null ? 0 : e.severity().ordinal());
            case "category" -> Comparator.comparing(e -> e.category() == null ? "" : e.category().name());
            default -> Comparator.comparing(ExceptionDto::detectedAt, Comparator.nullsLast(Comparator.naturalOrder()));
        };
        return "asc".equalsIgnoreCase(sortDir) ? cmp : cmp.reversed();
    }
}
