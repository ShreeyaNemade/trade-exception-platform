package com.tradeexception.platform.audit;

import com.tradeexception.platform.audit.api.AuditEventDto;
import com.tradeexception.platform.audit.api.AuditQueryService;
import com.tradeexception.platform.audit.domain.AuditEvent;
import com.tradeexception.platform.audit.domain.EventType;
import com.tradeexception.platform.audit.repository.AuditEventRepository;
import com.tradeexception.platform.common.api.PagedResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AuditQueryServiceTest {

    @Mock
    private AuditEventRepository repository;

    private AuditQueryService service;

    @BeforeEach
    void setUp() {
        service = new AuditQueryService(repository, 100);
    }

    @Test
    void firstAndSecondPagesReturnNewestRecordsWithoutDuplicatesAndLastPartialPageWorks() {
        List<AuditEvent> events = buildSequentialEvents(55, Instant.parse("2026-11-24T10:00:00Z"));
        when(repository.findAll()).thenReturn(events);

        PagedResponse<AuditEventDto> p0 = service.list(null, null, null, null, null, null, 0, 25, "occurredAt", "desc");
        PagedResponse<AuditEventDto> p1 = service.list(null, null, null, null, null, null, 1, 25, "occurredAt", "desc");
        PagedResponse<AuditEventDto> p2 = service.list(null, null, null, null, null, null, 2, 25, "occurredAt", "desc");

        // Page 0: newest 25 (EVT-055 down to EVT-031)
        assertThat(p0.items()).hasSize(25);
        assertThat(p0.page()).isZero();
        assertThat(p0.size()).isEqualTo(25);
        assertThat(p0.totalElements()).isEqualTo(55);
        assertThat(p0.totalPages()).isEqualTo(3);
        assertThat(p0.hasPrevious()).isFalse();
        assertThat(p0.hasNext()).isTrue();
        assertThat(p0.items().get(0).eventId()).isEqualTo("EVT-055");
        assertThat(p0.items().get(24).eventId()).isEqualTo("EVT-031");

        // Page 1: next 25 (EVT-030 down to EVT-006)
        assertThat(p1.items()).hasSize(25);
        assertThat(p1.page()).isEqualTo(1);
        assertThat(p1.hasPrevious()).isTrue();
        assertThat(p1.hasNext()).isTrue();
        assertThat(p1.items().get(0).eventId()).isEqualTo("EVT-030");
        assertThat(p1.items().get(24).eventId()).isEqualTo("EVT-006");

        // Page 2: final partial page (EVT-005 down to EVT-001)
        assertThat(p2.items()).hasSize(5);
        assertThat(p2.page()).isEqualTo(2);
        assertThat(p2.hasPrevious()).isTrue();
        assertThat(p2.hasNext()).isFalse();
        assertThat(p2.items().get(0).eventId()).isEqualTo("EVT-005");
        assertThat(p2.items().get(4).eventId()).isEqualTo("EVT-001");

        // Combined across all 3 pages: 55 unique records, zero duplicates
        Set<String> allIds = new HashSet<>();
        p0.items().forEach(e -> allIds.add(e.eventId()));
        p1.items().forEach(e -> allIds.add(e.eventId()));
        p2.items().forEach(e -> allIds.add(e.eventId()));
        assertThat(allIds).hasSize(55);
    }

    @Test
    void emptyResultReturnsZeroCountsAndNoNavigation() {
        when(repository.findAll()).thenReturn(List.of());

        PagedResponse<AuditEventDto> res = service.list(null, null, null, null, null, null, 0, 25, "occurredAt", "desc");

        assertThat(res.items()).isEmpty();
        assertThat(res.totalElements()).isZero();
        assertThat(res.totalPages()).isZero();
        assertThat(res.page()).isZero();
        assertThat(res.size()).isEqualTo(25);
        assertThat(res.hasNext()).isFalse();
        assertThat(res.hasPrevious()).isFalse();
    }

    @Test
    void invalidPageAndSizeAreHandledSafely() {
        List<AuditEvent> events = buildSequentialEvents(120, Instant.parse("2026-11-24T10:00:00Z"));
        when(repository.findAll()).thenReturn(events);

        // Negative page -> clamped to 0; non-positive size -> defaults to 25
        PagedResponse<AuditEventDto> neg = service.list(null, null, null, null, null, null, -5, 0, "occurredAt", "desc");
        assertThat(neg.page()).isZero();
        assertThat(neg.size()).isEqualTo(25);
        assertThat(neg.items()).hasSize(25);
        assertThat(neg.totalElements()).isEqualTo(120);

        // Size exceeding max (100) -> capped at 100
        PagedResponse<AuditEventDto> capped = service.list(null, null, null, null, null, null, 0, 500, "occurredAt", "desc");
        assertThat(capped.size()).isEqualTo(100);
        assertThat(capped.items()).hasSize(100);
        assertThat(capped.totalPages()).isEqualTo(2);
        assertThat(capped.hasNext()).isTrue();

        // Huge page beyond totalPages (including Integer.MAX_VALUE) -> safe empty slice, no overflow
        PagedResponse<AuditEventDto> huge = service.list(null, null, null, null, null, null, Integer.MAX_VALUE, 25, "occurredAt", "desc");
        assertThat(huge.items()).isEmpty();
        assertThat(huge.totalElements()).isEqualTo(120);
        assertThat(huge.hasNext()).isFalse();
        assertThat(huge.hasPrevious()).isTrue();
    }

    @Test
    void filteredQueryReportsAccurateTotalCount() {
        List<AuditEvent> events = buildSequentialEvents(40, Instant.parse("2026-11-24T10:00:00Z"));
        when(repository.findAll()).thenReturn(events);

        // Even-numbered events are CONFIRMATION_GENERATED (20 total), odd are TRADE_ACCEPTED (20 total)
        PagedResponse<AuditEventDto> filtered = service.list(
                null, null, EventType.CONFIRMATION_GENERATED, null, null, null, 0, 10, "occurredAt", "desc");

        assertThat(filtered.totalElements()).isEqualTo(20);
        assertThat(filtered.totalPages()).isEqualTo(2);
        assertThat(filtered.items()).hasSize(10);
        assertThat(filtered.hasNext()).isTrue();
        assertThat(filtered.hasPrevious()).isFalse();
        assertThat(filtered.items()).allMatch(e -> e.eventType() == EventType.CONFIRMATION_GENERATED);
    }

    @Test
    void orderingIsDeterministicWhenTimestampsMatch() {
        Instant sameInstant = Instant.parse("2026-11-24T12:00:00Z");
        List<AuditEvent> tiedEvents = List.of(
                buildEvent("EVT-B", EventType.TRADE_ACCEPTED, sameInstant),
                buildEvent("EVT-D", EventType.TRADE_ACCEPTED, sameInstant),
                buildEvent("EVT-A", EventType.TRADE_ACCEPTED, sameInstant),
                buildEvent("EVT-C", EventType.TRADE_ACCEPTED, sameInstant)
        );
        when(repository.findAll()).thenReturn(tiedEvents);

        PagedResponse<AuditEventDto> p0 = service.list(null, null, null, null, null, null, 0, 2, "occurredAt", "desc");
        PagedResponse<AuditEventDto> p1 = service.list(null, null, null, null, null, null, 1, 2, "occurredAt", "desc");

        assertThat(p0.items()).extracting(AuditEventDto::eventId).containsExactly("EVT-D", "EVT-C");
        assertThat(p1.items()).extracting(AuditEventDto::eventId).containsExactly("EVT-B", "EVT-A");
    }

    private static List<AuditEvent> buildSequentialEvents(int count, Instant base) {
        List<AuditEvent> list = new ArrayList<>(count);
        for (int i = 1; i <= count; i++) {
            EventType type = (i % 2 == 0) ? EventType.CONFIRMATION_GENERATED : EventType.TRADE_ACCEPTED;
            list.add(buildEvent(String.format("EVT-%03d", i), type, base.plusSeconds(i)));
        }
        return list;
    }

    private static AuditEvent buildEvent(String eventId, EventType eventType, Instant occurredAt) {
        AuditEvent ev = new AuditEvent();
        ev.setEventId(eventId);
        ev.setEventType(eventType);
        ev.setEntityType("trade");
        ev.setEntityId("TRD-1");
        ev.setTradeId("TRD-1");
        ev.setCorrelationId("CORR-1");
        ev.setActor("system");
        ev.setOccurredAt(occurredAt);
        return ev;
    }
}
