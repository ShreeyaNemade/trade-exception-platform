package com.tradeexception.platform.trade.api;

import com.tradeexception.platform.audit.api.AuditEventDto;
import com.tradeexception.platform.audit.api.AuditQueryService;
import com.tradeexception.platform.common.api.PagedResponse;
import com.tradeexception.platform.common.couchdb.CouchDbNotFoundException;
import com.tradeexception.platform.trade.domain.ProductType;
import com.tradeexception.platform.trade.domain.Trade;
import com.tradeexception.platform.trade.domain.TradeStatus;
import com.tradeexception.platform.trade.repository.TradeRepository;
import org.springframework.stereotype.Service;

import java.util.Comparator;
import java.util.List;
import java.util.Locale;

/** Query service for trade listing, lookup, and trade lifecycle event history. */
@Service
public class TradeQueryService {

    private final TradeRepository tradeRepository;
    private final AuditQueryService auditQueryService;

    public TradeQueryService(TradeRepository tradeRepository, AuditQueryService auditQueryService) {
        this.tradeRepository = tradeRepository;
        this.auditQueryService = auditQueryService;
    }

    public PagedResponse<TradeDto> list(
            TradeStatus status,
            ProductType productType,
            String search,
            int page,
            int size,
            String sortBy,
            String sortDir) {

        String needle = (search == null || search.isBlank()) ? null : search.trim().toLowerCase(Locale.ROOT);
        List<TradeDto> filtered = tradeRepository.findAll().stream()
                .filter(t -> status == null || t.getStatus() == status)
                .filter(t -> productType == null || t.getProductType() == productType)
                .filter(t -> needle == null || matchesSearch(t, needle))
                .map(TradeDto::from)
                .sorted(comparatorFor(sortBy, sortDir))
                .toList();

        return PagedResponse.of(filtered, page, size, sortBy, sortDir);
    }

    public TradeDto getById(String tradeId) {
        return tradeRepository.findByTradeId(tradeId)
                .map(TradeDto::from)
                .orElseThrow(() -> new CouchDbNotFoundException("trades", Trade.documentId(tradeId), null));
    }

    public List<AuditEventDto> getEvents(String tradeId) {
        // Ensure trade exists so unknown tradeId returns 404
        getById(tradeId);
        return auditQueryService.listByTradeId(tradeId);
    }

    private static boolean matchesSearch(Trade t, String needle) {
        if (contains(t.getTradeId(), needle) || contains(t.getSourceSystemTradeId(), needle)) {
            return true;
        }
        if (t.getHeader() != null && contains(t.getHeader().correlationId(), needle)) {
            return true;
        }
        return t.getParties() != null && t.getParties().stream()
                .anyMatch(p -> p != null && (contains(p.partyId(), needle) || contains(p.partyName(), needle)));
    }

    private static boolean contains(String value, String needle) {
        return value != null && value.toLowerCase(Locale.ROOT).contains(needle);
    }

    private static Comparator<TradeDto> comparatorFor(String sortBy, String sortDir) {
        Comparator<TradeDto> cmp = switch (sortBy == null ? "createdAt" : sortBy) {
            case "tradeId" -> Comparator.comparing(t -> t.tradeId() == null ? "" : t.tradeId());
            case "tradeDate" -> Comparator.comparing(TradeDto::tradeDate, Comparator.nullsLast(Comparator.naturalOrder()));
            case "status" -> Comparator.comparing(t -> t.status() == null ? "" : t.status().name());
            case "notionalAmount" -> Comparator.comparing(
                    t -> t.economics() == null ? null : t.economics().notionalAmount(),
                    Comparator.nullsLast(Comparator.naturalOrder()));
            default -> Comparator.comparing(TradeDto::createdAt, Comparator.nullsLast(Comparator.naturalOrder()));
        };
        return "asc".equalsIgnoreCase(sortDir) ? cmp : cmp.reversed();
    }
}
