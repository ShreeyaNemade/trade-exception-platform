package com.tradeexception.platform.confirmation.api;

import com.tradeexception.platform.common.api.PagedResponse;
import com.tradeexception.platform.common.couchdb.CouchDbNotFoundException;
import com.tradeexception.platform.confirmation.domain.Confirmation;
import com.tradeexception.platform.confirmation.domain.ConfirmationStatus;
import com.tradeexception.platform.confirmation.repository.ConfirmationRepository;
import com.tradeexception.platform.trade.domain.ProductType;
import org.springframework.stereotype.Service;

import java.util.Comparator;
import java.util.List;

/** Read-only query service for persisted confirmations (never regenerates on read). */
@Service
public class ConfirmationQueryService {

    private final ConfirmationRepository repository;

    public ConfirmationQueryService(ConfirmationRepository repository) {
        this.repository = repository;
    }

    public PagedResponse<ConfirmationDto> list(
            ConfirmationStatus status,
            ProductType productType,
            int page,
            int size,
            String sortBy,
            String sortDir) {

        List<ConfirmationDto> items = repository.findAll().stream()
                .filter(c -> status == null || c.getStatus() == status)
                .filter(c -> productType == null || c.getProductType() == productType)
                .map(ConfirmationDto::from)
                .sorted(comparatorFor(sortBy, sortDir))
                .toList();

        return PagedResponse.of(items, page, size, sortBy, sortDir);
    }

    public ConfirmationDto getById(String confirmationId) {
        return ConfirmationDto.from(loadOrThrow(confirmationId));
    }

    public List<ConfirmationDto> getByTradeId(String tradeId) {
        return repository.findByTradeId(tradeId).stream()
                .map(ConfirmationDto::from)
                .toList();
    }

    public String getRenderedDocument(String confirmationId) {
        Confirmation c = loadOrThrow(confirmationId);
        if (c.getRenderedContent() == null || c.getRenderedContent().isBlank()) {
            throw new CouchDbNotFoundException("confirmations", c.getId() + "/document", null);
        }
        return c.getRenderedContent();
    }

    private Confirmation loadOrThrow(String confirmationId) {
        String docId = Confirmation.TYPE + ":" + confirmationId;
        return repository.findById(docId)
                .orElseThrow(() -> new CouchDbNotFoundException("confirmations", docId, null));
    }

    private static Comparator<ConfirmationDto> comparatorFor(String sortBy, String sortDir) {
        Comparator<ConfirmationDto> cmp = switch (sortBy == null ? "generatedAt" : sortBy) {
            case "confirmationId" -> Comparator.comparing(c -> c.confirmationId() == null ? "" : c.confirmationId());
            case "status" -> Comparator.comparing(c -> c.status() == null ? "" : c.status().name());
            default -> Comparator.comparing(ConfirmationDto::generatedAt, Comparator.nullsLast(Comparator.naturalOrder()));
        };
        return "asc".equalsIgnoreCase(sortDir) ? cmp : cmp.reversed();
    }
}
