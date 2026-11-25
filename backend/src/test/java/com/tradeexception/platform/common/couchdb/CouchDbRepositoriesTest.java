package com.tradeexception.platform.common.couchdb;

import com.tradeexception.platform.audit.domain.AuditEvent;
import com.tradeexception.platform.audit.domain.EventType;
import com.tradeexception.platform.audit.repository.AuditEventRepository;
import com.tradeexception.platform.enrichment.domain.ReferenceData;
import com.tradeexception.platform.enrichment.domain.ReferenceDataCategory;
import com.tradeexception.platform.enrichment.repository.ReferenceDataRepository;
import com.tradeexception.platform.trade.domain.Trade;
import com.tradeexception.platform.trade.domain.TradeStatus;
import com.tradeexception.platform.trade.repository.TradeRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CouchDbRepositoriesTest {

    @Mock
    private CouchDbClient client;

    private CouchDbProperties properties;

    @BeforeEach
    void setUp() {
        properties = new CouchDbProperties();
    }

    @Test
    void tradeRepositorySetsTimestampsAndQueriesByTradeIdAndStatus() {
        TradeRepository repo = new TradeRepository(client, properties);
        Trade trade = new Trade();
        trade.setTradeId("TRD-1");
        trade.setStatus(TradeStatus.RECEIVED);

        when(client.save("trades", trade)).thenReturn(trade);
        Trade saved = repo.save(trade);

        assertThat(saved.getCreatedAt()).isNotNull();
        assertThat(saved.getUpdatedAt()).isNotNull();
        verify(client).save("trades", trade);

        when(client.findById("trades", "trade:TRD-1", Trade.class)).thenReturn(Optional.of(trade));
        assertThat(repo.findByTradeId("TRD-1")).contains(trade);

        when(client.findBySelector("trades", Map.of("status", "RECEIVED"), Trade.class)).thenReturn(List.of(trade));
        assertThat(repo.findByStatus(TradeStatus.RECEIVED)).containsExactly(trade);
    }

    @Test
    void auditEventRepositoryIsAppendOnly() {
        AuditEventRepository repo = new AuditEventRepository(client, properties);
        AuditEvent event = new AuditEvent();
        event.setEventId("AUD-1");
        event.setEventType(EventType.TRADE_RECEIVED);
        event.setCorrelationId("CORR-1");
        event.setActor("system:ingestion");
        event.setOccurredAt(Instant.now());

        when(client.save("audit-events", event)).thenReturn(event);
        repo.save(event);
        verify(client).save("audit-events", event);

        event.setRevision("1-existing");
        assertThatThrownBy(() -> repo.save(event))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> repo.delete("audit:AUD-1", "1-existing"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void referenceDataRepositoryBuildsDeterministicLookupId() {
        ReferenceDataRepository repo = new ReferenceDataRepository(client, properties);
        ReferenceData usd = new ReferenceData();
        usd.setCategory(ReferenceDataCategory.CURRENCY);
        usd.setCode("USD");

        when(client.findById(eq("reference-data"), eq("refdata:CURRENCY:USD"), eq(ReferenceData.class)))
                .thenReturn(Optional.of(usd));

        assertThat(repo.findByCategoryAndCode(ReferenceDataCategory.CURRENCY, "USD")).contains(usd);
    }
}
