package com.tradeexception.platform.trade.repository;

import com.tradeexception.platform.common.couchdb.AbstractCouchDbRepository;
import com.tradeexception.platform.common.couchdb.CouchDbClient;
import com.tradeexception.platform.common.couchdb.CouchDbProperties;
import com.tradeexception.platform.trade.domain.Trade;
import com.tradeexception.platform.trade.domain.TradeStatus;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/** CouchDB repository for {@link Trade} documents in the {@code trades} database. */
@Repository
public class TradeRepository extends AbstractCouchDbRepository<Trade> {

    public TradeRepository(CouchDbClient client, CouchDbProperties properties) {
        super(client, properties.getDatabases().getTrades(), Trade.class);
    }

    public Optional<Trade> findByTradeId(String tradeId) {
        return tradeId == null || tradeId.isBlank() ? Optional.empty() : findById(Trade.documentId(tradeId));
    }

    public List<Trade> findByStatus(TradeStatus status) {
        return findBySelector(Map.of("status", status.name()));
    }

    public List<Trade> findByCorrelationId(String correlationId) {
        return findBySelector(Map.of("header", Map.of("correlationId", correlationId)));
    }
}
