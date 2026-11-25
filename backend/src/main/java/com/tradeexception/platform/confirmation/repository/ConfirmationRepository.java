package com.tradeexception.platform.confirmation.repository;

import com.tradeexception.platform.common.couchdb.AbstractCouchDbRepository;
import com.tradeexception.platform.common.couchdb.CouchDbClient;
import com.tradeexception.platform.common.couchdb.CouchDbProperties;
import com.tradeexception.platform.confirmation.domain.Confirmation;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

/** CouchDB repository for {@link Confirmation} documents in the {@code confirmations} database. */
@Repository
public class ConfirmationRepository extends AbstractCouchDbRepository<Confirmation> {

    public ConfirmationRepository(CouchDbClient client, CouchDbProperties properties) {
        super(client, properties.getDatabases().getConfirmations(), Confirmation.class);
    }

    public List<Confirmation> findByTradeId(String tradeId) {
        return findBySelector(Map.of("tradeId", tradeId));
    }
}
