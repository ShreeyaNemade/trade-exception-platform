package com.tradeexception.platform.exception.repository;

import com.tradeexception.platform.common.couchdb.AbstractCouchDbRepository;
import com.tradeexception.platform.common.couchdb.CouchDbClient;
import com.tradeexception.platform.common.couchdb.CouchDbProperties;
import com.tradeexception.platform.exception.domain.ExceptionRecord;
import com.tradeexception.platform.exception.domain.ExceptionStatus;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

/** CouchDB repository for {@link ExceptionRecord} documents in the {@code exceptions} database. */
@Repository
public class ExceptionRepository extends AbstractCouchDbRepository<ExceptionRecord> {

    public ExceptionRepository(CouchDbClient client, CouchDbProperties properties) {
        super(client, properties.getDatabases().getExceptions(), ExceptionRecord.class);
    }

    public List<ExceptionRecord> findByTradeId(String tradeId) {
        return findBySelector(Map.of("tradeId", tradeId));
    }

    public List<ExceptionRecord> findByStatus(ExceptionStatus status) {
        return findBySelector(Map.of("status", status.name()));
    }
}
