package com.tradeexception.platform.audit.repository;

import com.tradeexception.platform.audit.domain.AuditEvent;
import com.tradeexception.platform.common.couchdb.AbstractCouchDbRepository;
import com.tradeexception.platform.common.couchdb.CouchDbClient;
import com.tradeexception.platform.common.couchdb.CouchDbProperties;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

/**
 * Append-only CouchDB repository for {@link AuditEvent} documents in the {@code audit-events} database.
 * Updates to existing revisions and deletions are rejected to preserve audit integrity.
 */
@Repository
public class AuditEventRepository extends AbstractCouchDbRepository<AuditEvent> {

    public AuditEventRepository(CouchDbClient client, CouchDbProperties properties) {
        super(client, properties.getDatabases().getAuditEvents(), AuditEvent.class);
    }

    @Override
    public AuditEvent save(AuditEvent document) {
        if (document != null && document.getRevision() != null) {
            throw new UnsupportedOperationException("Audit events are append-only and cannot be updated");
        }
        return super.save(document);
    }

    @Override
    public void delete(String id, String revision) {
        throw new UnsupportedOperationException("Audit events are immutable and cannot be deleted");
    }

    public List<AuditEvent> findByTradeId(String tradeId) {
        return findBySelector(Map.of("tradeId", tradeId));
    }

    public List<AuditEvent> findByCorrelationId(String correlationId) {
        return findBySelector(Map.of("correlationId", correlationId));
    }
}
