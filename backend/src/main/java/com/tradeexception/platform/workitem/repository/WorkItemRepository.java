package com.tradeexception.platform.workitem.repository;

import com.tradeexception.platform.common.couchdb.AbstractCouchDbRepository;
import com.tradeexception.platform.common.couchdb.CouchDbClient;
import com.tradeexception.platform.common.couchdb.CouchDbProperties;
import com.tradeexception.platform.workitem.domain.WorkItem;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

/** CouchDB repository for {@link WorkItem} documents in the {@code work-items} database. */
@Repository
public class WorkItemRepository extends AbstractCouchDbRepository<WorkItem> {

    public WorkItemRepository(CouchDbClient client, CouchDbProperties properties) {
        super(client, properties.getDatabases().getWorkItems(), WorkItem.class);
    }

    public List<WorkItem> findByExceptionId(String exceptionId) {
        return findBySelector(Map.of("exceptionId", exceptionId));
    }

    public List<WorkItem> findByTradeId(String tradeId) {
        return findBySelector(Map.of("tradeId", tradeId));
    }
}
