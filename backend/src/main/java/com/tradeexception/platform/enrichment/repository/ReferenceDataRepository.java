package com.tradeexception.platform.enrichment.repository;

import com.tradeexception.platform.common.couchdb.AbstractCouchDbRepository;
import com.tradeexception.platform.common.couchdb.CouchDbClient;
import com.tradeexception.platform.common.couchdb.CouchDbProperties;
import com.tradeexception.platform.enrichment.domain.ReferenceData;
import com.tradeexception.platform.enrichment.domain.ReferenceDataCategory;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/** CouchDB repository for {@link ReferenceData} documents in the {@code reference-data} database. */
@Repository
public class ReferenceDataRepository extends AbstractCouchDbRepository<ReferenceData> {

    public ReferenceDataRepository(CouchDbClient client, CouchDbProperties properties) {
        super(client, properties.getDatabases().getReferenceData(), ReferenceData.class);
    }

    public Optional<ReferenceData> findByCategoryAndCode(ReferenceDataCategory category, String code) {
        if (category == null || code == null || code.isBlank()) {
            return Optional.empty();
        }
        return findById(ReferenceData.TYPE + ":" + category.name() + ":" + code);
    }

    public List<ReferenceData> findActiveByCategory(ReferenceDataCategory category) {
        return findBySelector(Map.of("category", category.name(), "active", true));
    }
}
