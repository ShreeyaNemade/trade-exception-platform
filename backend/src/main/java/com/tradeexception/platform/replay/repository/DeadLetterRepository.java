package com.tradeexception.platform.replay.repository;

import com.tradeexception.platform.common.couchdb.AbstractCouchDbRepository;
import com.tradeexception.platform.common.couchdb.CouchDbClient;
import com.tradeexception.platform.common.couchdb.CouchDbProperties;
import com.tradeexception.platform.replay.domain.DeadLetterDocument;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/** CouchDB repository for {@link DeadLetterDocument} records in the {@code dead-letters} database. */
@Repository
public class DeadLetterRepository extends AbstractCouchDbRepository<DeadLetterDocument> {

    public DeadLetterRepository(CouchDbClient client, CouchDbProperties properties) {
        super(client, properties.getDatabases().getDeadLetters(), DeadLetterDocument.class);
    }

    public Optional<DeadLetterDocument> findByDeadLetterId(String deadLetterId) {
        return (deadLetterId == null || deadLetterId.isBlank())
                ? Optional.empty()
                : findById(DeadLetterDocument.documentId(deadLetterId));
    }
}
