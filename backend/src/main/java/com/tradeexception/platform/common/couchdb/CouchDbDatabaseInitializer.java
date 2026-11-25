package com.tradeexception.platform.common.couchdb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Safely and idempotently initializes required CouchDB databases and Mango indexes on startup.
 * Existing databases are never deleted or recreated.
 */
@Component
@Order(10)
public class CouchDbDatabaseInitializer implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(CouchDbDatabaseInitializer.class);

    private final CouchDbClient client;
    private final CouchDbProperties properties;

    public CouchDbDatabaseInitializer(CouchDbClient client, CouchDbProperties properties) {
        this.client = client;
        this.properties = properties;
    }

    @Override
    public void run(ApplicationArguments args) {
        if (!properties.isAutoInitialize()) {
            log.info("CouchDB auto-initialization is disabled");
            return;
        }
        try {
            initializeDatabases();
        } catch (CouchDbException ex) {
            if (properties.isFailFastOnInit()) {
                throw ex;
            }
            log.warn("CouchDB initialization skipped because database is unavailable: {}", ex.getMessage());
        }
    }

    /** Ensures all configured databases and core Mango indexes exist. */
    public void initializeDatabases() {
        CouchDbProperties.Databases dbs = properties.getDatabases();
        for (String db : dbs.allNames()) {
            if (client.databaseExists(db)) {
                log.debug("CouchDB database '{}' already exists", db);
            } else {
                boolean created = client.createDatabaseIfAbsent(db);
                if (created) {
                    log.info("Created CouchDB database '{}'", db);
                } else {
                    log.debug("CouchDB database '{}' already existed (concurrent creation)", db);
                }
            }
        }
        ensureCoreIndexes(dbs);
    }

    private void ensureCoreIndexes(CouchDbProperties.Databases dbs) {
        client.ensureIndex(dbs.getTrades(), "idx_trades_status", List.of("status"));
        client.ensureIndex(dbs.getTrades(), "idx_trades_correlation", List.of("header.correlationId"));
        client.ensureIndex(dbs.getConfirmations(), "idx_confirmations_trade", List.of("tradeId"));
        client.ensureIndex(dbs.getExceptions(), "idx_exceptions_trade", List.of("tradeId"));
        client.ensureIndex(dbs.getExceptions(), "idx_exceptions_status", List.of("status"));
        client.ensureIndex(dbs.getWorkItems(), "idx_workitems_exception", List.of("exceptionId"));
        client.ensureIndex(dbs.getWorkItems(), "idx_workitems_trade", List.of("tradeId"));
        client.ensureIndex(dbs.getAuditEvents(), "idx_audit_trade", List.of("tradeId"));
        client.ensureIndex(dbs.getAuditEvents(), "idx_audit_correlation", List.of("correlationId"));
        client.ensureIndex(dbs.getReferenceData(), "idx_refdata_category_active", List.of("category", "active"));
        client.ensureIndex(dbs.getDeadLetters(), "idx_dlq_category_status", List.of("errorCategory", "replayStatus"));
        client.ensureIndex(dbs.getDeadLetters(), "idx_dlq_topic", List.of("originalTopic"));
    }
}
