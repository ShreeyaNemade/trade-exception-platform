package com.tradeexception.platform.common.couchdb;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

/**
 * Environment-driven CouchDB configuration bound from {@code app.couchdb.*}.
 * Credentials are never hardcoded; leave username/password empty only in local tests.
 */
@ConfigurationProperties(prefix = "app.couchdb")
public class CouchDbProperties {

    private String url = "http://localhost:5984";
    private String username = "";
    private String password = "";
    private int connectTimeoutMs = 5000;
    private int readTimeoutMs = 10000;
    private boolean autoInitialize = true;
    private boolean failFastOnInit = false;
    private Databases databases = new Databases();

    public String getUrl() { return url; }
    public void setUrl(String url) { this.url = url; }
    public String getUsername() { return username; }
    public void setUsername(String username) { this.username = username; }
    public String getPassword() { return password; }
    public void setPassword(String password) { this.password = password; }
    public int getConnectTimeoutMs() { return connectTimeoutMs; }
    public void setConnectTimeoutMs(int connectTimeoutMs) { this.connectTimeoutMs = connectTimeoutMs; }
    public int getReadTimeoutMs() { return readTimeoutMs; }
    public void setReadTimeoutMs(int readTimeoutMs) { this.readTimeoutMs = readTimeoutMs; }
    public boolean isAutoInitialize() { return autoInitialize; }
    public void setAutoInitialize(boolean autoInitialize) { this.autoInitialize = autoInitialize; }
    public boolean isFailFastOnInit() { return failFastOnInit; }
    public void setFailFastOnInit(boolean failFastOnInit) { this.failFastOnInit = failFastOnInit; }
    public Databases getDatabases() { return databases; }
    public void setDatabases(Databases databases) { this.databases = databases; }

    public static class Databases {
        private String trades = "trades";
        private String confirmations = "confirmations";
        private String exceptions = "exceptions";
        private String workItems = "work-items";
        private String auditEvents = "audit-events";
        private String referenceData = "reference-data";
        private String deadLetters = "dead-letters";

        public List<String> allNames() {
            return List.of(trades, confirmations, exceptions, workItems, auditEvents, referenceData, deadLetters);
        }

        public String getTrades() { return trades; }
        public void setTrades(String trades) { this.trades = trades; }
        public String getConfirmations() { return confirmations; }
        public void setConfirmations(String confirmations) { this.confirmations = confirmations; }
        public String getExceptions() { return exceptions; }
        public void setExceptions(String exceptions) { this.exceptions = exceptions; }
        public String getWorkItems() { return workItems; }
        public void setWorkItems(String workItems) { this.workItems = workItems; }
        public String getAuditEvents() { return auditEvents; }
        public void setAuditEvents(String auditEvents) { this.auditEvents = auditEvents; }
        public String getReferenceData() { return referenceData; }
        public void setReferenceData(String referenceData) { this.referenceData = referenceData; }
        public String getDeadLetters() { return deadLetters; }
        public void setDeadLetters(String deadLetters) { this.deadLetters = deadLetters; }
    }
}
