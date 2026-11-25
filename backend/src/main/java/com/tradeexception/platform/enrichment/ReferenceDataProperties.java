package com.tradeexception.platform.enrichment;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** Environment-driven reference data seeding properties ({@code app.reference-data.*}). */
@ConfigurationProperties(prefix = "app.reference-data")
public class ReferenceDataProperties {

    private boolean autoSeed = true;
    private boolean failFast = false;

    public boolean isAutoSeed() { return autoSeed; }
    public void setAutoSeed(boolean autoSeed) { this.autoSeed = autoSeed; }
    public boolean isFailFast() { return failFast; }
    public void setFailFast(boolean failFast) { this.failFast = failFast; }
}
