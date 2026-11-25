package com.tradeexception.platform.enrichment.domain;

import com.tradeexception.platform.common.domain.BaseDocument;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Synthetic lookup data used by enrichment and validation.
 * Document id: {@code refdata:<category>:<code>}, e.g. {@code refdata:CURRENCY:USD}.
 */
public class ReferenceData extends BaseDocument {

    public static final String TYPE = "refdata";
    public static final int SCHEMA_VERSION = 1;

    @NotNull
    private ReferenceDataCategory category;

    @NotBlank @Size(max = 64)
    private String code;

    @Size(max = 256)
    private String description;

    /** Category-specific attributes, e.g. {@code decimals} for a currency. */
    private Map<String, String> attributes;

    private boolean active = true;

    public ReferenceData() {
        super(SCHEMA_VERSION);
    }

    @Override
    public String getType() { return TYPE; }

    @Override
    protected String businessKey() {
        return (category == null || code == null) ? null : category.name() + ":" + code;
    }

    public ReferenceDataCategory getCategory() { return category; }
    public void setCategory(ReferenceDataCategory category) { this.category = category; }
    public String getCode() { return code; }
    public void setCode(String code) { this.code = code; }
    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }
    public Map<String, String> getAttributes() { return attributes; }
    public void setAttributes(Map<String, String> attributes) { this.attributes = attributes == null ? null : Collections.unmodifiableMap(new LinkedHashMap<>(attributes)); }
    public boolean isActive() { return active; }
    public void setActive(boolean active) { this.active = active; }
}
