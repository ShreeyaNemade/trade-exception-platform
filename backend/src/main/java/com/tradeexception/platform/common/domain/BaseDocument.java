package com.tradeexception.platform.common.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

/**
 * Common metadata shared by every persisted document.
 * <p>
 * The document {@code _id} is derived deterministically as {@code <type>:<businessKey>} so the
 * same business entity always maps to the same document (natural idempotency). {@code _rev} is
 * CouchDB's MVCC revision and is used for optimistic concurrency.
 * <p>
 * Unknown JSON properties are ignored so older application versions can read documents written
 * by newer ones (forward-compatible schema evolution).
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(value = {"_id", "type"}, allowGetters = true, ignoreUnknown = true)
public abstract class BaseDocument {

    @JsonProperty("_rev")
    private String revision;

    private int documentSchemaVersion;
    private Instant createdAt;
    private Instant updatedAt;

    protected BaseDocument(int documentSchemaVersion) {
        this.documentSchemaVersion = documentSchemaVersion;
    }

    /** Deterministic document id, or {@code null} while the business key is not yet set. */
    @JsonProperty("_id")
    public final String getId() {
        String key = businessKey();
        return (key == null || key.isBlank()) ? null : getType() + ":" + key;
    }

    /** Discriminator stored in every document; also the id prefix. */
    @JsonProperty("type")
    public abstract String getType();

    /** Business identifier that uniquely identifies the document within its type. */
    protected abstract String businessKey();

    public String getRevision() {
        return revision;
    }

    public void setRevision(String revision) {
        this.revision = revision;
    }

    public int getDocumentSchemaVersion() {
        return documentSchemaVersion;
    }

    public void setDocumentSchemaVersion(int documentSchemaVersion) {
        this.documentSchemaVersion = documentSchemaVersion;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }
}
