package com.tradeexception.platform.trade.domain;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.tradeexception.platform.audit.domain.AuditEvent;
import com.tradeexception.platform.audit.domain.EventType;
import com.tradeexception.platform.confirmation.domain.Confirmation;
import com.tradeexception.platform.confirmation.domain.ConfirmationStatus;
import com.tradeexception.platform.enrichment.domain.ReferenceData;
import com.tradeexception.platform.enrichment.domain.ReferenceDataCategory;
import com.tradeexception.platform.exception.domain.ExceptionCategory;
import com.tradeexception.platform.exception.domain.ExceptionPriority;
import com.tradeexception.platform.exception.domain.ExceptionRecord;
import com.tradeexception.platform.exception.domain.ExceptionStatus;
import com.tradeexception.platform.workitem.domain.WorkItem;
import com.tradeexception.platform.workitem.domain.WorkItemStatus;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class DocumentSerializationTest {

    private final ObjectMapper mapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    @Test
    void tradeSerializesWithCouchDbMetadataAndDeserializesCleanly() throws Exception {
        Trade trade = DomainValidationTest.validSwapTrade();
        trade.setRevision("1-abc123");
        trade.setCreatedAt(Instant.parse("2026-11-24T09:00:00Z"));
        trade.setUpdatedAt(Instant.parse("2026-11-24T09:00:00Z"));

        String json = mapper.writeValueAsString(trade);
        JsonNode root = mapper.readTree(json);

        assertThat(root.get("_id").asText()).isEqualTo("trade:TRD-001");
        assertThat(root.get("_rev").asText()).isEqualTo("1-abc123");
        assertThat(root.get("type").asText()).isEqualTo("trade");
        assertThat(root.get("documentSchemaVersion").asInt()).isEqualTo(1);
        assertThat(root.get("productType").asText()).isEqualTo("INTEREST_RATE_SWAP");

        Trade restored = mapper.readValue(json, Trade.class);
        assertThat(restored.getId()).isEqualTo("trade:TRD-001");
        assertThat(restored.getRevision()).isEqualTo("1-abc123");
        assertThat(restored.getEconomics().notionalAmount()).isEqualByComparingTo(new BigDecimal("10000000.00"));
        assertThat(restored.getParties()).hasSize(2);
    }

    @Test
    void unknownFutureFieldsAreIgnoredForForwardCompatibility() throws Exception {
        String futureJson = """
                {
                  "_id": "confirmation:CNF-99",
                  "_rev": "2-xyz",
                  "type": "confirmation",
                  "documentSchemaVersion": 2,
                  "confirmationId": "CNF-99",
                  "tradeId": "TRD-99",
                  "correlationId": "CORR-99",
                  "status": "MATCHED",
                  "futureOptionalField": "ignored-safely"
                }
                """;
        Confirmation restored = mapper.readValue(futureJson, Confirmation.class);
        assertThat(restored.getId()).isEqualTo("confirmation:CNF-99");
        assertThat(restored.getRevision()).isEqualTo("2-xyz");
        assertThat(restored.getStatus()).isEqualTo(ConfirmationStatus.MATCHED);
    }

    @Test
    void remainingDocumentsRoundTripThroughJson() throws Exception {
        ExceptionRecord ex = new ExceptionRecord();
        ex.setExceptionId("EXC-1");
        ex.setTradeId("TRD-1");
        ex.setCorrelationId("CORR-1");
        ex.setCategory(ExceptionCategory.ECONOMIC_MISMATCH);
        ex.setPriority(ExceptionPriority.CRITICAL);
        ex.setStatus(ExceptionStatus.OPEN);
        ex.setReasonCode("RATE_DIFF");
        ex.setDetectedAt(Instant.parse("2026-11-24T09:05:00Z"));
        ExceptionRecord exOut = mapper.readValue(mapper.writeValueAsString(ex), ExceptionRecord.class);
        assertThat(exOut.getId()).isEqualTo("exception:EXC-1");
        assertThat(exOut.getCategory()).isEqualTo(ExceptionCategory.ECONOMIC_MISMATCH);

        WorkItem wi = new WorkItem();
        wi.setWorkItemId("WI-1");
        wi.setExceptionId("EXC-1");
        wi.setTradeId("TRD-1");
        wi.setCorrelationId("CORR-1");
        wi.setStatus(WorkItemStatus.ASSIGNED);
        wi.setPriority(ExceptionPriority.CRITICAL);
        wi.setAssignee("ops-1");
        WorkItem wiOut = mapper.readValue(mapper.writeValueAsString(wi), WorkItem.class);
        assertThat(wiOut.getId()).isEqualTo("workitem:WI-1");
        assertThat(wiOut.getStatus()).isEqualTo(WorkItemStatus.ASSIGNED);

        AuditEvent audit = new AuditEvent();
        audit.setEventId("AUD-1");
        audit.setEventType(EventType.EXCEPTION_CREATED);
        audit.setTradeId("TRD-1");
        audit.setExceptionId("EXC-1");
        audit.setCorrelationId("CORR-1");
        audit.setActor("system:stp");
        audit.setOccurredAt(Instant.parse("2026-11-24T09:05:00Z"));
        audit.setDetails(Map.of("rule", "RATE_CHECK"));
        AuditEvent auditOut = mapper.readValue(mapper.writeValueAsString(audit), AuditEvent.class);
        assertThat(auditOut.getId()).isEqualTo("audit:AUD-1");
        assertThat(auditOut.getDetails()).containsEntry("rule", "RATE_CHECK");

        ReferenceData ref = new ReferenceData();
        ref.setCategory(ReferenceDataCategory.CURRENCY);
        ref.setCode("EUR");
        ref.setAttributes(Map.of("decimals", "2"));
        ReferenceData refOut = mapper.readValue(mapper.writeValueAsString(ref), ReferenceData.class);
        assertThat(refOut.getId()).isEqualTo("refdata:CURRENCY:EUR");
        assertThat(refOut.getAttributes()).containsEntry("decimals", "2");
    }
}
