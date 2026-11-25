# Kafka Event Design

> **Status: Phase 5 Implemented** for all 10 core topics: `trade.raw`, `trade.accepted`, `trade.enriched`, `stp.validated`, `confirmation.generated`, `exception.created`, `exception.classified`, `workitem.linked`, `manual.review.required`, and `deadletter.trade`.

## 1. Event Envelope Structure

All Kafka events use `EventEnvelope<T>` serialized as JSON (UTF-8 strings via Jackson):

| Field | Type | Constraints / Purpose |
|---|---|---|
| `eventId` | `String` (UUID) | Unique per event; used for idempotency and audit linking |
| `eventType` | `String` | PascalCase past tense (`TradeReceived`, `TradeAccepted`, `TradeEnriched`, `StpValidated`, `ExceptionCreated`, `ManualReviewRequired`, `TradeDeadLettered`) |
| `correlationId` | `String` | Generated at ingestion and propagated unchanged across all downstream events |
| `schemaVersion` | `String` | `MAJOR.MINOR` contract version (`1.0`) |
| `timestamp` | `Instant` (UTC) | Event creation time |
| `source` | `String` | Originating module (`synthetic-trade-producer`, `trade-ingestion`, `trade-enrichment`, `stp-validation`, `trade-ingestion-dlq`) |
| `payload` | `T` | Domain payload (`Trade`, `EnrichedTrade`, `StpValidationResult`, `ExceptionRecord`, `DeadLetterRecord`) |

## 2. Topics and Consumer Groups

Default configuration (`app.kafka.*`): `3` partitions, replication factor `1` (local KRaft). Message key is `tradeId` for per-trade ordering.

| Topic | Producer | Consumer (Group) | Purpose |
|---|---|---|---|
| `trade.raw` | `SyntheticTradeProducer` | `TradeIngestionConsumer` (`trade-ingestion-group`) | Raw inbound synthetic trade events |
| `trade.accepted` | `TradeIngestionService` | `TradeEnrichmentConsumer` (`trade-enrichment-group`) | Validated, persisted trades (`ACCEPTED`) |
| `trade.enriched` | `TradeEnrichmentService` | `StpValidationConsumer` (`stp-validation-group`) | Trades enriched with reference data & provenance |
| `stp.validated` | `StpValidationService` | `ConfirmationGenerationConsumer` (`confirmation-generation-group`) | `STP_ELIGIBLE` validation outcomes |
| `confirmation.generated` | `ConfirmationGenerationService` | Downstream / Audit / UI (Phase 6) | Generated synthetic trade confirmation document |
| `exception.created` | `StpValidationService` / `ConfirmationGenerationService` | `ExceptionClassificationConsumer` (`exception-classification-group`) | Initial `ExceptionRecord` for rejected / manual-review / missing-template trades |
| `exception.classified` | `ExceptionClassificationService` | `WorkItemManagementConsumer` (`workitem-management-group`) | Classified exception with root cause, priority, severity & remediation |
| `workitem.linked` | `WorkItemManagementService` | Downstream / UI (Phase 6) | Remediation work item linked to classified exception |
| `manual.review.required` | `StpValidationService` | Operational Review Queue (Phase 6) | Trades requiring manual operational review |
| `deadletter.trade` | `DeadLetterPublisher` | Replay & DLQ (Phase 6+) | Malformed JSON or retry-exhausted technical failures |

## 3. Sample JSON Payloads (Synthetic Data Only)

### `trade.enriched` (`TradeEnriched`)
```json
{
  "eventId": "5a9e12b4-41c2-4f9e-b10a-99aa11bb22cc",
  "eventType": "TradeEnriched",
  "correlationId": "CORR-A1B2C3D4",
  "schemaVersion": "1.0",
  "timestamp": "2026-11-24T09:15:01Z",
  "source": "trade-enrichment",
  "payload": {
    "trade": { "tradeId": "TRD-A1B2C3D4", "productType": "FX_SPOT", "status": "ENRICHED" },
    "correlationId": "CORR-A1B2C3D4",
    "enrichedAt": "2026-11-24T09:15:01Z",
    "enrichedFields": {
      "buyer.legalName": {
        "fieldName": "buyer.legalName",
        "value": "Synthetic Bank Alpha",
        "source": "reference-data:refdata:COUNTERPARTY:SYNTHBANK00000000001",
        "enrichedAt": "2026-11-24T09:15:01Z",
        "origin": "SUPPLIED"
      },
      "instrument.code": {
        "fieldName": "instrument.code",
        "value": "FX_SPOT:USD/EUR",
        "source": "reference-data:refdata:INSTRUMENT:FX_SPOT:USD/EUR",
        "enrichedAt": "2026-11-24T09:15:01Z",
        "origin": "DERIVED"
      }
    },
    "buyerFound": true,
    "buyerEligible": true,
    "sellerFound": true,
    "sellerEligible": true,
    "currencySupported": true,
    "settlementCurrencySupported": true,
    "productFound": true,
    "productStpEligible": true,
    "requireNonEconomicTerms": false,
    "instrumentFound": true,
    "confirmationTemplateAvailable": true,
    "enrichmentFailures": []
  }
}
```

### `stp.validated` (`StpValidated`)
```json
{
  "eventId": "7b22f0a1-1234-4c56-9876-abcdef123456",
  "eventType": "StpValidated",
  "correlationId": "CORR-A1B2C3D4",
  "schemaVersion": "1.0",
  "timestamp": "2026-11-24T09:15:02Z",
  "source": "stp-validation",
  "payload": {
    "tradeId": "TRD-A1B2C3D4",
    "correlationId": "CORR-A1B2C3D4",
    "decision": "STP_ELIGIBLE",
    "validatedAt": "2026-11-24T09:15:02Z",
    "passedRules": [
      "COUNTERPARTY_VALIDATION", "CURRENCY_SUPPORT", "DATE_CONSISTENCY",
      "ECONOMIC_FIELDS", "INSTRUMENT_AND_TEMPLATE", "MANDATORY_IDENTIFIERS",
      "NON_ECONOMIC_TERMS", "PRODUCT_ELIGIBILITY"
    ],
    "failedRules": [],
    "reasonCodes": [],
    "explanation": "All 8 STP validation rules passed."
  }
}
```

### `exception.created` (`ExceptionCreated`)
```json
{
  "eventId": "9c33a1b2-5678-4d90-a123-456789abcdef",
  "eventType": "ExceptionCreated",
  "correlationId": "CORR-E5F60708",
  "schemaVersion": "1.0",
  "timestamp": "2026-11-24T09:16:00Z",
  "source": "stp-validation",
  "payload": {
    "_id": "exception:EXC-TRD-E5F60708",
    "_rev": "1-ab12cd34",
    "type": "exception",
    "documentSchemaVersion": 1,
    "exceptionId": "EXC-TRD-E5F60708",
    "tradeId": "TRD-E5F60708",
    "correlationId": "CORR-E5F60708",
    "category": "UNCLASSIFIED",
    "priority": "MEDIUM",
    "status": "OPEN",
    "reasonCode": "COUNTERPARTY_NOT_STP_ELIGIBLE",
    "description": "Manual review required (1 rule(s) flagged): COUNTERPARTY_VALIDATION: Counterparty exists in reference data but is restricted and requires manual review",
    "detectedAt": "2026-11-24T09:16:00Z"
  }
}
```

## 4. Idempotency and Failure Routing

- **Idempotency:** Each stage checks `Trade.status` before mutating (`ENRICHED` or beyond skips enrichment; `VALIDATED`/`EXCEPTION`/`CONFIRMED`/`REJECTED` skips STP validation). Exceptions use deterministic ID `EXC-<tradeId>` and check existence before saving (plus catch CouchDB HTTP `409`) so existing exceptions are never overwritten. Audit events use deterministic stage-prefixed IDs (`ingest-`, `enrich-`, `stp-`).
- **Business vs. Technical Failures:** Ordinary business rule failures (`STP_REJECTED`, `MANUAL_REVIEW_REQUIRED`) are persisted as `ExceptionRecord`s and published to `exception.created` (and `manual.review.required`). Only malformed payloads (`DESERIALIZATION_ERROR`) or retry-exhausted technical failures (`INFRASTRUCTURE_ERROR` after `maxAttempts=3`, `backoffMs=500`) are routed to `deadletter.trade`.
