# Database Design (Apache CouchDB)

> **Status: Implemented through Phase 5** — domain models, CouchDB HTTP client, idempotent database/index initializer, repositories, synthetic reference data seeder, confirmation documents (JSON + rendered HTML), classified exceptions, work items with operational team routing, and append-only audit history are implemented.

## 1. Architecture and Database Naming Conventions

The platform uses **Apache CouchDB** as a document store with **one dedicated database per bounded document family**. Separating databases avoids cross-concern compaction overhead, allows independent retention policies (for example, append-only `audit-events` vs. mutable `trades`), and keeps Mango indexes focused.

All database names follow CouchDB's lowercase + hyphen convention and are configurable via environment variables (`app.couchdb.databases.*`):

| Database | Env override | Document class | `type` | Mutability |
|---|---|---|---|---|
| `trades` | `COUCHDB_DB_TRADES` | `Trade` | `trade` | Mutable (lifecycle transitions) |
| `confirmations` | `COUCHDB_DB_CONFIRMATIONS` | `Confirmation` | `confirmation` | Mutable |
| `exceptions` | `COUCHDB_DB_EXCEPTIONS` | `ExceptionRecord` | `exception` | Mutable |
| `work-items` | `COUCHDB_DB_WORK_ITEMS` | `WorkItem` | `workitem` | Mutable |
| `audit-events` | `COUCHDB_DB_AUDIT_EVENTS` | `AuditEvent` | `audit` | **Append-only** (updates & deletes rejected) |
| `reference-data` | `COUCHDB_DB_REFERENCE_DATA` | `ReferenceData` | `refdata` | Slowly changing |

On startup (when `COUCHDB_AUTO_INITIALIZE=true`), `CouchDbDatabaseInitializer` checks each database with `HEAD /{db}` and creates it with `PUT /{db}` only if absent (HTTP `412` is treated as already created). Existing databases are never dropped or recreated.

## 2. Domain Model Overview

Every persisted aggregate extends `BaseDocument`, which carries:
- `_id` — deterministic document identifier (`<type>:<businessKey>`).
- `_rev` — CouchDB MVCC revision token used for optimistic concurrency.
- `type` — document discriminator matching the `_id` prefix.
- `documentSchemaVersion` — integer schema version (currently `1`).
- `createdAt` / `updatedAt` — UTC `Instant` timestamps managed by `AbstractCouchDbRepository`.

Financial amounts use `BigDecimal` (never `double`/`float`). Controlled values use Java enums (`ProductType`, `TradeStatus`, `PartyRole`, `ConfirmationStatus`, `ExceptionCategory`, `ExceptionPriority`, `ExceptionStatus`, `WorkItemStatus`, `EventType`, `ReferenceDataCategory`).

### Relationships Without Relational Joins

CouchDB does not support server-side SQL joins. Documents reference related aggregates by business identifier rather than embedding entire aggregates:

- `Confirmation.tradeId` → `Trade.tradeId` (`trade:<tradeId>`)
- `ExceptionRecord.tradeId` → `Trade.tradeId`
- `WorkItem.exceptionId` → `ExceptionRecord.exceptionId` (`exception:<exceptionId>`) and denormalised `WorkItem.tradeId` → `Trade.tradeId`
- `AuditEvent.tradeId` / `AuditEvent.exceptionId` / `AuditEvent.correlationId` → links back to the trade, exception, and end-to-end processing flow

Using references keeps each aggregate small, avoids write contention across modules, and lets each module evolve independently.

## 3. Document ID Strategy

IDs are deterministic rather than random CouchDB UUIDs:

| Document | `_id` pattern | Example |
|---|---|---|
| `Trade` | `trade:<tradeId>` | `trade:TRD-2026-0001` |
| `Confirmation` | `confirmation:<confirmationId>` | `confirmation:CNF-2026-0001` |
| `ExceptionRecord` | `exception:<exceptionId>` | `exception:EXC-2026-0001` |
| `WorkItem` | `workitem:<workItemId>` | `workitem:WI-2026-0001` |
| `AuditEvent` | `audit:<eventId>` | `audit:AUD-2026-0001` |
| `ReferenceData` | `refdata:<category>:<code>` | `refdata:CURRENCY:USD` |

Benefits: natural idempotency on retries (writing the same key without `_rev` yields HTTP `409` instead of silent duplicates) and direct `GET /{db}/{id}` lookups without scanning an index.

## 4. Sample JSON Documents (Synthetic Data Only)

### Trade (`trades` database)
```json
{
  "_id": "trade:TRD-2026-0001",
  "_rev": "1-967a00dff5e02add41819138abb3284d",
  "type": "trade",
  "documentSchemaVersion": 1,
  "createdAt": "2026-11-24T09:00:00Z",
  "updatedAt": "2026-11-24T09:00:00Z",
  "header": {
    "messageId": "MSG-0001",
    "correlationId": "CORR-0001",
    "schemaVersion": "1.0",
    "sourceSystem": "SYNTH-OMS",
    "sentAt": "2026-11-24T08:59:58Z"
  },
  "tradeId": "TRD-2026-0001",
  "sourceSystemTradeId": "OMS-77881",
  "productType": "INTEREST_RATE_SWAP",
  "tradeDate": "2026-11-24",
  "effectiveDate": "2026-11-26",
  "maturityDate": "2031-11-26",
  "parties": [
    { "partyId": "SYNTHBANK00000000001", "partyName": "Synthetic Bank A", "role": "BUYER" },
    { "partyId": "SYNTHCORP00000000002", "partyName": "Synthetic Corp B", "role": "SELLER" }
  ],
  "economics": {
    "notionalAmount": 10000000.00,
    "notionalCurrency": "USD",
    "fixedRate": 0.0425,
    "floatingRateIndex": "SOFR",
    "settlementCurrency": "USD"
  },
  "nonEconomicTerms": {
    "documentationType": "ISDA_2002",
    "governingLaw": "NEW_YORK",
    "businessDayConvention": "MODIFIED_FOLLOWING",
    "calculationAgentPartyId": "SYNTHBANK00000000001"
  },
  "status": "RECEIVED"
}
```

### Confirmation (`confirmations` database)
```json
{
  "_id": "confirmation:CNF-2026-0001",
  "_rev": "1-a1b2c3",
  "type": "confirmation",
  "documentSchemaVersion": 1,
  "createdAt": "2026-11-24T09:00:05Z",
  "updatedAt": "2026-11-24T09:00:05Z",
  "confirmationId": "CNF-2026-0001",
  "tradeId": "TRD-2026-0001",
  "correlationId": "CORR-0001",
  "status": "GENERATED",
  "generatedAt": "2026-11-24T09:00:05Z"
}
```

### ExceptionRecord (`exceptions` database)
```json
{
  "_id": "exception:EXC-2026-0001",
  "_rev": "1-d4e5f6",
  "type": "exception",
  "documentSchemaVersion": 1,
  "createdAt": "2026-11-24T09:01:00Z",
  "updatedAt": "2026-11-24T09:01:00Z",
  "exceptionId": "EXC-2026-0001",
  "tradeId": "TRD-2026-0002",
  "correlationId": "CORR-0002",
  "category": "COUNTERPARTY_UNKNOWN",
  "priority": "HIGH",
  "status": "OPEN",
  "reasonCode": "UNKNOWN_PARTY_LEI",
  "description": "Counterparty SYNTHCORP99999999999 not found in reference data.",
  "detectedAt": "2026-11-24T09:01:00Z"
}
```

### WorkItem (`work-items` database)
```json
{
  "_id": "workitem:WI-2026-0001",
  "_rev": "1-112233",
  "type": "workitem",
  "documentSchemaVersion": 1,
  "createdAt": "2026-11-24T09:01:05Z",
  "updatedAt": "2026-11-24T09:01:05Z",
  "workItemId": "WI-2026-0001",
  "exceptionId": "EXC-2026-0001",
  "tradeId": "TRD-2026-0002",
  "correlationId": "CORR-0002",
  "status": "OPEN",
  "priority": "HIGH",
  "assignee": "ops-analyst-1",
  "dueAt": "2026-11-25T09:01:05Z"
}
```

### AuditEvent (`audit-events` database)
```json
{
  "_id": "audit:AUD-2026-0001",
  "_rev": "1-445566",
  "type": "audit",
  "documentSchemaVersion": 1,
  "createdAt": "2026-11-24T09:00:00Z",
  "updatedAt": "2026-11-24T09:00:00Z",
  "eventId": "AUD-2026-0001",
  "eventType": "TRADE_RECEIVED",
  "tradeId": "TRD-2026-0001",
  "correlationId": "CORR-0001",
  "actor": "system:ingestion",
  "occurredAt": "2026-11-24T09:00:00Z",
  "details": {
    "sourceSystem": "SYNTH-OMS"
  }
}
```

### ReferenceData (`reference-data` database)
```json
{
  "_id": "refdata:CURRENCY:USD",
  "_rev": "1-778899",
  "type": "refdata",
  "documentSchemaVersion": 1,
  "createdAt": "2026-11-24T08:00:00Z",
  "updatedAt": "2026-11-24T08:00:00Z",
  "category": "CURRENCY",
  "code": "USD",
  "description": "US Dollar",
  "attributes": { "decimals": "2" },
  "active": true
}
```

## 5. Indexing Strategy

`CouchDbDatabaseInitializer` creates JSON Mango indexes (`POST /{db}/_index`, idempotent) for the core operational queries:

| Database | Index name | Fields |
|---|---|---|
| `trades` | `idx_trades_status` | `["status"]` |
| `trades` | `idx_trades_correlation` | `["header.correlationId"]` |
| `confirmations` | `idx_confirmations_trade` | `["tradeId"]` |
| `exceptions` | `idx_exceptions_trade` | `["tradeId"]` |
| `exceptions` | `idx_exceptions_status` | `["status"]` |
| `work-items` | `idx_workitems_exception` | `["exceptionId"]` |
| `work-items` | `idx_workitems_trade` | `["tradeId"]` |
| `audit-events` | `idx_audit_trade` | `["tradeId"]` |
| `audit-events` | `idx_audit_correlation` | `["correlationId"]` |
| `reference-data` | `idx_refdata_category_active` | `["category", "active"]` |

Primary lookups by `_id` (`trade:<tradeId>`, `refdata:<category>:<code>`) use CouchDB's built-in primary B-tree without a secondary index.

## 6. Optimistic Concurrency and Conflict Handling

CouchDB uses Multi-Version Concurrency Control (MVCC):
- Every update must supply the current `_rev` in the document body.
- `HttpCouchDbClient.save(...)` updates `document.revision` with the new `_rev` returned by CouchDB.
- If another writer modified the document concurrently (or a duplicate insert omits `_rev`), CouchDB responds with HTTP `409 Conflict`, which `HttpCouchDbClient` translates into `CouchDbConflictException` (mapped to HTTP `409` with error code `COUCHDB_CONFLICT` by `GlobalExceptionHandler`).
- Missing databases or documents (HTTP `404`) return `Optional.empty()` on `findById` and throw `CouchDbNotFoundException` on mutating or query operations.
- Network/timeout failures throw `CouchDbConnectionException` (HTTP `503`, `COUCHDB_UNAVAILABLE`).

## 7. Schema Evolution

- Every document carries `documentSchemaVersion` (integer) and inbound trade headers carry `header.schemaVersion` (`MAJOR.MINOR`).
- `BaseDocument` sets `@JsonIgnoreProperties(ignoreUnknown = true)` so older services can safely read documents written with newer optional fields.
- Additive changes (new optional fields) stay within the same `documentSchemaVersion`. Breaking structural changes increment `SCHEMA_VERSION` and are handled lazily on read or via a targeted migration script.
