# Kafka Event Design (Proposed)

> Proposed contracts for future phases. **Not implemented.**

## Conventions
- Topic names: lowercase, dot-separated `<domain>.<state>`.
- Event type (header/field `eventType`): PascalCase past tense, e.g. `TradeEnriched`.
- Message key: `tradeId` (guarantees per-trade ordering).
- Envelope fields: `eventId`, `eventType`, `eventVersion`, `occurredAt`, `correlationId`, `causationId`, `payload`.
- **correlationId**: generated at ingestion, carried unchanged through every downstream event for one trade. **causationId**: `eventId` of the event that triggered this one.
- Format: JSON.

## Topics
| Topic | Producer | Consumer(s) | Purpose |
|---|---|---|---|
| `trade.raw` | Ingestion | Trade Management | Unvalidated inbound trade |
| `trade.accepted` | Trade Management | Enrichment, Audit | Trade passed basic schema checks and persisted |
| `trade.enriched` | Enrichment | STP Validation, Audit | Trade with reference data attached |
| `stp.validated` | STP Validation | Confirmation, Exception Mgmt | Validation outcome (pass/fail + reasons) |
| `confirmation.generated` | Confirmation | Trade Mgmt, Audit | Confirmation produced for valid trade |
| `exception.created` | Exception Mgmt | Classification, Audit | Exception captured |
| `exception.classified` | Classification | Work Item, Exception Mgmt, Audit | Root cause assigned |
| `workitem.linked` | Work Item | Exception Mgmt, Audit | Work item attached to exception |
| `manual.review.required` | Classification / Exception Mgmt | Work Item, Audit | Needs human review |
| `deadletter.trade` | Any consumer on repeated failure | Replay & DLQ | Failed events with error metadata |

Retry policy, partition counts and retention will be defined in the implementation phase.
