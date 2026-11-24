# Database Design (Proposed)

> Proposed CouchDB document model. **Not implemented.** Every document has `_id`, `_rev`, `type`, `createdAt`, `updatedAt`. IDs use a type prefix, e.g. `trade:<tradeId>`.

| Type | Purpose | Key relationships |
|---|---|---|
| Trade | Trade state, raw and enriched attributes, processing status | Referenced by all others via `tradeId` |
| Confirmation | Confirmation generated for a valid trade | `tradeId` → Trade |
| Exception | Failure that blocked STP, with reasons and classification | `tradeId` → Trade; has 0..n WorkItems |
| WorkItem | Remediation task, assignee, status | `exceptionId` → Exception |
| AuditEvent | Append-only record of each state change | `tradeId`, optional `exceptionId`, `correlationId` |
| ReferenceData | Synthetic lookups (instruments, counterparties, currencies) | Read by Enrichment/Validation |

Queries will use Mango indexes / views on `type`, `tradeId`, `status` and `correlationId`; these will be defined when implemented. Relationships are by ID reference (no joins); audit events are never updated.
