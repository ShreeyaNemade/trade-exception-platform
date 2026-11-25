# Operational REST API Reference

All JSON endpoints return the standard `ApiResponse<T>` envelope:

```json
{
  "success": true,
  "data": { ... },
  "timestamp": "2026-11-24T09:30:00Z"
}
```

Error responses return `success: false` with `error: { code, message, details }`:
- `400 Bad Request` — `VALIDATION_ERROR`, `INVALID_PARAMETER`, `MALFORMED_REQUEST`
- `404 Not Found` — `DOCUMENT_NOT_FOUND`, `NOT_FOUND`
- `409 Conflict` — `COUCHDB_CONFLICT`, `INVALID_STATE_TRANSITION`
- `503 Service Unavailable` — `COUCHDB_UNAVAILABLE`

CORS on `/api/**` is restricted to `CORS_ALLOWED_ORIGINS` (default `http://localhost:5173`).

---

## Authentication & Authorization
Stateless Bearer token (HMAC-SHA256) and HTTP Basic authentication backed by Spring Security (`BCryptPasswordEncoder`) and environment-configured roles (`ADMIN`, `OPERATIONS`, `VIEWER`):
- `POST /api/v1/auth/login` — `{ "username": "...", "password": "..." }` → returns `{ accessToken, tokenType, username, roles, expiresAt }`.
- Public: `GET /api/v1/health`, `GET /actuator/health`, `POST /api/v1/auth/login`.
- `ADMIN` / `OPERATIONS`: `POST /api/v1/synthetic-trades/{scenario}`, `POST /api/v1/exceptions/{exceptionId}/remediate`, `PATCH /api/v1/work-items/**`, `POST /api/v1/dlq/{deadLetterId}/replay`.
- `ADMIN` / `OPERATIONS` / `VIEWER`: all `GET /api/v1/**` operational endpoints.

## DLQ & Controlled Replay
- `GET /api/v1/dlq` — filters: `errorCategory`, `originalTopic`, `replayStatus`, `from`, `to`, `page`, `size`, `sortBy`, `sortDir`.
- `GET /api/v1/dlq/{deadLetterId}` — retrieve persisted DLQ record details.
- `GET /api/v1/dlq/{deadLetterId}/history` — retrieve replay attempt history.
- `POST /api/v1/dlq/{deadLetterId}/replay` (`ADMIN` / `OPERATIONS`) — validates eligibility, acquires a CouchDB MVCC `IN_PROGRESS` lock, republishes the unmodified payload to its original Kafka topic preserving `eventId` and `correlationId`, updates status/history, and writes audit events. Malformed/validation-failed, already-replayed, in-progress, or max-attempt records return `409 INVALID_STATE_TRANSITION`.

---

## Health
- `GET /api/v1/health` — application liveness envelope.
- `GET /actuator/health` — Spring Boot Actuator health & probes.

## Dashboard
- `GET /api/v1/dashboard/summary` — returns live aggregated KPIs (`totalTrades`, `acceptedTrades`, `enrichedTrades`, `stpEligibleTrades`, `stpRejectedTrades`, `manualReviewRequired`, `confirmationsGenerated`, `openExceptions`, `criticalExceptions`, `openWorkItems`, `resolvedWorkItems`, `stpSuccessRatePercent`) and chart distributions (`tradeStatusDistribution`, `tradeVolumeByDate`, `exceptionCategoryDistribution`, `workItemStatusDistribution`).

## Trades (Read-Only)
- `GET /api/v1/trades` — query params: `status`, `productType`, `search`, `page` (0), `size` (20), `sortBy` (`createdAt`|`tradeId`|`tradeDate`|`status`|`notionalAmount`), `sortDir` (`asc`|`desc`).
- `GET /api/v1/trades/{tradeId}` — retrieve a single trade (`404` if missing).
- `GET /api/v1/trades/{tradeId}/events` — chronological audit events for the trade.

## Confirmations (Read-Only)
- `GET /api/v1/confirmations` — query params: `status`, `productType`, `page`, `size`, `sortBy`, `sortDir`.
- `GET /api/v1/confirmations/{confirmationId}` — retrieve confirmation metadata.
- `GET /api/v1/confirmations/trade/{tradeId}` — retrieve confirmations linked to a trade.
- `GET /api/v1/confirmations/{confirmationId}/document` — returns persisted `text/html` confirmation document (never regenerates on read).

## Exceptions & Remediation
- `GET /api/v1/exceptions` — query params: `category`, `priority`, `severity`, `classified` (`true`|`false`), `manualReviewOnly` (`true`|`false`), `status`, `tradeId`, `search`, `page`, `size`, `sortBy`, `sortDir`.
- `GET /api/v1/exceptions/{exceptionId}` — retrieve exception details including `rootCause`, `explanation`, `recommendedAction`, resolution metadata, and `revalidationHistory`.
- `POST /api/v1/exceptions/{exceptionId}/remediate` (`ADMIN` / `OPERATIONS`) — updates editable trade economic fields (`notionalAmount`, `notionalCurrency`, `price`, `fixedRate`, `floatingRateIndex`, `settlementCurrency`, optional `remediationNote`), re-runs enrichment and STP validation via the existing rule engine, and writes immutable audit events:
  - If `STP_ELIGIBLE`: updates `Trade.status = VALIDATED`, marks the exception `RESOLVED` (with `resolutionReason`, `resolvedAt`, `resolvedBy`), resolves any open linked work item, and publishes `StpValidated` to Kafka `stp.validated` so confirmation generation produces `CNF-<tradeId>` and sets `Trade.status = CONFIRMED`.
  - If `STP_REJECTED` or `MANUAL_REVIEW_REQUIRED`: keeps `Trade.status = EXCEPTION` and `ExceptionRecord.status = OPEN`, appends the attempt to `revalidationHistory`, and publishes `ExceptionClassified`.
  - Already-resolved exceptions return `409 INVALID_STATE_TRANSITION`.

## Work Items
- `GET /api/v1/work-items` — query params: `status`, `priority`, `assignedTeam`, `tradeId`, `exceptionId`, `page`, `size`, `sortBy`, `sortDir`.
- `GET /api/v1/work-items/{workItemId}` — retrieve a single work item.
- `PATCH /api/v1/work-items/{workItemId}/assign`
  ```json
  { "assignedTeam": "COUNTERPARTY_OPERATIONS", "assignee": "ops-queue-1", "actor": "ops-supervisor" }
  ```
- `PATCH /api/v1/work-items/{workItemId}/status`
  ```json
  { "status": "IN_PROGRESS", "actor": "ops-queue-1" }
  ```
  Validates lifecycle transitions (`OPEN` → `IN_PROGRESS` | `PENDING` | `RESOLVED` | `CLOSED`, etc.) and writes immutable audit events.

## Audit Trail (Read-Only)
- `GET /api/v1/audit-events` — query params: `entityId`, `entityType`, `eventType`, `correlationId`, `from` (ISO-8601), `to` (ISO-8601), `page` (0-based, default `0`), `size` (default `25`, max configurable via `app.audit.max-page-size`, default `100`), `sortBy` (default `occurredAt`), `sortDir` (default `desc`, newest first with `eventId` tie-breaker). Returns `PagedResponse<AuditEventDto>` with `items`, `page`, `size`, `totalElements`, `totalPages`, `hasNext`, `hasPrevious`, `sortBy`, `sortDir`.
- `GET /api/v1/audit-events/{eventId}` — retrieve a single audit event.

## Synthetic Trade Trigger (Kafka Only)
- `POST /api/v1/synthetic-trades/{scenario}` — returns `202 Accepted` with `{ eventId, tradeId, correlationId, topic, scenario, messagesPublished }`.
  - Allowed `{scenario}` values: `VALID_FX_SPOT`, `VALID_FX_FORWARD`, `MISSING_MANDATORY_ECONOMIC_FIELD`, `INVALID_COUNTERPARTY_REFERENCE`, `DUPLICATE_MESSAGE`, `MALFORMED_PAYLOAD`.
  - Publishes directly to Kafka `trade.raw`; never writes directly to CouchDB.

### Example Calls
```bash
curl -s -X POST http://localhost:8080/api/v1/synthetic-trades/VALID_FX_SPOT
curl -s "http://localhost:8080/api/v1/trades?status=CONFIRMED&page=0&size=10"
curl -s http://localhost:8080/api/v1/dashboard/summary
curl -s -X PATCH http://localhost:8080/api/v1/work-items/WI-EXC-TRD-1/status \
  -H "Content-Type: application/json" -d '{"status":"IN_PROGRESS"}'
```
