# Demonstration Guide — Trade Confirmation Exception Management Platform

> **Disclaimer:** Educational synthetic post-trade confirmation & exception management platform. Uses synthetic financial data only — not a production financial-market integration and confirmations are not legally binding.

## 1. Project Introduction
Post-trade confirmation verifies trade economics and legal terms between buyer and seller after execution. When static data is missing, counterparties are restricted, or economic terms fail validation, straight-through processing (STP) breaks and requires structured operational remediation. This platform demonstrates an enterprise-style, event-driven modular monolith built with Java 21, Spring Boot 3.3.5, Apache Kafka (KRaft), Apache CouchDB, and React + TypeScript.

## 2. Architecture Explanation
- **Modular Monolith:** Single Spring Boot deployable organized into clean domain modules (`ingestion`, `trade`, `enrichment`, `stp`, `confirmation`, `exception`, `classification`, `workitem`, `audit`, `replay`, `security`, `dashboard`).
- **Asynchronous Event Spine:** Kafka decouples each processing stage (`trade.raw` → `trade.accepted` → `trade.enriched` → `stp.validated` / `exception.created` → `confirmation.generated` / `exception.classified` → `workitem.linked`), keyed by `tradeId` for per-trade ordering and linked by `correlationId`.
- **Document Store:** Apache CouchDB with 7 bounded databases (`trades`, `confirmations`, `exceptions`, `work-items`, `audit-events`, `reference-data`, `dead-letters`) using deterministic IDs and MVCC `_rev` optimistic concurrency.

## 3. Trade Ingestion Flow
1. Trigger a synthetic trade via `POST /api/v1/synthetic-trades/VALID_FX_SPOT` (requires `ADMIN` or `OPERATIONS` role).
2. `SyntheticTradeProducer` publishes an `EventEnvelope<Trade>` (`TradeReceived`) to `trade.raw` — never writes directly to CouchDB.
3. `TradeIngestionConsumer` validates the envelope & `Trade` domain rules (Jakarta Bean Validation, `BigDecimal` amounts, chronological dates, buyer/seller roles, product-specific terms), enforces idempotency (`trade:<tradeId>`), persists `ACCEPTED`, writes `TRADE_ACCEPTED` audit, and publishes `trade.accepted`.

## 4. Enrichment & STP Validation
- `ReferenceDataSeeder` idempotently seeds synthetic currencies, counterparties (`SYNTHBANK00000000001`, `SYNTHCORP00000000002`, restricted `SYNTHFUND00000000003`), instruments, products, and confirmation templates.
- `TradeEnrichmentService` enriches `trade.accepted` with field-level provenance (`SUPPLIED`, `DERIVED`, `DEFAULTED`) without inventing missing financial data, then publishes `trade.enriched`.
- `StpRuleEngine` evaluates 8 deterministic rules (`MANDATORY_IDENTIFIERS`, `COUNTERPARTY_VALIDATION`, `PRODUCT_ELIGIBILITY`, `CURRENCY_SUPPORT`, `ECONOMIC_FIELDS`, `DATE_CONSISTENCY`, `INSTRUMENT_AND_TEMPLATE`, `NON_ECONOMIC_TERMS`) into `STP_ELIGIBLE`, `STP_REJECTED`, or `MANUAL_REVIEW_REQUIRED` (zero arbitrary risk scores).

## 5. Confirmation Generation
- `STP_ELIGIBLE` trades flow on `stp.validated` to `ConfirmationGenerationService`.
- Generates deterministic `CNF-<tradeId>` with JSON terms + structured non-legally-binding HTML (`ConfirmationTemplateRenderer`), updates `Trade.status = CONFIRMED`, records `CONFIRMATION_GENERATED`, and publishes `confirmation.generated`.
- Retrieve via `GET /api/v1/confirmations/CNF-<tradeId>/document`.

## 6. Exception Management & Classification
- Business failures create deterministic `EXC-<tradeId>` on `exception.created` (never routed to technical DLQ).
- `ExceptionClassificationService` applies deterministic rule-based root-cause classification (`MISSING_TRADE_DATA`, `INVALID_COUNTERPARTY`, `UNSUPPORTED_PRODUCT`, `CURRENCY_MISMATCH`, `INVALID_ECONOMIC_TERMS`, `DATE_INCONSISTENCY`, `MISSING_REFERENCE_DATA`, `CONFIRMATION_TEMPLATE_MISSING`, `OTHER`) with priority, severity, explanation, and remediation action, publishing `exception.classified`.

## 7. Work Item Lifecycle
- `WorkItemManagementService` consumes `exception.classified`, creates deterministic `WI-<exceptionId>`, routes to synthetic teams (`TRADE_OPERATIONS`, `COUNTERPARTY_OPERATIONS`, `PRODUCT_CONTROL`, `CONFIRMATION_OPERATIONS`), sets SLA due dates, and publishes `workitem.linked`.
- Demonstrate live team assignment (`PATCH /api/v1/work-items/{id}/assign`) and validated status transitions (`PATCH /api/v1/work-items/{id}/status` — e.g., `OPEN` → `IN_PROGRESS` → `RESOLVED` → `CLOSED`, and show `409 INVALID_STATE_TRANSITION` on `CLOSED` → `OPEN`).

## 8. DLQ & Controlled Replay
- Trigger `POST /api/v1/synthetic-trades/MALFORMED_PAYLOAD` → inspect via `GET /api/v1/dlq`.
- Attempt `POST /api/v1/dlq/{id}/replay` on the malformed record → rejected with `409` ("cannot be replayed without payload correction").
- Explain eligible `INFRASTRUCTURE_ERROR` replay: MVCC `IN_PROGRESS` lock prevents concurrent duplicates, bounded `maxReplayAttempts=3`, unmodified payload republished to original topic, full `ReplayAttempt` history and `DLQ_REPLAY_*` audit trail.

## 9. Authentication & Authorization
- Demonstrate `GET /api/v1/dashboard/summary` without token → `401 UNAUTHORIZED`.
- Login as `viewer` (`POST /api/v1/auth/login`) → can read dashboard (`200`), blocked from `POST /api/v1/synthetic-trades/VALID_FX_SPOT` and `POST /api/v1/dlq/{id}/replay` (`403 FORBIDDEN`).
- Login as `operations` → authorized to trigger synthetic trades, manage work items, and replay eligible DLQ messages.

## 10. Dashboard Walkthrough
1. Click **Sign in** in the header and authenticate as `operations`.
2. **Overview Dashboard:** Trigger `VALID_FX_SPOT` & `VALID_FX_FORWARD`, refresh to see live KPIs, pipeline summary, and 4 charts.
3. **Trades:** Inspect a `CONFIRMED` trade, expand its lifecycle event timeline, and open the rendered HTML confirmation.
4. **Exceptions & Manual Review:** Filter by category/priority, view root cause and recommended remediation action.
5. **Work Items:** Reassign operational team and transition status with live feedback.
6. **Audit Trail:** Filter by `correlationId` to show the complete end-to-end immutable audit chain.

## 11. Technology Choices & Design Decisions
- **Modular monolith over microservices:** Clean package boundaries + Kafka decoupling without premature distributed-ops overhead ([ADR-001](design-decisions/ADR-001-modular-monolith.md)).
- **CouchDB + deterministic IDs:** Natural idempotency, schema flexibility, and MVCC `_rev` optimistic concurrency.
- **HMAC-SHA256 stateless tokens + BCrypt:** Lightweight, environment-configured RBAC with zero external identity-provider dependency.

## 12. Known Limitations & Future Improvements
- Live Docker Compose verification requires Docker Desktop running on the host.
- Kafka + CouchDB writes are not a single distributed transaction (mitigated by idempotent consumers and MVCC locks; outbox pattern is a natural future enhancement).
- Corrected-payload DLQ replay UI, real-time WebSocket dashboard push, and Prometheus/Grafana dashboards are planned stretch goals.
