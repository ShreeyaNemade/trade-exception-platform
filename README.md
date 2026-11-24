# Trade Confirmation Exception Management Platform

An event-driven platform (educational) that will process **synthetic** trade events, enrich and validate them, generate confirmations for valid trades, and capture exceptions for trades that cannot be processed straight-through. An operations dashboard will expose trades, exceptions, root causes and remediation.

> **Status: Phase 1 – foundation only.** No trade processing, Kafka consumers/producers, CouchDB access or classification logic exists yet. No real banking, customer or proprietary data is used.

## Business problem
After a trade is executed it must be confirmed with the counterparty. Missing or inconsistent data (bad instrument, unknown counterparty, mismatched amounts) breaks straight-through processing (STP) and forces manual investigation. Operations teams need visibility into *what* failed, *why*, and *who is fixing it*.

## Objectives
- Ingest, enrich and validate trade events asynchronously.
- Confirm valid trades automatically; capture and classify the rest as exceptions.
- Track remediation via work items with a full audit trail.
- Support replay and dead-letter handling.

## Technology stack
| Area | Technology |
|---|---|
| Backend | Java 21, Spring Boot 3.3.5, Maven, Spring Web/Validation/Kafka/Actuator |
| Frontend | React 18, TypeScript 5.6, Vite 5, Tailwind CSS 3.4, React Router 6 |
| Messaging | Apache Kafka 3.8.1 (KRaft) |
| Database | Apache CouchDB 3.3.3 |
| Infra | Docker, Docker Compose |
| Testing | JUnit 5, Mockito (via spring-boot-starter-test), Vitest 2 |

## Major features (planned)
Trade ingestion · enrichment · STP validation · confirmations · exception capture & classification · manual review · work items · audit trail · replay/DLQ · operations dashboard.

## Architecture overview
A **modular monolith**: one Spring Boot app with ten business modules (packages under `com.tradeexception.platform`): `ingestion`, `trade`, `enrichment`, `stp`, `confirmation`, `exception`, `classification`, `workitem`, `audit`, `replay`. Modules will talk via Kafka events and persist to CouchDB. Currently these packages are empty placeholders (`package-info.java`). Implemented so far: `GET /api/v1/health`, Actuator, a global exception handler and the `ApiResponse` envelope. See [docs/architecture](docs/architecture/system-architecture.md).

## Repository structure
```
backend/            Spring Boot app (Maven) + Dockerfile
frontend/           React/TS/Vite/Tailwind app + Dockerfile + nginx.conf
infrastructure/     docker/ and kafka/ (placeholders)
docs/               architecture/, api/, design-decisions/
sample-data/        (placeholder)  scripts/ (placeholder)
docker-compose.yml  .env.example  .gitignore
```
Changes vs. the suggested layout: added `frontend/nginx.conf`, `.dockerignore` files and an ADR.

## Local development
Prerequisites: JDK 21, Maven 3.9+, Node 20+ (22 tested), Docker with Compose v2.

```bash
# Backend
cd backend && ./mvnw verify && ./mvnw spring-boot:run   # Windows: mvnw.cmd (Maven Wrapper, no local Maven needed)
# http://localhost:8080/api/v1/health and /actuator/health

# Frontend
cd frontend && npm install && npm run dev      # http://localhost:5173
npm test && npm run build

# Full stack
cp .env.example .env     # set CHANGE_ME values
docker compose up --build
```
Exposed ports (configurable in `.env`): frontend 3000, backend 8080, CouchDB 5984, Kafka (host) 9094. Kafka's internal listener `kafka:9092` is only reachable inside Docker. The frontend does not call the backend yet.

## Roadmap
See [development-roadmap.md](docs/architecture/development-roadmap.md).
