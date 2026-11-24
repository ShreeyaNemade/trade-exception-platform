# Development Roadmap

| Phase | Scope | Status |
|---|---|---|
| 1 | Foundation: repo, backend skeleton, frontend shell, Docker Compose, docs | Done (pending review) |
| 2 | Domain model, CouchDB persistence, Kafka topics/config, trade ingestion API | Planned |
| 3 | Trade management, enrichment, STP validation pipeline | Planned |
| 4 | Confirmation generation | Planned |
| 5 | Exception management and classification | Planned |
| 6 | Work items, manual review, audit trail | Planned |
| 7 | Replay and dead-letter handling | Planned |
| 8 | Dashboard connected to APIs, analytics | Planned |
| 9 | Hardening: tests, Postman collection, CI, docs | Planned |

## MVP features
Pipeline from raw trade to confirmation or exception; classification; work items; audit trail; dashboard; replay/DLQ.

## Stretch goals
Authentication/roles, SLA tracking, rule configuration UI, metrics (Micrometer/Prometheus), multi-broker Kafka, CI/CD.
