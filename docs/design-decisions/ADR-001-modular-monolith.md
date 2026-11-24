# ADR-001: Modular monolith for the MVP

**Status:** Accepted

The platform is a single Spring Boot deployable with ten business modules, each a top-level package under `com.tradeexception.platform`. Modules communicate through Kafka events, not direct calls to each other's persistence, so any module can later be extracted into a service without redesign. Microservices are rejected for the MVP to avoid operational overhead.

Other fixed decisions: Kafka (KRaft) for messaging, CouchDB for documents, React + Vite + Tailwind for the UI.
