# Employee & Company Management — Event-Driven Backend (CQRS)

A reactive, event-driven backend for managing companies and their employees, built in
**plain Java with Project Reactor** (no Spring / no DI framework). Writes flow through
domain events; reads are served from a **separate** read model (Elasticsearch projections
and Kafka Streams state stores) — a CQRS split.

Its defining feature is that the **persistence/transport layer is pluggable**: the same
event-driven core runs against different infrastructure implementations, selected at
startup, without changing any business logic.

> Personal project built to explore event-driven architecture, reactive Java, CQRS and
> Domain-Driven Design end to end, without leaning on a framework to wire things together.

---

## Runtime modes

The application runs in one of several **modes**, selected by the `APP_MODE` environment
variable. Each mode is a distinct implementation of the `AppEnvInitializer` interface,
chosen at startup by `AppInitializerFactory` (a switch-based **Strategy** implementation).
The event-driven core — services, domain events, handlers — is identical across modes;
only the persistence and transport wiring changes.

In **every** mode, reads are served from a separate read model, never from the write path:
Elasticsearch holds query-optimized projections and Kafka Streams state stores serve
interactive queries. The read side is updated asynchronously from the events, so it is
**eventually consistent** by design.

## Tech stack

| Concern | Technology |
|---|---|
| Language / runtime | Java 17 
| Reactive core | Project Reactor |
| HTTP server | Reactor Netty (`HttpServer`) |
| Messaging | Apache Kafka (reactor-kafka) |
| Stream processing | Kafka Streams (Processor API) |
| Relational store | PostgreSQL via R2DBC |
| Search / read model | Elasticsearch |
| Messaging (legacy path) | RabbitMQ |
| Infra | Docker Compose |

---

## Architecture

The core idea: **the service layer never writes to a database directly.** It only
publishes domain events. Dedicated listeners react to those events and own all
persistence and projection.

```
HTTP request
    │
    ▼
Service layer ──► publishes DomainEvent
                        │
   write model (chosen by AppMode)         read model (always separate)
     ┌──────────────────┴───────────┐        ┌───────────────┐
     ▼                              ▼         ▼               ▼
 PostgreSQL (R2DBC)          Kafka Streams  Elasticsearch   Kafka Streams
 [Postgres mode]             state stores   projections     interactive
                             [Kafka mode]                    queries
```

Notable pieces:

- **Pluggable modes** — `AppEnvInitializer` + `AppInitializerFactory` (Strategy) let the
  same core run on different persistence/transport backends via `APP_MODE`.
- **Custom polymorphic (de)serialization** — a single `DomainEvent` type is dispatched by
  an event-type header, so one topic/consumer can carry multiple event kinds.
- **Kafka Streams processors** (`CompanyProcessor`, `EmployeeProcessor`) build local state
  stores with secondary indexes; `StreamCompanyRepository` / `StreamEmployeeRepository`
  serve reads through interactive queries against those stores.
- **Cascade projections** — e.g. a company-name change propagates to every affected
  employee document in Elasticsearch via an update listener.
- **Composition Root / Pure DI** — everything is wired by hand in `Application.main`
  through `AppConfig`, `AppInitializerFactory` and the `AppMode` strategy, with no
  container doing it magically.

## Prerequisites

- JDK 17 
- Docker + Docker Compose
- Gradle

---

## Getting started

1. **Start the infrastructure** (Postgres, Elasticsearch, Kafka, RabbitMQ, plus pgAdmin /
   Kibana / Redpanda console):

```bash
   docker compose up -d
```

2. **Create your local env file** from the template and adjust if needed:

```bash
   cp .env.example .env
```

3. **Run the application.** From your IDE, load the `.env` (e.g. the *EnvFile* plugin in
   IntelliJ) and run `Application`. The env vars must be present in the run
   configuration's **Environment variables** (not VM options).

   The server starts on **http://127.0.0.1:8080**.

---

## Configuration

All configuration is read from environment variables at startup by `AppConfig.fromEnv()`,
which fails fast with a clear message if any variable is missing. No credentials live in
the code.

## Local service ports

| Service | URL |
|---|---|
| Application | http://127.0.0.1:8080 |
| PostgreSQL | localhost:5433 |
| pgAdmin | http://localhost:8888 |
| Elasticsearch | http://localhost:9200 |
| Kibana | http://localhost:5601 |
| RabbitMQ management | http://localhost:15672 |
| Redpanda console (Kafka UI) | http://localhost:8081 |

---

## Author
Nikolaos Michos - github.com/Nikolasama89