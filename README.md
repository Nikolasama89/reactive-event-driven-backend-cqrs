# Employee & Company Management — Event-Driven Backend

A reactive, event-driven backend for managing companies and their employees, built in
**plain Java with Project Reactor** (no Spring / no DI framework). Writes flow through
Kafka as domain events; reads are served from projections in Elasticsearch and from
Kafka Streams state stores.

> Personal project built to explore event-driven architecture, reactive Java and
> Domain-Driven Design end to end, without leaning on a framework to wire things together.

---

## Tech stack

| Concern | Technology |
|---|---|
| Language / runtime | Java [17? 21?] |
| Reactive core | Project Reactor |
| HTTP server | Reactor Netty (`HttpServer`) |
| Messaging | Apache Kafka (reactor-kafka) |
| Stream processing | Kafka Streams (Processor API) |
| Relational store | PostgreSQL via R2DBC |
| Search / read model | Elasticsearch |
| Messaging (legacy path) | RabbitMQ |
| Infra | Docker Compose |

*(Adjust the Java version and drop RabbitMQ from the table if it's no longer used.)*

---

## Architecture

The core idea: **the service layer never writes to a database directly.** It only
publishes domain events. Dedicated listeners react to those events and own all
persistence and projection.

```
HTTP request
    │
    ▼
Service layer ──► publishes DomainEvent to Kafka
                   (topic per aggregate, key = aggregateId,
                    event type carried in a Kafka header)
                        │
        ┌───────────────┼────────────────────┐
        ▼               ▼                      ▼
  Postgres          Elasticsearch        Kafka Streams
  (write model,     (read projections)   (state stores +
   R2DBC)                                  secondary indexes,
                                           interactive queries)
```

Notable pieces:

- **Custom polymorphic (de)serialization** — a single `DomainEvent` type is dispatched
  by an event-type header, so one topic/consumer can carry multiple event kinds.
- **Kafka Streams processors** (`CompanyProcessor`, `EmployeeProcessor`) build local
  state stores with secondary indexes; `StreamCompanyRepository` / `StreamEmployeeRepository`
  serve reads through interactive queries against those stores.
- **Cascade projections** — e.g. a company-name change propagates to every affected
  employee document in Elasticsearch via an update listener.
- **Composition Root / Pure DI** — everything is wired by hand in `Application.main`
  through `AppConfig`, `AppInitializerFactory` and the `AppMode` strategy, with no
  container doing it magically.


---

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

## API

Employee and company routes are registered by `EmployeeHandler` and `CompanyHandler`.

| Method | Path | Description |
|---|---|---|
| `[GET]` | `[/employees]` | `[List employees]` |
| `[POST]` | `[/employees]` | `[Create an employee]` |
| `[GET]` | `[/companies]` | `[List companies]` |
| `[POST]` | `[/companies]` | `[Create a company]` |

*(Fill in your real endpoints — copy them straight from `employeeRoutes` / `companyRoutes`.)*
---

## Author

Nikolaos Michos