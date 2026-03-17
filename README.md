# ARGUS
### Real-Time Event Analytics & Observability Platform

ARGUS is a production-grade backend pipeline that ingests, streams, stores, and serves behavioral and system events in real time. Built on a fully reactive stack — Spring Boot WebFlux, Apache Kafka, Redis, and TimescaleDB — everything runs in Docker with a single command.

---

## Architecture

```
Your App / Service
      │
      │  POST /api/events  (X-API-Key required)
      ▼
┌─────────────────────────────────────┐
│         Ingestion API               │
│   validates · rate-limits · GeoIP   │
│         (Spring WebFlux)            │
└────────────────┬────────────────────┘
                 │ publishes
                 ▼
┌──────────────────────────────────────┐
│   Apache Kafka — raw-events topic    │
│   (KRaft, no Zookeeper)              │
│   raw-events-dlq  ← poison pills     │
└──────────────────┬───────────────────┘
                   │ consumed by
                   ▼
┌──────────────────────────────────────────────────┐
│           Kafka Streams Processor                │
│                                                  │
│  Redis  ◄── live counters (total, errors, type)  │
│  TimescaleDB ◄── 1 row per event, ON CONFLICT    │
│                   accumulates counts             │
└──────────────────────────────────────────────────┘
                   │
         ┌─────────┴──────────┐
         ▼                    ▼
  GET /api/v1/           Prometheus       Slack (Alerts)
  dashboard/*         scrapes /actuator        (Webhooks)
  (Redis + TSDB)       every 15 seconds
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Runtime | Java 21, Spring Boot 3.2 |
| Web | Spring WebFlux (fully non-blocking) |
| Messaging | Apache Kafka (KRaft), Spring Cloud Stream |
| Stream Processing | Kafka Streams |
| Live Counters | Redis 7, Reactive RedisTemplate |
| Time-Series Storage | TimescaleDB (PostgreSQL 15) — 30-day auto-retention |
| Rate Limiting | Redis Token-Bucket Lua script |
| GeoIP Enrichment | DB-IP City Lite mmdb (bundled at build time) |
| Observability | Spring Actuator + Micrometer + Prometheus |
| Security | API key auth + dual rate-limit (per-IP + per-key) |
| Testing | JUnit 5, Testcontainers (Kafka, PostgreSQL, Redis) |
| Container | Docker, Docker Compose |

---

## Project Structure

```
ARGUS/
└── ingestion-api/
    ├── docker-compose.yml          # Full stack incl. Prometheus
    ├── Dockerfile                  # 3-stage: build → GeoIP download → runtime
    ├── init-db.sql                 # Schema, hypertable, 30-day retention policy
    ├── monitoring/
    │   └── prometheus.yml          # Scrape config for /actuator/prometheus
    ├── pom.xml
    └── src/
        ├── main/java/com/streamforge/ingestion/
        │   ├── controller/
        │   │   ├── EventController.java        # POST /api/events
        │   │   └── DashboardController.java    # GET /api/v1/dashboard/*
        │   ├── processor/
        │   │   └── StreamProcessor.java        # Kafka Streams topology + DLQ routing
        │   ├── repository/
        │   │   └── AnalyticsRepository.java    # TimescaleDB upsert (explicit SQL types)
        │   ├── security/
        │   │   └── ApiKeyAuthFilter.java       # Protects all /api/** endpoints
        │   ├── service/
        │   │   ├── KafkaProducerService.java
        │   │   ├── RateLimitService.java       # Token-bucket (50/s IP, 5000/s key)
        │   │   ├── RedisStatsUpdater.java
        │   │   ├── GeoIpService.java           # Real MaxMind DB-IP lookup
        │   │   └── SlackAlertService.java      # Formatted Block Kit notifications
        │   └── model/
        │       └── StreamEvent.java            # Validated payload record
        └── test/java/com/streamforge/ingestion/
            └── IngestionApiApplicationTests.java  # 6 integration tests (Testcontainers)
```

---

## Getting Started

### Prerequisites
- Docker Desktop (or Docker Engine + Compose)
- No local Java or Maven required — the multi-stage Dockerfile handles everything

### 1. Start the full stack

```bash
cd ARGUS/ingestion-api
docker compose up --build
```

This starts **six containers** on the `argus-net` bridge network:

| Container | Port | Purpose |
|---|---|---|
| `argus-redis` | 6379 | Live metric counters + rate-limit state |
| `argus-kafka` | 9092 | Event bus (KRaft, no Zookeeper) |
| `argus-kafka-init` | — | One-shot topic creator (`raw-events`, `raw-events-dlq`) |
| `argus-timescaledb` | 5433 | Time-series storage, 30-day auto-retention |
| `argus-ingestion-api` | **8100** | The Spring Boot application |
| `argus-prometheus` | **9090** | Metrics scraper (scrapes API every 15s) |

### 2. Send your first event

All `/api/**` endpoints require `X-API-Key`. Default keys are `test-key-1` and `test-key-2`.

```bash
curl -s -X POST http://localhost:8100/api/events \
  -H "Content-Type: application/json" \
  -H "X-API-Key: test-key-1" \
  -d '{
    "eventId": "550e8400-e29b-41d4-a716-446655440000",
    "eventType": "page_view",
    "timestamp": "2026-03-14T12:00:00Z",
    "sessionId": "sess-abc-123",
    "userId": "user-42",
    "url": "/home",
    "service": "frontend",
    "statusCode": 200,
    "latency": 45
  }'
# → 202 Accepted
```

### 3. Query the Dashboard API

**Live counters (Redis):**
```bash
curl -s http://localhost:8100/api/v1/dashboard/counters \
  -H "X-API-Key: test-key-1" | jq
```
```json
{
  "totalEvents": 142,
  "errorEvents": 3,
  "eventsByType": { "page_view": 89, "api_call": 41, "error": 12 }
}
```

**Historical time-series (TimescaleDB):**
```bash
curl -s http://localhost:8100/api/v1/dashboard/series \
  -H "X-API-Key: test-key-1" | jq
```
```json
[
  { "time": "2026-03-14T12:01:00Z", "eventType": "page_view", "eventCount": 5 },
  { "time": "2026-03-14T12:01:00Z", "eventType": "api_call",  "eventCount": 2 }
]
```

### 4. View metrics in Prometheus

Open **http://localhost:9090** and try:
- `http_server_requests_seconds_count` — request throughput
- `jvm_memory_used_bytes` — heap usage
- `kafka_producer_record_send_total` — events published

---

## API Reference

### `POST /api/events`
Ingests a single event. Validates, GeoIP-enriches, and publishes to Kafka.

| Header | Required | Description |
|---|---|---|
| `X-API-Key` | ✅ | Must match a key in `STREAMFORGE_API_KEYS` |
| `Content-Type` | ✅ | `application/json` |

**Valid `eventType` values:** `page_view` `click` `api_call` `error` `signup` `purchase` `service_health`

| Status | Meaning |
|---|---|
| `202 Accepted` | Event queued in Kafka |
| `400 Bad Request` | Validation failure (field errors in body) |
| `401 Unauthorized` | Missing or invalid API key |
| `429 Too Many Requests` | Rate limit exceeded |

---

### `GET /api/v1/dashboard/counters`
Live Redis counters. Returns `Mono<DashboardStats>` (non-blocking).

| Parameter | Required | Description |
|---|---|---|
| `projectId` | ❌ | Filter counts by a specific project UUID. If omitted, returns global aggregate. |

| Field | Type | Description |
|---|---|---|
| `totalEvents` | `long` | Event count for the given scope (project or global) |
| `errorEvents` | `long` | Failed events flagged by the processor |
| `eventsByType` | `Map<String, Long>` | Count broken down by `eventType` |

---

### `GET /api/v1/dashboard/series`
TimescaleDB aggregation points.

| Parameter | Required | Description |
|---|---|---|
| `projectId` | ❌ | Filter series by project UUID. If omitted, returns global aggregate. |

### `GET /api/v1/dashboard/events`
Audit log of raw event payloads from the `raw_events` table.

| Parameter | Required | Description |
|---|---|---|
| `projectId` | ❌ | Filter log by project UUID. |
| `limit` | ❌ | Max records to return (default: 50) |

---

## Configuration

| Variable | Default | Description |
|---|---|---|
| `SERVER_PORT` | `8100` | HTTP port |
| `SPRING_KAFKA_BOOTSTRAP_SERVERS` | `kafka:29092` | Kafka broker |
| `SPRING_DATA_REDIS_HOST` | `redis` | Redis hostname |
| `SPRING_DATA_REDIS_PORT` | `6379` | Redis port |
| `SPRING_DATASOURCE_HOST` | `timescaledb` | TimescaleDB hostname |
| `SPRING_DATASOURCE_PASSWORD` | `holdontohope` | PostgreSQL password |
| `STREAMFORGE_API_KEYS` | `test-key-1,test-key-2` | Comma-separated valid API keys |
| `GEOIP_DATABASE_PATH` | `/app/geoip/GeoLite2-City.mmdb` | Path to mmdb file |

---

## Security Model

All `/api/**` endpoints are gated by `ApiKeyAuthFilter`. Only `/actuator/**` is public (for Prometheus scraping).

**Two-layer rate limiting** (Redis token-bucket Lua script):
- **Per IP:** 50 requests/second
- **Per API key:** 5,000 requests/second

Invalid requests return `401 Unauthorized` immediately, before any rate-limit check.

---

## Pipeline Deep-Dive

### Ingestion
1. `ApiKeyAuthFilter` validates key + runs both rate-limit checks reactively.
2. `GeoIpService` resolves client IP → country using the bundled DB-IP mmdb.
3. `KafkaProducerService` publishes the `StreamEvent` as JSON to `raw-events`.

### Stream Processing
4. `StreamProcessor` (Kafka Streams inside the API JVM) consumes `raw-events`.
5. **Poison pills** (malformed JSON) → `.to("raw-events-dlq")` for inspection.
6. **Alerts branch** — `statusCode >= 500` increments Redis `stats:events:errors`.
7. **Redis branch** — every event increments `stats:events:total` and `stats:events:type:<type>`.
8. **TimescaleDB branch** — direct `INSERT ... ON CONFLICT DO UPDATE SET event_count = event_count + EXCLUDED.event_count` (no windowing delay).
9. **Slack Alerts branch** — `StreamProcessor` checks per-project `alert_config`. If an error occurs or a tracked event type matches, `SlackAlertService` sends a formatted alert via WebClient.

### Data Retention
TimescaleDB automatically drops chunks older than 30 days via `add_retention_policy()`. No cron job needed.

---

## Testing

```bash
# Requires Docker running (Testcontainers spins up real infra)
cd ARGUS/ingestion-api
mvn test
```

6 integration tests covering: context load, auth rejection (no key), auth rejection (wrong key), successful ingestion (202), dashboard auth protection, and dashboard response schema.

---

## Development Commands

```bash
docker compose up --build        # Start full stack
docker compose down              # Stop all containers
docker compose down -v           # Stop + wipe all volumes (full reset)
docker compose logs -f ingestion-api   # Tail API logs

# Inspect Kafka topics live
docker exec -it argus-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 --topic raw-events --from-beginning

# Inspect the DLQ
docker exec -it argus-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 --topic raw-events-dlq --from-beginning
```
