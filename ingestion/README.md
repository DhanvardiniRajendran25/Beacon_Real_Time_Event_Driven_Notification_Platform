# Ingestion API

HTTP service that accepts notification events and publishes them to Kafka. Contract for the Kafka record shape and downstream Redis mapping lives in the repo root [`CONTRACTS.md`](../CONTRACTS.md).

## `POST /event`

JSON body (`EventRequest`):

| Field | JSON key | Required | Notes |
|-------|----------|----------|--------|
| Type | `type` | yes | Exactly one of **`POST`**, **`LIKE`**, **`COMMENT`** (case-sensitive). |
| From user | `from_user` | yes | Kafka message key (recipient id for fan-out). |
| Detail | `detail` | no | Optional body text; omitted or empty becomes `""` in Kafka. |

Responses: **200** empty body on success; **400** invalid JSON, missing `type` / `from_user`, or invalid `type`; **405** wrong method; **500** publish failure.

Default port **3000** unless `PORT` is set. Docker Compose maps `3000:3000`.

### Examples

Local run (default port):

```bash
curl -sS -X POST http://localhost:3000/event \
  -H 'Content-Type: application/json' \
  -d '{"type":"POST","from_user":"user-123","detail":"You have a new task."}'
```

Minimal body (no `detail`):

```bash
curl -sS -X POST http://localhost:3000/event \
  -H 'Content-Type: application/json' \
  -d '{"type":"COMMENT","from_user":"user-456"}'
```

Docker Compose service name (from another container on the same network):

```bash
curl -sS -X POST http://ingestion:3000/event \
  -H 'Content-Type: application/json' \
  -d '{"type":"LIKE","from_user":"alice","detail":"Check out this update"}'
```

## `GET /health`

```bash
curl -sS http://localhost:3000/health
```

## Environment

| Variable | Default | Purpose |
|----------|---------|---------|
| `PORT` | `3000` | HTTP listen port. |
| `KAFKA_BROKERS` / `KAFKA_BROKER` | `localhost:9092` | Comma-separated bootstrap brokers. |
| `KAFKA_TOPIC` | `worker-events` | Topic for published events. |
