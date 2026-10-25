# Contracts: Kafka and Redis

This document is the agreed contract for **topic names**, **message shapes**, and **Redis channels** used by the notification fan-out path.

---

## Kafka

### Topic

| Name | Environment variable | Notes |
|------|----------------------|--------|
| `worker-events` | `KAFKA_TOPIC` | Default; overridden per deployment. Docker Compose creates this topic with **3 partitions** and replication factor **1** (see `docker-compose.yml` `kafka-init`). |

### Consumer group

| Name | Environment variable | Notes |
|------|----------------------|--------|
| `worker-skeleton` | `KAFKA_GROUP_ID` | Default consumer group for the worker process. |

### Message key

- **Preferred:** non-empty UTF-8 string identifying the **sender** (`from_user`). The worker looks up the sender's followers in PostgreSQL and fans out to them.
- **If omitted:** the worker falls back to `NOTIFY_DEFAULT_USER_ID` (default `default`) for routing, and `NOTIFY_FROM_USER` for the Redis `from_user` field. Producers should set the key whenever possible so routing is explicit.

### Message value (JSON)

Each record **value** must be JSON with the following fields. The **ingestion** service (see below) is the reference producer: it always emits `id` as a **JSON number** and sets the Kafka **key** from the HTTP `from_user` field.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `id` | `number` or decimal string | Yes | Notification id. **Ingestion:** 64-bit snowflake integer from `ingestion/snowflake.go` — high 41 bits are Unix milliseconds, low 22 bits are a monotonic sequence counter. Always emitted as a JSON number. **Consumer:** also accepts a base-10 int64 string (see `fanout/internal/consumer` `parseSnowflakeID`). |
| `type` | `string` | Yes | Notification kind. **Allowed values:** `POST`, `LIKE`, `COMMENT` only (case-sensitive). Ingestion rejects any other value. |
| `detail` | `string` | Yes | Free-text context for the recipient; forwarded to Redis as `message` (see below). May be empty when the producer omits it (ingestion sends `""` if the HTTP body has no `detail`). |
| `timestamp` | `number` | Yes | Unix time in **milliseconds** since epoch. **Ingestion** sets this to `time.Now().UnixMilli()` at publish time. |

Example (numeric snowflake `id`, as emitted by ingestion):

```json
{
  "id": 1234567890123456789,
  "type": "POST",
  "detail": "Alice posted a photo",
  "timestamp": 1743273600000
}
```

Example (decimal string `id`, accepted by the parser):

```json
{
  "id": "1234567890123456789",
  "type": "COMMENT",
  "detail": "Nice shot!",
  "timestamp": 1743273600000
}
```

Malformed JSON or an invalid `id` is logged; the message is still **committed** (no infinite retry on bad payload).

### HTTP ingestion (`ingestion` service)

The ingestion API accepts **`POST /event`** with JSON matching `ingestion/handler.go` `EventRequest`. It builds `KafkaEvent`, publishes to `KAFKA_TOPIC` (default `worker-events`), and uses the Kafka **message key** = `from_user`.

| Field | JSON | Required | Description |
|-------|------|----------|-------------|
| `Type` | `type` | Yes | Must be `POST`, `LIKE`, or `COMMENT`. Maps to Kafka `type`. |
| `FromUser` | `from_user` | Yes | Kafka **message key** (sender / actor who triggered the event; becomes Redis JSON `from_user` when non-empty). |
| `Details` | `detail` | No | Maps to Kafka `detail`; omitted or empty becomes `""`. |

Kafka fields **`id`** and **`timestamp`** are assigned by the service (`NewSnowflakeID()` returns a 64-bit snowflake int64, `timestamp` is current time in ms); clients do not send them on this endpoint.

Listen port defaults to **`3000`** (`PORT`). **`GET /health`** returns 200 for readiness checks.

### HTTP producer (test harness)

The `kafka-producer` service accepts `POST /messages` with a body that maps to the same Kafka **value** shape, plus optional routing:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `id` | `number` | No | Defaults to `time.Now().UnixNano()` if zero. |
| `type` | `string` | Yes | Must be `POST`, `LIKE`, or `COMMENT`. Maps to `type`. |
| `detail` | `string` | Yes | Maps to `detail`. |
| `timestamp` | `number` | No | Defaults to `time.Now().UnixMilli()` if zero. |
| `key` | `string` | No | If set, becomes the Kafka **message key** (recipient user id). |

---

## Redis (Pub/Sub)

### Channel naming

| Pattern | Example | Purpose |
|---------|---------|---------|
| `notif:{userID}` | `notif:user-42` | One Pub/Sub channel per **recipient**. Gateway clients pass the same string as the WebSocket query `user_id` (`gateway/handler.go`, `gateway/pubsub.go`). |

Subscribers in production typically `SUBSCRIBE` to a single user channel. The test harness `redis-subscriber` uses **`PSUBSCRIBE notif:*`** to observe all notification channels.

### Message payload (JSON)

Published as the **string body** of `PUBLISH`. Shape:

| Field | Type | Description |
|-------|------|-------------|
| `id` | `number` | From Kafka event `id`. |
| `type` | `string` | From Kafka event `type`. |
| `from_user` | `string` | Kafka **message key** when non-empty (`NotificationEvent.FromUser`). |
| `message` | `string` | From Kafka event `detail`. |
| `timestamp` | `number` | From Kafka event `timestamp` (Unix **milliseconds**). |

Example:

```json
{
  "id": 1234567890123456789,
  "type": "POST",
  "from_user": "alice",
  "message": "Alice posted a photo",
  "timestamp": 1743273600000
}
```

---

## Field mapping (Kafka → Redis)

| Kafka | Redis payload |
|-------|----------------|
| Value `id` (parsed per `parseSnowflakeID`) | `id` |
| Value `type` | `type` |
| Value `detail` | `message` |
| Value `timestamp` | `timestamp` |
| Record **key** (bytes as UTF-8), or `NOTIFY_FROM_USER` if key empty | `from_user` |

---

## WebSocket (Gateway → Client)

### Message types

The gateway sends two distinct message types over the WebSocket connection, distinguished by the `type` field.

#### `history` — sent on connect and on pagination response

| Field | Type | Description |
|-------|------|-------------|
| `type` | `string` | Always `"history"` |
| `unread_count` | `number` | Total undelivered notifications at the time of the fetch |
| `has_more` | `bool` | Whether another page of older notifications exists |
| `notifications` | `array` | Up to 50 notification objects, newest-first |

Each notification object:

| Field | Type | Description |
|-------|------|-------------|
| `id` | `number` | Notification ID (int64) |
| `from_user` | `string` | User who triggered the event |
| `event_type` | `string` | `POST`, `LIKE`, or `COMMENT` |
| `message` | `string` | Notification body |
| `delivered` | `bool` | Whether it had been delivered before this fetch |
| `timestamp` | `number` | Unix milliseconds from `created_at` |

Example:
```json
{
  "type": "history",
  "unread_count": 2,
  "has_more": false,
  "notifications": [
    { "id": 1743273600001, "from_user": "alice", "event_type": "POST", "message": "Alice posted a photo", "delivered": false, "timestamp": 1743273600123 }
  ]
}
```

#### `notification` — real-time push

| Field | Type | Description |
|-------|------|-------------|
| `type` | `string` | Always `"notification"` |
| `id` | `number` | Notification ID |
| `from_user` | `string` | Sender |
| `event_type` | `string` | `POST`, `LIKE`, or `COMMENT` |
| `message` | `string` | Notification body |
| `timestamp` | `number` | Unix milliseconds |

Example:
```json
{
  "type": "notification",
  "id": 1743273700001,
  "from_user": "bob",
  "event_type": "LIKE",
  "message": "Bob liked your post",
  "timestamp": 1743273700000
}
```

### Message types (Client → Gateway)

#### `fetch_history` — request the next page

| Field | Type | Description |
|-------|------|-------------|
| `type` | `string` | Always `"fetch_history"` |
| `before_id` | `number` | ID of the oldest notification in the previous page (cursor) |
| `limit` | `number` | Page size, capped at 50 server-side |

Example:
```json
{ "type": "fetch_history", "before_id": 1743273600001, "limit": 50 }
```

---

## Versioning

Any change to topic names, channel patterns, or required JSON fields should be updated here and treated as a **breaking** change for producers and subscribers unless backward compatibility is explicitly preserved.
