# Load Test Plan: End-to-End Notification Latency

## Overview

This plan describes a custom Go load test client that measures end-to-end latency from the moment an event is confirmed by the ingestion API to the moment all WebSocket subscribers receive their notification.

**Latency definition:** `t1` = moment just after receiving the 200 OK response from `POST /event` (meaning the event is confirmed in Kafka), `t2` = moment each subscriber goroutine receives the WS notification. This captures the full fan-out pipeline: Kafka → fan-out worker → Redis → gateway → client.

---

## Part 1: Ingestion API Change

**File:** `ingestion/handler.go`

Modify the `POST /event` response to return the generated Snowflake ID:

```json
{ "event_id": 1234567890123456789 }
```

This is the correlation key — every WS notification already carries this same `id` field, so the load test client can match deliveries back to the originating POST.

---

## Part 2: Project Structure

New top-level directory `loadtest/`:

```
loadtest/
  main.go          # CLI entry point, wires everything together
  config.go        # Config struct, flag parsing
  subscriber.go    # WebSocket connection pool
  publisher.go     # Event poster, registers events in tracker
  tracker.go       # In-flight event map, latency computation
  metrics.go       # Live HTTP metrics endpoint + CSV writer
```

---

## Part 3: Configuration

All configurable via CLI flags:

| Flag | Description | Default |
|---|---|---|
| `--alb-url` | ALB WebSocket URL (e.g. `ws://alb.example.com/ws`) | required |
| `--ingestion-url` | Ingestion API base URL | required |
| `--sender-user` | `from_user` value for all posted events | required |
| `--follower-start` | First user ID in the subscriber range | `1` |
| `--follower-count` | Number of WS connections to open | `5000` |
| `--event-rate` | Events to POST per second | `1` |
| `--duration` | How long to run | `60s` |
| `--csv-out` | Path to write CSV results | `results.csv` |
| `--metrics-port` | Port for live metrics endpoint | `9090` |
| `--event-timeout` | Max wait for all subscribers to receive | `30s` |

---

## Part 4: Subscriber Pool (`subscriber.go`)

- Spawn `follower-count` goroutines, each connecting to the ALB as a distinct user: `/ws?user_id={follower-start + i}`
- Each goroutine runs a read loop: parse incoming JSON, extract the `id` field, record `t2 = time.Now()`, and report `(event_id, userID, t2)` to the tracker via a shared channel
- On disconnect: reconnect with exponential backoff (cap at ~10s) — handles ALB idle timeouts and gateway restarts
- Track active connection count atomically so live metrics can report it

**OS tuning note:** Opening 15K+ connections from one machine requires raising `ulimit -n` (e.g. to `65536`) and widening `net.ipv4.ip_local_port_range`. Include a setup section in the loadtest README covering these adjustments.

---

## Part 5: Publisher (`publisher.go`)

- Use a `time.Ticker` at the configured rate
- On each tick: POST `{"type":"POST","from_user":"{sender-user}","detail":"loadtest"}` to the ingestion API
- On 200 OK: parse `event_id` from response, record `t1 = time.Now()`, register in tracker
- Runs until duration elapses, then signals shutdown

---

## Part 6: Tracker (`tracker.go`)

Central state for matching deliveries to events:

```go
type inflightEvent struct {
    eventID       int64
    sentAt        time.Time
    mu            sync.Mutex
    received      map[string]time.Time  // userID → receive time
    totalExpected int
    done          chan struct{}
}
```

- Stored in a `sync.Map` keyed by `event_id`
- Subscriber reports arrive via a buffered channel; a single goroutine drains it to avoid lock contention
- When `len(received) == totalExpected`: compute stats and emit
  - `p50`, `p95`, `p99` latency across all subscribers for this event
  - `max latency` = time until the last subscriber received ("all users notified")
  - Write one CSV row, update live metrics accumulators
- If `event-timeout` elapses before all subscribers receive: mark as **partial**, write a row with `complete=false` and however many were received

---

## Part 7: CSV Output

One row written per completed (or timed-out) event. Written incrementally as events complete so data is not lost if the test is interrupted.

```
event_id, sent_at_unix_ms, last_received_unix_ms, max_latency_ms, p50_ms, p95_ms, p99_ms, subscribers_received, subscribers_total, complete
```

---

## Part 8: Live Metrics (`metrics.go`)

Expose a JSON endpoint at `http://localhost:{metrics-port}/metrics`:

```json
{
  "events_sent": 42,
  "events_complete": 38,
  "events_partial": 1,
  "events_in_flight": 3,
  "notifications_received": 189950,
  "ws_connections_active": 4987,
  "latency_p50_ms": 18,
  "latency_p95_ms": 47,
  "latency_p99_ms": 103,
  "latency_max_ms": 210
}
```

Latency percentiles update as events complete. Also expose `/metrics/prometheus` in Prometheus text format for optional Grafana integration.

---

## Part 9: Graceful Shutdown

On `SIGINT`/`SIGTERM`:
1. Stop the publisher ticker
2. Wait up to `event-timeout` for all in-flight events to complete or time out
3. Flush and close the CSV file
4. Print a final summary to stdout (total events, completion rate, overall p50/p95/p99/max)
5. Exit

---

## Execution Flow

```
main
 ├─ open CSV file
 ├─ start metrics HTTP server
 ├─ start tracker drain loop
 ├─ spawn N subscriber goroutines (connect to ALB)
 ├─ wait for all connections to be established (or timeout)
 ├─ start publisher ticker
 │    └─ POST /event → get event_id → register in tracker (t1)
 ├─ subscribers receive WS notifications → report to tracker (t2)
 ├─ tracker computes latency per event → writes CSV row, updates metrics
 └─ on shutdown → flush → print summary
```

---

## Metrics Reference

| Metric | Meaning |
|---|---|
| `max_latency_ms` per event | Time until the slowest subscriber received — "all users notified" |
| `p99_ms` across events | Worst-case individual subscriber latency at scale |
| `events_partial` | Events where ≥1 subscriber never received — delivery reliability |
| `ws_connections_active` | Whether the connection pool stays stable under load |
