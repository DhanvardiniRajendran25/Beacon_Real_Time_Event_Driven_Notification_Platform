# Notification System Load Test

A custom Go load test client that measures end-to-end notification latency — from the moment an event is confirmed by the ingestion API to the moment every WebSocket subscriber receives it.

## How It Works

1. Opens `--follower-count` persistent WebSocket connections to the gateway ALB, one per follower user ID.
2. Once all connections are established, begins POSTing events to the ingestion API at the configured rate.
3. Each POST returns the event's Snowflake ID. That ID is registered in the tracker with a timestamp (`t1`).
4. When a subscriber receives a notification over WebSocket, it records its receive time (`t2`) and reports `(event_id, t2)` to the tracker.
5. When all subscribers have reported for a given event, the tracker computes per-event latency statistics and writes a row to the CSV.
6. Events not fully delivered within `--event-timeout` are marked `complete=false` in the CSV.

Live metrics are available over HTTP throughout the test run.

## Prerequisites

- Go 1.21+
- Follower relationships must be pre-seeded in the database before running. The load test does not create users or follows.
- The ingestion API must be running the updated handler that returns `{"event_id": ...}` in the POST response.

## OS Tuning

Opening thousands of WebSocket connections from a single machine requires raising system limits. Run these before starting the test:

```bash
# Raise open file descriptor limit for the current shell session
ulimit -n 65536

# Widen the ephemeral port range (Linux)
sudo sysctl -w net.ipv4.ip_local_port_range="1024 65535"

# Increase the max number of open files system-wide (Linux)
sudo sysctl -w fs.file-max=200000
```

On macOS:
```bash
ulimit -n 65536
sudo sysctl -w kern.maxfiles=200000
sudo sysctl -w kern.maxfilesperproc=65536
```

## Usage

```bash
go run ./loadtest/ [flags]
```

### Required Flags

| Flag | Description |
|---|---|
| `--alb-url` | ALB WebSocket URL, e.g. `ws://your-alb.example.com/ws` |
| `--ingestion-url` | Ingestion API base URL, e.g. `http://your-ingestion.example.com` |
| `--sender-user` | User ID of the event sender (must have pre-seeded followers) |

### Optional Flags

| Flag | Default | Description |
|---|---|---|
| `--follower-start` | `1` | First user ID in the subscriber range |
| `--follower-count` | `5000` | Number of WebSocket connections to open |
| `--event-rate` | `1.0` | Events to POST per second |
| `--duration` | `60s` | How long to run the test |
| `--csv-out` | `results.csv` | Path to write CSV output |
| `--metrics-port` | `9090` | Port for the live metrics HTTP endpoint |
| `--event-timeout` | `30s` | Max time to wait for all subscribers to receive a single event |

### Example

```bash
go run ./loadtest/ \
  --alb-url ws://your-alb.example.com/ws \
  --ingestion-url http://your-ingestion.example.com \
  --sender-user 42 \
  --follower-start 1 \
  --follower-count 5000 \
  --event-rate 2 \
  --duration 5m \
  --csv-out results.csv
```

## Live Metrics

While the test is running, metrics are available at:

- **JSON:** `http://localhost:9090/metrics`
- **Prometheus:** `http://localhost:9090/metrics/prometheus`

Example JSON response:

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

Latency figures reflect the **max latency per event** (i.e. time until the last subscriber received), so p99 here means "99% of events had all subscribers notified within this time."

## CSV Output

One row is written per event as soon as it completes (or times out), so data is not lost if the test is interrupted.

```
event_id, sent_at_unix_ms, last_received_unix_ms, max_latency_ms, p50_ms, p95_ms, p99_ms, subscribers_received, subscribers_total, complete
```

| Column | Description |
|---|---|
| `event_id` | Snowflake ID returned by the ingestion API |
| `sent_at_unix_ms` | When the 200 OK was received from `POST /event` |
| `last_received_unix_ms` | When the last subscriber received the notification |
| `max_latency_ms` | `last_received - sent_at` — time until all users were notified |
| `p50_ms` / `p95_ms` / `p99_ms` | Per-event subscriber latency percentiles |
| `subscribers_received` | How many subscribers received the notification |
| `subscribers_total` | Total subscribers expected |
| `complete` | `false` if the event timed out before all subscribers received |

## Graceful Shutdown

Send `SIGINT` (Ctrl+C) or `SIGTERM` to stop the publisher, wait for in-flight events to complete or time out, flush the CSV, and print a final summary.
