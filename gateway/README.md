# WebSocket Gateway

The gateway accepts WebSocket connections on **`/ws`**, registers the client in Redis, delivers notification history from PostgreSQL on connect, then forwards live **Redis Pub/Sub** messages on channel `notif:{userID}` as WebSocket text frames.

## Connect

```bash
websocat 'ws://localhost:8080/ws?user_id=alice'
```

On connect the gateway immediately sends a **history envelope** containing the 50 most recent notifications and the total unread count, then subscribes to `notif:{userID}` on Redis for real-time delivery.

## Message types

### `history` — sent on connect and on each pagination response

```json
{
  "type": "history",
  "unread_count": 3,
  "has_more": false,
  "notifications": [
    {
      "id": 1743273600001,
      "from_user": "alice",
      "event_type": "POST",
      "message": "Alice posted a photo",
      "delivered": false,
      "timestamp": 1743273600123
    }
  ]
}
```

`notifications` is empty (`[]`) when the user has no history. All returned notifications are marked delivered after being sent.

### `notification` — real-time push

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

### `fetch_history` — client → server pagination request

Send this to fetch the next page. `before_id` is the `id` of the oldest notification in the previous page.

```json
{ "type": "fetch_history", "before_id": 1743273600001, "limit": 50 }
```

`limit` is capped at 50 server-side.

## Testing history

```bash
# Terminal 1 — connect and observe the history envelope on connect
websocat 'ws://localhost:8080/ws?user_id=follower_a_1'

# Terminal 2 — fire a notification so there is history to see
curl -X POST http://localhost:3000/event \
  -H "Content-Type: application/json" \
  -d '{"type":"POST","from_user":"alice","detail":"Alice posted a photo"}'
```

Disconnect and reconnect — the notification appears in the history envelope with `"delivered": true` (already seen), and `"unread_count": 0`.

## Health check

```bash
curl -sf http://localhost:8080/health
```

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_ADDR` | `localhost:6379` | Redis address |
| `DB_DSN` | `postgres://notif:notif@localhost:5432/notifications?sslmode=disable` | PostgreSQL DSN for notification history |
| `TASK_ID` | `local` | Identifies this gateway instance in Redis |

If `DB_DSN` is unreachable at startup, history is disabled and real-time delivery continues normally.
