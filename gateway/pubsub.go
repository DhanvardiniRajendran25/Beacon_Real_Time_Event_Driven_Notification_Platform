package main

import (
	"context"
	"encoding/json"
	"log"
)

// realtimeNotif is the envelope sent to clients for live notifications
// delivered via Redis Pub/Sub. The "type" field distinguishes these
// from "history" envelopes sent on connect or pagination.
type realtimeNotif struct {
	Type      string `json:"type"`       // always "notification"
	ID        int64  `json:"id"`
	EventType string `json:"event_type"` // POST, LIKE, COMMENT
	FromUser  string `json:"from_user"`
	Message   string `json:"message"`
	Timestamp int64  `json:"timestamp"`
}

// rawNotifPayload matches the JSON shape published by the fanout worker to Redis.
type rawNotifPayload struct {
	ID        int64  `json:"id"`
	Type      string `json:"type"`
	FromUser  string `json:"from_user"`
	Message   string `json:"message"`
	Timestamp int64  `json:"timestamp"`
}

// subscribeNotifications subscribes to the Redis Pub/Sub channel for a specific user
// and forwards any incoming messages to their WebSocket writeCh.
//
// Channel naming convention: notif:{userID}
// e.g. for user123 → subscribes to "notif:user123"
//
// This runs as its own goroutine per connection (spawned in handler.go).
// It exits cleanly when ctx is cancelled (i.e. the user disconnects).
//
// writeCh is the same channel the writer goroutine drains —
// we never call ws.WriteMessage directly from here, always go through writeCh
// to respect the single-writer rule.
func subscribeNotifications(ctx context.Context, userID string, writeCh chan<- []byte) {
	channel := "notif:" + userID

	// Subscribe to the user's personal notification channel in Redis Pub/Sub.
	// Any fan-out worker that publishes to "notif:user123" will be received here.
	sub := rdb.Subscribe(ctx, channel)
	defer sub.Close()

	log.Printf("subscribed to Redis channel %s", channel)

	// sub.Channel() returns a Go channel that delivers incoming Pub/Sub messages
	ch := sub.Channel()

	for {
		select {
		case <-ctx.Done():
			// User disconnected — ctx was cancelled, exit cleanly
			log.Printf("unsubscribed from channel %s", channel)
			return

		case msg, ok := <-ch:
			if !ok {
				// Redis channel was closed — exit
				log.Printf("Redis channel closed for user %s", userID)
				return
			}

			log.Printf("redis pub/sub received user=%s channel=%q payload=%s", userID, msg.Channel, msg.Payload)

			// Parse the fanout worker's payload and re-marshal with the
			// "type": "notification" envelope so clients can distinguish
			// real-time messages from "history" envelopes.
			var raw rawNotifPayload
			if err := json.Unmarshal([]byte(msg.Payload), &raw); err != nil {
				log.Printf("malformed redis payload for user %s: %v", userID, err)
				continue
			}
			wrapped, err := json.Marshal(realtimeNotif{
				Type:      "notification",
				ID:        raw.ID,
				EventType: raw.Type,
				FromUser:  raw.FromUser,
				Message:   raw.Message,
				Timestamp: raw.Timestamp,
			})
			if err != nil {
				log.Printf("marshal realtime notif for user %s: %v", userID, err)
				continue
			}

			// Forward the wrapped message to the writer goroutine via writeCh.
			// Using a non-blocking select so a slow/stuck client doesn't block
			// the subscriber — we just drop the message and log it instead.
			select {
			case writeCh <- wrapped:
				// Mark delivered optimistically now that the message is queued.
				if historySvc != nil {
					if err := historySvc.MarkDelivered(ctx, userID, raw.ID); err != nil {
						log.Printf("mark delivered error user=%s id=%d: %v", userID, raw.ID, err)
					}
				}
			default:
				log.Printf("writeCh full for user %s, dropping message", userID)
			}
		}
	}
}
