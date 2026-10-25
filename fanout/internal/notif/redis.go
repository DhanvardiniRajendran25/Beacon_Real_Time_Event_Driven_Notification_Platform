package notif

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Tanyayya/NotificationSystem/fanout/internal/consumer"
	"github.com/redis/go-redis/v9"
)

// notifTTL is how long a user's notification history lives in Redis without a new
// notification arriving. On each write the TTL is refreshed, so active users are
// never evicted; inactive users expire after 7 days and the Read API falls back to
// PostgreSQL.
const notifTTL = 7 * 24 * time.Hour

// Payload is the JSON body published to Redis channels notif:{userID}.
type Payload struct {
	ID        int64  `json:"id"`
	Type      string `json:"type"`
	FromUser  string `json:"from_user"`
	Message   string `json:"message"`
	Timestamp int64  `json:"timestamp"` // Unix milliseconds
}

// Publisher sends fixed-shape notifications to Redis Pub/Sub.
type Publisher struct {
	rdb     *redis.Client
	payload Payload
}

// NewPublisher connects to Redis and builds the notification template from config fields.
func NewPublisher(redisAddr, notifyType, fromUser, message string) *Publisher {
	return &Publisher{
		rdb: redis.NewClient(&redis.Options{Addr: redisAddr}),
		payload: Payload{
			Type:     notifyType,
			FromUser: fromUser,
			Message:  message,
		},
	}
}

// Close releases the Redis client.
func (p *Publisher) Close() error {
	return p.rdb.Close()
}

// Publish sends a notification derived from the Kafka event to channel notif:{userID},
// persists it in the sorted set notifications:{userID} (score = timestamp ms),
// and increments the unread badge counter unread:{userID}.
// FromUser comes from worker config; id, type, message (detail), and timestamp come from the event.
func (p *Publisher) Publish(ctx context.Context, userID string, ev consumer.NotificationEvent) error {
	fromUser := ev.FromUser
	if fromUser == "" {
		fromUser = p.payload.FromUser
	}
	body, err := json.Marshal(Payload{
		ID:        ev.ID,
		Type:      ev.Type,
		FromUser:  fromUser,
		Message:   ev.Detail,
		Timestamp: ev.Timestamp,
	})
	if err != nil {
		return fmt.Errorf("marshal notification: %w", err)
	}

	// Pub/Sub for real-time delivery to connected WebSocket clients.
	if err := p.rdb.Publish(ctx, "notif:"+userID, body).Err(); err != nil {
		return fmt.Errorf("publish to channel: %w", err)
	}

	// Persist in sorted set so the Read API can page through history.
	// Score is the event timestamp in milliseconds for chronological ordering.
	notifKey := "notifications:" + userID
	if err := p.rdb.ZAdd(ctx, notifKey, redis.Z{
		Score:  float64(ev.Timestamp),
		Member: string(body),
	}).Err(); err != nil {
		return fmt.Errorf("zadd notification: %w", err)
	}
	// Refresh TTL on every write; the key expires only after 7 days of silence.
	if err := p.rdb.Expire(ctx, notifKey, notifTTL).Err(); err != nil {
		return fmt.Errorf("expire notifications key: %w", err)
	}

	// Increment unread badge counter; decremented by the Mark-Read API.
	unreadKey := "unread:" + userID
	if err := p.rdb.Incr(ctx, unreadKey).Err(); err != nil {
		return fmt.Errorf("incr unread counter: %w", err)
	}
	if err := p.rdb.Expire(ctx, unreadKey, notifTTL).Err(); err != nil {
		return fmt.Errorf("expire unread key: %w", err)
	}

	return nil
}

// PublishPipeline sends the same notification payload to notif:{userID} for each userID
// using a Redis pipeline (one round trip). Returns the number of PUBLISH commands that failed.
// Empty userIDs returns failCount 0.
func (p *Publisher) PublishPipeline(ctx context.Context, userIDs []string, ev consumer.NotificationEvent) (failCount int, err error) {
	if len(userIDs) == 0 {
		return 0, nil
	}
	fromUser := ev.FromUser
	if fromUser == "" {
		fromUser = p.payload.FromUser
	}
	body, err := json.Marshal(Payload{
		ID:        ev.ID,
		Type:      ev.Type,
		FromUser:  fromUser,
		Message:   ev.Detail,
		Timestamp: ev.Timestamp,
	})
	if err != nil {
		return 0, fmt.Errorf("marshal notification: %w", err)
	}
	pipe := p.rdb.Pipeline()
	for _, userID := range userIDs {
		pipe.Publish(ctx, "notif:"+userID, body)
	}
	cmds, pipeErr := pipe.Exec(ctx)
	for _, cmd := range cmds {
		if cmd.Err() != nil {
			failCount++
		}
	}
	if pipeErr != nil && len(cmds) == 0 {
		return len(userIDs), pipeErr
	}
	return failCount, nil
}
