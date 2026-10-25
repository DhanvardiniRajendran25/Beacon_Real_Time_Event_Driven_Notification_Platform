package db

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	_ "github.com/lib/pq" // PostgreSQL driver

	"github.com/Tanyayya/NotificationSystem/fanout/internal/consumer"
)

// DB wraps a PostgreSQL connection pool and exposes the queries
// needed by the fan-out worker.
type DB struct {
	pool *sql.DB
}

// New opens a connection pool to PostgreSQL using the given DSN.
// Example DSN: postgres://notif:notif@localhost:5432/notifications?sslmode=disable
func New(dsn string) (*DB, error) {
	pool, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("open postgres: %w", err)
	}
	if err := pool.Ping(); err != nil {
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	log.Printf("connected to PostgreSQL")
	return &DB{pool: pool}, nil
}

// Close releases the connection pool.
func (d *DB) Close() error {
	return d.pool.Close()
}

// GetFollowers returns the list of follower IDs for a given user.
// These are the users who should receive a notification when followingID posts.
func (d *DB) GetFollowers(ctx context.Context, followingID string) ([]string, error) {
	rows, err := d.pool.QueryContext(ctx,
		`SELECT follower_id FROM followers WHERE following_id = $1`,
		followingID,
	)
	if err != nil {
		return nil, fmt.Errorf("query followers: %w", err)
	}
	defer rows.Close()

	var followers []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan follower: %w", err)
		}
		followers = append(followers, id)
	}
	return followers, rows.Err()
}

// InsertNotification writes one notification record per recipient.
// Called during fan-out on write for each follower.
// delivered=false means the user hasn't received it yet —
// the gateway replays these on reconnect.
func (d *DB) InsertNotification(ctx context.Context, recipientID string, ev consumer.NotificationEvent) error {
	createdAt := time.UnixMilli(ev.ID >> 22)
	_, err := d.pool.ExecContext(ctx,
		`INSERT INTO notifications (id, recipient_id, from_user, type, message, delivered, created_at)
		 VALUES ($1, $2, $3, $4, $5, false, $6)
		 ON CONFLICT (id, recipient_id) DO NOTHING`,
		ev.ID,
		recipientID,
		ev.FromUser,
		ev.Type,
		ev.Detail,
		createdAt,
	)
	if err != nil {
		return fmt.Errorf("insert notification recipient=%s: %w", recipientID, err)
	}
	return nil
}

// InsertNotificationsBatch inserts one notification row per recipient in a single statement.
// recipientIDs must be non-empty; callers chunk to stay within Postgres parameter limits.
func (d *DB) InsertNotificationsBatch(ctx context.Context, recipientIDs []string, ev consumer.NotificationEvent) error {
	if len(recipientIDs) == 0 {
		return nil
	}
	createdAt := time.UnixMilli(ev.ID >> 22)
	var b strings.Builder
	b.WriteString(`INSERT INTO notifications (id, recipient_id, from_user, type, message, delivered, created_at) VALUES `)
	args := make([]interface{}, 0, len(recipientIDs)*6)
	for i, recipientID := range recipientIDs {
		if i > 0 {
			b.WriteString(", ")
		}
		base := i*6 + 1
		fmt.Fprintf(&b, "($%d, $%d, $%d, $%d, $%d, false, $%d)", base, base+1, base+2, base+3, base+4, base+5)
		args = append(args, ev.ID, recipientID, ev.FromUser, ev.Type, ev.Detail, createdAt)
	}
	b.WriteString(` ON CONFLICT (id, recipient_id) DO NOTHING`)
	_, err := d.pool.ExecContext(ctx, b.String(), args...)
	if err != nil {
		return fmt.Errorf("insert notifications batch n=%d: %w", len(recipientIDs), err)
	}
	return nil
}

// InsertEvent writes a single event record used by fan-out on read.
// Called for high-follower accounts instead of writing one row per follower.
// Recipients are resolved at read time from the followers table.
func (d *DB) InsertEvent(ctx context.Context, ev consumer.NotificationEvent) error {
	createdAt := time.UnixMilli(ev.ID >> 22)
	_, err := d.pool.ExecContext(ctx,
		`INSERT INTO events (id, from_user, type, message, created_at)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (id) DO NOTHING`,
		ev.ID,
		ev.FromUser,
		ev.Type,
		ev.Detail,
		createdAt,
	)
	if err != nil {
		return fmt.Errorf("insert event: %w", err)
	}
	return nil
}

// GetUndeliveredNotifications returns all undelivered notifications for a user.
// Called by the gateway on reconnect to replay the offline backlog.
// Results are ordered oldest first so notifications replay in correct order.
func (d *DB) GetUndeliveredNotifications(ctx context.Context, recipientID string) ([]consumer.NotificationEvent, error) {
	rows, err := d.pool.QueryContext(ctx,
		`SELECT id, from_user, type, message
		 FROM notifications
		 WHERE recipient_id = $1 AND delivered = false
		 ORDER BY created_at ASC`,
		recipientID,
	)
	if err != nil {
		return nil, fmt.Errorf("query notifications: %w", err)
	}
	defer rows.Close()

	var events []consumer.NotificationEvent
	for rows.Next() {
		var ev consumer.NotificationEvent
		if err := rows.Scan(&ev.ID, &ev.FromUser, &ev.Type, &ev.Detail); err != nil {
			return nil, fmt.Errorf("scan notification: %w", err)
		}
		events = append(events, ev)
	}
	return events, rows.Err()
}

// MarkDelivered marks a notification as delivered for a recipient.
// Called by the gateway after successfully sending the notification over WebSocket.
func (d *DB) MarkDelivered(ctx context.Context, notificationID int64, recipientID string) error {
	_, err := d.pool.ExecContext(ctx,
		`UPDATE notifications SET delivered = true
		 WHERE id = $1 AND recipient_id = $2`,
		notificationID,
		recipientID,
	)
	if err != nil {
		return fmt.Errorf("mark delivered id=%d recipient=%s: %w", notificationID, recipientID, err)
	}
	return nil
}