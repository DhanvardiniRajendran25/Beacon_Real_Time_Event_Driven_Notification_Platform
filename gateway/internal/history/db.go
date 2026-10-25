package history

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	"github.com/lib/pq"
)

// OpenDB opens a PostgreSQL connection pool using the DB_DSN environment variable.
// Falls back to the standard local dev DSN if unset.
func OpenDB() (*sql.DB, error) {
	dsn := os.Getenv("DB_DSN")
	if dsn == "" {
		dsn = "postgres://notif:notif@localhost:5432/notifications?sslmode=disable"
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("open postgres: %w", err)
	}
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return db, nil
}

// historyRow is the internal scan target for the merged UNION query.
type historyRow struct {
	id          int64
	recipientID string
	fromUser    string
	eventType   string
	message     string
	delivered   bool
	createdAt   time.Time
	fromEvents  bool // true = sourced from events table, needs write-through
}

// fetchRows runs the merged UNION query across the notifications and events tables.
// Returns at most limit rows ordered newest-first.
// beforeID = 0 starts from the latest; non-zero is used as a cursor for pagination.
func fetchRows(ctx context.Context, db *sql.DB, recipientID string, beforeID int64, limit int) ([]historyRow, error) {
	const q = `
		SELECT id, recipient_id, from_user, type, message, delivered, created_at, FALSE AS from_events
		FROM notifications
		WHERE recipient_id = $1
		  AND ($2::bigint = 0 OR id < $2::bigint)

		UNION ALL

		SELECT e.id, $1 AS recipient_id, e.from_user, e.type, e.message,
		       FALSE AS delivered, e.created_at, TRUE AS from_events
		FROM events e
		JOIN followers f ON f.following_id = e.from_user
		WHERE f.follower_id = $1
		  AND ($2::bigint = 0 OR e.id < $2::bigint)
		  AND NOT EXISTS (
		      SELECT 1 FROM notifications n
		      WHERE n.id = e.id AND n.recipient_id = $1
		  )

		ORDER BY created_at DESC
		LIMIT $3`

	rows, err := db.QueryContext(ctx, q, recipientID, beforeID, limit)
	if err != nil {
		return nil, fmt.Errorf("fetch history: %w", err)
	}
	defer rows.Close()

	var result []historyRow
	for rows.Next() {
		var r historyRow
		if err := rows.Scan(
			&r.id, &r.recipientID, &r.fromUser, &r.eventType,
			&r.message, &r.delivered, &r.createdAt, &r.fromEvents,
		); err != nil {
			return nil, fmt.Errorf("scan history row: %w", err)
		}
		result = append(result, r)
	}
	return result, rows.Err()
}

// writeThrough inserts events-sourced rows into the notifications table so that
// future fetches see them with the correct delivered status rather than re-resolving
// them from the events table again.
func writeThrough(ctx context.Context, db *sql.DB, rows []historyRow) error {
	for _, r := range rows {
		if !r.fromEvents {
			continue
		}
		_, err := db.ExecContext(ctx,
			`INSERT INTO notifications (id, recipient_id, from_user, type, message, delivered, created_at)
			 VALUES ($1, $2, $3, $4, $5, FALSE, $6)
			 ON CONFLICT (id, recipient_id) DO NOTHING`,
			r.id, r.recipientID, r.fromUser, r.eventType, r.message, r.createdAt,
		)
		if err != nil {
			return fmt.Errorf("write-through id=%d recipient=%s: %w", r.id, r.recipientID, err)
		}
	}
	return nil
}

// countUnread returns the total number of undelivered notifications for recipientID.
// Must be called after writeThrough so events-sourced records are included in the count.
func countUnread(ctx context.Context, db *sql.DB, recipientID string) (int, error) {
	var count int
	err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM notifications WHERE recipient_id = $1 AND delivered = FALSE`,
		recipientID,
	).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count unread: %w", err)
	}
	return count, nil
}

// markDelivered sets delivered=TRUE for all given notification IDs belonging to recipientID.
func markDelivered(ctx context.Context, db *sql.DB, recipientID string, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := db.ExecContext(ctx,
		`UPDATE notifications SET delivered = TRUE
		 WHERE recipient_id = $1 AND id = ANY($2)`,
		recipientID, pq.Array(ids),
	)
	if err != nil {
		return fmt.Errorf("mark delivered recipient=%s: %w", recipientID, err)
	}
	return nil
}
