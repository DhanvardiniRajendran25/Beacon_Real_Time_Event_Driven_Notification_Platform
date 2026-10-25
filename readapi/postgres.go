package main

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DBNotification is one row from the notifications table.
// Payload is the JSON string stored by the fan-out worker; TsMs is its Unix
// millisecond timestamp, used as the score when re-inserting into Redis.
type DBNotification struct {
	Payload string
	TsMs    int64
}

// PGStore queries PostgreSQL for notification history used as the Redis fallback.
type PGStore struct {
	pool *pgxpool.Pool
}

// NewPGStore opens a connection pool to the given PostgreSQL URL.
// Returns an error if the database is unreachable so the caller can decide
// whether to start without a DB or abort.
func NewPGStore(ctx context.Context, url string) (*PGStore, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("pgxpool.New: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return &PGStore{pool: pool}, nil
}

// Close releases all pool connections.
func (s *PGStore) Close() {
	s.pool.Close()
}

// GetRecentNotifications returns the latest notifications for a user from
// PostgreSQL, newest-first, capped at limit rows.
func (s *PGStore) GetRecentNotifications(ctx context.Context, userID string, limit int64) ([]DBNotification, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT payload::text, ts_ms
		   FROM notifications
		  WHERE user_id = $1
		  ORDER BY ts_ms DESC
		  LIMIT $2`,
		userID, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("query notifications for user %q: %w", userID, err)
	}
	defer rows.Close()

	var result []DBNotification
	for rows.Next() {
		var n DBNotification
		if err := rows.Scan(&n.Payload, &n.TsMs); err != nil {
			return nil, fmt.Errorf("scan notification row: %w", err)
		}
		result = append(result, n)
	}
	return result, rows.Err()
}
