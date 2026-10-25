package history

import (
	"context"
	"database/sql"
)

// Notification is a single notification item returned in a history result.
type Notification struct {
	ID        int64  `json:"id"`
	FromUser  string `json:"from_user"`
	EventType string `json:"event_type"`
	Message   string `json:"message"`
	Delivered bool   `json:"delivered"`
	Timestamp int64  `json:"timestamp"` // Unix milliseconds derived from created_at
}

// Result is the value returned by GetHistory.
type Result struct {
	UnreadCount   int            `json:"unread_count"`
	Notifications []Notification `json:"notifications"`
	HasMore       bool           `json:"has_more"`
}

// Service fetches and manages notification history from PostgreSQL.
type Service struct {
	db *sql.DB
}

// NewService wraps db in a Service ready to serve history requests.
func NewService(db *sql.DB) *Service {
	return &Service{db: db}
}

// MarkDelivered marks a single notification as delivered for recipientID.
// Called by the gateway after queuing a real-time notification onto writeCh.
func (s *Service) MarkDelivered(ctx context.Context, recipientID string, id int64) error {
	return markDelivered(ctx, s.db, recipientID, []int64{id})
}

// GetHistory returns the most recent notifications for recipientID.
//
// beforeID is a pagination cursor — pass 0 to start from the latest notification,
// or the ID of the oldest notification in the previous page to fetch the next page.
// limit controls the page size; at most limit notifications are returned.
//
// Fan-out-on-read records (stored in the events table) are resolved by joining
// against the followers table, then written through to the notifications table
// so future fetches see them with accurate delivered status.
//
// All returned notifications are marked as delivered before returning.
func (s *Service) GetHistory(ctx context.Context, recipientID string, beforeID int64, limit int) (Result, error) {
	// Fetch one extra row to detect whether another page exists.
	rows, err := fetchRows(ctx, s.db, recipientID, beforeID, limit+1)
	if err != nil {
		return Result{}, err
	}

	hasMore := len(rows) > limit
	if hasMore {
		rows = rows[:limit]
	}

	// Persist fan-out-on-read records before counting unread,
	// so they are included in the count.
	if err := writeThrough(ctx, s.db, rows); err != nil {
		return Result{}, err
	}

	// Total unread across all pages (not just this batch).
	unread, err := countUnread(ctx, s.db, recipientID)
	if err != nil {
		return Result{}, err
	}

	ids := make([]int64, len(rows))
	notifs := make([]Notification, len(rows))
	for i, r := range rows {
		ids[i] = r.id
		notifs[i] = Notification{
			ID:        r.id,
			FromUser:  r.fromUser,
			EventType: r.eventType,
			Message:   r.message,
			Delivered: r.delivered,
			Timestamp: r.createdAt.UnixMilli(),
		}
	}

	if err := markDelivered(ctx, s.db, recipientID, ids); err != nil {
		return Result{}, err
	}

	return Result{
		UnreadCount:   unread,
		Notifications: notifs,
		HasMore:       hasMore,
	}, nil
}
