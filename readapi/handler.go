package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
)

// Storer is the interface the handler uses to query Redis.
type Storer interface {
	GetNotifications(ctx context.Context, userID string, offset, limit int64) ([]string, error)
	GetUnreadCount(ctx context.Context, userID string) (int64, error)
	HasHistory(ctx context.Context, userID string) (bool, error)
	Repopulate(ctx context.Context, userID string, notifs []DBNotification) error
}

// DBStorer is the interface the handler uses to query PostgreSQL on cache miss.
type DBStorer interface {
	GetRecentNotifications(ctx context.Context, userID string, limit int64) ([]DBNotification, error)
}

// Handler holds the HTTP handler methods for the read API.
type Handler struct {
	store Storer
	db    DBStorer // nil when DATABASE_URL is not configured
}

// NewHandler creates a Handler backed by the given Storer and an optional DBStorer.
// Pass nil for db to disable the PostgreSQL fallback.
func NewHandler(store Storer, db DBStorer) *Handler {
	return &Handler{store: store, db: db}
}

// NotificationResponse is the JSON body returned by GET /notifications.
type NotificationResponse struct {
	UserID        string            `json:"user_id"`
	UnreadCount   int64             `json:"unread_count"`
	Notifications []json.RawMessage `json:"notifications"`
}

// GetNotifications handles GET /notifications?user_id=&limit=50&offset=0.
//
// Cache hit:  reads directly from the Redis sorted set notifications:{userID}.
// Cache miss: falls back to PostgreSQL, repopulates the sorted set with a fresh
//
//	7-day TTL, then serves from Redis so subsequent requests stay fast.
func (h *Handler) GetNotifications(c *gin.Context) {
	userID := c.Query("user_id")
	if userID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "user_id is required"})
		return
	}

	limit := int64(50)
	offset := int64(0)

	if v, err := strconv.ParseInt(c.Query("limit"), 10, 64); err == nil && v > 0 {
		limit = v
	}
	if v, err := strconv.ParseInt(c.Query("offset"), 10, 64); err == nil && v >= 0 {
		offset = v
	}

	ctx := c.Request.Context()

	// On cache miss, rebuild from PostgreSQL before reading Redis.
	if h.db != nil {
		ok, err := h.store.HasHistory(ctx, userID)
		if err != nil {
			log.Printf("redis exists check userID=%q: %v", userID, err)
		}
		if !ok {
			dbNotifs, err := h.db.GetRecentNotifications(ctx, userID, 100)
			if err != nil {
				log.Printf("postgres fallback userID=%q: %v", userID, err)
			} else if len(dbNotifs) > 0 {
				if err := h.store.Repopulate(ctx, userID, dbNotifs); err != nil {
					log.Printf("redis repopulate userID=%q: %v", userID, err)
				}
			}
		}
	}

	notifs, err := h.store.GetNotifications(ctx, userID, offset, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to fetch notifications"})
		return
	}

	unread, err := h.store.GetUnreadCount(ctx, userID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to fetch unread count"})
		return
	}

	raw := make([]json.RawMessage, 0, len(notifs))
	for _, n := range notifs {
		raw = append(raw, json.RawMessage(n))
	}

	c.JSON(http.StatusOK, NotificationResponse{
		UserID:        userID,
		UnreadCount:   unread,
		Notifications: raw,
	})
}
