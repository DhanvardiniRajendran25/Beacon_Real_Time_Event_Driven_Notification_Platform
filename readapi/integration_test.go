//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Tanyayya/NotificationSystem/fanout/internal/consumer"
	"github.com/Tanyayya/NotificationSystem/fanout/internal/notif"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
)

// redisAddr returns the Redis address to use for integration tests and skips
// the test if Redis is not reachable.
func redisAddr(t *testing.T) string {
	t.Helper()
	addr := getEnv("REDIS_ADDR", "localhost:6379")
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer rdb.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("redis not reachable at %s — set REDIS_ADDR or start docker-compose: %v", addr, err)
	}
	return addr
}

// uniqueUser returns a test-scoped user ID that won't collide across runs.
func uniqueUser(label string) string {
	return fmt.Sprintf("test-%s-%d", label, time.Now().UnixNano())
}

// TestFanoutToRedis verifies the full fan-out path:
// notif.Publisher writes to Pub/Sub, sorted set, and increments the unread counter.
func TestFanoutToRedis(t *testing.T) {
	addr := redisAddr(t)
	userID := uniqueUser("fanout")
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer rdb.Close()
	t.Cleanup(func() {
		rdb.Del(ctx, "notifications:"+userID, "unread:"+userID)
	})

	pub := notif.NewPublisher(addr, "like", "alice", "stub")
	defer pub.Close()

	ev := consumer.NotificationEvent{
		ID:        42,
		Type:      "like",
		Detail:    "Alice liked your post",
		Timestamp: time.Now().UnixMilli(),
	}

	if err := pub.Publish(ctx, userID, ev); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// Sorted set should contain exactly one entry.
	entries, err := rdb.ZRevRange(ctx, "notifications:"+userID, 0, -1).Result()
	if err != nil {
		t.Fatalf("ZREVRANGE: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 sorted-set entry, got %d", len(entries))
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(entries[0]), &payload); err != nil {
		t.Fatalf("unmarshal sorted-set entry: %v", err)
	}
	if payload["type"] != "like" {
		t.Errorf("type: got %v, want like", payload["type"])
	}
	if payload["from_user"] != "alice" {
		t.Errorf("from_user: got %v, want alice", payload["from_user"])
	}

	// Unread counter should be 1.
	count, err := rdb.Get(ctx, "unread:"+userID).Int64()
	if err != nil {
		t.Fatalf("GET unread: %v", err)
	}
	if count != 1 {
		t.Errorf("unread count: got %d, want 1", count)
	}
}

// TestGetNotificationsEndpoint verifies GET /notifications returns the sorted-set
// contents and the unread badge count through the HTTP handler.
func TestGetNotificationsEndpoint(t *testing.T) {
	addr := redisAddr(t)
	userID := uniqueUser("endpoint")
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer rdb.Close()
	t.Cleanup(func() {
		rdb.Del(ctx, "notifications:"+userID, "unread:"+userID)
	})

	pub := notif.NewPublisher(addr, "comment", "bob", "stub")
	defer pub.Close()

	// Publish two notifications so we can verify ordering.
	for i, detail := range []string{"first comment", "second comment"} {
		ev := consumer.NotificationEvent{
			ID:        int64(100 + i),
			Type:      "comment",
			Detail:    detail,
			Timestamp: time.Now().UnixMilli() + int64(i)*1000,
		}
		if err := pub.Publish(ctx, userID, ev); err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
	}

	// Set up the handler with the same Redis.
	gin.SetMode(gin.TestMode)
	store := NewStore(addr)
	defer store.Close()
	h := NewHandler(store)

	router := gin.New()
	router.GET("/notifications", h.GetNotifications)

	req := httptest.NewRequest(http.MethodGet, "/notifications?user_id="+userID, nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status: got %d, want 200 — body: %s", w.Code, w.Body.String())
	}

	var resp NotificationResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}

	if resp.UserID != userID {
		t.Errorf("user_id: got %q, want %q", resp.UserID, userID)
	}
	if resp.UnreadCount != 2 {
		t.Errorf("unread_count: got %d, want 2", resp.UnreadCount)
	}
	if len(resp.Notifications) != 2 {
		t.Fatalf("notifications count: got %d, want 2", len(resp.Notifications))
	}

	// ZREVRANGE returns newest first — the second notification should be first.
	var first map[string]any
	if err := json.Unmarshal(resp.Notifications[0], &first); err != nil {
		t.Fatalf("unmarshal first notification: %v", err)
	}
	if first["message"] != "second comment" {
		t.Errorf("first notification message: got %v, want 'second comment'", first["message"])
	}
}

// TestGetNotificationsMissingUserID verifies the endpoint returns 400 when
// user_id is omitted.
func TestGetNotificationsMissingUserID(t *testing.T) {
	addr := redisAddr(t)

	gin.SetMode(gin.TestMode)
	store := NewStore(addr)
	defer store.Close()
	h := NewHandler(store)

	router := gin.New()
	router.GET("/notifications", h.GetNotifications)

	req := httptest.NewRequest(http.MethodGet, "/notifications", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400", w.Code)
	}
}

// TestGetNotificationsEmpty verifies a user with no notifications gets an empty list
// and a zero unread count.
func TestGetNotificationsEmpty(t *testing.T) {
	addr := redisAddr(t)
	userID := uniqueUser("empty")

	gin.SetMode(gin.TestMode)
	store := NewStore(addr)
	defer store.Close()
	h := NewHandler(store)

	router := gin.New()
	router.GET("/notifications", h.GetNotifications)

	req := httptest.NewRequest(http.MethodGet, "/notifications?user_id="+userID, nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status: got %d, want 200", w.Code)
	}

	var resp NotificationResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.UnreadCount != 0 {
		t.Errorf("unread_count: got %d, want 0", resp.UnreadCount)
	}
	if len(resp.Notifications) != 0 {
		t.Errorf("notifications: got %d entries, want 0", len(resp.Notifications))
	}
}

// TestGetNotificationsPagination verifies the limit and offset query parameters.
func TestGetNotificationsPagination(t *testing.T) {
	addr := redisAddr(t)
	userID := uniqueUser("pagination")
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer rdb.Close()
	t.Cleanup(func() {
		rdb.Del(ctx, "notifications:"+userID, "unread:"+userID)
	})

	pub := notif.NewPublisher(addr, "like", "alice", "stub")
	defer pub.Close()

	// Publish 5 notifications with distinct timestamps.
	now := time.Now().UnixMilli()
	for i := 0; i < 5; i++ {
		ev := consumer.NotificationEvent{
			ID:        int64(200 + i),
			Type:      "like",
			Detail:    fmt.Sprintf("notification %d", i),
			Timestamp: now + int64(i)*1000,
		}
		if err := pub.Publish(ctx, userID, ev); err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
	}

	gin.SetMode(gin.TestMode)
	store := NewStore(addr)
	defer store.Close()
	h := NewHandler(store)

	router := gin.New()
	router.GET("/notifications", h.GetNotifications)

	// First page: limit=2, offset=0 → newest two (notifications 4 and 3).
	req := httptest.NewRequest(http.MethodGet, "/notifications?user_id="+userID+"&limit=2&offset=0", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("page1 status: got %d", w.Code)
	}
	var page1 NotificationResponse
	json.Unmarshal(w.Body.Bytes(), &page1)
	if len(page1.Notifications) != 2 {
		t.Errorf("page1 notifications: got %d, want 2", len(page1.Notifications))
	}

	// Second page: limit=2, offset=2 → notifications 2 and 1.
	req = httptest.NewRequest(http.MethodGet, "/notifications?user_id="+userID+"&limit=2&offset=2", nil)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("page2 status: got %d", w.Code)
	}
	var page2 NotificationResponse
	json.Unmarshal(w.Body.Bytes(), &page2)
	if len(page2.Notifications) != 2 {
		t.Errorf("page2 notifications: got %d, want 2", len(page2.Notifications))
	}

	// Verify no overlap between pages.
	if string(page1.Notifications[0]) == string(page2.Notifications[0]) {
		t.Error("pages overlap — pagination is broken")
	}
}
