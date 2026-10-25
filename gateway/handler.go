package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"sync/atomic"

	"github.com/Tanyayya/NotificationSystem/gateway/internal/history"
	"github.com/gorilla/websocket"
)

const (
	pingInterval = 30 * time.Second // must be well under ALB idle timeout (60s)
	pongDeadline = 60 * time.Second // if no pong within this window, treat client as dead
)

var activeConnections int64

// upgrader upgrades a regular HTTP connection to a WebSocket connection.
// CheckOrigin is set to allow all origins for now — tighten this in production.
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

// conn represents a single connected WebSocket client.
// It holds everything needed to manage that client's lifecycle:
//   - userID:  who this connection belongs to
//   - ws:      the actual WebSocket connection
//   - writeCh: a channel for safely sending messages (gorilla/websocket
//              does NOT allow concurrent writes — all writes must go
//              through this channel to a single dedicated writer goroutine)
//   - cancel:  cancels the context to shut down all goroutines for this connection
type conn struct {
	userID  string
	ws      *websocket.Conn
	writeCh chan []byte
	cancel  context.CancelFunc
}

// historyMsg is the envelope sent to the client for both the on-connect history
// push and subsequent pagination responses.
type historyMsg struct {
	Type          string               `json:"type"`
	UnreadCount   int                  `json:"unread_count"`
	HasMore       bool                 `json:"has_more"`
	Notifications []history.Notification `json:"notifications"`
}

// marshalHistory serialises a history.Result into the wire format sent to clients.
func marshalHistory(r history.Result) ([]byte, error) {
	notifs := r.Notifications
	if notifs == nil {
		notifs = []history.Notification{}
	}
	return json.Marshal(historyMsg{
		Type:          "history",
		UnreadCount:   r.UnreadCount,
		HasMore:       r.HasMore,
		Notifications: notifs,
	})
}

// HandleWS is the entry point for all WebSocket connections.
// It is called by main.go whenever a client hits the /ws endpoint.
//
// Flow:
//  1. Read user_id from the query param (?user_id=123)
//  2. Upgrade the HTTP connection to WebSocket
//  3. Register user in Redis (ws:user:{userID} → taskID)
//  4. Fetch and enqueue notification history (if PostgreSQL is available)
//  5. Spawn 3 goroutines: writer, reader, Redis subscriber
//  6. Block until all goroutines exit (i.e. client disconnects)
//  7. Deregister user from Redis
func HandleWS(w http.ResponseWriter, r *http.Request) {
	// Extract user_id from query params — e.g. ws://localhost:8080/ws?user_id=123
	userID := r.URL.Query().Get("user_id")
	if userID == "" {
		http.Error(w, "missing user_id", http.StatusBadRequest)
		return
	}

	// Upgrade the HTTP connection to a persistent WebSocket connection
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("upgrade error for user %s: %v", userID, err)
		return
	}

	// Set initial read deadline. The pong handler below resets it on every pong.
	// If no pong arrives within pongDeadline, ReadMessage returns an error and
	// the reader goroutine tears down the connection cleanly.
	ws.SetReadDeadline(time.Now().Add(pongDeadline))
	ws.SetPongHandler(func(string) error {
		ws.SetReadDeadline(time.Now().Add(pongDeadline))
		return nil
	})

	// ctx is tied to this specific connection's lifetime.
	// When cancel() is called (by reader or writer on error/disconnect),
	// it signals all goroutines for this connection to shut down.
	ctx, cancel := context.WithCancel(context.Background())

	c := &conn{
		userID:  userID,
		ws:      ws,
		writeCh: make(chan []byte, 64), // buffered so senders don't block on slow clients
		cancel:  cancel,
	}

	// TASK_ID identifies this gateway instance in Redis.
	// On ECS this will be set to the task ARN or IP so workers know exactly
	// which gateway to route through. Falls back to "local" for local dev.
	taskID := os.Getenv("TASK_ID")
	if taskID == "" {
		taskID = "local"
	}

	// Register the user in Redis immediately after connecting
	if err := registerUser(ctx, userID, taskID); err != nil {
		log.Printf("failed to register user %s: %v", userID, err)
	}

	atomic.AddInt64(&activeConnections, 1)
	defer atomic.AddInt64(&activeConnections, -1)

	log.Printf("user %s connected", userID)

	// Fetch and enqueue notification history before starting goroutines.
	// writeCh is buffered (64 slots) so this single message fits without blocking.
	// If PostgreSQL is unavailable historySvc is nil and history is silently skipped.
	if historySvc != nil {
		result, err := historySvc.GetHistory(ctx, userID, 0, 50)
		if err != nil {
			log.Printf("history fetch error for user %s: %v", userID, err)
		} else {
			if b, err := marshalHistory(result); err == nil {
				c.writeCh <- b
			} else {
				log.Printf("history marshal error for user %s: %v", userID, err)
			}
		}
	}

	// WaitGroup ensures HandleWS blocks until all goroutines have fully exited
	// before we run cleanup
	var wg sync.WaitGroup
	wg.Add(3)

	// Writer goroutine — the ONLY goroutine allowed to call ws.WriteMessage.
	// It drains writeCh and sends each message to the client.
	go func() {
		defer wg.Done()
		c.writer(ctx)
	}()

	// Reader goroutine — listens for incoming messages from the client.
	// Handles fetch_history pagination requests and detects disconnects.
	go func() {
		defer wg.Done()
		c.reader(ctx)
	}()

	// Subscriber goroutine — listens on Redis Pub/Sub for notif:{userID}
	// and forwards any incoming messages to writeCh for delivery to the client.
	// Defined in pubsub.go.
	go func() {
		defer wg.Done()
		subscribeNotifications(ctx, userID, c.writeCh)
	}()

	// Block here until all goroutines exit
	wg.Wait()

	// Deregister the user from Redis now that the connection is gone
	log.Printf("user %s disconnected, cleaning up", userID)
	if err := deregisterUser(context.Background(), userID); err != nil {
		log.Printf("failed to deregister user %s: %v", userID, err)
	}
}

// writer drains writeCh and sends each message to the WebSocket client.
// This is the single writer goroutine — no other goroutine should ever
// call ws.WriteMessage directly.
// It also owns the ping ticker: every pingInterval it sends a WebSocket
// PING frame. The client (browser) automatically replies with a PONG,
// which resets the read deadline in the pong handler registered in HandleWS.
// Exits when ctx is cancelled, writeCh is closed, or a write error occurs.
func (c *conn) writer(ctx context.Context) {
	defer c.ws.Close()
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			// Connection is shutting down — exit cleanly
			return
		case msg, ok := <-c.writeCh:
			if !ok {
				// writeCh was closed — exit
				return
			}
			if err := c.ws.WriteMessage(websocket.TextMessage, msg); err != nil {
				log.Printf("write error for user %s: %v", c.userID, err)
				c.cancel() // signal reader and other goroutines to shut down too
				return
			}
		case <-ticker.C:
			if err := c.ws.WriteMessage(websocket.PingMessage, nil); err != nil {
				log.Printf("ping error for user %s: %v", c.userID, err)
				c.cancel()
				return
			}
		}
	}
}

// fetchHistoryReq is the shape of an inbound fetch_history message from the client.
type fetchHistoryReq struct {
	Type     string `json:"type"`
	BeforeID int64  `json:"before_id"`
	Limit    int    `json:"limit"`
}

// reader blocks waiting for messages from the client.
// It handles fetch_history pagination requests and detects disconnects,
// calling cancel() to tear down the whole connection cleanly on exit.
func (c *conn) reader(ctx context.Context) {
	defer c.cancel() // always trigger shutdown when reader exits
	for {
		_, msg, err := c.ws.ReadMessage()
		if err != nil {
			// IsUnexpectedCloseError filters out normal closure codes (e.g. browser tab closed)
			// and only logs truly unexpected disconnects
			if websocket.IsUnexpectedCloseError(err,
				websocket.CloseGoingAway,
				websocket.CloseNormalClosure,
			) {
				log.Printf("unexpected close for user %s: %v", c.userID, err)
			}
			return
		}

		log.Printf("received message from user %s: %s", c.userID, msg)

		var req fetchHistoryReq
		if err := json.Unmarshal(msg, &req); err != nil || req.Type != "fetch_history" {
			// Ignore malformed or unknown messages
			continue
		}

		if historySvc == nil {
			continue
		}

		limit := req.Limit
		if limit <= 0 || limit > 50 {
			limit = 50
		}

		result, err := historySvc.GetHistory(ctx, c.userID, req.BeforeID, limit)
		if err != nil {
			log.Printf("fetch_history error for user %s: %v", c.userID, err)
			continue
		}

		b, err := marshalHistory(result)
		if err != nil {
			log.Printf("fetch_history marshal error for user %s: %v", c.userID, err)
			continue
		}

		select {
		case c.writeCh <- b:
		default:
			log.Printf("writeCh full for user %s, dropping history page", c.userID)
		}
	}
}
