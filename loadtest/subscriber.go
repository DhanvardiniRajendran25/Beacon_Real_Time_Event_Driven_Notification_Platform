package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// wsNotification is the minimal shape we care about from the gateway.
type wsNotification struct {
	Type string `json:"type"`
	ID   int64  `json:"id"`
}

// SubscriberPool manages N persistent WebSocket connections, one per follower user.
type SubscriberPool struct {
	albURL         string
	followerPrefix string
	followerStart  int
	followerCount  int
	tracker        *Tracker
	metrics        *Metrics // set before Start (same as tracker)
	activeConns    atomic.Int64
	connectedUsers sync.Map // map[string]struct{} — subscribers past dial + history (or first app JSON)
	wg             sync.WaitGroup
}

func NewSubscriberPool(albURL, followerPrefix string, followerStart, followerCount int, tracker *Tracker) *SubscriberPool {
	return &SubscriberPool{
		albURL:         albURL,
		followerPrefix: followerPrefix,
		followerStart:  followerStart,
		followerCount:  followerCount,
		tracker:        tracker, // may be set after construction
	}
}

// Start spawns all subscriber goroutines. It closes ready once every goroutine
// has established its first connection (or the context is cancelled).
func (p *SubscriberPool) Start(ctx context.Context, ready chan<- struct{}) {
	var connectedOnce atomic.Int64
	total := int64(p.followerCount)

	for i := 0; i < p.followerCount; i++ {
		userID := fmt.Sprintf("%s%d", p.followerPrefix, p.followerStart+i)
		p.wg.Add(1)
		go func(uid string) {
			defer p.wg.Done()
			notified := false
			p.connectLoop(ctx, uid, func() {
				if !notified {
					notified = true
					if connectedOnce.Add(1) == total {
						close(ready)
					}
				}
			})
		}(userID)
	}
}

// connectLoop maintains a single WebSocket connection for a given user, reconnecting
// with exponential backoff on failure.
func (p *SubscriberPool) connectLoop(ctx context.Context, userID string, onFirstConnect func()) {
	backoff := 500 * time.Millisecond
	const maxBackoff = 10 * time.Second
	first := true

	for {
		if ctx.Err() != nil {
			return
		}

		conn, err := p.dial(ctx, userID)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("subscriber %s: dial error: %v (retry in %s)", userID, err, backoff)
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return
			}
			backoff = min(backoff*2, maxBackoff)
			continue
		}

		p.activeConns.Add(1)
		p.connectedUsers.Store(userID, struct{}{})
		log.Printf("subscriber %s: connected", userID)
		backoff = 500 * time.Millisecond

		if first {
			first = false
			onFirstConnect()
		}

		p.readLoop(ctx, conn, userID)

		conn.Close()
		p.connectedUsers.Delete(userID)
		p.activeConns.Add(-1)

		if ctx.Err() != nil {
			return
		}
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return
		}
		backoff = min(backoff*2, maxBackoff)
	}
}

func (p *SubscriberPool) dial(ctx context.Context, userID string) (*websocket.Conn, error) {
	u, err := url.Parse(p.albURL)
	if err != nil {
		return nil, err
	}
	q := u.Query()
	q.Set("user_id", userID)
	u.RawQuery = q.Encode()

	conn, _, err := websocket.DefaultDialer.DialContext(ctx, u.String(), nil)
	return conn, err
}

// readLoop reads messages from an open connection until it errors or ctx is done.
func (p *SubscriberPool) readLoop(ctx context.Context, conn *websocket.Conn, userID string) {
	// Gateway sends a PING every 30s; we set 90s read deadline so we're dropped
	// only if two consecutive pings are missed.
	conn.SetPongHandler(func(_ string) error {
		conn.SetReadDeadline(time.Now().Add(90 * time.Second))
		return nil
	})

	for {
		if ctx.Err() != nil {
			return
		}
		conn.SetReadDeadline(time.Now().Add(90 * time.Second))

		_, msg, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() == nil {
				log.Printf("subscriber %s: read error: %v", userID, err)
			}
			return
		}

		var n wsNotification
		if err := json.Unmarshal(msg, &n); err != nil {
			continue
		}
		// Only track real-time pushes, not history payloads sent on connect.
		if n.Type == "notification" && n.ID != 0 {
			p.tracker.Reports <- report{
				eventID: n.ID,
				userID:  userID,
				at:      time.Now(),
			}
		}
	}
}

func (p *SubscriberPool) ActiveConns() int64 {
	return p.activeConns.Load()
}

// IsConnected reports whether userID currently has an open WebSocket connection.
func (p *SubscriberPool) IsConnected(userID string) bool {
	_, ok := p.connectedUsers.Load(userID)
	return ok
}

// ConnectedIDs returns a snapshot of all currently-connected user IDs.
func (p *SubscriberPool) ConnectedIDs() map[string]struct{} {
	result := make(map[string]struct{})
	p.connectedUsers.Range(func(k, _ any) bool {
		result[k.(string)] = struct{}{}
		return true
	})
	return result
}

func (p *SubscriberPool) Wait() {
	p.wg.Wait()
}
