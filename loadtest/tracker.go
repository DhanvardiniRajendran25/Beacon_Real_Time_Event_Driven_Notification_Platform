package main

import (
	"context"
	"sort"
	"sync"
	"time"
)

const perSubscriberTimeout = 15 * time.Second

// report is sent by a subscriber goroutine when it receives a notification.
type report struct {
	eventID int64
	userID  string
	at      time.Time
}

// eventResult is emitted by the tracker when an event is complete or timed out.
type eventResult struct {
	eventID             int64
	sentAtMS            int64
	lastReceivedMS      int64
	maxLatencyMS        int64
	p50MS               int64
	p95MS               int64
	p99MS               int64
	subscribersReceived int
	subscribersTotal    int
	complete            bool
}

// subState tracks the delivery status for a single (event, subscriber) pair.
type subState struct {
	timer      *time.Timer
	received   bool
	timedOut   bool
	skipped    bool
	receivedAt time.Time
}

type inflightEvent struct {
	eventID  int64
	sentAt   time.Time
	mu       sync.Mutex
	subs     map[string]*subState
	resolved int
	total    int
}

// Tracker matches subscriber delivery reports to sent events and computes latency.
type Tracker struct {
	events  sync.Map
	Reports chan report
	Results chan eventResult
	pool    *SubscriberPool
	metrics *Metrics
}

func NewTracker(pool *SubscriberPool, metrics *Metrics) *Tracker {
	return &Tracker{
		Reports: make(chan report, pool.followerCount*4),
		Results: make(chan eventResult, 256),
		pool:    pool,
		metrics: metrics,
	}
}

// Register records a new in-flight event. Call immediately after POST /event returns.
// It snapshots the currently-connected subscribers and starts a 2-second per-subscriber timer.
func (t *Tracker) Register(eventID int64, sentAt time.Time) {
	connectedIDs := t.pool.ConnectedIDs()
	ev := &inflightEvent{
		eventID: eventID,
		sentAt:  sentAt,
		subs:    make(map[string]*subState, len(connectedIDs)),
		total:   len(connectedIDs),
	}

	// Lock before storing so any concurrent handleReport blocks until all subs are initialised.
	ev.mu.Lock()
	t.events.Store(eventID, ev)

	for userID := range connectedIDs {
		uid := userID // capture for closure
		sub := &subState{}
		ev.subs[uid] = sub
		sub.timer = time.AfterFunc(perSubscriberTimeout, func() {
			t.handleTimeout(eventID, uid)
		})
	}
	ev.mu.Unlock()

	if ev.total == 0 {
		t.finalize(eventID)
	}
}

// Run drains the Reports channel and processes delivery confirmations.
// Must be called in its own goroutine.
func (t *Tracker) Run(ctx context.Context) {
	for {
		select {
		case r := <-t.Reports:
			t.handleReport(r)
		case <-ctx.Done():
			return
		}
	}
}

func (t *Tracker) handleReport(r report) {
	val, ok := t.events.Load(r.eventID)
	if !ok {
		return
	}
	ev := val.(*inflightEvent)

	ev.mu.Lock()
	sub, ok := ev.subs[r.userID]
	if !ok || sub.received || sub.timedOut || sub.skipped {
		ev.mu.Unlock()
		return
	}
	sub.timer.Stop()
	sub.received = true
	sub.receivedAt = r.at
	ev.resolved++
	complete := ev.resolved >= ev.total
	ev.mu.Unlock()

	t.metrics.RecordNotificationReceived(r.eventID, r.userID, ev.sentAt, r.at)

	if complete {
		t.finalize(r.eventID)
	}
}

// handleTimeout fires when a subscriber's 2-second window elapses without delivery.
// If the subscriber is still connected it is recorded as not_delivered; if it has
// disconnected it is silently skipped.
func (t *Tracker) handleTimeout(eventID int64, userID string) {
	val, ok := t.events.Load(eventID)
	if !ok {
		return
	}
	ev := val.(*inflightEvent)

	ev.mu.Lock()
	sub := ev.subs[userID]
	if sub.received || sub.timedOut || sub.skipped {
		ev.mu.Unlock()
		return
	}

	sentAt := ev.sentAt
	if t.pool.IsConnected(userID) {
		sub.timedOut = true
		ev.resolved++
		complete := ev.resolved >= ev.total
		ev.mu.Unlock()
		t.metrics.RecordNotDelivered(eventID, userID, sentAt)
		if complete {
			t.finalize(eventID)
		}
	} else {
		// Subscriber disconnected — skip without recording not_delivered.
		sub.skipped = true
		ev.resolved++
		complete := ev.resolved >= ev.total
		ev.mu.Unlock()
		if complete {
			t.finalize(eventID)
		}
	}
}

// finalize is safe to call from multiple goroutines; sync.Map.LoadAndDelete ensures
// only the first call proceeds.
func (t *Tracker) finalize(eventID int64) {
	val, loaded := t.events.LoadAndDelete(eventID)
	if !loaded {
		return
	}
	ev := val.(*inflightEvent)

	ev.mu.Lock()
	defer ev.mu.Unlock()

	var latencies []int64
	var lastReceived time.Time
	allReceived := true
	receivedCount := 0

	for _, sub := range ev.subs {
		if sub.received {
			d := sub.receivedAt.Sub(ev.sentAt).Milliseconds()
			latencies = append(latencies, d)
			if sub.receivedAt.After(lastReceived) {
				lastReceived = sub.receivedAt
			}
			receivedCount++
		} else {
			allReceived = false
		}
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	result := eventResult{
		eventID:             eventID,
		sentAtMS:            ev.sentAt.UnixMilli(),
		subscribersReceived: receivedCount,
		subscribersTotal:    ev.total,
		complete:            allReceived,
	}
	if !lastReceived.IsZero() {
		result.lastReceivedMS = lastReceived.UnixMilli()
	}
	if len(latencies) > 0 {
		result.maxLatencyMS = latencies[len(latencies)-1]
		result.p50MS = percentile(latencies, 50)
		result.p95MS = percentile(latencies, 95)
		result.p99MS = percentile(latencies, 99)
	}

	select {
	case t.Results <- result:
	default:
		// Results channel full — drop rather than block; metrics will show events_in_flight not draining
	}
}

func percentile(sorted []int64, p int) int64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := (p * len(sorted)) / 100
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}
