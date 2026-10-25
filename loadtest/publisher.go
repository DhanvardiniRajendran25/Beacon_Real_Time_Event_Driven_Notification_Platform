package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

type eventRequest struct {
	Type     string `json:"type"`
	FromUser string `json:"from_user"`
	Detail   string `json:"detail"`
}

type eventResponse struct {
	EventID int64 `json:"event_id"`
}

// Publisher POSTs events to the ingestion API and registers each confirmed
// event in the Tracker.
type Publisher struct {
	ingestionURL string
	senderUser   string
	rate         float64
	tracker      *Tracker
	metrics      *Metrics
	client       *http.Client
}

func NewPublisher(ingestionURL, senderUser string, rate float64, tracker *Tracker, metrics *Metrics) *Publisher {
	return &Publisher{
		ingestionURL: ingestionURL,
		senderUser:   senderUser,
		rate:         rate,
		tracker:      tracker,
		metrics:      metrics,
		client:       &http.Client{Timeout: 10 * time.Second},
	}
}

// Run ticks at the configured rate and POSTs events until ctx is done.
func (p *Publisher) Run(ctx context.Context) {
	interval := time.Duration(float64(time.Second) / p.rate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.postEvent(ctx)
		case <-ctx.Done():
			return
		}
	}
}

func (p *Publisher) postEvent(ctx context.Context) {
	body, _ := json.Marshal(eventRequest{
		Type:     "POST",
		FromUser: p.senderUser,
		Detail:   "loadtest",
	})

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		fmt.Sprintf("%s/event", p.ingestionURL), bytes.NewReader(body))
	if err != nil {
		log.Printf("publisher: build request: %v", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := p.client.Do(req)
	if err != nil {
		log.Printf("publisher: POST /event: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("publisher: unexpected status %d", resp.StatusCode)
		return
	}

	var evResp eventResponse
	if err := json.NewDecoder(resp.Body).Decode(&evResp); err != nil || evResp.EventID == 0 {
		log.Printf("publisher: decode event_id: %v", err)
		return
	}

	// t1 is recorded after the 200 OK — the event is confirmed in Kafka at this point.
	t1 := time.Now()
	p.tracker.Register(evResp.EventID, t1)
	p.metrics.RecordEventSent(evResp.EventID, t1)
	p.metrics.IncInFlight()

	log.Printf("publisher: event_id=%d registered", evResp.EventID)
}
