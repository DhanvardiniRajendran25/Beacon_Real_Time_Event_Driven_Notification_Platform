package main

import (
	"encoding/json"
	"log"
	"net/http"
	"slices"
	"time"
)

var allowedEventTypes = []string{"POST", "LIKE", "COMMENT"}

// EventRequest is the body the client sends to POST /event.
type EventRequest struct {
	Type     string `json:"type"`
	FromUser string `json:"from_user"`
	Details  string `json:"detail"`
}

// KafkaEvent is the full payload published to Kafka.
// Shape matches fanout/internal/consumer.NotificationEvent JSON fields.
type KafkaEvent struct {
	ID        int64  `json:"id"`
	Type      string `json:"type"`
	Detail    string `json:"detail"`
	Timestamp int64  `json:"timestamp"`
}

type Handler struct {
	producer *Producer
}

func (h *Handler) HandleEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req EventRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	if req.Type == "" || req.FromUser == "" {
		http.Error(w, "type and from_user are required", http.StatusBadRequest)
		return
	}
	if !slices.Contains(allowedEventTypes, req.Type) {
		http.Error(w, "type must be one of POST, LIKE, COMMENT", http.StatusBadRequest)
		return
	}

	event := KafkaEvent{
		ID:        NewSnowflakeID(),
		Type:      req.Type,
		Detail:    req.Details,
		Timestamp: time.Now().UnixMilli(),
	}


	payload, err := json.Marshal(event)
	if err != nil {
		log.Printf("marshal error: %v", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	if err := h.producer.Publish(req.FromUser, payload); err != nil {
		log.Printf("kafka publish error: %v", err)
		http.Error(w, "failed to publish event", http.StatusInternalServerError)
		return
	}

	log.Printf("published event id=%d type=%s key=%s detail=%q ts_ms=%d",
		event.ID, event.Type, req.FromUser, event.Detail, event.Timestamp)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(struct {
		EventID int64 `json:"event_id"`
	}{EventID: event.ID})
}