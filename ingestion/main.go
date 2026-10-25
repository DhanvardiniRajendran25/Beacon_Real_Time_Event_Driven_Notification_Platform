package main

import (
	"log"
	"net/http"
	"os"
	"strings"
)

func parseKafkaBrokers() []string {
	raw := strings.TrimSpace(os.Getenv("KAFKA_BROKERS"))
	if raw == "" {
		raw = strings.TrimSpace(os.Getenv("KAFKA_BROKER"))
	}
	if raw == "" {
		raw = "localhost:9092"
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return []string{"localhost:9092"}
	}
	return out
}

func main() {
	brokers := parseKafkaBrokers()
	topic := strings.TrimSpace(os.Getenv("KAFKA_TOPIC"))
	if topic == "" {
		topic = "worker-events"
	}

	producer, err := NewProducer(brokers, topic)
	if err != nil {
		log.Fatalf("failed to create kafka producer: %v", err)
	}
	defer producer.Close()

	h := &Handler{producer: producer}

	http.HandleFunc("/event", h.HandleEvent)
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))

	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "3000"
	}
	log.Printf("ingestion API listening on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}