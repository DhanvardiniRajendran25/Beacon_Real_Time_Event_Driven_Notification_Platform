package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"

	"github.com/Tanyayya/NotificationSystem/gateway/internal/history"
)

// historySvc is nil when PostgreSQL is unavailable — history is disabled but
// real-time WebSocket delivery continues to work normally.
var historySvc *history.Service

func main() {
	// Initialize Redis connection before accepting any WebSocket connections.
	// This will fatal if Redis is unreachable — better to fail fast on startup
	// than to discover Redis is down mid-connection.
	initRedis()

	// Initialize PostgreSQL for notification history.
	// A failure here is non-fatal: the gateway degrades gracefully by skipping
	// history on connect rather than refusing all connections.
	db, err := history.OpenDB()
	if err != nil {
		log.Printf("warning: PostgreSQL unavailable, notification history disabled: %v", err)
	} else {
		historySvc = history.NewService(db)
		log.Println("connected to PostgreSQL for notification history")
	}

	// /ws is the WebSocket endpoint — clients connect here to receive real-time notifications
	// HandleWS is defined in handler.go
	http.HandleFunc("/ws", HandleWS)

	// /health is a simple health check endpoint
	// ECS will ping this to know the task is alive — must return 200
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	http.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"goroutine_count":    runtime.NumGoroutine(),
			"heap_inuse_mb":      float64(ms.HeapInuse) / (1024 * 1024),
			"active_connections": atomic.LoadInt64(&activeConnections),
		})
	})

	go func() {
		cfg, err := awsconfig.LoadDefaultConfig(context.Background(), awsconfig.WithRegion("us-east-1"))
		if err != nil {
			log.Printf("cloudwatch: failed to load config: %v", err)
			return
		}
		cw := cloudwatch.NewFromConfig(cfg)
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			var ms runtime.MemStats
			runtime.ReadMemStats(&ms)
			_, err := cw.PutMetricData(context.Background(), &cloudwatch.PutMetricDataInput{
				Namespace: aws.String("NotifSystem/Gateway"),
				MetricData: []types.MetricDatum{
					{MetricName: aws.String("GoroutineCount"), Value: aws.Float64(float64(runtime.NumGoroutine())), Unit: types.StandardUnitCount},
					{MetricName: aws.String("HeapInuseMB"), Value: aws.Float64(float64(ms.HeapInuse) / (1024 * 1024)), Unit: types.StandardUnitNone},
					{MetricName: aws.String("ActiveConnections"), Value: aws.Float64(float64(atomic.LoadInt64(&activeConnections))), Unit: types.StandardUnitCount},
				},
			})
			if err != nil {
				log.Printf("cloudwatch: PutMetricData: %v", err)
			}
		}
	}()

	log.Println("Gateway listening on :8080")

	// ListenAndServe blocks forever — if it returns, something went wrong
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
