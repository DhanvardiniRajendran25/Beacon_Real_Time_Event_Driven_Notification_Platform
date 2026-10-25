package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	cfg := parseConfig()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle SIGINT / SIGTERM for graceful shutdown.
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	pool := NewSubscriberPool(cfg.ALBURL, cfg.FollowerPrefix, cfg.FollowerStart, cfg.FollowerCount, nil)

	metrics, err := NewMetrics(cfg.CSVOut, pool)
	if err != nil {
		log.Fatalf("metrics init: %v", err)
	}
	defer metrics.Close()

	tracker := NewTracker(pool, metrics)
	pool.tracker = tracker

	go metrics.ServeHTTP(cfg.MetricsPort)

	// Drain tracker results and record them.
	go func() {
		for result := range tracker.Results {
			metrics.RecordResult(result)
		}
	}()

	// Process delivery reports sequentially.
	go tracker.Run(ctx)

	// Open all WebSocket connections before publishing events.
	ready := make(chan struct{})
	pool.Start(ctx, ready)

	log.Printf("waiting for %d subscriber connections...", cfg.FollowerCount)
	select {
	case <-ready:
		log.Printf("all %d subscribers connected, starting publisher", cfg.FollowerCount)
	case <-time.After(2 * time.Minute):
		log.Printf("timed out waiting for all subscribers (%d/%d active), proceeding anyway",
			pool.ActiveConns(), cfg.FollowerCount)
	case <-sigs:
		log.Println("interrupted before test started")
		return
	}

	testCtx, testCancel := context.WithTimeout(ctx, cfg.Duration)
	defer testCancel()

	publisher := NewPublisher(cfg.IngestionURL, cfg.SenderUser, cfg.EventRate, tracker, metrics)
	go publisher.Run(testCtx)

	// Block until test duration elapses or the user interrupts.
	select {
	case <-testCtx.Done():
		log.Println("test duration elapsed, waiting for in-flight events...")
	case <-sigs:
		log.Println("interrupted, waiting for in-flight events...")
		cancel()
	}

	// Wait for in-flight events to drain. Each event self-resolves within the
	// 2-second per-subscriber timeout, so this should complete quickly.
	// cfg.EventTimeout is a safety-net outer deadline.
	deadline := time.After(cfg.EventTimeout)
	poll := time.NewTicker(500 * time.Millisecond)
	defer poll.Stop()
wait:
	for {
		select {
		case <-poll.C:
			if n := metrics.InFlight(); n == 0 {
				log.Println("all in-flight events resolved")
				break wait
			} else {
				log.Printf("waiting for %d in-flight events...", n)
			}
		case <-deadline:
			log.Printf("event timeout elapsed, %d events still in-flight", metrics.InFlight())
			break wait
		}
	}

	metrics.PrintSummary()
	log.Printf("results written to %s", cfg.CSVOut)
}
