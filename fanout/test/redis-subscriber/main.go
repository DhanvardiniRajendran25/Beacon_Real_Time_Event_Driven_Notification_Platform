// Command redis-subscriber is a local test harness: it listens on every notification
// channel by pattern-subscribing to notif:* (all user-specific channels), and logs
// payloads so we can verify Redis publishes from the worker without a per-user client.
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/redis/go-redis/v9"
)

func main() {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}

	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer func() {
		if err := rdb.Close(); err != nil {
			log.Printf("redis close: %v", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// PSUBSCRIBE notif:* matches all channels (e.g. notif:user-123); production code
	// typically subscribes to one user channel instead.
	pubsub := rdb.PSubscribe(ctx, "notif:*")
	defer func() {
		if err := pubsub.Close(); err != nil {
			log.Printf("pubsub close: %v", err)
		}
	}()

	log.Printf("redis subscriber PSUBSCRIBE notif:* addr=%s", addr)

	ch := pubsub.Channel()
	for {
		select {
		case <-ctx.Done():
			log.Println("shutdown signal received")
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			if msg == nil {
				continue
			}
			log.Printf("redis message channel=%q payload=%s", msg.Channel, msg.Payload)
		}
	}
}
