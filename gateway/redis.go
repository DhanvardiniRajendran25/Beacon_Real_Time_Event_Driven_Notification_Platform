package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	// registrationTTL is how long a user's registration key lives in Redis.
	// This is a safety net — if the gateway crashes without cleanup,
	// the key expires automatically after 120 seconds instead of lingering forever.
	registrationTTL = 120 * time.Second

	// registrationPrefix is the Redis key prefix for user → gateway mappings.
	// Full key looks like: ws:user:123
	registrationPrefix = "ws:user:"
)

// rdb is the global Redis client — initialized once in initRedis()
// and reused across all connections.
var rdb *redis.Client

// initRedis creates the Redis client and verifies the connection on startup.
// Called once from main.go before the server starts accepting connections.
// Reads REDIS_ADDR from the environment — falls back to localhost for local dev.
func initRedis() {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		// No env var set — assume local development
		addr = "localhost:6379"
	}

	rdb = redis.NewClient(&redis.Options{Addr: addr})

	// Ping Redis to verify the connection is alive before we start serving traffic
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("cannot connect to Redis at %s: %v", addr, err)
	}
	log.Printf("connected to Redis at %s", addr)
}

// registerUser writes ws:user:{userID} → taskID into Redis with a TTL.
// Called immediately after a WebSocket connection is established.
// taskID identifies which gateway instance owns this user's connection —
// fan-out workers use this to know where to route notifications.
func registerUser(ctx context.Context, userID, taskID string) error {
	key := registrationPrefix + userID
	if err := rdb.Set(ctx, key, taskID, registrationTTL).Err(); err != nil {
		return fmt.Errorf("registerUser %s: %w", userID, err)
	}
	log.Printf("registered user %s → task %s", userID, taskID)
	return nil
}

// deregisterUser deletes ws:user:{userID} from Redis on disconnect.
// Always called as a pair with registerUser — register on connect, deregister on disconnect.
func deregisterUser(ctx context.Context, userID string) error {
	key := registrationPrefix + userID
	if err := rdb.Del(ctx, key).Err(); err != nil {
		return fmt.Errorf("deregisterUser %s: %w", userID, err)
	}
	log.Printf("deregistered user %s", userID)
	return nil
}