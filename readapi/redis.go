package main

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const notifTTL = 7 * 24 * time.Hour

// Store is the Redis-backed storage layer for the read API.
type Store struct {
	rdb *redis.Client
}

// NewStore connects to Redis at the given address.
func NewStore(addr string) *Store {
	return &Store{
		rdb: redis.NewClient(&redis.Options{Addr: addr}),
	}
}

// Close releases the Redis client.
func (s *Store) Close() error {
	return s.rdb.Close()
}

// GetNotifications returns up to limit notifications for a user in reverse-chronological
// order, skipping the first offset entries (for pagination).
// Uses ZREVRANGE on the sorted set notifications:{userID} where score = timestamp ms.
func (s *Store) GetNotifications(ctx context.Context, userID string, offset, limit int64) ([]string, error) {
	result, err := s.rdb.ZRevRange(ctx, "notifications:"+userID, offset, offset+limit-1).Result()
	if err != nil {
		return nil, fmt.Errorf("zrevrange notifications:%s: %w", userID, err)
	}
	return result, nil
}

// GetUnreadCount returns the number of unread notifications for a user.
// Returns 0 when the key does not exist (user has no unread notifications).
func (s *Store) GetUnreadCount(ctx context.Context, userID string) (int64, error) {
	count, err := s.rdb.Get(ctx, "unread:"+userID).Int64()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("get unread:%s: %w", userID, err)
	}
	return count, nil
}

// HasHistory reports whether the sorted set for userID exists in Redis.
// A missing key means the cache either expired or was never populated, and the
// caller should fall back to PostgreSQL.
func (s *Store) HasHistory(ctx context.Context, userID string) (bool, error) {
	n, err := s.rdb.Exists(ctx, "notifications:"+userID).Result()
	if err != nil {
		return false, fmt.Errorf("exists notifications:%s: %w", userID, err)
	}
	return n > 0, nil
}

// Repopulate bulk-writes notifications fetched from PostgreSQL into the Redis
// sorted set and sets a fresh 7-day TTL. Called on cache miss.
func (s *Store) Repopulate(ctx context.Context, userID string, notifs []DBNotification) error {
	members := make([]redis.Z, len(notifs))
	for i, n := range notifs {
		members[i] = redis.Z{Score: float64(n.TsMs), Member: n.Payload}
	}
	key := "notifications:" + userID
	if err := s.rdb.ZAdd(ctx, key, members...).Err(); err != nil {
		return fmt.Errorf("zadd repopulate notifications:%s: %w", userID, err)
	}
	if err := s.rdb.Expire(ctx, key, notifTTL).Err(); err != nil {
		return fmt.Errorf("expire notifications:%s: %w", userID, err)
	}
	return nil
}
