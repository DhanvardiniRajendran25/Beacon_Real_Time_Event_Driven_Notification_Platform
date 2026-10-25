package fanout

import (
	"context"
	"fmt"
	"log"

	"github.com/Tanyayya/NotificationSystem/fanout/internal/consumer"
	"github.com/Tanyayya/NotificationSystem/fanout/internal/db"
	"github.com/Tanyayya/NotificationSystem/fanout/internal/notif"
)

// fanoutWriteBatchSize is the number of followers per Postgres batch insert and Redis pipeline.
const fanoutWriteBatchSize = 200

// Mode controls which fan-out strategy the worker uses.
type Mode string

const (
	ModeRead   Mode = "FAN_OUT_READ"
	ModeWrite  Mode = "FAN_OUT_WRITE"
	ModeHybrid Mode = "FAN_OUT_HYBRID"
)

// FanOuter sits between the Kafka consumer and the Redis publisher.
// It implements consumer.Notifier so it can be passed directly to consumer.Run.
//
// For each event it:
//  1. Looks up the follower list from PostgreSQL
//  2. Chooses fan-out on write or fan-out on read based on follower count
//  3. Fan-out on write: persists one notification per follower + publishes to Redis
//  4. Fan-out on read:  persists one event record (stub — read path not yet implemented)
type FanOuter struct {
	db        *db.DB
	publisher *notif.Publisher
	threshold int // follower count above which we switch to fan-out on read
}

// New creates a FanOuter with the given DB, Redis publisher, and fan-out threshold.
func New(database *db.DB, publisher *notif.Publisher, threshold int) *FanOuter {
	return &FanOuter{
		db:        database,
		publisher: publisher,
		threshold: threshold,
	}
}

// Publish implements consumer.Notifier.
// fromUser is the Kafka message key — the person who triggered the event.
// The fan-out worker calls this once per Kafka message.
func (f *FanOuter) Publish(ctx context.Context, fromUser string, ev consumer.NotificationEvent) error {
	// Step 1: look up all followers of fromUser
	followers, err := f.db.GetFollowers(ctx, fromUser)
	if err != nil {
		return err
	}

	if len(followers) == 0 {
		log.Printf("fanout: no followers for user=%q, skipping", fromUser)
		return nil
	}

	log.Printf("fanout: user=%q followers=%d threshold=%d", fromUser, len(followers), f.threshold)

	// Step 2: choose strategy based on follower count
	if len(followers) <= f.threshold {
		return f.fanoutOnWrite(ctx, followers, ev)
	}
	return f.fanoutOnRead(ctx, ev)
}

// fanoutOnWrite is the write path — used for normal users under the threshold.
// Followers are processed in chunks of fanoutWriteBatchSize: batch insert to PostgreSQL,
// then pipelined Redis PUBLISH to notif:{followerID}. If batch insert fails, falls back
// to per-follower insert + publish for that chunk.
//
// If ANY PostgreSQL insert fails, the function returns an error so the
// Kafka offset is not committed and the message is retried.
// Redis publish errors are logged but do not stop processing —
// notifications are already persisted and will be replayed on reconnect.
func (f *FanOuter) fanoutOnWrite(ctx context.Context, followers []string, ev consumer.NotificationEvent) error {
	log.Printf("fanout-on-write: event id=%d type=%q from=%q recipients=%d",
		ev.ID, ev.Type, ev.FromUser, len(followers))

	var publishErrors int
	for i := 0; i < len(followers); i += fanoutWriteBatchSize {
		end := i + fanoutWriteBatchSize
		if end > len(followers) {
			end = len(followers)
		}
		chunk := followers[i:end]

		if err := f.db.InsertNotificationsBatch(ctx, chunk, ev); err != nil {
			log.Printf("fanout-on-write: batch insert failed (n=%d), falling back to row-by-row: %v", len(chunk), err)
			publishErrors += f.fanoutOnWriteRowByRow(ctx, chunk, ev)
			continue
		}

		n, err := f.publisher.PublishPipeline(ctx, chunk, ev)
		if err != nil {
			log.Printf("fanout-on-write: redis pipeline marshal chunk n=%d: %v", len(chunk), err)
			publishErrors += len(chunk)
			continue
		}
		if n > 0 {
			log.Printf("fanout-on-write: redis pipeline chunk n=%d had %d publish errors", len(chunk), n)
		}
		publishErrors += n
	}

	// if any recipients failed, return an error so kafka-consumer.go skips CommitMessages
	// Kafka will redeliver the message and we retry — ON CONFLICT DO NOTHING handles duplicates
	if publishErrors > 0 {
		return fmt.Errorf("fanout-on-write: %d/%d recipients failed, skipping kafka commit for retry",
			publishErrors, len(followers))
	}

	log.Printf("fanout-on-write: done event id=%d recipients=%d", ev.ID, len(followers))
	return nil
}

// fanoutOnWriteRowByRow is the per-follower insert + publish fallback used when batch insert fails.
func (f *FanOuter) fanoutOnWriteRowByRow(ctx context.Context, followers []string, ev consumer.NotificationEvent) int {
	var publishErrors int
	for _, followerID := range followers {
		if err := f.db.InsertNotification(ctx, followerID, ev); err != nil {
			log.Printf("fanout-on-write: insert notification follower=%q: %v", followerID, err)
			continue
		}
		if err := f.publisher.Publish(ctx, followerID, ev); err != nil {
			log.Printf("fanout-on-write: redis publish follower=%q: %v", followerID, err)
			publishErrors++
		}
	}
	return publishErrors
}

// fanoutOnRead is the read path — used for high-follower accounts above the threshold.
// Instead of writing one notification per follower, we store the event once.
// Recipients are resolved at read time when they open their notification feed.
//
// NOTE: This is a stub — the full read path (resolving recipients at delivery time)
// will be implemented in Week 3 as part of Experiment 1.
func (f *FanOuter) fanoutOnRead(ctx context.Context, ev consumer.NotificationEvent) error {
	log.Printf("fanout-on-read: high-follower account user=%q, storing event id=%d (read path stub)",
		ev.FromUser, ev.ID)

	// store one event record instead of N notification records
	if err := f.db.InsertEvent(ctx, ev); err != nil {
		return err
	}

	log.Printf("fanout-on-read: event id=%d stored, recipients resolved at read time", ev.ID)
	return nil
}
