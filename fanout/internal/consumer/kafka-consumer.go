package consumer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"hash/fnv"
	"io"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

// NotificationEvent combines the Kafka message key (FromUser) with the JSON value body.
type NotificationEvent struct {
	ID        int64
	Type      string
	Detail    string
	Timestamp int64
	FromUser  string
}

// parseSnowflakeID decodes JSON id as a number, a base-10 int string, or the
// ingestion format "<unix_ms>-<seq>" from NewSnowflakeID in ingestion/snowflake.go.
func parseSnowflakeID(raw json.RawMessage) (int64, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return 0, errors.New("missing id")
	}
	if raw[0] == '"' {
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return 0, err
		}
		s = strings.TrimSpace(s)
		if msStr, seqStr, ok := strings.Cut(s, "-"); ok && msStr != "" && seqStr != "" {
			if _, errMS := strconv.ParseUint(msStr, 10, 64); errMS == nil {
				if _, errSeq := strconv.ParseUint(seqStr, 10, 64); errSeq == nil {
					return hyphenSnowflakeToInt64(s), nil
				}
			}
		}
		return strconv.ParseInt(s, 10, 64)
	}
	var n int64
	if err := json.Unmarshal(raw, &n); err != nil {
		return 0, err
	}
	return n, nil
}

func hyphenSnowflakeToInt64(s string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return int64(h.Sum64() & 0x7FFFFFFFFFFFFFFF)
}

// Notifier publishes to Redis after a Kafka message is processed.
type Notifier interface {
	Publish(ctx context.Context, userID string, ev NotificationEvent) error
}

// Run reads messages from the given topic until the context is cancelled.
//
// Commit behaviour:
//   - If Publish returns nil (all DB inserts succeeded), CommitMessages is called
//     and Kafka marks the offset as processed.
//   - If Publish returns an error (one or more DB inserts failed), CommitMessages
//     is skipped. Kafka will redeliver the message on restart so the worker can
//     retry. ON CONFLICT DO NOTHING in the DB layer ensures already-inserted rows
//     are safely skipped on retry.
func Run(ctx context.Context, brokers []string, topic, groupID string, defaultUserID string, n Notifier) error {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		GroupID:  groupID,
		MinBytes: 1,
		MaxBytes: 10e6,
		MaxWait:  2 * time.Second,
	})
	defer func() {
		if err := r.Close(); err != nil {
			log.Printf("kafka reader close: %v", err)
		}
	}()

	log.Printf("kafka consumer started topic=%q group=%q brokers=%v", topic, groupID, brokers)

	for {
		m, err := r.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}

		var raw struct {
			ID        json.RawMessage `json:"id"`
			Type      string          `json:"type"`
			Detail    string          `json:"detail"`
			Timestamp int64           `json:"timestamp"`
		}

		shouldCommit := true // default to committing unless processing fails

		if err := json.Unmarshal(m.Value, &raw); err != nil {
			log.Printf("kafka message json error partition=%d offset=%d: %v", m.Partition, m.Offset, err)
			// malformed JSON — commit anyway, retrying won't fix a bad message
		} else {
			id, err := parseSnowflakeID(raw.ID)
			if err != nil {
				log.Printf("kafka message id partition=%d offset=%d: %v", m.Partition, m.Offset, err)
				// bad ID — commit anyway, retrying won't fix it
			} else {
				ev := NotificationEvent{
					ID:        id,
					Type:      raw.Type,
					Detail:    raw.Detail,
					Timestamp: raw.Timestamp,
					FromUser:  string(m.Key),
				}
				log.Printf(
					"kafka message partition=%d offset=%d key=%q id=%d type=%q detail=%q ts_ms=%d",
					m.Partition, m.Offset, string(m.Key), ev.ID, ev.Type, ev.Detail, ev.Timestamp,
				)

				userID := string(m.Key)
				if userID == "" {
					userID = defaultUserID
				}

				if err := n.Publish(ctx, userID, ev); err != nil {
					// Publish returned an error — one or more DB inserts failed
					// Skip commit so Kafka redelivers the message for retry
					log.Printf("publish failed, skipping kafka commit for retry userID=%q: %v", userID, err)
					shouldCommit = false
				}
			}
		}

		// only commit if processing succeeded
		if shouldCommit {
			if err := r.CommitMessages(ctx, m); err != nil {
				log.Printf("kafka commit: %v", err)
			}
		}
	}
}