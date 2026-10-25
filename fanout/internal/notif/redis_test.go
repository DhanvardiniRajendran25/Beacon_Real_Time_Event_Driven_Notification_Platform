package notif

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/Tanyayya/NotificationSystem/fanout/internal/consumer"
)

func TestPublishPipeline_empty(t *testing.T) {
	mr := miniredis.RunT(t)
	pub := NewPublisher(mr.Addr(), "POST", "alice", "")
	defer pub.Close()

	n, err := pub.PublishPipeline(context.Background(), nil, consumer.NotificationEvent{ID: 1})
	if err != nil || n != 0 {
		t.Fatalf("PublishPipeline empty: n=%d err=%v", n, err)
	}
}

func TestPublishPipeline_twoRecipients(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	sub := rdb.PSubscribe(context.Background(), "notif:*")
	defer sub.Close()
	msgCh := sub.Channel()

	pub := NewPublisher(mr.Addr(), "POST", "alice", "")
	defer pub.Close()
	ev := consumer.NotificationEvent{
		ID:        42,
		Type:      "LIKE",
		Detail:    "x",
		Timestamp: 99,
		FromUser:  "bob",
	}

	fail, err := pub.PublishPipeline(context.Background(), []string{"a", "b"}, ev)
	if err != nil {
		t.Fatal(err)
	}
	if fail != 0 {
		t.Fatalf("failCount=%d", fail)
	}

	deadline := time.After(2 * time.Second)
	var n int
	for n < 2 {
		select {
		case <-deadline:
			t.Fatalf("timed out after %d messages", n)
		case m := <-msgCh:
			if m == nil {
				continue
			}
			n++
		}
	}
}
