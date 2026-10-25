package db

import (
	"context"
	"testing"

	"github.com/Tanyayya/NotificationSystem/fanout/internal/consumer"
)

func TestInsertNotificationsBatch_empty(t *testing.T) {
	d := &DB{pool: nil}
	ev := consumer.NotificationEvent{ID: 1}
	ctx := context.Background()

	if err := d.InsertNotificationsBatch(ctx, nil, ev); err != nil {
		t.Fatal(err)
	}
	if err := d.InsertNotificationsBatch(ctx, []string{}, ev); err != nil {
		t.Fatal(err)
	}
}
