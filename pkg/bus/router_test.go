package bus

import (
	"context"
	"errors"
	"testing"

	"codeberg.org/agnoie/shepherd/protocol"
)

func TestRouterDispatchMetrics(t *testing.T) {
	router := NewRouter()
	header := &protocol.Header{MessageType: 0x42}

	router.Register(header.MessageType, func(ctx context.Context, h *protocol.Header, payload interface{}) error {
		return nil
	})

	if err := router.Dispatch(context.Background(), header, nil); err != nil {
		t.Fatalf("dispatch failed: %v", err)
	}

	stats := router.Stats()
	if stats == nil {
		t.Fatalf("expected stats map")
	}
	counter, ok := stats[header.MessageType]
	if !ok {
		t.Fatalf("expected counter for message type")
	}
	if counter.Dispatched != 1 {
		t.Fatalf("unexpected dispatched count: %d", counter.Dispatched)
	}
	if counter.Errors != 0 {
		t.Fatalf("unexpected error count: %d", counter.Errors)
	}

	failHandler := func(ctx context.Context, h *protocol.Header, payload interface{}) error {
		return errors.New("boom")
	}
	router.Register(header.MessageType, failHandler)
	err := router.Dispatch(context.Background(), header, nil)
	if err == nil {
		t.Fatalf("expected joined error from dispatch")
	}

	stats = router.Stats()
	counter = stats[header.MessageType]
	if counter.Dispatched != 3 { // two handlers invoked across two dispatches
		t.Fatalf("unexpected dispatched count after error handler: %d", counter.Dispatched)
	}
	if counter.Errors == 0 {
		t.Fatalf("expected error count to increase")
	}

	router.RecordDrop(header.MessageType)
	stats = router.Stats()
	counter = stats[header.MessageType]
	if counter.Drops != 1 {
		t.Fatalf("unexpected drop count: %d", counter.Drops)
	}
}
