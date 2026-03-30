package utils

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestMailboxConcurrentEnqueue(t *testing.T) {
	out := make(chan interface{}, 128)
	mb := NewMailbox("test-mailbox", out, 32)
	defer mb.Close()

	const producers = 16
	const perProducer = 32

	total := producers * perProducer
	var delivered int32
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case msg := <-out:
				if msg == nil {
					continue
				}
				atomic.AddInt32(&delivered, 1)
			case <-stop:
				return
			}
		}
	}()

	var wg sync.WaitGroup
	wg.Add(producers)
	for i := 0; i < producers; i++ {
		go func(id int) {
			defer wg.Done()
			ctx := context.Background()
			for j := 0; j < perProducer; j++ {
				payload := fmt.Sprintf("p%d-%d", id, j)
				if _, err := mb.Enqueue(ctx, payload); err != nil {
					t.Errorf("enqueue failed: %v", err)
					return
				}
			}
		}(i)
	}

	wg.Wait()

	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	for {
		expected := total - int(mb.Dropped())
		if expected < 0 {
			expected = 0
		}
		if int(atomic.LoadInt32(&delivered)) >= expected {
			break
		}
		select {
		case <-timer.C:
			t.Fatalf("timeout waiting for mailbox deliveries")
		case <-time.After(10 * time.Millisecond):
		}
	}

	close(stop)
}

func TestMailboxDropOldest(t *testing.T) {
	out := make(chan interface{})
	mb := NewMailbox("drop-mailbox", out, 2)
	ctx := context.Background()

	const total = 6
	drops := 0
	for i := 0; i < total; i++ {
		dropped, err := mb.Enqueue(ctx, i)
		if err != nil {
			t.Fatalf("enqueue failed: %v", err)
		}
		if dropped {
			drops++
		}
	}
	if drops == 0 {
		t.Fatalf("expected messages to be dropped when mailbox is saturated")
	}

	deliveries := total - drops
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < deliveries; i++ {
			select {
			case <-out:
			case <-time.After(time.Second):
				return
			}
		}
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout draining mailbox outputs")
	}

	mb.Close()
	if got := mb.Dropped(); got == 0 {
		t.Fatalf("dropped counter not updated")
	}
}
