package reconnect

import (
	"context"
	"testing"
	"time"
)

func TestSchedulerAttempts(t *testing.T) {
	strategy := Strategy{
		BaseDelay:      10 * time.Millisecond,
		MaxDelay:       40 * time.Millisecond,
		Multiplier:     2,
		MaxAttempts:    3,
		JitterFactor:   0,
		ImmediateFirst: true,
	}
	sched := NewScheduler(strategy)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	attempts := sched.Attempts(ctx)
	expected := []time.Duration{0, 10 * time.Millisecond, 20 * time.Millisecond}
	idx := 0
	for attempt := range attempts {
		if idx >= len(expected) {
			t.Fatalf("unexpected attempt index %d", attempt.Index)
		}
		if attempt.Delay != expected[idx] {
			t.Fatalf("attempt %d delay mismatch, want %v got %v", attempt.Index, expected[idx], attempt.Delay)
		}
		idx++
	}
	if idx != len(expected) {
		t.Fatalf("expected %d attempts, got %d", len(expected), idx)
	}
}

func TestSchedulerJitterBounds(t *testing.T) {
	strategy := Strategy{
		BaseDelay:      100 * time.Millisecond,
		MaxDelay:       200 * time.Millisecond,
		Multiplier:     2,
		MaxAttempts:    5,
		JitterFactor:   0.5,
		ImmediateFirst: false,
	}
	sched := NewScheduler(strategy)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	attempts := sched.Attempts(ctx)
	count := 0
	for attempt := range attempts {
		count++
		if attempt.Index == 1 {
			if attempt.Delay != strategy.BaseDelay {
				t.Fatalf("expected first delay %v, got %v", strategy.BaseDelay, attempt.Delay)
			}
		} else {
			if attempt.Delay < strategy.BaseDelay/2 || attempt.Delay > strategy.MaxDelay {
				t.Fatalf("delay out of expected bounds: %v", attempt.Delay)
			}
		}
		if count == strategy.MaxAttempts {
			cancel()
		}
	}
	if count != strategy.MaxAttempts {
		t.Fatalf("expected %d attempts, got %d", strategy.MaxAttempts, count)
	}
}
