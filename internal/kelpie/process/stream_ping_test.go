package process

import (
	"errors"
	"testing"
	"time"

	"codeberg.org/agnoie/shepherd/internal/kelpie/dtn"
)

func TestStreamPingEnqueuesFrames(t *testing.T) {
	admin := &Admin{}
	calls := 0
	admin.enqueueDiagOverride = func(target, data string, priority dtn.Priority, ttl time.Duration) (string, error) {
		calls++
		if target != "node-1" {
			t.Fatalf("unexpected target %s", target)
		}
		return "", nil
	}
	if err := admin.StreamPing("node-1", 3, 2); err != nil {
		t.Fatalf("StreamPing failed: %v", err)
	}
	if calls != 5 { // open + 3 data + close
		t.Fatalf("expected 5 enqueues, got %d", calls)
	}
}

func TestStreamPingValidation(t *testing.T) {
	admin := &Admin{}
	admin.enqueueDiagOverride = func(target, data string, priority dtn.Priority, ttl time.Duration) (string, error) {
		return "", nil
	}
	if err := admin.StreamPing("", 1, 0); !errors.Is(err, ErrStreamPingMissingTarget) {
		t.Fatalf("expected missing target error, got %v", err)
	}
	if err := admin.StreamPing("node", 0, 0); !errors.Is(err, ErrStreamPingInvalidCount) {
		t.Fatalf("expected invalid count error, got %v", err)
	}
	if err := admin.StreamPing("node", 1, -1); !errors.Is(err, ErrStreamPingInvalidSize) {
		t.Fatalf("expected invalid size error, got %v", err)
	}
}
