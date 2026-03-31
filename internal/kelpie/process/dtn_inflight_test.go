package process

import (
	"testing"
	"time"

	"codeberg.org/agnoie/shepherd/internal/kelpie/dtn"
	"codeberg.org/agnoie/shepherd/protocol"
)

func TestDTNInflightTimeoutRequeuesAndLateAckCancelsRetry(t *testing.T) {
	admin := &Admin{
		adminDTNState: adminDTNState{
			dtnManager:              dtn.NewManager(dtn.DefaultConfig()),
			dtnInflight:             make(map[string]*dtnInflightRecord),
			dtnMaxInflightPerTarget: 2,
		},
	}

	target := "n3"
	if _, err := admin.dtnManager.Enqueue(target, []byte("memo:hello"), dtn.WithTTL(10*time.Minute)); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	ready := admin.dtnManager.Ready(time.Now(), 1)
	if len(ready) != 1 {
		t.Fatalf("expected 1 ready bundle, got %d", len(ready))
	}
	bundle := ready[0]

	timeout := admin.dtnAckTimeout(target)
	admin.rememberInflightAt(bundle, time.Now().Add(-timeout-time.Second))
	admin.requeueExpiredInflight(time.Now())

	if got := admin.inflightForTarget(target); got != 0 {
		t.Fatalf("expected inflight cleared after timeout, got %d", got)
	}
	if admin.dtnRetried != 1 {
		t.Fatalf("expected dtnRetried=1 after timeout requeue, got %d", admin.dtnRetried)
	}

	found := false
	for _, s := range admin.dtnManager.List(target, 10) {
		if s.ID == bundle.ID {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected timed-out bundle requeued")
	}

	ack := &protocol.DTNAck{BundleIDLen: uint16(len(bundle.ID)), BundleID: bundle.ID, OK: 1}
	admin.onDTNAck(ack)
	if admin.dtnDelivered != 1 {
		t.Fatalf("expected dtnDelivered=1 after late ACK, got %d", admin.dtnDelivered)
	}
	for _, s := range admin.dtnManager.List(target, 10) {
		if s.ID == bundle.ID {
			t.Fatalf("expected late ACK to cancel queued retry")
		}
	}

	// 重复 ACK 不应导致投递计数被重复累加。
	admin.onDTNAck(ack)
	if admin.dtnDelivered != 1 {
		t.Fatalf("expected duplicate ACK ignored, got dtnDelivered=%d", admin.dtnDelivered)
	}
}
