package process

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"codeberg.org/agnoie/shepherd/pkg/config/defaults"
)

func TestAdminReconcileListenerStartsPending(t *testing.T) {
	admin := &Admin{}
	admin.listeners = newListenerRegistry(nil, nil)
	record := ListenerRecord{
		ID:         "lis-test",
		TargetUUID: "node-1",
		Bind:       "0.0.0.0:8080",
		Mode:       ListenerModeNormal,
		Status:     ListenerStatusPending,
	}
	if err := admin.listeners.save(record); err != nil {
		t.Fatalf("save listener: %v", err)
	}
	var starts atomic.Int32
	admin.listenerStartOverride = func(targetUUID, bind string, mode int, listenerID string) (string, error) {
		starts.Add(1)
		return "route-1", nil
	}
	admin.reconcileListenerRecord(record, false)
	if starts.Load() != 1 {
		t.Fatalf("expected 1 start attempt, got %d", starts.Load())
	}
	updated, ok := admin.listeners.get(record.ID)
	if !ok {
		t.Fatalf("listener missing after reconcile")
	}
	if updated.Status != ListenerStatusRunning {
		t.Fatalf("expected running status, got %s", updated.Status)
	}
	if updated.Route != "route-1" {
		t.Fatalf("expected route-1, got %s", updated.Route)
	}
}

func TestAdminReconcileListenerRespectsRetryInterval(t *testing.T) {
	admin := &Admin{}
	admin.listeners = newListenerRegistry(nil, nil)
	record := ListenerRecord{
		ID:         "lis-retry",
		TargetUUID: "node-2",
		Bind:       "127.0.0.1:9090",
		Mode:       ListenerModeNormal,
		Status:     ListenerStatusFailed,
		UpdatedAt:  time.Now(),
	}
	if err := admin.listeners.save(record); err != nil {
		t.Fatalf("save listener: %v", err)
	}
	var starts atomic.Int32
	admin.listenerStartOverride = func(targetUUID, bind string, mode int, listenerID string) (string, error) {
		starts.Add(1)
		return "route-retry", nil
	}
	admin.reconcileListenerRecord(record, false)
	if starts.Load() != 0 {
		t.Fatalf("unexpected start attempt when retry interval not elapsed")
	}
	record.Status = ListenerStatusFailed
	record.UpdatedAt = time.Now().Add(-defaults.ListenerRetryInterval - time.Second)
	if err := admin.listeners.save(record); err != nil {
		t.Fatalf("save listener (retry): %v", err)
	}
	admin.reconcileListenerRecord(record, false)
	if starts.Load() != 1 {
		t.Fatalf("expected retry start, got %d", starts.Load())
	}
}

func TestAdminReconcileListenerBootstrapRunning(t *testing.T) {
	admin := &Admin{}
	admin.listeners = newListenerRegistry(nil, nil)
	record := ListenerRecord{
		ID:         "lis-boot",
		TargetUUID: "node-3",
		Bind:       "0.0.0.0:7070",
		Mode:       ListenerModeNormal,
		Status:     ListenerStatusRunning,
	}
	if err := admin.listeners.save(record); err != nil {
		t.Fatalf("save listener: %v", err)
	}
	var starts atomic.Int32
	admin.listenerStartOverride = func(targetUUID, bind string, mode int, listenerID string) (string, error) {
		starts.Add(1)
		return "boot-route", nil
	}
	admin.reconcileListenerRecord(record, false)
	if starts.Load() != 0 {
		t.Fatalf("running listener should not restart without bootstrap flag")
	}
	admin.reconcileListenerRecord(record, true)
	if starts.Load() != 1 {
		t.Fatalf("expected bootstrap restart, got %d", starts.Load())
	}
}

func TestUpdateListenerResumeAndPause(t *testing.T) {
	admin := &Admin{}
	admin.listeners = newListenerRegistry(nil, nil)
	record := ListenerRecord{
		ID:         "lis-update",
		TargetUUID: "node-4",
		Bind:       "127.0.0.1:6060",
		Mode:       ListenerModeNormal,
		Status:     ListenerStatusStopped,
	}
	if err := admin.listeners.save(record); err != nil {
		t.Fatalf("save listener: %v", err)
	}
	var starts atomic.Int32
	var stops atomic.Int32
	admin.listenerStartOverride = func(targetUUID, bind string, mode int, listenerID string) (string, error) {
		starts.Add(1)
		return "resume-route", nil
	}
	admin.listenerStopOverride = func(targetUUID, listenerID string) error {
		stops.Add(1)
		return nil
	}
	updated, err := admin.UpdateListener(context.Background(), record.ID, nil, "resume")
	if err != nil {
		t.Fatalf("resume listener: %v", err)
	}
	if starts.Load() != 1 {
		t.Fatalf("expected start override once, got %d", starts.Load())
	}
	if updated.Status != ListenerStatusRunning {
		t.Fatalf("expected running status, got %s", updated.Status)
	}
	updated, err = admin.UpdateListener(context.Background(), record.ID, nil, "pause")
	if err != nil {
		t.Fatalf("pause listener: %v", err)
	}
	if stops.Load() != 1 {
		t.Fatalf("expected stop override once, got %d", stops.Load())
	}
	if updated.Status != ListenerStatusStopped {
		t.Fatalf("expected stopped status, got %s", updated.Status)
	}
}
