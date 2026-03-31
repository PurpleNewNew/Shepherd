package planner

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"codeberg.org/agnoie/shepherd/internal/kelpie/supp"
)

func TestBackoffDuration(t *testing.T) {
	tests := []struct {
		attempt int
		expect  time.Duration
	}{
		{0, plannerRetryBase},
		{1, plannerRetryBase},
		{2, plannerRetryBase * 2},
		{3, plannerRetryBase * 4},
	}
	for _, tc := range tests {
		if got := backoffDuration(plannerRetryBase, plannerRetryMax, tc.attempt); got != tc.expect {
			t.Fatalf("backoffDuration(%d) = %s, want %s", tc.attempt, got, tc.expect)
		}
	}
	if got := backoffDuration(plannerRetryBase, plannerRetryMax, 10); got != plannerRetryMax {
		t.Fatalf("backoffDuration capped = %s, want %s", got, plannerRetryMax)
	}
}

func TestApplyRemovalRespectsPeerQuota(t *testing.T) {
	planner := &SupplementalPlanner{}
	links := []nodeLink{
		{linkUUID: "link-1", peer: "peer-b", overlap: 3},
		{linkUUID: "link-2", peer: "peer-c", overlap: 1},
	}
	remaining := map[string]int{
		"node-a": 2,
		"peer-b": 1,
		"peer-c": 2,
	}
	removeSet := make(map[string]string)

	left := planner.applyRemoval("node-a", links, removeSet, remaining, 1, true, 2)
	if left != 1 {
		t.Fatalf("expected 1 removal blocked by peer quota, got %d", left)
	}
	if len(removeSet) != 1 {
		t.Fatalf("expected exactly one link scheduled for removal, got %d", len(removeSet))
	}
	if _, ok := removeSet["link-2"]; !ok {
		t.Fatalf("expected link-2 to be recycled, removal set: %+v", removeSet)
	}
	if remaining["node-a"] != 1 {
		t.Fatalf("node-a remaining should be 1, got %d", remaining["node-a"])
	}
	if remaining["peer-c"] != 1 {
		t.Fatalf("peer-c remaining should be 1, got %d", remaining["peer-c"])
	}
	if remaining["peer-b"] != 1 {
		t.Fatalf("peer-b remaining should stay at 1, got %d", remaining["peer-b"])
	}
}

func TestMetricsSnapshotDefaults(t *testing.T) {
	planner := NewSupplementalPlanner(nil, nil, nil, nil)
	snapshot := planner.MetricsSnapshot()
	if snapshot.Dispatched != 0 || snapshot.Success != 0 || snapshot.Failures != 0 {
		t.Fatalf("expected zeroed counters, got %+v", snapshot)
	}
	if snapshot.QueueDepth != 0 || snapshot.QueueHigh != 0 {
		t.Fatalf("expected zero queue depth, got %+v", snapshot)
	}
}

func TestRepairMetricsSnapshot(t *testing.T) {
	planner := NewSupplementalPlanner(nil, nil, nil, nil)
	planner.metricsRecordRepairFailure("node-a", errors.New("dial failed"))
	planner.metricsRecordRepairSuccess("node-a")

	snapshot := planner.MetricsSnapshot()
	if snapshot.RepairAttempts != 2 || snapshot.RepairSuccess != 1 || snapshot.RepairFailures != 1 {
		t.Fatalf("unexpected repair metrics: %+v", snapshot)
	}
	if snapshot.LastFailure == "" {
		t.Fatalf("expected lastFailure to record the last error message")
	}
	events := planner.EventLog(5)
	if len(events) == 0 {
		t.Fatalf("expected at least one event entry after recording repair metrics")
	}
}

func TestStatusSnapshot(t *testing.T) {
	planner := NewSupplementalPlanner(nil, nil, nil, nil)
	status := planner.StatusSnapshot()
	if !status.Enabled {
		t.Fatalf("expected planner enabled by default")
	}
	planner.SetEnabled(false)
	status = planner.StatusSnapshot()
	if status.Enabled {
		t.Fatalf("expected disabled state to be reflected")
	}
	if got, want := status.ActiveLinks, supp.ActiveSupplementalLinkCount(); got != want {
		t.Fatalf("expected active link count %d, got %d", want, got)
	}
}

func TestEventLogLimit(t *testing.T) {
	planner := NewSupplementalPlanner(nil, nil, nil, nil)
	for i := 0; i < 5; i++ {
		planner.recordPlannerEvent("kind", "action", fmt.Sprintf("src-%d", i), fmt.Sprintf("dst-%d", i), "detail")
	}
	events := planner.EventLog(3)
	if len(events) != 3 {
		t.Fatalf("expected 3 events, got %d", len(events))
	}
	if events[0].Seq <= events[1].Seq {
		t.Fatalf("expected newest event first, got seq=%d then %d", events[0].Seq, events[1].Seq)
	}
}

func TestQualitySnapshot(t *testing.T) {
	planner := NewSupplementalPlanner(nil, nil, nil, nil)
	planner.updateNodeQuality("node-a", true, time.Now())
	planner.updateNodeQuality("node-a", false, time.Now())
	planner.updateNodeQuality("node-b", true, time.Now())
	planner.qualityMu.Lock()
	planner.nodeQuality["node-b"].QueueDepth = 10
	planner.qualityMu.Unlock()
	all := planner.QualitySnapshot(0, nil)
	if len(all) == 0 {
		t.Fatalf("expected quality entries")
	}
	filtered := planner.QualitySnapshot(1, []string{"node-a"})
	if len(filtered) != 1 || filtered[0].NodeUUID != "node-a" {
		t.Fatalf("expected single filtered node, got %+v", filtered)
	}
	if filtered[0].HealthScore < 0 || filtered[0].HealthScore > 1 {
		t.Fatalf("expected normalized health score, got %f", filtered[0].HealthScore)
	}
	if filtered[0].LastHeartbeat.IsZero() {
		t.Fatalf("expected heartbeat timestamp present")
	}
}

func TestRepairStatuses(t *testing.T) {
	planner := NewSupplementalPlanner(nil, nil, nil, nil)
	planner.failuresMu.Lock()
	planner.failures = map[string]*failureState{
		"node-b": {repairAttempts: 1},
		"node-a": {repairAttempts: 3, repairNext: time.Unix(10, 0), broken: true},
	}
	planner.failuresMu.Unlock()
	statuses := planner.RepairStatuses()
	if len(statuses) != 2 {
		t.Fatalf("expected 2 repair statuses, got %d", len(statuses))
	}
	if statuses[0].TargetUUID != "node-a" || !statuses[0].Broken || statuses[0].Attempts != 3 {
		t.Fatalf("unexpected snapshot for node-a: %+v", statuses[0])
	}
	if statuses[1].TargetUUID != "node-b" || statuses[1].Attempts != 1 {
		t.Fatalf("unexpected snapshot for node-b: %+v", statuses[1])
	}
}
