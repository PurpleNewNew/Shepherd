package planner

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"codeberg.org/agnoie/shepherd/internal/kelpie/supp"
	"codeberg.org/agnoie/shepherd/internal/kelpie/topology"
	"codeberg.org/agnoie/shepherd/protocol"
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

func TestCandidateScoreTreatsRedundancyAsLowerCost(t *testing.T) {
	planner := NewSupplementalPlanner(nil, nil, nil, nil)
	base := topology.SuppCandidate{
		Path:        []string{"ADMIN", "parent", "candidate"},
		Depth:       2,
		Overlap:     1,
		WorkSeconds: int(candidateWorkRef),
	}

	lowRedundancy := base
	lowRedundancy.UUID = "low-redundancy"
	lowRedundancy.Redundancy = 0.2
	highRedundancy := base
	highRedundancy.UUID = "high-redundancy"
	highRedundancy.Redundancy = 0.8

	lowScore := planner.candidateScore("target", &lowRedundancy)
	highScore := planner.candidateScore("target", &highRedundancy)
	if highScore >= lowScore {
		t.Fatalf("high redundancy should lower candidate cost, high=%f low=%f", highScore, lowScore)
	}

	candidates := []*topology.SuppCandidate{&lowRedundancy, &highRedundancy}
	planner.sortCandidates("target", candidates)
	if got := candidates[0].UUID; got != highRedundancy.UUID {
		t.Fatalf("expected high-redundancy candidate first, got %s", got)
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

func TestTopologyBlockSkipsPeriodicUntilTopologyChanges(t *testing.T) {
	planner := NewSupplementalPlanner(nil, nil, nil, nil)

	if !planner.markTopologyBlocked("node-a", "no candidates available") {
		t.Fatalf("expected first topology block to be recorded")
	}
	if planner.markTopologyBlocked("node-a", "no candidates available") {
		t.Fatalf("expected duplicate topology block to be suppressed")
	}

	skip, wait := planner.shouldDelay(PlanAction{Reason: reasonPeriodic, TargetUUID: "node-a"})
	if !skip || wait != 0 {
		t.Fatalf("expected periodic action to be skipped while topology-blocked, skip=%v wait=%s", skip, wait)
	}
	skip, _ = planner.shouldDelay(PlanAction{Reason: reasonManual, TargetUUID: "node-a"})
	if skip {
		t.Fatalf("manual repair should bypass topology block")
	}

	planner.clearTopologyBlocks()
	skip, wait = planner.shouldDelay(PlanAction{Reason: reasonPeriodic, TargetUUID: "node-a"})
	if skip || wait != 0 {
		t.Fatalf("expected periodic action after topology change to proceed, skip=%v wait=%s", skip, wait)
	}
}

func TestPlanForNodeSuspendsUnsatisfiableNoCandidateTopology(t *testing.T) {
	supp.TestOnlyResetSupplementalController()
	t.Cleanup(supp.TestOnlyResetSupplementalController)

	topo := topology.NewTopology()
	go topo.Run()
	t.Cleanup(topo.Stop)

	addPlannerTestNode(t, topo, "NODE-ROOT", protocol.ADMIN_UUID, "10.0.0.1", true)
	addPlannerTestNode(t, topo, "NODE-MID", "NODE-ROOT", "10.0.0.2", false)
	addPlannerTestNode(t, topo, "NODE-LEAF", "NODE-MID", "10.0.0.3", false)
	mustTopo(t, topo, &topology.TopoTask{
		Mode:         topology.ADDEDGE,
		UUID:         "NODE-ROOT",
		NeighborUUID: "NODE-LEAF",
		EdgeType:     topology.SupplementalEdge,
	})
	mustTopo(t, topo, &topology.TopoTask{Mode: topology.CALCULATE})

	supp.TestOnlyRegisterSupplementalLink("link-root-leaf", "NODE-ROOT", "NODE-LEAF", "10.0.0.1")
	supp.TestOnlyMarkSupplementalReady("link-root-leaf")

	planner := NewSupplementalPlanner(topo, topo.Service(), nil, nil)
	planner.planForNode(PlanAction{Reason: reasonPeriodic, TargetUUID: "NODE-MID"})

	if !planner.isTopologyBlocked("NODE-MID") {
		t.Fatalf("expected middle node to be blocked as topology-unsatisfiable")
	}
	planner.failuresMu.Lock()
	state := planner.failures["NODE-MID"]
	if state == nil || state.timer != nil || state.attempts != 0 {
		planner.failuresMu.Unlock()
		t.Fatalf("expected topology block without retry timer, state=%+v", state)
	}
	planner.failuresMu.Unlock()

	planner.inspectTopology()
	if got := len(planner.queue); got != 0 {
		t.Fatalf("expected periodic inspect to skip topology-blocked node, queued=%d", got)
	}
}

func TestRescueCleanupRunsForUnavailableOldParent(t *testing.T) {
	topo := topology.NewTopology()
	go topo.Run()
	t.Cleanup(topo.Stop)

	addPlannerTestNode(t, topo, "NODE-ROOT", protocol.ADMIN_UUID, "10.0.0.1", true)
	addPlannerTestNode(t, topo, "NODE-MID", "NODE-ROOT", "10.0.0.2", false)
	addPlannerTestNode(t, topo, "NODE-LEAF", "NODE-MID", "10.0.0.3", false)
	mustTopo(t, topo, &topology.TopoTask{Mode: topology.CALCULATE})
	mustTopo(t, topo, &topology.TopoTask{Mode: topology.MARKNODEOFFLINE, UUID: "NODE-MID"})

	planner := NewSupplementalPlanner(topo, topo.Service(), nil, nil)
	var gotParent, gotChild, gotNewParent string
	planner.SetRescueCleanup(func(failedParent, child, newParent string) {
		gotParent = failedParent
		gotChild = child
		gotNewParent = newParent
	})

	if err := planner.rescue.applyRescueResult(&protocol.RescueResponse{
		ParentUUID: "NODE-ROOT",
		ChildUUID:  "NODE-LEAF",
	}); err != nil {
		t.Fatalf("apply rescue result: %v", err)
	}
	if gotParent != "NODE-MID" || gotChild != "NODE-LEAF" || gotNewParent != "NODE-ROOT" {
		t.Fatalf("unexpected cleanup callback parent=%s child=%s newParent=%s", gotParent, gotChild, gotNewParent)
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

func addPlannerTestNode(t *testing.T, topo *topology.Topology, uuid, parent, ip string, isFirst bool) {
	t.Helper()
	mustTopo(t, topo, &topology.TopoTask{
		Mode:       topology.ADDNODE,
		Target:     topology.NewNode(uuid, ip),
		ParentUUID: parent,
		IsFirst:    isFirst,
	})
	if parent != "" && parent != protocol.TEMP_UUID {
		mustTopo(t, topo, &topology.TopoTask{
			Mode:         topology.ADDEDGE,
			UUID:         parent,
			NeighborUUID: uuid,
			EdgeType:     topology.TreeEdge,
		})
	}
}

func mustTopo(t *testing.T, topo *topology.Topology, task *topology.TopoTask) *topology.Result {
	t.Helper()
	result, err := topo.Execute(task)
	if err != nil {
		t.Fatalf("topology task %d failed: %v", task.Mode, err)
	}
	return result
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

func TestRepairStatusesReturnsSnapshotWithoutWaitingOnFailuresLock(t *testing.T) {
	planner := NewSupplementalPlanner(nil, nil, nil, nil)
	planner.failuresMu.Lock()
	defer planner.failuresMu.Unlock()

	start := time.Now()
	statuses := planner.RepairStatuses()
	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Fatalf("RepairStatuses waited on lock for %s", elapsed)
	}
	if len(statuses) != 0 {
		t.Fatalf("expected empty snapshot while lock is contended, got %+v", statuses)
	}
}

func TestManualRepairRejectsQuarantinedNode(t *testing.T) {
	topo := topology.NewTopology()
	go topo.Run()
	t.Cleanup(topo.Stop)

	addPlannerTestNode(t, topo, "NODE-DEAD", protocol.ADMIN_UUID, "10.0.0.2", true)
	mustTopo(t, topo, &topology.TopoTask{Mode: topology.QUARANTINENODE, UUID: "NODE-DEAD"})

	planner := NewSupplementalPlanner(topo, topo.Service(), nil, nil)
	if err := planner.RequestManualRepair("NODE-DEAD"); err == nil {
		t.Fatalf("expected manual repair for quarantined node to be rejected")
	}
}
