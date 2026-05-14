package process

import (
	"testing"
	"time"

	"codeberg.org/agnoie/shepherd/internal/kelpie/dtn"
	"codeberg.org/agnoie/shepherd/internal/kelpie/topology"
	"codeberg.org/agnoie/shepherd/protocol"
)

func TestDTNBundleLifecycleByTargetStatus(t *testing.T) {
	topo := topology.NewTopology()
	go topo.Run()
	t.Cleanup(topo.Stop)

	addTopologyEdge(t, topo, "offline-node", protocol.ADMIN_UUID, true)
	addTopologyEdge(t, topo, "quarantined-node", protocol.ADMIN_UUID, true)
	recalcTopology(t, topo)

	if _, err := topo.Execute(&topology.TopoTask{Mode: topology.MARKNODEOFFLINE, UUID: "offline-node"}); err != nil {
		t.Fatalf("mark offline: %v", err)
	}
	if _, err := topo.Execute(&topology.TopoTask{Mode: topology.QUARANTINENODE, UUID: "quarantined-node"}); err != nil {
		t.Fatalf("quarantine node: %v", err)
	}

	cfg := dtn.DefaultConfig()
	cfg.DispatchBatch = 8
	cfg.DispatchInterval = time.Second
	admin := &Admin{
		topology:    topo,
		topoService: topology.NewService(topo),
		adminDTNState: adminDTNState{
			dtnManager:  dtn.NewManager(cfg),
			dtnInflight: make(map[string]*dtnInflightRecord),
		},
	}

	offlineBundle, err := admin.dtnManager.Enqueue("offline-node", []byte("hold"), dtn.WithTTL(time.Minute))
	if err != nil {
		t.Fatalf("enqueue offline bundle: %v", err)
	}
	ready := admin.dtnManager.Ready(time.Now(), 1)
	if len(ready) != 1 || ready[0].ID != offlineBundle.ID {
		t.Fatalf("expected offline bundle ready, got %+v", ready)
	}
	if !admin.handleBundleLifecycle(ready[0]) {
		t.Fatalf("expected offline bundle to be handled by lifecycle")
	}
	stats := admin.dtnManager.Stats("offline-node")
	if stats.Total != 1 || stats.Held != 1 {
		t.Fatalf("expected offline bundle to be held, got %+v", stats)
	}
	list := admin.dtnManager.List("offline-node", 1)
	if len(list) != 1 || list[0].Attempts != 0 {
		t.Fatalf("expected hold without retry attempt, got %+v", list)
	}

	deadBundle, err := admin.dtnManager.Enqueue("quarantined-node", []byte("drop"), dtn.WithTTL(time.Minute))
	if err != nil {
		t.Fatalf("enqueue quarantined bundle: %v", err)
	}
	ready = admin.dtnManager.Ready(time.Now(), 8)
	if len(ready) != 1 || ready[0].ID != deadBundle.ID {
		t.Fatalf("expected quarantined bundle ready, got %+v", ready)
	}
	if !admin.handleBundleLifecycle(ready[0]) {
		t.Fatalf("expected quarantined bundle to be handled by lifecycle")
	}
	stats = admin.dtnManager.Stats("quarantined-node")
	if stats.Total != 0 {
		t.Fatalf("expected quarantined bundle to be dropped, got %+v", stats)
	}
	if admin.dtnFailed != 1 {
		t.Fatalf("expected failed counter to increment, got %d", admin.dtnFailed)
	}
}

func TestOfflineCleanupKeepsRecoverableDTN(t *testing.T) {
	admin := &Admin{
		adminSessionState: adminSessionState{
			sessionMeta: make(map[string]sessionState),
		},
		adminDTNState: adminDTNState{
			dtnManager: dtn.NewManager(dtn.DefaultConfig()),
			dtnInflight: map[string]*dtnInflightRecord{
				"inflight-offline": {
					bundle: &dtn.Bundle{ID: "inflight-offline", Target: "NODE-OFFLINE", Payload: []byte("pending")},
					sentAt: time.Now(),
				},
			},
		},
	}
	if _, err := admin.dtnManager.Enqueue("NODE-OFFLINE", []byte("queued"), dtn.WithTTL(time.Minute)); err != nil {
		t.Fatalf("enqueue offline bundle: %v", err)
	}

	admin.cleanupUnavailableNode("NODE-OFFLINE", "node offline", topology.NodeStatusOffline)

	if stats := admin.dtnManager.Stats("NODE-OFFLINE"); stats.Total != 1 {
		t.Fatalf("expected offline cleanup to keep queued DTN, got %+v", stats)
	}
	if got := admin.inflightForTarget("NODE-OFFLINE"); got != 1 {
		t.Fatalf("expected offline cleanup to keep inflight DTN, got %d", got)
	}
}

func TestReonlineDifferentParentCleansUnavailableOldParent(t *testing.T) {
	topo := topology.NewTopology()
	go topo.Run()
	t.Cleanup(topo.Stop)

	addTopologyEdge(t, topo, "NODE-ROOT", protocol.ADMIN_UUID, true)
	addTopologyEdge(t, topo, "NODE-MID", "NODE-ROOT", false)
	addTopologyEdge(t, topo, "NODE-LEAF", "NODE-MID", false)
	recalcTopology(t, topo)
	if _, err := topo.Execute(&topology.TopoTask{Mode: topology.MARKNODEOFFLINE, UUID: "NODE-MID"}); err != nil {
		t.Fatalf("mark mid offline: %v", err)
	}

	var gotParent, gotChild, gotNewParent string
	nodeReonline(nil, topo, &protocol.NodeReonline{
		UUID:       "NODE-LEAF",
		ParentUUID: "NODE-ROOT",
		IP:         "127.0.0.1",
	}, func(failedParent, child, newParent string) {
		gotParent = failedParent
		gotChild = child
		gotNewParent = newParent
	})

	if gotParent != "NODE-MID" || gotChild != "NODE-LEAF" || gotNewParent != "NODE-ROOT" {
		t.Fatalf("unexpected cleanup callback parent=%s child=%s newParent=%s", gotParent, gotChild, gotNewParent)
	}
}

func TestFailoverCleanupQuarantinesAndClearsDeadParentDTN(t *testing.T) {
	topo := topology.NewTopology()
	go topo.Run()
	t.Cleanup(topo.Stop)

	addTopologyEdge(t, topo, "NODE-MID", protocol.ADMIN_UUID, true)
	addTopologyEdge(t, topo, "NODE-LEAF", "NODE-MID", false)
	recalcTopology(t, topo)

	admin := &Admin{
		topology:    topo,
		topoService: topology.NewService(topo),
		adminSessionState: adminSessionState{
			sessionMeta: make(map[string]sessionState),
		},
		adminDTNState: adminDTNState{
			dtnManager: dtn.NewManager(dtn.DefaultConfig()),
			dtnInflight: map[string]*dtnInflightRecord{
				"inflight-mid": {
					bundle: &dtn.Bundle{ID: "inflight-mid", Target: "NODE-MID", Payload: []byte("pending")},
					sentAt: time.Now(),
				},
			},
		},
	}
	if _, err := admin.dtnManager.Enqueue("NODE-MID", []byte("queued"), dtn.WithTTL(time.Minute)); err != nil {
		t.Fatalf("enqueue dead parent bundle: %v", err)
	}

	admin.handleFailoverCleanup("NODE-MID", "NODE-LEAF", protocol.ADMIN_UUID)
	recalcTopology(t, topo)

	if status, ok := topo.NodeStatus("NODE-MID"); !ok || status != topology.NodeStatusQuarantined {
		t.Fatalf("expected NODE-MID quarantined, got status=%s ok=%v", status, ok)
	}
	if route := requestTopo(t, topo, &topology.TopoTask{Mode: topology.GETROUTE, UUID: "NODE-MID"}); route != nil && route.Route != "" {
		t.Fatalf("expected quarantined parent route to be empty, got %+v", route)
	}
	if stats := admin.dtnManager.Stats("NODE-MID"); stats.Total != 0 {
		t.Fatalf("expected queued DTN for dead parent to be cleared, got %+v", stats)
	}
	if got := admin.inflightForTarget("NODE-MID"); got != 0 {
		t.Fatalf("expected inflight DTN for dead parent to be cleared, got %d", got)
	}
}
