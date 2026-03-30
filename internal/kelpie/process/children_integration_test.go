package process

import (
	"context"
	"testing"
	"time"

	"codeberg.org/agnoie/shepherd/internal/kelpie/manager"
	"codeberg.org/agnoie/shepherd/internal/kelpie/printer"
	"codeberg.org/agnoie/shepherd/internal/kelpie/supp"
	"codeberg.org/agnoie/shepherd/internal/kelpie/topology"
	"codeberg.org/agnoie/shepherd/pkg/global"
	"codeberg.org/agnoie/shepherd/protocol"
)

func mustSetPreAuthToken(t *testing.T, store *global.Store, token string) {
	t.Helper()
	if err := store.SetPreAuthToken(token); err != nil {
		t.Fatalf("set preauth token: %v", err)
	}
}

func requestTopo(t *testing.T, topo *topology.Topology, task *topology.TopoTask) *topology.Result {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := topo.Request(ctx, task)
	if err != nil {
		t.Fatalf("topology request %+v failed: %v", task, err)
	}
	return result
}

func containsString(list []string, target string) bool {
	for _, item := range list {
		if item == target {
			return true
		}
	}
	return false
}

func TestNodeOfflineMarksSubtreeOffline(t *testing.T) {
	printer.InitPrinter()

	topo := topology.NewTopology()
	go topo.Run()
	t.Cleanup(topo.Stop)

	addNode := func(uuid, parent, ip string, first bool) {
		node := topology.NewNode(uuid, ip)
		task := &topology.TopoTask{
			Mode:       topology.ADDNODE,
			Target:     node,
			ParentUUID: parent,
			IsFirst:    first,
		}
		requestTopo(t, topo, task)
		if parent != "" {
			requestTopo(t, topo, &topology.TopoTask{
				Mode:         topology.ADDEDGE,
				UUID:         parent,
				NeighborUUID: uuid,
				EdgeType:     topology.TreeEdge,
			})
		}
	}

	addNode("NODE-PARENT", protocol.ADMIN_UUID, "10.0.0.1", true)
	addNode("NODE-CHILD", "NODE-PARENT", "10.0.0.2", false)
	addNode("NODE-GRAND", "NODE-CHILD", "10.0.0.3", false)

	requestTopo(t, topo, &topology.TopoTask{Mode: topology.CALCULATE})

	store := global.NewStoreWithTransports(nil)
	mustSetPreAuthToken(t, store, "offline-integration-token")
	mgr := manager.NewManager(store)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go mgr.Run(ctx)

	nodeOffline(mgr, topo, "NODE-PARENT")

	// Nodes should remain in topology (DTN carry-forward), but be excluded from "online node" queries.
	checkPresent := func(uuid string) {
		result := requestTopo(t, topo, &topology.TopoTask{Mode: topology.GETNODEINFO, UUID: uuid})
		if result == nil || result.UUID != uuid {
			t.Fatalf("expected node %s to remain in topology, got %+v", uuid, result)
		}
	}

	checkPresent("NODE-PARENT")
	checkPresent("NODE-CHILD")
	checkPresent("NODE-GRAND")

	online := requestTopo(t, topo, &topology.TopoTask{Mode: topology.GETALLNODES})
	if online != nil && len(online.AllNodes) != 0 {
		t.Fatalf("expected no online nodes after offline, got %+v", online.AllNodes)
	}

	// Route display should remain available even when offline.
	route := requestTopo(t, topo, &topology.TopoTask{Mode: topology.GETROUTE, UUID: "NODE-GRAND"})
	if route == nil || route.Route == "" {
		t.Fatalf("expected route to remain available for offline node, got %+v", route)
	}
}

func TestAttemptSupplementalFailoverReparentsNode(t *testing.T) {
	printer.InitPrinter()

	topo := topology.NewTopology()
	go topo.Run()

	addNode := func(uuid, parent, ip string, first bool) {
		node := topology.NewNode(uuid, ip)
		task := &topology.TopoTask{
			Mode:       topology.ADDNODE,
			Target:     node,
			ParentUUID: parent,
			IsFirst:    first,
		}
		requestTopo(t, topo, task)
		if parent != "" {
			requestTopo(t, topo, &topology.TopoTask{
				Mode:         topology.ADDEDGE,
				UUID:         parent,
				NeighborUUID: uuid,
				EdgeType:     topology.TreeEdge,
			})
		}
	}

	addNode("NODE-ROOT", protocol.ADMIN_UUID, "10.0.0.1", true)
	addNode("NODE-PRIMARY", "NODE-ROOT", "10.0.0.2", false)
	addNode("NODE-FAIL", "NODE-PRIMARY", "10.0.0.3", false)
	addNode("NODE-CAND", "NODE-PRIMARY", "10.0.0.4", false)

	requestTopo(t, topo, &topology.TopoTask{
		Mode:         topology.ADDEDGE,
		UUID:         "NODE-FAIL",
		NeighborUUID: "NODE-CAND",
		EdgeType:     topology.SupplementalEdge,
	})

	requestTopo(t, topo, &topology.TopoTask{Mode: topology.CALCULATE})

	_ = supp.RegisterSuppFailoverHook(nil)
	supp.TestOnlyResetSupplementalController()
	supp.TestOnlyRegisterSupplementalLink("link-failover", "NODE-FAIL", "NODE-CAND", "10.0.0.3")
	supp.TestOnlyMarkSupplementalReady("link-failover")
	supp.TestOnlyRecordSupplementalHeartbeat("link-failover", "NODE-FAIL", true)
	supp.TestOnlyRecordSupplementalHeartbeat("link-failover", "NODE-CAND", true)

	store := global.NewStoreWithTransports(nil)
	mustSetPreAuthToken(t, store, "failover-integration-token")
	mgr := manager.NewManager(store)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go mgr.Run(ctx)

	// 在执行补链切换前先刷新路由信息。
	requestTopo(t, topo, &topology.TopoTask{Mode: topology.CALCULATE})

	if ok := attemptSupplementalFailover(mgr, topo, "NODE-FAIL"); !ok {
		t.Fatalf("expected supplemental failover to succeed")
	}

	meta := requestTopo(t, topo, &topology.TopoTask{Mode: topology.GETNODEMETA, UUID: "NODE-FAIL"})
	if meta == nil || meta.Parent != "NODE-CAND" {
		t.Fatalf("expected NODE-FAIL parent to become NODE-CAND, got %+v", meta)
	}

	primaryMeta := requestTopo(t, topo, &topology.TopoTask{Mode: topology.GETNODEMETA, UUID: "NODE-PRIMARY"})
	if primaryMeta != nil {
		if containsString(primaryMeta.Children, "NODE-FAIL") {
			t.Fatalf("expected NODE-FAIL to be removed from NODE-PRIMARY children")
		}
	}

	neighbors := requestTopo(t, topo, &topology.TopoTask{Mode: topology.GETNEIGHBORS, UUID: "NODE-CAND"})
	if neighbors == nil || !containsString(neighbors.Neighbors, "NODE-FAIL") {
		t.Fatalf("expected NODE-CAND to have NODE-FAIL as neighbor after failover")
	}

	if status, ok := supp.TestOnlySupplementalStatus("link-failover"); !ok || status != supp.SuppLinkPromoted {
		t.Fatalf("expected supplemental link to be promoted, got status=%v ok=%v", status, ok)
	}
}

func TestSupplementalFailoverRejectedAcrossNetworks(t *testing.T) {
	printer.InitPrinter()

	topo := topology.NewTopology()
	go topo.Run()

	addNode := func(uuid, parent string, first bool) {
		node := topology.NewNode(uuid, "10.0.0.1")
		task := &topology.TopoTask{Mode: topology.ADDNODE, Target: node, ParentUUID: parent, IsFirst: first}
		if err := topo.Enqueue(task); err != nil {
			t.Fatalf("enqueue add node: %v", err)
		}
		<-topo.ResultChan
		if parent != "" {
			if err := topo.Enqueue(&topology.TopoTask{Mode: topology.ADDEDGE, UUID: parent, NeighborUUID: uuid, EdgeType: topology.TreeEdge}); err != nil {
				t.Fatalf("enqueue add edge: %v", err)
			}
			<-topo.ResultChan
		}
	}

	addNode("ENTRY-A", protocol.ADMIN_UUID, true)
	addNode("ENTRY-B", protocol.ADMIN_UUID, true)
	addNode("NODE-FAIL", "ENTRY-A", false)
	addNode("NODE-CAND", "ENTRY-B", false)

	requestTopo(t, topo, &topology.TopoTask{Mode: topology.CALCULATE})

	if err := topo.SetNetwork("ENTRY-A", "NET-A"); err != nil {
		t.Fatalf("assign network: %v", err)
	}
	if err := topo.SetNetwork("ENTRY-B", "NET-B"); err != nil {
		t.Fatalf("assign network: %v", err)
	}

	requestTopo(t, topo, &topology.TopoTask{Mode: topology.CALCULATE})
	requestTopo(t, topo, &topology.TopoTask{Mode: topology.ADDEDGE, UUID: "NODE-FAIL", NeighborUUID: "NODE-CAND", EdgeType: topology.SupplementalEdge})

	supp.TestOnlyResetSupplementalController()
	supp.TestOnlyRegisterSupplementalLink("link-cross", "NODE-FAIL", "NODE-CAND", "10.0.0.3")
	supp.TestOnlyMarkSupplementalReady("link-cross")
	supp.TestOnlyRecordSupplementalHeartbeat("link-cross", "NODE-FAIL", true)
	supp.TestOnlyRecordSupplementalHeartbeat("link-cross", "NODE-CAND", true)

	store := global.NewStoreWithTransports(nil)
	mustSetPreAuthToken(t, store, "cross-network-token")
	mgr := manager.NewManager(store)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go mgr.Run(ctx)

	if ok := attemptSupplementalFailover(mgr, topo, "NODE-FAIL"); ok {
		t.Fatalf("expected cross-network failover to be rejected")
	}

	meta := requestTopo(t, topo, &topology.TopoTask{Mode: topology.GETNODEMETA, UUID: "NODE-FAIL"})
	if meta == nil || meta.Parent != "ENTRY-A" {
		t.Fatalf("expected NODE-FAIL to remain under ENTRY-A, got %+v", meta)
	}
}
