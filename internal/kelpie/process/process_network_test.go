package process

import (
	"testing"

	"codeberg.org/agnoie/shepherd/internal/kelpie/topology"
	"codeberg.org/agnoie/shepherd/protocol"
)

func TestAdminNetworkLifecycle(t *testing.T) {
	admin, topo := newTestAdmin(t)

	addTopologyEdge(t, topo, "ENTRY-A", protocol.ADMIN_UUID, true)
	addTopologyEdge(t, topo, "ENTRY-B", protocol.ADMIN_UUID, true)
	recalcTopology(t, topo)

	if err := topo.SetNetwork("ENTRY-B", "NET-B"); err != nil {
		t.Fatalf("assign network: %v", err)
	}

	infos, active, err := admin.ListNetworks()
	if err != nil {
		t.Fatalf("list networks: %v", err)
	}
	if len(infos) != 2 {
		t.Fatalf("expected two networks, got %d", len(infos))
	}
	if active != "" {
		t.Fatalf("expected no active network, got %q", active)
	}

	if _, err := admin.SetActiveNetwork("NET-B"); err != nil {
		t.Fatalf("set active network: %v", err)
	}
	if got := admin.ActiveNetworkID(); got != "NET-B" {
		t.Fatalf("expected active network NET-B, got %s", got)
	}
	if reset := admin.ResetActiveNetwork(); reset != "" {
		t.Fatalf("expected reset to empty, got %s", reset)
	}

	if err := admin.SetNodeNetwork("ENTRY-A", "NET-C"); err != nil {
		t.Fatalf("assign entry network: %v", err)
	}
	if net := topo.NetworkFor("ENTRY-A"); net != "NET-C" {
		t.Fatalf("expected ENTRY-A network NET-C, got %s", net)
	}
}

func newTestAdmin(t *testing.T) (*Admin, *topology.Topology) {
	t.Helper()
	topo := topology.NewTopology()
	go topo.Run()
	t.Cleanup(topo.Stop)

	admin := &Admin{
		topology:    topo,
		topoService: topology.NewService(topo),
	}
	admin.handleNetworkChange("")
	return admin, topo
}

func addTopologyEdge(t *testing.T, topo *topology.Topology, uuid, parent string, isFirst bool) {
	t.Helper()
	node := topology.NewNode(uuid, "127.0.0.1")
	if err := topo.Enqueue(&topology.TopoTask{
		Mode:       topology.ADDNODE,
		Target:     node,
		ParentUUID: parent,
		IsFirst:    isFirst,
	}); err != nil {
		t.Fatalf("enqueue add node: %v", err)
	}
	<-topo.ResultChan
	if parent != "" {
		if err := topo.Enqueue(&topology.TopoTask{Mode: topology.ADDEDGE, UUID: parent, NeighborUUID: uuid}); err != nil {
			t.Fatalf("enqueue add parent edge: %v", err)
		}
		<-topo.ResultChan
		if err := topo.Enqueue(&topology.TopoTask{Mode: topology.ADDEDGE, UUID: uuid, NeighborUUID: parent}); err != nil {
			t.Fatalf("enqueue add child edge: %v", err)
		}
		<-topo.ResultChan
	}
}

func recalcTopology(t *testing.T, topo *topology.Topology) {
	t.Helper()
	if err := topo.Enqueue(&topology.TopoTask{Mode: topology.CALCULATE}); err != nil {
		t.Fatalf("enqueue recalc: %v", err)
	}
	<-topo.ResultChan
}
