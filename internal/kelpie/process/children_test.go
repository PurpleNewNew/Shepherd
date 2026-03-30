package process

import (
	"testing"

	"codeberg.org/agnoie/shepherd/internal/kelpie/topology"
	"codeberg.org/agnoie/shepherd/protocol"
)

func TestSelectFailoverCandidate(t *testing.T) {
	topo := topology.NewTopology()
	go topo.Run()
	t.Cleanup(topo.Stop)

	addNode := func(uuid, parent, ip string, edge topology.EdgeType) {
		node := topology.NewNode(uuid, ip)
		if err := topo.Enqueue(&topology.TopoTask{
			Mode:       topology.ADDNODE,
			Target:     node,
			ParentUUID: parent,
			IsFirst:    parent == protocol.ADMIN_UUID,
		}); err != nil {
			t.Fatalf("enqueue add node: %v", err)
		}
		<-topo.ResultChan
		if parent != "" {
			if err := topo.Enqueue(&topology.TopoTask{
				Mode:         topology.ADDEDGE,
				UUID:         parent,
				NeighborUUID: uuid,
				EdgeType:     edge,
			}); err != nil {
				t.Fatalf("enqueue add edge: %v", err)
			}
			<-topo.ResultChan
		}
	}

	addNode("NODE-A", protocol.ADMIN_UUID, "10.0.0.1", topology.TreeEdge)
	addNode("NODE-B", "NODE-A", "10.0.0.2", topology.TreeEdge)
	addNode("NODE-C", "NODE-A", "10.0.0.3", topology.TreeEdge)

	if err := topo.Enqueue(&topology.TopoTask{
		Mode:         topology.ADDEDGE,
		UUID:         "NODE-B",
		NeighborUUID: "NODE-C",
		EdgeType:     topology.SupplementalEdge,
	}); err != nil {
		t.Fatalf("enqueue supplemental edge: %v", err)
	}
	<-topo.ResultChan
	if err := topo.Enqueue(&topology.TopoTask{Mode: topology.CALCULATE}); err != nil {
		t.Fatalf("enqueue calculate: %v", err)
	}
	<-topo.ResultChan

	candidate := selectFailoverCandidate(topo, "NODE-B", "NODE-A", []string{"NODE-C"})
	if candidate != "NODE-C" {
		t.Fatalf("expected NODE-C as failover candidate, got %s", candidate)
	}
}

func TestSelectFailoverCandidateRespectsEntry(t *testing.T) {
	topo := topology.NewTopology()
	go topo.Run()

	addNode := func(uuid, parent string, isFirst bool) {
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
			if err := topo.Enqueue(&topology.TopoTask{
				Mode:         topology.ADDEDGE,
				UUID:         parent,
				NeighborUUID: uuid,
			}); err != nil {
				t.Fatalf("enqueue add edge: %v", err)
			}
			<-topo.ResultChan
		}
	}

	addNode("ENTRY-1", protocol.ADMIN_UUID, true)
	addNode("NODE-1A", "ENTRY-1", false)
	addNode("ENTRY-2", protocol.ADMIN_UUID, true)
	addNode("NODE-2A", "ENTRY-2", false)

	if err := topo.Enqueue(&topology.TopoTask{
		Mode:         topology.ADDEDGE,
		UUID:         "NODE-1A",
		NeighborUUID: "NODE-2A",
		EdgeType:     topology.SupplementalEdge,
	}); err != nil {
		t.Fatalf("enqueue supplemental edge: %v", err)
	}
	<-topo.ResultChan
	if err := topo.Enqueue(&topology.TopoTask{Mode: topology.CALCULATE}); err != nil {
		t.Fatalf("enqueue calculate: %v", err)
	}
	<-topo.ResultChan

	if candidate := selectFailoverCandidate(topo, "NODE-1A", "ENTRY-1", []string{"NODE-2A"}); candidate != "" {
		t.Fatalf("cross-entry candidate should be ignored, got %s", candidate)
	}

	addNode("NODE-1B", "ENTRY-1", false)
	if err := topo.Enqueue(&topology.TopoTask{
		Mode:         topology.ADDEDGE,
		UUID:         "NODE-1A",
		NeighborUUID: "NODE-1B",
		EdgeType:     topology.SupplementalEdge,
	}); err != nil {
		t.Fatalf("enqueue supplemental edge: %v", err)
	}
	<-topo.ResultChan
	if err := topo.Enqueue(&topology.TopoTask{Mode: topology.CALCULATE}); err != nil {
		t.Fatalf("enqueue calculate: %v", err)
	}
	<-topo.ResultChan

	if candidate := selectFailoverCandidate(topo, "NODE-1A", "ENTRY-1", []string{"NODE-2A", "NODE-1B"}); candidate != "NODE-1B" {
		t.Fatalf("expected same-entry candidate NODE-1B, got %s", candidate)
	}
}
