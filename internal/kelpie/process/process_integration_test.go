package process

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"
	"unsafe"

	"codeberg.org/agnoie/shepherd/internal/kelpie/initial"
	"codeberg.org/agnoie/shepherd/internal/kelpie/topology"
	"codeberg.org/agnoie/shepherd/pkg/global"
	"codeberg.org/agnoie/shepherd/protocol"
)

func addNode(t *testing.T, topo *topology.Topology, uuid, parent, ip string, isFirst bool) {
	t.Helper()
	node := topology.NewNode(uuid, ip)
	task := &topology.TopoTask{
		Mode:       topology.ADDNODE,
		Target:     node,
		ParentUUID: parent,
		IsFirst:    isFirst,
	}
	if err := topo.Enqueue(task); err != nil {
		t.Fatalf("enqueue add node: %v", err)
	}
	<-topo.ResultChan
}

func addEdge(t *testing.T, topo *topology.Topology, from, to string) {
	t.Helper()
	task := &topology.TopoTask{
		Mode:         topology.ADDEDGE,
		UUID:         from,
		NeighborUUID: to,
	}
	if err := topo.Enqueue(task); err != nil {
		t.Fatalf("enqueue add edge: %v", err)
	}
	<-topo.ResultChan
}

func edgeWeightsSnapshot(topo *topology.Topology) map[string]map[string]uint32 {
	val := reflect.ValueOf(topo).Elem().FieldByName("edgeWeights")
	ptr := unsafe.Pointer(val.UnsafeAddr())
	raw := *(*map[string]map[string]uint32)(ptr)
	clone := make(map[string]map[string]uint32, len(raw))
	for k, v := range raw {
		inner := make(map[string]uint32, len(v))
		for nk, nv := range v {
			inner[nk] = nv
		}
		clone[k] = inner
	}
	return clone
}

func TestApplyGossipUpdateReconcilesTopology(t *testing.T) {
	topo := topology.NewTopology()
	go topo.Run()
	t.Cleanup(topo.Stop)

	addNode(t, topo, "NODE-ROOT", protocol.ADMIN_UUID, "10.0.0.1", true)
	addNode(t, topo, "NODE-CHILD", "NODE-ROOT", "10.0.0.2", false)

	addEdge(t, topo, "NODE-ROOT", protocol.ADMIN_UUID)
	addEdge(t, topo, protocol.ADMIN_UUID, "NODE-ROOT")
	addEdge(t, topo, "NODE-ROOT", "NODE-CHILD")
	addEdge(t, topo, "NODE-CHILD", "NODE-ROOT")

	store := global.NewStoreWithTransports(nil)
	if err := store.SetPreAuthToken("test-preauth-token"); err != nil {
		t.Fatalf("set preauth token: %v", err)
	}
	opts := &initial.Options{PreAuthToken: "test-preauth-token"}
	admin := NewAdmin(context.Background(), opts, topo, store, nil, topology.PlannerMetricsSnapshot{}, nil, nil, nil, nil, nil)

	info := &protocol.NodeInfo{
		UUID:      "NODE-ROOT",
		IP:        "10.0.0.1",
		Port:      9000,
		Hostname:  "root-host",
		Username:  "root-user",
		Neighbors: []string{"NODE-CHILD", protocol.ADMIN_UUID, "NODE-FAKE"},
		Health:    3,
		LastSeen:  time.Now().Unix(),
	}
	body, err := json.Marshal(info)
	if err != nil {
		t.Fatalf("marshal node info: %v", err)
	}

	update := &protocol.GossipUpdate{
		TTL:        3,
		NodeData:   body,
		SenderUUID: "NODE-ROOT",
		Timestamp:  time.Now().UnixNano(),
	}

	admin.applyGossipUpdate(update)

	if err := topo.Enqueue(&topology.TopoTask{
		Mode: topology.GETNEIGHBORS,
		UUID: "NODE-ROOT",
	}); err != nil {
		t.Fatalf("enqueue neighbors: %v", err)
	}
	result := <-topo.ResultChan
	if len(result.Neighbors) == 0 {
		t.Fatalf("expected neighbors for NODE-ROOT")
	}
	for _, neighbor := range result.Neighbors {
		if neighbor == "NODE-FAKE" {
			t.Fatalf("unexpected stale neighbor retained in topology")
		}
	}

	weights := edgeWeightsSnapshot(topo)
	if weights["NODE-ROOT"]["NODE-CHILD"] != info.Health+1 {
		t.Fatalf("expected weight %d for NODE-ROOT->NODE-CHILD, got %d", info.Health+1, weights["NODE-ROOT"]["NODE-CHILD"])
	}
}
