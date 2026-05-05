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

func TestDispatchGossipUpdateRefreshesCarrierLiveness(t *testing.T) {
	topo := topology.NewTopology()
	go topo.Run()
	t.Cleanup(topo.Stop)

	addNode(t, topo, "NODE-ROOT", protocol.ADMIN_UUID, "10.0.0.1", true)
	addNode(t, topo, "NODE-CHILD", "NODE-ROOT", "10.0.0.2", false)

	requestTopo(t, topo, &topology.TopoTask{Mode: topology.MARKNODEOFFLINE, UUID: "NODE-ROOT"})
	if aliveInSnapshot(topo, "NODE-ROOT") {
		t.Fatalf("expected root to start offline")
	}

	store := global.NewStoreWithTransports(nil)
	if err := store.SetPreAuthToken("test-preauth-token"); err != nil {
		t.Fatalf("set preauth token: %v", err)
	}
	admin := NewAdmin(context.Background(), &initial.Options{PreAuthToken: "test-preauth-token"}, topo, store, nil, topology.PlannerMetricsSnapshot{}, nil, nil, nil, nil, nil)

	info := &protocol.NodeInfo{UUID: "NODE-CHILD", LastSeen: time.Now().Unix()}
	body, err := json.Marshal(info)
	if err != nil {
		t.Fatalf("marshal node info: %v", err)
	}
	update := &protocol.GossipUpdate{
		TTL:        3,
		NodeData:   body,
		SenderUUID: "NODE-CHILD",
		Timestamp:  time.Now().UnixNano(),
	}
	handler := admin.dispatchGossipUpdate()
	if err := handler(context.Background(), &protocol.Header{Sender: "NODE-ROOT", MessageType: uint16(protocol.GOSSIP_UPDATE)}, update); err != nil {
		t.Fatalf("dispatch gossip update: %v", err)
	}

	if !aliveInSnapshot(topo, "NODE-ROOT") {
		t.Fatalf("expected gossip carrier root to be marked alive")
	}
}

func TestSupplementalHeartbeatRefreshesEndpointLiveness(t *testing.T) {
	topo := topology.NewTopology()
	go topo.Run()
	t.Cleanup(topo.Stop)

	addNode(t, topo, "NODE-ROOT", protocol.ADMIN_UUID, "10.0.0.1", true)
	addNode(t, topo, "NODE-LEAF", "NODE-ROOT", "10.0.0.2", false)
	requestTopo(t, topo, &topology.TopoTask{Mode: topology.MARKNODEOFFLINE, UUID: "NODE-LEAF"})
	if aliveInSnapshot(topo, "NODE-LEAF") {
		t.Fatalf("expected leaf to start offline")
	}

	core := &routerCore{topo: topo}
	handler := core.dispatchSuppLinkHeartbeat()
	err := handler(context.Background(), &protocol.Header{Sender: "NODE-LEAF"}, &protocol.SuppLinkHeartbeat{
		LinkUUID:  "supp-link",
		PeerUUID:  "NODE-ROOT",
		Status:    1,
		Timestamp: time.Now().Unix(),
	})
	if err != nil {
		t.Fatalf("dispatch supplemental heartbeat: %v", err)
	}
	if !aliveInSnapshot(topo, "NODE-LEAF") {
		t.Fatalf("expected supplemental heartbeat sender to be marked alive")
	}
}

func TestDTNAckRefreshesSenderLiveness(t *testing.T) {
	topo := topology.NewTopology()
	go topo.Run()
	t.Cleanup(topo.Stop)

	addNode(t, topo, "NODE-ROOT", protocol.ADMIN_UUID, "10.0.0.1", true)
	addNode(t, topo, "NODE-LEAF", "NODE-ROOT", "10.0.0.2", false)
	requestTopo(t, topo, &topology.TopoTask{Mode: topology.MARKNODEOFFLINE, UUID: "NODE-LEAF"})
	if aliveInSnapshot(topo, "NODE-LEAF") {
		t.Fatalf("expected leaf to start offline")
	}

	core := &routerCore{topo: topo}
	handler := core.dispatchDTNAck()
	err := handler(context.Background(), &protocol.Header{Sender: "NODE-LEAF"}, &protocol.DTNAck{
		BundleID: "bundle-1",
		OK:       1,
	})
	if err != nil {
		t.Fatalf("dispatch DTN ack: %v", err)
	}
	if !aliveInSnapshot(topo, "NODE-LEAF") {
		t.Fatalf("expected DTN ack sender to be marked alive")
	}
}

func TestRescueResponseRefreshesSenderLiveness(t *testing.T) {
	topo := topology.NewTopology()
	go topo.Run()
	t.Cleanup(topo.Stop)

	addNode(t, topo, "NODE-ROOT", protocol.ADMIN_UUID, "10.0.0.1", true)
	addNode(t, topo, "NODE-LEAF", "NODE-ROOT", "10.0.0.2", false)
	requestTopo(t, topo, &topology.TopoTask{Mode: topology.MARKNODEOFFLINE, UUID: "NODE-LEAF"})
	if aliveInSnapshot(topo, "NODE-LEAF") {
		t.Fatalf("expected leaf to start offline")
	}

	core := &routerCore{topo: topo}
	handler := core.dispatchRescueResponse()
	err := handler(context.Background(), &protocol.Header{Sender: "NODE-LEAF"}, &protocol.RescueResponse{
		TargetUUID:  "NODE-ROOT",
		RescuerUUID: "NODE-LEAF",
		Status:      1,
	})
	if err != nil {
		t.Fatalf("dispatch rescue response: %v", err)
	}
	if !aliveInSnapshot(topo, "NODE-LEAF") {
		t.Fatalf("expected rescue response sender to be marked alive")
	}
}

func TestNodeConnInfoRefreshesPayloadLiveness(t *testing.T) {
	topo := topology.NewTopology()
	go topo.Run()
	t.Cleanup(topo.Stop)

	addNode(t, topo, "NODE-ROOT", protocol.ADMIN_UUID, "10.0.0.1", true)
	addNode(t, topo, "NODE-LEAF", "NODE-ROOT", "10.0.0.2", false)
	requestTopo(t, topo, &topology.TopoTask{Mode: topology.MARKNODEOFFLINE, UUID: "NODE-LEAF"})
	if aliveInSnapshot(topo, "NODE-LEAF") {
		t.Fatalf("expected leaf to start offline")
	}

	core := &routerCore{topo: topo}
	handler := core.dispatchNodeConnInfo()
	err := handler(context.Background(), &protocol.Header{Sender: "NODE-ROOT"}, &protocol.NodeConnInfo{
		UUID:            "NODE-LEAF",
		DialAddress:     "10.0.0.2:42000",
		ListenPort:      42000,
		Transport:       "raw",
		LastSuccessUnix: time.Now().Unix(),
	})
	if err != nil {
		t.Fatalf("dispatch node conn info: %v", err)
	}
	if !aliveInSnapshot(topo, "NODE-LEAF") {
		t.Fatalf("expected node connection info payload uuid to be marked alive")
	}
}

func aliveInSnapshot(topo *topology.Topology, uuid string) bool {
	if topo == nil || uuid == "" {
		return false
	}
	for _, node := range topo.UISnapshot("", "").Nodes {
		if node.UUID == uuid {
			return node.IsAlive
		}
	}
	return false
}
