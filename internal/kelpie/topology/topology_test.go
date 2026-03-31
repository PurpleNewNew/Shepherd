package topology

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"codeberg.org/agnoie/shepherd/protocol"
)

func TestTopologyBFSCreatesExpectedRoute(t *testing.T) {
	topology := NewTopology()
	topology.ResultChan = make(chan *topoResult, 10)

	adminNode := NewNode(protocol.ADMIN_UUID, "127.0.0.1")
	topology.addNode(&TopoTask{
		Target:  adminNode,
		IsFirst: true,
	})

	nodeA := NewNode("NODE_AAA", "10.0.0.1")
	topology.addNode(&TopoTask{
		Target:     nodeA,
		ParentUUID: protocol.ADMIN_UUID,
	})

	nodeB := NewNode("NODE_BBB", "10.0.0.2")
	topology.addNode(&TopoTask{
		Target:     nodeB,
		ParentUUID: nodeA.uuid,
	})

	topology.addEdge(&TopoTask{
		UUID:         protocol.ADMIN_UUID,
		NeighborUUID: nodeA.uuid,
	})
	topology.addEdge(&TopoTask{
		UUID:         nodeA.uuid,
		NeighborUUID: nodeB.uuid,
	})

	topology.calculateOriginalBFS()

	expected := nodeA.uuid + ":" + nodeB.uuid
	info := topology.RouteInfo(nodeB.uuid)
	if info == nil {
		t.Fatalf("expected route info for nodeB")
	}
	if info.Display != expected {
		t.Fatalf("unexpected route to nodeB, want %s got %s", expected, info.Display)
	}
	if info.Depth != 2 {
		t.Fatalf("expected depth 2 for nodeB, got %d", info.Depth)
	}
}

func TestSupplementalEdgeType(t *testing.T) {
	topology := NewTopology()
	topology.ResultChan = make(chan *topoResult, 10)

	n1 := NewNode("NODE_A", "10.0.0.1")
	topology.addNode(&TopoTask{Target: n1, IsFirst: true})
	n2 := NewNode("NODE_B", "10.0.0.2")
	topology.addNode(&TopoTask{Target: n2, ParentUUID: protocol.ADMIN_UUID})

	topology.addEdge(&TopoTask{
		UUID:         n1.uuid,
		NeighborUUID: n2.uuid,
		EdgeType:     SupplementalEdge,
	})

	if !topology.isSupplementalEdge(n1.uuid, n2.uuid) {
		t.Fatalf("expected supplemental edge flag")
	}
}

func TestNetworkAssignment(t *testing.T) {
	topology := NewTopology()
	topology.ResultChan = make(chan *topoResult, 10)

	adminNode := NewNode(protocol.ADMIN_UUID, "127.0.0.1")
	topology.addNode(&TopoTask{Target: adminNode, IsFirst: true})

	entryA := NewNode("ENTRY-A", "10.0.0.1")
	topology.addNode(&TopoTask{Target: entryA, ParentUUID: protocol.TEMP_UUID, IsFirst: true})
	entryB := NewNode("ENTRY-B", "10.0.0.2")
	topology.addNode(&TopoTask{Target: entryB, ParentUUID: protocol.TEMP_UUID, IsFirst: true})
	topology.addEdge(&TopoTask{UUID: protocol.ADMIN_UUID, NeighborUUID: entryA.uuid})
	topology.addEdge(&TopoTask{UUID: protocol.ADMIN_UUID, NeighborUUID: entryB.uuid})

	child := NewNode("CHILD-A", "10.0.0.3")
	topology.addNode(&TopoTask{Target: child, ParentUUID: entryA.uuid})
	topology.addEdge(&TopoTask{UUID: entryA.uuid, NeighborUUID: child.uuid})

	topology.calculate()

	if netID := topology.NetworkFor(entryA.uuid); netID != entryA.uuid {
		t.Fatalf("expected default network to match entry, got %s", netID)
	}

	if err := topology.SetNetwork(entryA.uuid, "NET-A"); err != nil {
		t.Fatalf("SetEntryNetwork failed: %v", err)
	}
	if err := topology.SetNetwork(entryB.uuid, "NET-B"); err != nil {
		t.Fatalf("SetEntryNetwork failed: %v", err)
	}

	if netID := topology.NetworkFor(child.uuid); netID != "NET-A" {
		t.Fatalf("expected child to inherit parent's network NET-A, got %s", netID)
	}

	if entries := topology.NetworkEntries("NET-B"); len(entries) != 1 || entries[0] != entryB.uuid {
		t.Fatalf("expected network NET-B to contain ENTRY-B, got %v", entries)
	}

	if err := topology.SetNetwork(entryB.uuid, "NET-A"); err != nil {
		t.Fatalf("SetEntryNetwork reassign failed: %v", err)
	}

	ids := topology.NetworkIDs()
	if len(ids) != 1 || ids[0] != "NET-A" {
		t.Fatalf("expected only NET-A remaining, got %v", ids)
	}
}

func TestNetworkForBreaksOnParentCycle(t *testing.T) {
	topology := NewTopology()
	topology.ResultChan = make(chan *topoResult, 10)

	adminNode := NewNode(protocol.ADMIN_UUID, "127.0.0.1")
	topology.addNode(&TopoTask{Target: adminNode, IsFirst: true})

	a := NewNode("NODE-A", "10.0.0.1")
	b := NewNode("NODE-B", "10.0.0.2")
	topology.addNode(&TopoTask{Target: a, ParentUUID: protocol.ADMIN_UUID})
	topology.addNode(&TopoTask{Target: b, ParentUUID: a.uuid})

	aid := topology.id2IDNum(a.uuid)
	bid := topology.id2IDNum(b.uuid)
	if aid < 0 || bid < 0 {
		t.Fatalf("unexpected ids: aid=%d bid=%d", aid, bid)
	}

	// 模拟一个损坏的父链环。
	topology.setParentRelationLocked(a.uuid, b.uuid)
	topology.setParentRelationLocked(b.uuid, a.uuid)

	done := make(chan struct{})
	go func() {
		_ = topology.NetworkFor(a.uuid)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(300 * time.Millisecond):
		t.Fatalf("NetworkFor stuck on parent cycle")
	}
}

func TestReparentNodeGuardsAgainstCycle(t *testing.T) {
	topology := NewTopology()
	go topology.Run()
	defer topology.Stop()

	adminNode := NewNode(protocol.ADMIN_UUID, "127.0.0.1")
	waitForTask(t, topology, &TopoTask{Mode: ADDNODE, Target: adminNode, IsFirst: true})

	a := NewNode("NODE-A", "10.0.0.1")
	b := NewNode("NODE-B", "10.0.0.2")
	waitForTask(t, topology, &TopoTask{Mode: ADDNODE, Target: a, ParentUUID: protocol.ADMIN_UUID})
	waitForTask(t, topology, &TopoTask{Mode: ADDNODE, Target: b, ParentUUID: a.uuid})

	// 把 A 重新挂到自己的后代 B 之下应当被拒绝，并保留原有父节点。
	waitForTask(t, topology, &TopoTask{Mode: REPARENTNODE, UUID: a.uuid, ParentUUID: b.uuid})

	metaA := waitForTask(t, topology, &TopoTask{Mode: GETNODEMETA, UUID: a.uuid})
	if metaA == nil || metaA.Parent != protocol.ADMIN_UUID {
		t.Fatalf("expected NODE-A parent remain ADMIN, got %+v", metaA)
	}
	metaB := waitForTask(t, topology, &TopoTask{Mode: GETNODEMETA, UUID: b.uuid})
	if metaB == nil || metaB.Parent != a.uuid {
		t.Fatalf("expected NODE-B parent remain NODE-A, got %+v", metaB)
	}
}

func TestReonlinePreservesSleepMetadata(t *testing.T) {
	topology := NewTopology()
	topology.ResultChan = make(chan *topoResult, 10)

	adminNode := NewNode(protocol.ADMIN_UUID, "127.0.0.1")
	topology.addNode(&TopoTask{Target: adminNode, IsFirst: true})

	parent := NewNode("PARENT", "10.0.0.1")
	topology.addNode(&TopoTask{Target: parent, ParentUUID: protocol.ADMIN_UUID})
	topology.addEdge(&TopoTask{UUID: protocol.ADMIN_UUID, NeighborUUID: parent.uuid})

	child := NewNode("CHILD", "10.0.0.2")
	topology.addNode(&TopoTask{Target: child, ParentUUID: parent.uuid})
	topology.addEdge(&TopoTask{UUID: parent.uuid, NeighborUUID: child.uuid})

	nextWake := time.Now().Add(2 * time.Minute).Unix()
	topology.updateDetail(&TopoTask{
		UUID:         child.uuid,
		SleepSeconds: 8,
		WorkSeconds:  2,
		NextWakeUnix: nextWake,
	})

	before, ok := topology.NodeRuntime(child.uuid)
	if !ok {
		t.Fatalf("expected runtime for child before reonline")
	}
	if before.SleepSeconds != 8 || before.WorkSeconds != 2 || before.NextWake.IsZero() {
		t.Fatalf("unexpected child runtime before reonline: sleep=%d work=%d nextWake=%v", before.SleepSeconds, before.WorkSeconds, before.NextWake)
	}

	// Reonline 应更新连通性与父指针，但不能覆盖
	// duty-cycled 的 sleep 元数据。
	reonline := NewNode(child.uuid, "10.0.0.99")
	topology.reonlineNode(&TopoTask{
		Target:     reonline,
		ParentUUID: parent.uuid,
		IsFirst:    false,
	})

	after, ok := topology.NodeRuntime(child.uuid)
	if !ok {
		t.Fatalf("expected runtime for child after reonline")
	}
	if after.SleepSeconds != 8 || after.WorkSeconds != 2 || after.NextWake.IsZero() {
		t.Fatalf("unexpected child runtime after reonline: sleep=%d work=%d nextWake=%v", after.SleepSeconds, after.WorkSeconds, after.NextWake)
	}
}

func TestTopologyHighVolumeTasks(t *testing.T) {
	topology := NewTopology()
	go topology.Run()
	defer topology.Stop()

	adminNode := NewNode(protocol.ADMIN_UUID, "127.0.0.1")
	waitForTask(t, topology, &TopoTask{Mode: ADDNODE, Target: adminNode, IsFirst: true})

	const nodes = 64
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for i := 0; i < nodes; i++ {
			uuid := fmt.Sprintf("NODE-%04d", i)
			node := NewNode(uuid, fmt.Sprintf("10.0.0.%d", i+10))
			waitForTask(t, topology, &TopoTask{Mode: ADDNODE, Target: node, ParentUUID: protocol.ADMIN_UUID})
			waitForTask(t, topology, &TopoTask{Mode: ADDEDGE, UUID: protocol.ADMIN_UUID, NeighborUUID: uuid})
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < nodes; i++ {
			waitForTask(t, topology, &TopoTask{Mode: CALCULATE})
		}
	}()

	wg.Wait()
	waitForTask(t, topology, &TopoTask{Mode: CALCULATE})

	for i := 0; i < nodes; i++ {
		uuid := fmt.Sprintf("NODE-%04d", i)
		if res := waitForTask(t, topology, &TopoTask{Mode: GETROUTE, UUID: uuid}); res == nil || res.Route == "" {
			t.Fatalf("missing route for %s", uuid)
		}
	}
}

func TestTopologyConcurrentReadersDoNotPanic(t *testing.T) {
	topology := NewTopology()
	go topology.Run()
	defer topology.Stop()

	adminNode := NewNode(protocol.ADMIN_UUID, "127.0.0.1")
	waitForTask(t, topology, &TopoTask{Mode: ADDNODE, Target: adminNode, IsFirst: true})

	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)

	// Writer：在读侧持续调用公开 API 的同时修改拓扑（包括删除）。
	go func() {
		defer wg.Done()
		for i := 0; i < 128; i++ {
			uuid := fmt.Sprintf("NODE-%04d", i)
			node := NewNode(uuid, fmt.Sprintf("10.0.0.%d", i+10))
			waitForTask(t, topology, &TopoTask{Mode: ADDNODE, Target: node, ParentUUID: protocol.ADMIN_UUID})
			waitForTask(t, topology, &TopoTask{Mode: ADDEDGE, UUID: protocol.ADMIN_UUID, NeighborUUID: uuid})
			waitForTask(t, topology, &TopoTask{Mode: UPDATEDETAIL, UUID: uuid, SleepSeconds: i % 3, WorkSeconds: 2})
			if i%7 == 0 && i > 0 {
				// 删除操作曾通过 id2IDNum 触发并发 map 读写 panic。
				duuid := fmt.Sprintf("NODE-%04d", i-1)
				waitForTask(t, topology, &TopoTask{Mode: DELNODE, UUID: duuid})
			}
			if i%9 == 0 {
				waitForTask(t, topology, &TopoTask{Mode: CALCULATE})
			}
		}
		close(done)
	}()

	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
			}

			// 在拓扑任务循环修改 map 时，这些调用也应该保持安全。
			_, _ = topology.NodeRuntime("NODE-0001")
			_ = topology.UISnapshot("", "")
			_ = topology.RootTargets()
			_ = topology.NetworkFor("NODE-0001")
			_ = topology.RecommendSendDelay("NODE-0001", time.Now())
			_ = topology.PathSleepBudget("NODE-0001")
			_ = topology.WorkSeconds("NODE-0001")
			_ = topology.NetworkIDs()
			_ = topology.NetworkEntries("NODE-0001")

			time.Sleep(1 * time.Millisecond)
		}
	}()

	wg.Wait()
}

func waitForTask(t *testing.T, topology *Topology, task *TopoTask) *topoResult {
	t.Helper()
	if topology == nil {
		t.Fatalf("nil topology")
	}
	response := make(chan *topoResult, 1)
	taskCopy := *task
	taskCopy.Response = response
	if err := topology.Enqueue(&taskCopy); err != nil {
		t.Fatalf("enqueue task %+v: %v", task, err)
	}
	select {
	case res := <-response:
		return res
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for task %+v", task)
	}
	return nil
}
