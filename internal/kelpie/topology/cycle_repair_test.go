package topology

import (
	"testing"

	"codeberg.org/agnoie/shepherd/protocol"
)

// TestBreakParentCycleTowardsLockedBasic 覆盖 helper 的基本行为：
// 构造 A → B & B → A 反向环，从 A 出发破环后，B.parent 应为空。
func TestBreakParentCycleTowardsLockedBasic(t *testing.T) {
	topology := NewTopology()
	topology.ResultChan = make(chan *topoResult, 10)

	adminNode := NewNode(protocol.ADMIN_UUID, "127.0.0.1")
	topology.addNode(&TopoTask{Target: adminNode, IsFirst: true})

	a := NewNode("NODE-A", "10.0.0.1")
	b := NewNode("NODE-B", "10.0.0.2")
	topology.addNode(&TopoTask{Target: a, ParentUUID: protocol.ADMIN_UUID})
	topology.addNode(&TopoTask{Target: b, ParentUUID: a.uuid})

	// 手动污染：A → B 和 B → A，形成 2-环。
	topology.setParentRelationLocked(a.uuid, b.uuid)
	topology.setParentRelationLocked(b.uuid, a.uuid)

	// 调 helper：想把 A 挂在 B 下（声明 A.parent=B），应先断掉反向边 B.parent=A。
	broken := topology.breakParentCycleTowardsLocked(a.uuid, b.uuid)
	if broken == 0 {
		t.Fatalf("expected at least 1 reverse edge broken")
	}
	if got := topology.parentOfUnlocked(b.uuid); got != "" {
		t.Fatalf("expected B.parent cleared after break, got %q", got)
	}
	// 至此可以安全地设 A.parent=B。
	topology.setParentRelationLocked(a.uuid, b.uuid)
	if topology.createsParentCycle(a.uuid, topology.parentOfUnlocked(a.uuid)) {
		t.Fatalf("still cyclic after repair")
	}
}

// TestReonlineNodeBreaksStaleReverseEdgeAndAcceptsDeclaredParent 复现
// 生产现象：kelpie 内存/db 里残留了 parent→child 的反向边，导致新节点
// 以 child→parent 做 reonline 时触发 cycle guard，被 fallback 到 ADMIN
// 呈现"孤立节点"。修复后期望新声明胜出，反向边被打破。
func TestReonlineNodeBreaksStaleReverseEdgeAndAcceptsDeclaredParent(t *testing.T) {
	topology := NewTopology()
	topology.ResultChan = make(chan *topoResult, 10)

	adminNode := NewNode(protocol.ADMIN_UUID, "127.0.0.1")
	topology.addNode(&TopoTask{Target: adminNode, IsFirst: true})

	mid := NewNode("NODE-MID", "10.0.0.1")
	leaf := NewNode("NODE-LEAF", "10.0.0.2")
	topology.addNode(&TopoTask{Target: mid, ParentUUID: protocol.ADMIN_UUID})
	topology.addNode(&TopoTask{Target: leaf, ParentUUID: mid.uuid})

	// 污染：假设上一轮 supplemental failover 把 mid.parent 写成了 leaf。
	topology.setParentRelationLocked(mid.uuid, leaf.uuid)
	if !topology.createsParentCycle(leaf.uuid, mid.uuid) {
		t.Fatalf("precondition failed: expected cycle between leaf and mid")
	}

	// leaf 重新握手上来，声明 parent=mid。
	reonline := NewNode(leaf.uuid, "10.0.0.99")
	topology.reonlineNode(&TopoTask{
		Target:     reonline,
		ParentUUID: mid.uuid,
		IsFirst:    false,
	})

	// 关键断言：leaf.parent 应被接受为 mid（而不是回落到 ADMIN）。
	if got := topology.parentOfUnlocked(leaf.uuid); got != mid.uuid {
		t.Fatalf("expected leaf.parent=%s after reonline, got %q", mid.uuid, got)
	}
	// 反向残留边应已被打破（mid.parent 不再指向 leaf）。
	if got := topology.parentOfUnlocked(mid.uuid); got == leaf.uuid {
		t.Fatalf("expected mid.parent cleared, still points to leaf")
	}
}

// TestReonlineNodeFallsBackWhenCycleCannotBeBroken 守住极端 case：
// 若破环之后仍然存在环（理论上不该发生，防御式），应回落到原先的
// ADMIN fallback，保证老的"永不悄悄卡死"语义不丢。
func TestReonlineNodeFallsBackWhenCycleCannotBeBroken(t *testing.T) {
	// 这个行为目前由 reonlineNode 的 double-check + fallback 路径保证。
	// 此处通过构造"自回路"（A 声称自己是自己的父）间接触发：
	// createsParentCycle 会视之为环；break 逻辑删掉 A.parent 后再查，
	// 此时环已消除，fallback 不会触发——因此此用例主要起回归
	// 守位作用，断言"自回路被接受并得到 ADMIN 兜底或新声明生效"。
	topology := NewTopology()
	topology.ResultChan = make(chan *topoResult, 10)

	adminNode := NewNode(protocol.ADMIN_UUID, "127.0.0.1")
	topology.addNode(&TopoTask{Target: adminNode, IsFirst: true})

	a := NewNode("NODE-A", "10.0.0.1")
	topology.addNode(&TopoTask{Target: a, ParentUUID: protocol.ADMIN_UUID})
	topology.setParentRelationLocked(a.uuid, a.uuid) // 自环毒化

	reonline := NewNode(a.uuid, "10.0.0.1")
	topology.reonlineNode(&TopoTask{
		Target:     reonline,
		ParentUUID: a.uuid, // 仍声明自己为父，典型毒化场景
		IsFirst:    false,
	})

	got := topology.parentOfUnlocked(a.uuid)
	if got == a.uuid {
		t.Fatalf("A.parent must never remain pointing to itself after reonline, got %q", got)
	}
}

// TestApplySnapshotSanitizesParentCycles 验证 bootstrap 阶段的自洗：
// 给 ApplySnapshot 一个带反向环的 snapshot，load 完后不应残留环。
func TestApplySnapshotSanitizesParentCycles(t *testing.T) {
	topology := NewTopology()

	snap := &Snapshot{
		Nodes: []NodeSnapshot{
			{UUID: protocol.ADMIN_UUID, IP: "127.0.0.1", IsAlive: true},
			// 污染：上一轮 supplemental failover 留下的反向边，
			// mid.parent=leaf 同时 leaf.parent=mid。
			{UUID: "NODE-MID", Parent: "NODE-LEAF", IP: "10.0.0.1", IsAlive: true},
			{UUID: "NODE-LEAF", Parent: "NODE-MID", IP: "10.0.0.2", IsAlive: true},
		},
		Networks: map[string]string{},
	}

	topology.ApplySnapshot(snap)

	// 对两个节点都做一遍环检测，任何一条链都不该再有环。
	midParent := topology.parentOfUnlocked("NODE-MID")
	leafParent := topology.parentOfUnlocked("NODE-LEAF")
	// 环要求两者互为父；sanitize 之后至少其中一条应该被断掉。
	if midParent == "NODE-LEAF" && leafParent == "NODE-MID" {
		t.Fatalf("snapshot cycle should have been sanitized, got mid.parent=%s leaf.parent=%s",
			midParent, leafParent)
	}
	if topology.createsParentCycle("NODE-MID", midParent) {
		t.Fatalf("NODE-MID still in cycle after sanitize, parent=%s", midParent)
	}
	if topology.createsParentCycle("NODE-LEAF", leafParent) {
		t.Fatalf("NODE-LEAF still in cycle after sanitize, parent=%s", leafParent)
	}
}
