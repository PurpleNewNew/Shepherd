package topology

import "codeberg.org/agnoie/shepherd/protocol"

// NodeRuntimeSnapshot 会在节点存在时返回其运行时元数据。
func (topology *Topology) NodeRuntime(uuid string) (NodeRuntime, bool) {
	if topology == nil || uuid == "" || uuid == protocol.ADMIN_UUID {
		return NodeRuntime{}, false
	}
	topology.mu.RLock()
	defer topology.mu.RUnlock()
	id := topology.id2IDNum(uuid)
	if id < 0 {
		return NodeRuntime{}, false
	}
	n := topology.nodes[id]
	if n == nil || n.uuid == "" {
		return NodeRuntime{}, false
	}
	return NodeRuntime{
		UUID:         n.uuid,
		Memo:         n.memo,
		NetworkID:    topology.networkForUnlocked(n.uuid),
		LastSeen:     n.lastSeen,
		SleepSeconds: n.sleepSeconds,
		WorkSeconds:  n.workSeconds,
		NextWake:     n.nextWake,
	}, true
}
