package topology

import (
	"time"

	"codeberg.org/agnoie/shepherd/internal/kelpie/printer"
	"codeberg.org/agnoie/shepherd/protocol"
)

// ApplySnapshot 根据持久化快照重新构建内存中的拓扑。
func (topology *Topology) ApplySnapshot(snapshot *Snapshot) {
	if topology == nil || snapshot == nil {
		return
	}
	topology.mu.Lock()
	defer topology.mu.Unlock()
	// 重置核心结构
	topology.nodes = make(map[int]*node)
	topology.parentByChild = make(map[string]string)
	topology.childrenByParent = make(map[string]map[string]struct{})
	topology.edges = make(map[string][]string)
	topology.edgeWeights = make(map[string]map[string]uint32)
	topology.edgeTypes = make(map[string]map[string]EdgeType)
	topology.routeInfo = make(map[string]*RouteInfo)
	topology.history = make(map[string]int)
	topology.uuidIndex = make(map[string]int)
	topology.depthCache = make(map[string]int)
	topology.targetNetworks = make(map[string]string)
	topology.networks = make(map[string]*Network)
	topology.lastUpdateTime = time.Now()

	// 还原节点信息
	for idx, snap := range snapshot.Nodes {
		n := &node{
			uuid:            snap.UUID,
			currentIP:       snap.IP,
			listenPort:      snap.ListenPort,
			dialAddress:     snap.DialAddress,
			fallbackPort:    snap.FallbackPort,
			transport:       snap.Transport,
			tlsEnabled:      snap.TLSEnabled,
			lastSuccess:     snap.LastSuccess,
			repairFailures:  snap.RepairFailures,
			repairUpdated:   snap.RepairUpdated,
			currentHostname: snap.Hostname,
			currentUser:     snap.Username,
			memo:            snap.Memo,
			lastSeen:        snap.LastSeen,
			isAlive:         snap.IsAlive,
		}
		topology.nodes[idx] = n
		topology.history[snap.UUID] = idx
		topology.uuidIndex[snap.UUID] = idx
		topology.setParentRelationLocked(snap.UUID, snap.Parent)
	}
	topology.currentIDNum = len(snapshot.Nodes)

	// 还原网络映射
	for targetUUID, networkID := range snapshot.Networks {
		topology.restoreNetwork(targetUUID, networkID)
	}
	for _, snap := range snapshot.Nodes {
		if snap.UUID == "" || snap.UUID == protocol.ADMIN_UUID {
			continue
		}
		if topology.networkForUnlocked(snap.UUID) != "" {
			continue
		}
		networkID := topology.selectNetworkID(snap.UUID, snap.Parent, snap.Network)
		topology.restoreNetwork(snap.UUID, networkID)
	}

	// 还原边及邻居关系
	addNeighbor := func(from, to string) {
		list := topology.edges[from]
		for _, existing := range list {
			if existing == to {
				return
			}
		}
		topology.edges[from] = append(list, to)
	}
	for _, edge := range snapshot.Edges {
		if edge.From == "" || edge.To == "" {
			continue
		}
		addNeighbor(edge.From, edge.To)
		addNeighbor(edge.To, edge.From)
		if topology.edgeWeights[edge.From] == nil {
			topology.edgeWeights[edge.From] = make(map[string]uint32)
		}
		topology.edgeWeights[edge.From][edge.To] = edge.Weight
		if topology.edgeWeights[edge.To] == nil {
			topology.edgeWeights[edge.To] = make(map[string]uint32)
		}
		topology.edgeWeights[edge.To][edge.From] = edge.Weight
		if topology.edgeTypes[edge.From] == nil {
			topology.edgeTypes[edge.From] = make(map[string]EdgeType)
		}
		if topology.edgeTypes[edge.To] == nil {
			topology.edgeTypes[edge.To] = make(map[string]EdgeType)
		}
		topology.edgeTypes[edge.From][edge.To] = edge.Type
		topology.edgeTypes[edge.To][edge.From] = edge.Type
	}

	// 快照里可能含有损坏的父子链（例如上一轮运行中 supplemental failover
	// 写出的反向边、或旧版没有 cycle guard 的代码路径）。还原完基础关系
	// 后主动扫一次，遇到环就断开引入环的那条边，保证后续 reonline/reparent
	// 不会被历史负担污染。
	if fixed := topology.sanitizeParentCyclesLocked(); fixed > 0 {
		printer.Warning("\r\n[*] Bootstrap sanitize: broke %d stale reverse edge(s) loaded from snapshot\r\n", fixed)
	}

	// 基于复原后的拓扑重新计算路由。此时拓扑调度器尚未启动，
	// 因此将 ResultChan 切换到静默通道，避免 calculate 中的通知写阻塞。
	originalResultChan := topology.ResultChan
	silentResultChan := make(chan *topoResult, 1)
	topology.ResultChan = silentResultChan
	topology.calculate()
	topology.ResultChan = originalResultChan
}

// sanitizeParentCyclesLocked 扫描当前父子关系映射，对每个节点向上走父链，
// 若遇到环就断开触发环的那条反向边。返回断开的边数。
// 仅在 topology.mu 锁内调用。
func (topology *Topology) sanitizeParentCyclesLocked() int {
	if topology == nil || len(topology.parentByChild) == 0 {
		return 0
	}
	// 先快照出所有 child，避免在删除过程中迭代同一 map。
	candidates := make([]string, 0, len(topology.parentByChild))
	for child := range topology.parentByChild {
		candidates = append(candidates, child)
	}
	broken := 0
	for _, child := range candidates {
		parent := topology.parentOfUnlocked(child)
		if parent == "" || parent == protocol.ADMIN_UUID {
			continue
		}
		broken += topology.breakParentCycleTowardsLocked(child, parent)
	}
	return broken
}
