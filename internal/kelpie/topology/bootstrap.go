package topology

import (
	"time"

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

	// 基于复原后的拓扑重新计算路由。此时拓扑调度器尚未启动，
	// 因此临时替换 ResultChan，避免 calculate 中的通知写阻塞。
	originalResultChan := topology.ResultChan
	silentResultChan := make(chan *topoResult, 1)
	topology.ResultChan = silentResultChan
	topology.calculate()
	topology.ResultChan = originalResultChan
}
