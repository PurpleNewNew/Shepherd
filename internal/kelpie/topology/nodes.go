package topology

import (
	"strings"
	"time"

	"codeberg.org/agnoie/shepherd/internal/kelpie/printer"
	"codeberg.org/agnoie/shepherd/pkg/config/defaults"
	"codeberg.org/agnoie/shepherd/protocol"
)

const (
	perHopSleepGrace         = 200 * time.Millisecond
	maxAggregatedSleepBudget = 5 * defaults.NodeStaleTimeout
	defaultWorkSeconds       = 10
)

func (topology *Topology) getDepthTask(task *TopoTask) {
	depth := topology.depthOf(task.UUID)
	topology.ResultChan <- &topoResult{Depth: depth}
}

func (topology *Topology) depthOf(uuid string) int {
	if uuid == "" {
		return 0
	}
	if depth, ok := topology.depthCache[uuid]; ok {
		return depth
	}
	depth := 0
	current := uuid
	visited := make(map[string]bool)
	for current != "" && !visited[current] {
		visited[current] = true
		if current == protocol.ADMIN_UUID {
			break
		}
		parent := topology.getParent(current)
		if parent == "" {
			break
		}
		depth++
		current = parent
	}
	return depth
}

func (topology *Topology) getParent(uuid string) string {
	return topology.parentOfUnlocked(uuid)
}

func (topology *Topology) id2IDNum(uuid string) int {
	if uuid == "" {
		return -1
	}
	if id, ok := topology.uuidIndex[uuid]; ok {
		return id
	}
	if id, ok := topology.history[uuid]; ok {
		return id
	}
	return -1
}

func (topology *Topology) idNum2ID(uuidNum int) string {
	if node, ok := topology.nodes[uuidNum]; ok && node != nil {
		return node.uuid
	}
	return ""
}

func (topology *Topology) getUUID(task *TopoTask) {
	uuid := topology.idNum2ID(task.UUIDNum)
	if uuid == "" || !topology.matchesNetwork(uuid, strings.TrimSpace(task.NetworkID)) {
		topology.ResultChan <- &topoResult{}
		return
	}
	topology.ResultChan <- &topoResult{UUID: uuid}
}

func (topology *Topology) getUUIDNum(task *TopoTask) {
	if task == nil || task.UUID == "" {
		topology.ResultChan <- &topoResult{}
		return
	}
	if !topology.matchesNetwork(task.UUID, strings.TrimSpace(task.NetworkID)) {
		topology.ResultChan <- &topoResult{IDNum: -1}
		return
	}
	topology.ResultChan <- &topoResult{IDNum: topology.id2IDNum(task.UUID)}
}

func (topology *Topology) checkNode(task *TopoTask) {
	if node, ok := topology.nodes[task.UUIDNum]; ok {
		target := strings.TrimSpace(task.NetworkID)
		if target == "" || topology.matchesNetwork(node.uuid, target) {
			topology.ResultChan <- &topoResult{IsExist: true}
			return
		}
	}
	topology.ResultChan <- &topoResult{IsExist: false}
}

func (topology *Topology) addNode(task *TopoTask) {
	parentUUID := topology.resolveParentUUID(task.Target.uuid, task.ParentUUID, task.IsFirst)
	topology.setParentRelationLocked(task.Target.uuid, parentUUID)
	if task.Target.workSeconds <= 0 {
		task.Target.workSeconds = defaultWorkSeconds
	}

	networkID := topology.selectNetworkID(task.Target.uuid, parentUUID, task.NetworkID)
	if parentUUID == protocol.ADMIN_UUID || strings.TrimSpace(task.NetworkID) != "" {
		topology.assignNodeNetwork(task.Target.uuid, networkID)
	} else {
		topology.clearNodeNetwork(task.Target.uuid)
	}

	// 添加到节点映射
	topology.nodes[topology.currentIDNum] = task.Target

	// 添加到历史记录
	topology.history[task.Target.uuid] = topology.currentIDNum
	topology.uuidIndex[task.Target.uuid] = topology.currentIDNum

	// 为此节点初始化边
	if _, exists := topology.edges[task.Target.uuid]; !exists {
		topology.edges[task.Target.uuid] = make([]string, 0)
	}

	// 刷新最后更新时间
	topology.lastUpdateTime = time.Now()

	topology.persistNode(task.Target.uuid)

	topology.ResultChan <- &topoResult{IDNum: topology.currentIDNum}

	topology.currentIDNum++
}

func (topology *Topology) updateDetail(task *TopoTask) {
	routingInputsChanged := false
	uuidNum := topology.id2IDNum(task.UUID)
	if uuidNum >= 0 {
		node := topology.nodes[uuidNum]
		if node == nil {
			goto updateLatency
		}
		if task.UserName != "" {
			node.currentUser = task.UserName
		}
		if task.HostName != "" {
			node.currentHostname = task.HostName
		}
		if task.Memo != "" {
			node.memo = task.Memo
		}
		if task.Port != 0 {
			node.listenPort = task.Port
		}
		if task.SleepSeconds >= 0 { // 允许0表示长连接
			if node.sleepSeconds != task.SleepSeconds {
				routingInputsChanged = true
			}
			node.sleepSeconds = task.SleepSeconds
		}
		if task.WorkSeconds > 0 {
			if node.workSeconds != task.WorkSeconds {
				routingInputsChanged = true
			}
			node.workSeconds = task.WorkSeconds
		} else if node.workSeconds <= 0 {
			if node.workSeconds != defaultWorkSeconds {
				routingInputsChanged = true
			}
			node.workSeconds = defaultWorkSeconds
		}
		if task.NextWakeUnix > 0 {
			nextWake := time.Unix(task.NextWakeUnix, 0)
			if !node.nextWake.Equal(nextWake) {
				routingInputsChanged = true
			}
			node.nextWake = nextWake
		}
		if !task.SkipLiveness {
			node.lastSeen = time.Now()
			node.isAlive = true
		}
	}

updateLatency:
	if task.LatencyMs > 0 {
		if topology.applyNodeLatency(task.UUID, task.LatencyMs) {
			routingInputsChanged = true
		}
	}

	topology.persistNode(task.UUID)
	if routingInputsChanged {
		topology.ScheduleCalculate()
	}

	topology.ResultChan <- &topoResult{}
}

func (topology *Topology) updateConnInfo(task *TopoTask) {
	uuidNum := topology.id2IDNum(task.UUID)
	if uuidNum >= 0 {
		node := topology.nodes[uuidNum]
		if task.Port != 0 {
			node.listenPort = task.Port
		}
		if task.DialAddress != "" {
			node.dialAddress = task.DialAddress
		}
		if task.FallbackPort != 0 {
			node.fallbackPort = task.FallbackPort
		}
		if task.Transport != "" {
			node.transport = task.Transport
		}
		if !task.LastSuccess.IsZero() {
			node.lastSuccess = task.LastSuccess
		}
		if task.RepairAttempts >= 0 {
			node.repairFailures = task.RepairAttempts
			node.repairUpdated = time.Now()
		}
		node.tlsEnabled = task.TlsEnabled
		node.lastSeen = time.Now()
	}

	topology.persistNode(task.UUID)

	topology.ResultChan <- &topoResult{}
}

func (topology *Topology) getConnMeta(uuid string) NodeConnectionMeta {
	meta := NodeConnectionMeta{}
	uuidNum := topology.id2IDNum(uuid)
	if uuidNum >= 0 {
		node := topology.nodes[uuidNum]
		meta.ListenPort = node.listenPort
		meta.DialAddress = node.dialAddress
		meta.FallbackPort = node.fallbackPort
		meta.Transport = node.transport
		meta.LastSuccess = node.lastSuccess
		meta.RepairAttempts = node.repairFailures
		meta.TLSEnabled = node.tlsEnabled
	}
	return meta
}

func (topology *Topology) getConnInfo(task *TopoTask) {
	meta := topology.getConnMeta(task.UUID)
	result := &topoResult{
		UUID:           task.UUID,
		DialAddress:    meta.DialAddress,
		FallbackPort:   meta.FallbackPort,
		Transport:      meta.Transport,
		LastSuccess:    meta.LastSuccess,
		RepairFailures: meta.RepairAttempts,
		TLSEnabled:     meta.TLSEnabled,
	}
	uuidNum := topology.id2IDNum(task.UUID)
	if uuidNum >= 0 {
		node := topology.nodes[uuidNum]
		result.Port = node.listenPort
		result.IP = node.currentIP
	}
	topology.ResultChan <- result
}

func (topology *Topology) updateMemo(task *TopoTask) {
	uuidNum := topology.id2IDNum(task.UUID)
	if uuidNum >= 0 {
		topology.nodes[uuidNum].memo = task.Memo
		topology.nodes[uuidNum].lastSeen = time.Now()
		topology.nodes[uuidNum].isAlive = true
	}
	topology.persistNode(task.UUID)
}

func (topology *Topology) delNode(task *TopoTask) {
	var (
		ready     []int
		readyUUID []string
	)

	idNum := topology.id2IDNum(task.UUID)
	if idNum < 0 {
		topology.ResultChan <- &topoResult{}
		return
	}

	topology.findChildrenNodes(&ready, idNum)
	ready = append(ready, idNum)

	seen := make(map[int]struct{}, len(ready))
	for _, nodeID := range ready {
		if _, exists := seen[nodeID]; exists {
			continue
		}
		seen[nodeID] = struct{}{}

		uuid := topology.idNum2ID(nodeID)
		if uuid == "" {
			delete(topology.nodes, nodeID)
			continue
		}

		topology.removeNodeRelations(nodeID)
		topology.deleteNodeRelationsLocked(uuid)
		topology.clearNodeNetwork(uuid)
		readyUUID = append(readyUUID, uuid)
		topology.deleteNode(uuid)
		delete(topology.uuidIndex, uuid)
		delete(topology.history, uuid)
		delete(topology.nodes, nodeID)
		printer.Fail("\r\n[*] Node %d is offline!", nodeID)
	}

	topology.lastUpdateTime = time.Now()
	topology.ResultChan <- &topoResult{AllNodes: readyUUID}
}

func (topology *Topology) findChildrenNodes(ready *[]int, idNum int) {
	topology.findChildrenNodesVisited(ready, idNum, make(map[int]struct{}, 8))
}

func (topology *Topology) findChildrenNodesVisited(ready *[]int, idNum int, visited map[int]struct{}) {
	if _, seen := visited[idNum]; seen {
		return
	}
	visited[idNum] = struct{}{}
	node, ok := topology.nodes[idNum]
	if !ok {
		return
	}
	for _, uuid := range topology.childrenOfUnlocked(node.uuid) {
		childID := topology.id2IDNum(uuid)
		if childID < 0 {
			continue
		}
		if _, seen := visited[childID]; seen {
			continue
		}
		*ready = append(*ready, childID)
		topology.findChildrenNodesVisited(ready, childID, visited)
	}
}

func (topology *Topology) reonlineNode(task *TopoTask) {
	parentUUID := topology.resolveParentUUID(task.Target.uuid, task.ParentUUID, task.IsFirst)
	// Reonline 源自节点重新握手/上线，是一次物理事实声明。若当前内存/持久化
	// 状态里遗留了反向边（常见原因：历史 supplemental failover，或旧版
	// admin.db 残留），应优先信任新声明，主动断开导致环的那条反向边，
	// 而不是把新节点悄悄挂到 ADMIN 下造成"孤立节点"的 UI 假象。
	if topology.createsParentCycle(task.Target.uuid, parentUUID) {
		broken := topology.breakParentCycleTowardsLocked(task.Target.uuid, parentUUID)
		if topology.createsParentCycle(task.Target.uuid, parentUUID) {
			// 破环失败的极端情况下仍保留旧兜底：fallback 到 ADMIN。
			printer.Warning("\r\n[*] Reonline cycle guard: break failed (parent=%s child=%s broken=%d), fallback to ADMIN\r\n",
				parentUUID, task.Target.uuid, broken)
			parentUUID = protocol.ADMIN_UUID
		} else {
			printer.Warning("\r\n[*] Reonline cycle guard: broke %d stale reverse edge(s) before reparenting %s under %s\r\n",
				broken, task.Target.uuid, parentUUID)
		}
	}
	now := time.Now()

	// NodeReonline 仅携带 UUID、IP 和 parent，现有的运行时元数据需继续保留。
	var (
		idNum int
		n     *node
	)
	if existingID, ok := topology.history[task.Target.uuid]; ok {
		idNum = existingID
		if existing := topology.nodes[idNum]; existing != nil && existing.uuid != "" {
			// 只更新连通性和父指针；其余字段保持不变。
			if task.Target.currentIP != "" {
				existing.currentIP = task.Target.currentIP
			}
			if existing.workSeconds <= 0 {
				existing.workSeconds = defaultWorkSeconds
			}
			existing.lastSeen = now
			existing.isAlive = true
			n = existing
		} else {
			topology.nodes[idNum] = task.Target
			n = task.Target
		}
	} else {
		idNum = topology.currentIDNum
		topology.history[task.Target.uuid] = idNum
		topology.nodes[idNum] = task.Target
		topology.currentIDNum++
		n = task.Target
	}
	topology.uuidIndex[task.Target.uuid] = idNum

	topology.setParentRelationLocked(task.Target.uuid, parentUUID)

	networkID := topology.selectNetworkID(task.Target.uuid, parentUUID, task.NetworkID)
	if parentUUID == protocol.ADMIN_UUID || strings.TrimSpace(task.NetworkID) != "" {
		topology.assignNodeNetwork(task.Target.uuid, networkID)
	} else {
		topology.clearNodeNetwork(task.Target.uuid)
	}

	// 如果不存在，为此节点初始化边
	if _, exists := topology.edges[task.Target.uuid]; !exists {
		topology.edges[task.Target.uuid] = make([]string, 0)
	}

	// 标记节点为活跃
	if n != nil {
		n.lastSeen = now
		n.isAlive = true
		if n.workSeconds <= 0 {
			n.workSeconds = defaultWorkSeconds
		}
	}

	// 刷新最后更新时间
	topology.lastUpdateTime = now

	topology.persistNode(task.Target.uuid)

	topology.ResultChan <- &topoResult{}
}

func (topology *Topology) resolveParentUUID(uuid, parent string, isFirst bool) string {
	if uuid == protocol.ADMIN_UUID {
		return ""
	}
	switch parent {
	case protocol.TEMP_UUID:
		return protocol.ADMIN_UUID
	case "":
		if isFirst {
			return protocol.ADMIN_UUID
		}
		return ""
	default:
		return parent
	}
}

func (topology *Topology) getNodeMeta(task *TopoTask) {
	idNum := topology.id2IDNum(task.UUID)
	if idNum < 0 {
		topology.ResultChan <- &topoResult{}
		return
	}
	if !topology.matchesNetwork(task.UUID, strings.TrimSpace(task.NetworkID)) {
		topology.ResultChan <- &topoResult{}
		return
	}

	childrenCopy := topology.childrenOfUnlocked(task.UUID)

	topology.ResultChan <- &topoResult{
		Parent:   topology.parentOfUnlocked(task.UUID),
		Children: childrenCopy,
	}
}

func (topology *Topology) getNodeInfo(task *TopoTask) {
	uuidNum := topology.id2IDNum(task.UUID)
	if uuidNum < 0 {
		topology.ResultChan <- &topoResult{}
		return
	}
	node := topology.nodes[uuidNum]
	if !topology.matchesNetwork(node.uuid, strings.TrimSpace(task.NetworkID)) {
		topology.ResultChan <- &topoResult{}
		return
	}
	topology.ResultChan <- &topoResult{
		UUID:  node.uuid,
		IP:    node.currentIP,
		Port:  node.listenPort,
		Depth: topology.depthOf(node.uuid),
	}
}

func (topology *Topology) getAllNodes(task *TopoTask) {
	if topology == nil {
		topology.ResultChan <- &topoResult{}
		return
	}
	network := ""
	if task != nil {
		network = strings.TrimSpace(task.NetworkID)
	}
	nodes := make([]string, 0, len(topology.nodes))
	for _, node := range topology.nodes {
		if node == nil || node.uuid == "" || node.uuid == protocol.ADMIN_UUID {
			continue
		}
		// 仅返回在线节点，避免调度器等组件将离线节点视为可用。
		if !node.isAlive {
			continue
		}
		if network != "" && !topology.matchesNetwork(node.uuid, network) {
			continue
		}
		nodes = append(nodes, node.uuid)
	}
	topology.ResultChan <- &topoResult{AllNodes: nodes}
}

// MarkAllOffline 将当前已知的所有节点标记为离线（不含 ADMIN）。
// 用于 Kelpie 重启后清除快照中的在线标记，等待新的上线消息重新置位。
func (topology *Topology) MarkAllOffline() {
	if topology == nil {
		return
	}
	topology.mu.Lock()
	defer topology.mu.Unlock()
	for _, n := range topology.nodes {
		if n == nil || n.uuid == "" || n.uuid == protocol.ADMIN_UUID {
			continue
		}
		if n.isAlive {
			n.isAlive = false
			// 持久化更新，避免下次启动再次误判
			topology.persistNode(n.uuid)
		}
	}
}

// pruneOffline 删除所有“离线”的节点（及其离线子树），并返回被删除的 UUID 列表。
// 注意：为避免误删，若某离线节点存在任意在线后代，则跳过该节点（保留其记录）。
func (topology *Topology) pruneOffline(task *TopoTask) {
	if topology == nil {
		return
	}
	// 收集离线候选：父节点在线或不存在，且自身离线
	candidates := make([]int, 0)
	for id, n := range topology.nodes {
		if n == nil || n.uuid == "" || n.uuid == protocol.ADMIN_UUID {
			continue
		}
		if n.isAlive {
			continue
		}
		// 若父节点也离线，则由更高层处理，避免重复
		parentOffline := false
		if p := topology.parentOfUnlocked(n.uuid); p != "" {
			if pid := topology.id2IDNum(p); pid >= 0 {
				if pn := topology.nodes[pid]; pn != nil && !pn.isAlive {
					parentOffline = true
				}
			}
		}
		if parentOffline {
			continue
		}
		// 若该离线节点的子树中存在在线节点，则跳过（保守处理）
		if topology.hasOnlineDescendant(id) {
			continue
		}
		candidates = append(candidates, id)
	}

	pruned := make([]string, 0)

	// 实施删除：仅删除离线子树
	for _, rootID := range candidates {
		// 重新校验仍存在且离线
		root := topology.nodes[rootID]
		if root == nil || root.isAlive || root.uuid == "" || root.uuid == protocol.ADMIN_UUID {
			continue
		}

		// 收集子树下所有节点（包括 root）
		ready := []int{rootID}
		topology.findChildrenNodes(&ready, rootID)

		// 仅删除离线节点，在线节点跳过（不会出现在 candidates 的子树中，但保持防御）
		for _, id := range ready {
			n := topology.nodes[id]
			if n == nil || n.uuid == "" || n.uuid == protocol.ADMIN_UUID {
				continue
			}
			if n.isAlive {
				continue
			}
			uuid := n.uuid
			topology.removeNodeRelations(id)
			topology.deleteNodeRelationsLocked(uuid)
			topology.clearNodeNetwork(uuid)
			pruned = append(pruned, uuid)
			topology.deleteNode(uuid) // 持久化删除
			delete(topology.uuidIndex, uuid)
			delete(topology.history, uuid)
			delete(topology.nodes, id)
		}
	}

	if len(pruned) > 0 {
		topology.lastUpdateTime = time.Now()
	}
	topology.ResultChan <- &topoResult{AllNodes: pruned}
}

// markNodeOffline 会把给定节点（及其后代子树）标记为离线。
// 与 DELNODE 不同，它会保留拓扑中的节点和边，使 DTN 仍能计算路由并 carry-forward bundle。
func (topology *Topology) markNodeOffline(task *TopoTask) {
	if topology == nil || task == nil {
		return
	}
	uuid := strings.TrimSpace(task.UUID)
	idNum := topology.id2IDNum(uuid)
	if uuid == "" || idNum < 0 {
		topology.ResultChan <- &topoResult{}
		return
	}

	ready := []int{idNum}
	topology.findChildrenNodes(&ready, idNum)

	now := time.Now()
	changed := false
	offline := make([]string, 0, len(ready))
	for _, id := range ready {
		n := topology.nodes[id]
		if n == nil || n.uuid == "" || n.uuid == protocol.ADMIN_UUID {
			continue
		}
		offline = append(offline, n.uuid)
		if n.isAlive {
			n.isAlive = false
			topology.persistNode(n.uuid)
			changed = true
		}
	}
	if changed {
		topology.lastUpdateTime = now
	}
	topology.ResultChan <- &topoResult{AllNodes: offline}
}

// markStaleOffline 遍历节点，将超过阈值未更新(lastSeen)的节点标记为离线。
// 不执行删除，仅置位 isAlive=false 并持久化，供 detail/GETALLNODES/prune 等使用。
func (topology *Topology) markStaleOffline() {
	if topology == nil {
		return
	}
	now := time.Now()
	changed := false
	baseGrace := topology.zeroSleepGrace
	if baseGrace <= 0 {
		baseGrace = defaults.NodeStaleTimeout
	}
	for _, n := range topology.nodes {
		if n == nil || n.uuid == "" || n.uuid == protocol.ADMIN_UUID {
			continue
		}
		if !n.isAlive {
			continue
		}
		if n.lastSeen.IsZero() {
			continue
		}
		threshold := baseGrace + topology.pathSleepBudget(n.uuid)
		if now.Sub(n.lastSeen) > threshold {
			n.isAlive = false
			topology.persistNode(n.uuid)
			changed = true
		}
	}
	if changed {
		topology.lastUpdateTime = now
	}
	topology.ResultChan <- &topoResult{}
}

func (topology *Topology) perHopSleepBudget(uuid string) time.Duration {
	if topology == nil || uuid == "" {
		return 0
	}
	n := topology.nodeByUUID(uuid)
	if n == nil || n.sleepSeconds <= 0 {
		return 0
	}
	waitSec := dutyCycleWaitSeconds(n.sleepSeconds, n.workSeconds)
	if waitSec <= 0 {
		return 0
	}
	dur := time.Duration(waitSec * float64(time.Second))
	if dur <= 0 {
		return 0
	}
	return dur
}

func (topology *Topology) pathSleepBudget(uuid string) time.Duration {
	if topology == nil || uuid == "" {
		return 0
	}
	info := topology.routeInfoUnlocked(uuid)
	if info == nil || len(info.Path) == 0 {
		if topology.staleTimeout > 0 {
			return topology.staleTimeout
		}
		return 0
	}
	total := time.Duration(0)
	seen := make(map[string]struct{})
	for _, hop := range info.Path {
		if hop == "" || hop == protocol.ADMIN_UUID {
			continue
		}
		if _, exists := seen[hop]; exists {
			continue
		}
		seen[hop] = struct{}{}
		total += topology.perHopSleepBudget(hop)
		total += perHopSleepGrace
		if total >= maxAggregatedSleepBudget {
			return maxAggregatedSleepBudget
		}
	}
	if total == 0 && topology.staleTimeout > 0 {
		return topology.staleTimeout
	}
	if total > maxAggregatedSleepBudget {
		return maxAggregatedSleepBudget
	}
	return total
}

// PathSleepBudget 是一个导出辅助函数，用于返回给定节点当前路由上的累计 sleep 预算。
func (topology *Topology) PathSleepBudget(uuid string) time.Duration {
	if topology == nil || uuid == "" {
		return 0
	}
	topology.mu.RLock()
	defer topology.mu.RUnlock()
	return topology.pathSleepBudget(uuid)
}

func (topology *Topology) WorkSeconds(uuid string) int {
	if topology == nil || uuid == "" {
		return defaultWorkSeconds
	}
	topology.mu.RLock()
	defer topology.mu.RUnlock()
	if node := topology.nodeByUUID(uuid); node != nil && node.workSeconds > 0 {
		return node.workSeconds
	}
	return defaultWorkSeconds
}

// hasOnlineDescendant 判断该 id 节点的后代中是否存在在线节点。
func (topology *Topology) hasOnlineDescendant(id int) bool {
	node, ok := topology.nodes[id]
	if !ok || node == nil {
		return false
	}
	for _, cu := range topology.childrenOfUnlocked(node.uuid) {
		cid := topology.id2IDNum(cu)
		if cid < 0 {
			continue
		}
		cn := topology.nodes[cid]
		if cn == nil {
			continue
		}
		if cn.isAlive {
			return true
		}
		if topology.hasOnlineDescendant(cid) {
			return true
		}
	}
	return false
}

// removeNodeRelations 移除一个节点的边关系以及父子引用，但不删除节点本身。
func (topology *Topology) removeNodeRelations(id int) {
	node, ok := topology.nodes[id]
	if !ok || node == nil {
		return
	}
	nodeUUID := node.uuid
	if nodeUUID == "" {
		return
	}
	// 移除与其相连的边（双向）
	if neighbors, exists := topology.edges[nodeUUID]; exists {
		for _, neighborUUID := range neighbors {
			if revNeighbors, exists := topology.edges[neighborUUID]; exists {
				for i, revNeighbor := range revNeighbors {
					if revNeighbor == nodeUUID {
						topology.edges[neighborUUID] = append(revNeighbors[:i], revNeighbors[i+1:]...)
						break
					}
				}
			}
			topology.deleteEdge(nodeUUID, neighborUUID)
			topology.deleteEdge(neighborUUID, nodeUUID)
		}
		delete(topology.edges, nodeUUID)
	}
	topology.removeParentRelationLocked(nodeUUID)
}

func (topology *Topology) reparentNode(task *TopoTask) {
	if task == nil || task.UUID == "" {
		topology.ResultChan <- &topoResult{}
		return
	}
	nodeID := topology.id2IDNum(task.UUID)
	if nodeID < 0 {
		topology.ResultChan <- &topoResult{}
		return
	}
	oldParent := topology.parentOfUnlocked(task.UUID)
	newParent := topology.resolveParentUUID(task.UUID, task.ParentUUID, false)
	if topology.createsParentCycle(task.UUID, newParent) {
		printer.Warning("\r\n[*] Reparent cycle guard: reject parent %s for %s, fallback to ADMIN\r\n",
			newParent, task.UUID)
		newParent = protocol.ADMIN_UUID
	}
	if oldParent == newParent {
		topology.ResultChan <- &topoResult{}
		return
	}

	topology.setParentRelationLocked(task.UUID, newParent)

	networkID := topology.selectNetworkID(task.UUID, newParent, task.NetworkID)
	if newParent == protocol.ADMIN_UUID || strings.TrimSpace(task.NetworkID) != "" {
		topology.assignNodeNetwork(task.UUID, networkID)
	} else {
		topology.clearNodeNetwork(task.UUID)
	}

	topology.lastUpdateTime = time.Now()
	topology.ResultChan <- &topoResult{}
}

func (topology *Topology) createsParentCycle(nodeUUID, proposedParent string) bool {
	nodeUUID = strings.TrimSpace(nodeUUID)
	proposedParent = strings.TrimSpace(proposedParent)
	if nodeUUID == "" || proposedParent == "" || proposedParent == protocol.ADMIN_UUID {
		return false
	}
	current := proposedParent
	visited := make(map[string]struct{}, 8)
	for current != "" && current != protocol.ADMIN_UUID {
		if current == nodeUUID {
			return true
		}
		if _, seen := visited[current]; seen {
			// 现有父链已经损坏成环，也视为不可接受的新父节点。
			return true
		}
		visited[current] = struct{}{}
		current = topology.parentOfUnlocked(current)
	}
	return false
}
