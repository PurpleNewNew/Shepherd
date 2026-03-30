package topology

import (
	"container/heap"
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"codeberg.org/agnoie/shepherd/protocol"
)

type DijkstraNode struct {
	uuid     string
	distance uint32
	index    int
}

type PriorityQueue []*DijkstraNode

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].distance < pq[j].distance
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*DijkstraNode)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // 避免内存泄露
	item.index = -1 // 出于安全考虑
	*pq = old[0 : n-1]
	return item
}

func clonePath(path []string) []string {
	if len(path) == 0 {
		return nil
	}
	cp := make([]string, len(path))
	copy(cp, path)
	return cp
}

func (topology *Topology) buildRouteInfoFromPath(path []string) *RouteInfo {
	info := &RouteInfo{
		Path: clonePath(path),
	}
	switch len(path) {
	case 0:
		info.Depth = -1
		return info
	case 1:
		info.Depth = 0
	default:
		info.Depth = len(path) - 1
		info.Entry = path[1]
	}
	if len(path) > 1 {
		info.Display = topology.buildRouteFromPath(path)
	}
	return info
}

func (topology *Topology) assignRouteInfo(uuid string, path []string, depth map[string]int, infos map[string]*RouteInfo) {
	info := topology.buildRouteInfoFromPath(path)
	if infos != nil {
		infos[uuid] = info
	}
	if depth != nil {
		depth[uuid] = info.Depth
	}
}

func (topology *Topology) bfsShortestPath(start, end string) []string {
	// 如果开始等于结束，只返回节点
	if start == end {
		return []string{start}
	}

	// BFS初始化
	queue := [][]string{{start}} // 路径队列
	visited := make(map[string]bool)
	visited[start] = true

	for len(queue) > 0 {
		// 出队第一个路径
		path := queue[0]
		queue = queue[1:]

		// 获取路径中的最后一个节点
		lastNode := path[len(path)-1]

		// 获取最后一个节点的邻居
		neighbors := topology.edges[lastNode]

		// 检查所有邻居
		for _, neighbor := range neighbors {
			// 跳过已访问的节点
			if visited[neighbor] {
				continue
			}

			// 通过添加邻居创建新路径
			newPath := make([]string, len(path)+1)
			copy(newPath, path)
			newPath[len(path)] = neighbor

			// 如果找到目标节点，返回路径
			if neighbor == end {
				return newPath
			}

			// 标记为已访问
			visited[neighbor] = true

			// 将新路径入队
			queue = append(queue, newPath)
		}
	}

	// 未找到路径
	return []string{}
}

func (topology *Topology) getRoute(task *TopoTask) {
	if task == nil || task.UUID == "" {
		topology.ResultChan <- &topoResult{}
		return
	}
	if !topology.matchesNetwork(task.UUID, strings.TrimSpace(task.NetworkID)) {
		topology.ResultChan <- &topoResult{}
		return
	}
	if info := topology.routeInfoUnlocked(task.UUID); info != nil {
		topology.ResultChan <- &topoResult{Route: info.Display}
		return
	}
	topology.ResultChan <- &topoResult{}
}

func (topology *Topology) weightedEdgeCost(from, to string) uint32 {
	if topology.edgeWeights == nil {
		return 0
	}
	if neighbors, ok := topology.edgeWeights[from]; ok {
		if weight, exists := neighbors[to]; exists && weight > 0 {
			return weight
		}
	}
	return 0
}

func (topology *Topology) dijkstraShortestPath(ctx context.Context, start, end string, weightFn func(string, string) uint32, defaultWeight uint32) ([]string, error) {
	// 检查上下文取消信号
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if start == end {
		return []string{start}, nil
	}

	// 初始化距离和前驱节点
	distances := make(map[string]uint32)
	previous := make(map[string]string)
	pq := make(PriorityQueue, 0)

	// 将初始距离设置为无穷大
	for nodeUUID := range topology.edges {
		distances[nodeUUID] = math.MaxUint32
	}
	distances[start] = 0

	// 将起始节点添加到优先队列
	heap.Push(&pq, &DijkstraNode{uuid: start, distance: 0})

	for pq.Len() > 0 {
		// 检查上下文
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		// 提取距离最小的节点
		current := heap.Pop(&pq).(*DijkstraNode)
		currentUUID := current.uuid

		// 如果到达目标，重构路径
		if currentUUID == end {
			var path []string
			for currentUUID != "" {
				path = append([]string{currentUUID}, path...)
				currentUUID = previous[currentUUID]
			}
			return path, nil
		}

		// 如果已经找到更短的路径，跳过
		if current.distance > distances[currentUUID] {
			continue
		}

		// 更新到邻居的距离
		if neighbors, exists := topology.edges[currentUUID]; exists {
			for _, neighborUUID := range neighbors {
				weight := defaultWeight
				if weightFn != nil {
					if custom := weightFn(currentUUID, neighborUUID); custom > 0 {
						weight = custom
					}
				}

				newDistance := current.distance + weight
				if newDistance < distances[neighborUUID] {
					distances[neighborUUID] = newDistance
					previous[neighborUUID] = currentUUID
					heap.Push(&pq, &DijkstraNode{uuid: neighborUUID, distance: newDistance})
				}
			}
		}
	}

	// 未找到路径
	return nil, fmt.Errorf("no path found from %s to %s", start, end)
}

func (topology *Topology) calculateWeightedRoutes() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	newDepth := make(map[string]int)
	newInfos := make(map[string]*RouteInfo)

	// 对于每个节点，使用Dijkstra计算到所有其他节点的最短路径
	for currentIDNum := range topology.nodes {
		currentID := topology.nodes[currentIDNum].uuid

		var path []string
		if currentID == protocol.ADMIN_UUID {
			path = []string{protocol.ADMIN_UUID}
		} else {
			var err error
			path, err = topology.dijkstraShortestPath(ctx, protocol.ADMIN_UUID, currentID, topology.weightedEdgeCost, 1)
			if err != nil {
				path = nil
			}
		}
		topology.assignRouteInfo(currentID, path, newDepth, newInfos)
	}

	topology.ensureAdminRouteInfo(newDepth, newInfos)

	topology.routeInfo = newInfos
	topology.depthCache = newDepth
	topology.lastUpdateTime = time.Now()

	topology.ResultChan <- &topoResult{} // 通知上游处理完成
}

func (topology *Topology) calculateLatencyRoutes() {
	// 使用时间依赖的 ETTD 最早到达算法；无样本也可运行（会用默认边成本）。
	newDepth := make(map[string]int)
	newInfos := make(map[string]*RouteInfo)

	prev, _ := topology.earliestArrivalFrom(time.Now(), protocol.ADMIN_UUID)

	// 为每个节点还原路径
	for idNum := range topology.nodes {
		uuid := topology.nodes[idNum].uuid
		// ADMIN 自身
		if uuid == protocol.ADMIN_UUID {
			topology.assignRouteInfo(uuid, []string{protocol.ADMIN_UUID}, newDepth, newInfos)
			continue
		}
		// 还原路径：从 uuid 回溯至 ADMIN
		var rev []string
		u := uuid
		// 防御性上限，避免环
		steps := 0
		for u != "" && steps <= len(topology.nodes)+4 {
			rev = append(rev, u)
			if u == protocol.ADMIN_UUID {
				break
			}
			p, ok := prev[u]
			if !ok || p == u {
				// 不可达或异常
				rev = nil
				break
			}
			u = p
			steps++
		}
		// 反向得到从 ADMIN→uuid 的路径
		var path []string
		if len(rev) > 0 && rev[len(rev)-1] == protocol.ADMIN_UUID {
			for i := len(rev) - 1; i >= 0; i-- {
				path = append(path, rev[i])
			}
		}
		topology.assignRouteInfo(uuid, path, newDepth, newInfos)
	}

	topology.ensureAdminRouteInfo(newDepth, newInfos)

	topology.routeInfo = newInfos
	topology.depthCache = newDepth
	topology.lastUpdateTime = time.Now()

	topology.ResultChan <- &topoResult{}
}

func (topology *Topology) setRoutingStrategy(strategy RoutingStrategy) {
	topology.routingStrategy.Store(int32(strategy))

	// 根据新策略重新计算路由
	switch strategy {
	case RoutingByWeight:
		topology.calculateWeightedRoutes()
	case RoutingByHops:
		topology.calculate() // 使用现有的BFS算法
	case RoutingByLatency:
		topology.calculateLatencyRoutes()
	}
}

func (topology *Topology) calculate() {
	switch RoutingStrategy(topology.routingStrategy.Load()) {
	case RoutingByWeight:
		topology.calculateWeightedRoutes()
	case RoutingByLatency:
		topology.calculateLatencyRoutes()
	default:
		// 对于跳数使用原始BFS计算
		topology.calculateOriginalBFS()
	}
}

func (topology *Topology) calculateOriginalBFS() {
	newDepth := make(map[string]int)
	newInfos := make(map[string]*RouteInfo)

	// 对于每个节点，使用BFS计算到所有其他节点的最短路径
	for currentIDNum := range topology.nodes {
		currentID := topology.nodes[currentIDNum].uuid

		// BFS查找从ADMIN_UUID到currentID的最短路径
		var path []string
		if currentID == protocol.ADMIN_UUID {
			path = []string{protocol.ADMIN_UUID}
		} else {
			path = topology.bfsShortestPath(protocol.ADMIN_UUID, currentID)
		}
		topology.assignRouteInfo(currentID, path, newDepth, newInfos)
	}

	topology.ensureAdminRouteInfo(newDepth, newInfos)

	topology.routeInfo = newInfos
	topology.depthCache = newDepth
	topology.lastUpdateTime = time.Now()

	topology.ResultChan <- &topoResult{} // 通知上游处理完成
}

func (topology *Topology) buildRouteFromPath(path []string) string {
	if len(path) <= 1 {
		return ""
	}
	segments := make([]string, 0, len(path)-1)
	for i := 1; i < len(path); i++ {
		prev := path[i-1]
		hop := path[i]
		segment := hop
		if topology.isSupplementalEdge(prev, hop) {
			segment = hop + "#supp"
		}
		segments = append(segments, segment)
	}
	return strings.Join(segments, ":")
}

func (topology *Topology) routeInfoUnlocked(uuid string) *RouteInfo {
	if topology == nil {
		return nil
	}
	if info, ok := topology.routeInfo[uuid]; ok {
		return info
	}
	return nil
}

func (topology *Topology) RouteInfo(uuid string) *RouteInfo {
	if topology == nil {
		return nil
	}
	topology.mu.RLock()
	defer topology.mu.RUnlock()
	return topology.routeInfoUnlocked(uuid)
}

func (topology *Topology) ensureAdminRouteInfo(depth map[string]int, infos map[string]*RouteInfo) {
	if infos == nil {
		return
	}
	if _, ok := infos[protocol.ADMIN_UUID]; ok {
		return
	}
	info := &RouteInfo{
		Path:  []string{protocol.ADMIN_UUID},
		Depth: 0,
	}
	infos[protocol.ADMIN_UUID] = info
	if depth != nil {
		depth[protocol.ADMIN_UUID] = 0
	}
}
