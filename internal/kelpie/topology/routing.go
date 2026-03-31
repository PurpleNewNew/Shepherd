package topology

import (
	"strings"
	"time"

	"codeberg.org/agnoie/shepherd/protocol"
)

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

func (topology *Topology) calculate() {
	// 固定使用时间依赖的 ETTD 最早到达算法；无样本也可运行（会用默认边成本）。
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
