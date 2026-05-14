package process

import (
	"context"
	"fmt"
	"math"
	"strings"

	"codeberg.org/agnoie/shepherd/internal/kelpie/manager"
	"codeberg.org/agnoie/shepherd/internal/kelpie/printer"
	"codeberg.org/agnoie/shepherd/internal/kelpie/supp"
	"codeberg.org/agnoie/shepherd/internal/kelpie/topology"
	"codeberg.org/agnoie/shepherd/pkg/config/defaults"
	"codeberg.org/agnoie/shepherd/protocol"
)

const topoOpTimeout = defaults.TopologyRequestTimeout

func topoRequest(ctx context.Context, topo *topology.Topology, task *topology.TopoTask) (*topology.Result, error) {
	if topo == nil {
		return nil, topology.ErrNilTopology
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return topo.Request(ctx, task)
}

func topoRequestDefault(topo *topology.Topology, task *topology.TopoTask) (*topology.Result, error) {
	ctx, cancel := context.WithTimeout(context.Background(), topoOpTimeout)
	defer cancel()
	return topoRequest(ctx, topo, task)
}

func topoExecute(topo *topology.Topology, task *topology.TopoTask) error {
	_, err := topoRequestDefault(topo, task)
	return err
}

func networkForNode(topo *topology.Topology, uuid string) string {
	if topo == nil || uuid == "" {
		return ""
	}
	return topo.NetworkFor(uuid)
}

func sameNetwork(topo *topology.Topology, a, b string) bool {
	if topo == nil {
		return true
	}
	networkA := networkForNode(topo, a)
	networkB := networkForNode(topo, b)
	if networkA == "" || networkB == "" {
		return true
	}
	return networkA == networkB
}

func nodeOffline(mgr *manager.Manager, topo *topology.Topology, uuid string) {
	// DTN/短连接友好：不删除拓扑节点，仅标记离线，保留路由信息以便 carry-forward。
	result, err := topoRequestDefault(topo, &topology.TopoTask{Mode: topology.MARKNODEOFFLINE, UUID: uuid})
	if err != nil || result == nil {
		printer.Warn("ADMIN_CHILDREN_MARK_OFFLINE", true, err, "mark node %s offline failed", uuid)
		return
	}
	allNodes := result.AllNodes

	for _, nodeUUID := range allNodes {
		supp.PublishNodeRemoved(nodeUUID)
	}
}

type failoverCleanupFunc func(failedParent, child, newParent string)

func attemptSupplementalFailover(mgr *manager.Manager, topo *topology.Topology, uuid string, cleanup ...failoverCleanupFunc) bool {
	if topo == nil || uuid == "" {
		diagSuppFailover("invalid_input", uuid, "", "", nil, "topology_nil_or_empty_uuid")
		return false
	}

	candidates := supp.GetActiveSuppNeighbors(uuid)
	if len(candidates) == 0 {
		diagSuppFailover("no_candidates", uuid, "", "", nil, "active_supp_neighbors=0")
		return false
	}

	parentUUID := fetchParentUUID(topo, uuid)
	if parentUUID == "" {
		diagSuppFailover("no_parent", uuid, "", "", candidates, "parent_lookup_empty")
		return false
	}

	if !sameNetwork(topo, uuid, parentUUID) {
		diagSuppFailover("reject_parent_network", uuid, parentUUID, "", candidates,
			fmt.Sprintf("node_network=%s parent_network=%s", networkForNode(topo, uuid), networkForNode(topo, parentUUID)))
		return false
	}

	target := selectFailoverCandidate(topo, uuid, parentUUID, candidates)
	if target == "" {
		diagSuppFailover("no_target", uuid, parentUUID, "", candidates, "no_valid_candidate")
		return false
	}

	if !sameNetwork(topo, uuid, target) {
		diagSuppFailover("reject_target_network", uuid, parentUUID, target, candidates,
			fmt.Sprintf("node_network=%s target_network=%s", networkForNode(topo, uuid), networkForNode(topo, target)))
		return false
	}

	if prt, ok := topo.NodeRuntime(parentUUID); ok && prt.SleepSeconds > 0 {
		// A duty-cycled parent can disappear during its normal sleep window.
		// Do not mutate the tree until a later repair/rescue path proves that
		// the parent is genuinely unavailable; otherwise a harmless sleep can
		// leave a child reparented away without promoting/quarantining the old
		// parent.
		supp.PublishNodeRemoved(uuid)
		diagSuppFailover("skip_reparent_parent_sleepy", uuid, parentUUID, target, candidates,
			"publish_node_removed_only")
		return false
	}
	diagSuppFailover("reparent_selected", uuid, parentUUID, target, candidates, "")

	if !reparentTree(topo, uuid, target) {
		diagSuppFailover("reparent_rejected", uuid, parentUUID, target, candidates,
			fmt.Sprintf("current_parent=%s", shortID(fetchParentUUID(topo, uuid))))
		return false
	}
	detachBrokenTreeEdge(topo, parentUUID, uuid)

	// Failover 命令必须能够立即路由；如果路由计算带去抖，
	// 就可能与消息下发竞争，把晋升消息卡在已断裂的父边之后。
	if topo != nil {
		if err := topoExecute(topo, &topology.TopoTask{Mode: topology.CALCULATE}); err != nil {
			printer.Warn("ADMIN_CHILDREN_FAILOVER_CALC", true, err, "calculate routes after reparent %s->%s failed", uuid, target)
		}
	}

	if linkUUID := supp.FindSuppLinkUUID(uuid, target); linkUUID != "" {
		diagSuppFailover("start_promote", uuid, parentUUID, target, candidates,
			fmt.Sprintf("link_uuid=%s", shortID(linkUUID)))
		supp.StartSuppFailover(topo, mgr, linkUUID, target, uuid)
		for _, fn := range cleanup {
			if fn != nil {
				fn(parentUUID, uuid, target)
			}
		}
	} else {
		diagSuppFailover("no_link_uuid", uuid, parentUUID, target, candidates,
			fmt.Sprintf("supp_link_not_found target=%s", shortID(target)))
	}

	printer.Warning("\r\n[*] Node %s lost parent %s, failover via supplemental link to %s\r\n",
		shortID(uuid), shortID(parentUUID), shortID(target))
	diagSuppFailover("done", uuid, parentUUID, target, candidates, "")
	return true
}

// salvageOfflineSubtree 会尝试为离线节点的后代执行 failover。
//
// NodeOffline 由原父节点上报。如果这个父节点已经死亡，更深层的后代不会被逐个上报，
// 但它们仍可能持有可被晋升的存活 supplemental 链路。
func salvageOfflineSubtree(mgr *manager.Manager, topo *topology.Topology, offlineUUID string, cleanup ...failoverCleanupFunc) {
	if topo == nil || offlineUUID == "" {
		return
	}

	visited := make(map[string]struct{}, 16)
	visited[offlineUUID] = struct{}{}
	queue := []string{offlineUUID}

	for len(queue) > 0 {
		parent := queue[0]
		queue = queue[1:]

		res, err := topoRequestDefault(topo, &topology.TopoTask{Mode: topology.GETNODEMETA, UUID: parent})
		if err != nil || res == nil {
			continue
		}
		for _, child := range res.Children {
			child = strings.TrimSpace(child)
			if child == "" {
				continue
			}
			if _, ok := visited[child]; ok {
				continue
			}
			visited[child] = struct{}{}

			// 递归之前先尝试给子节点换父：如果 failover 成功，
			// 它就会从离线子树中脱离出来，后续的 MARKNODEOFFLINE
			// 也就不会再把它一并盲目标记为离线。
			_ = attemptSupplementalFailover(mgr, topo, child, cleanup...)
			queue = append(queue, child)
		}
	}
}

func nodeReonline(mgr *manager.Manager, topo *topology.Topology, mess *protocol.NodeReonline, cleanup ...failoverCleanupFunc) {
	oldParent := fetchParentUUID(topo, mess.UUID)
	oldParentUnavailable := false
	if oldParent != "" && oldParent != mess.ParentUUID && oldParent != protocol.ADMIN_UUID && topo != nil {
		status, ok := topo.NodeStatus(oldParent)
		oldParentUnavailable = !ok || status != topology.NodeStatusOnline
	}

	node := topology.NewNode(mess.UUID, mess.IP)

	if err := topoExecute(topo, &topology.TopoTask{
		Mode:       topology.REONLINENODE,
		Target:     node,
		ParentUUID: mess.ParentUUID,
		IsFirst:    false,
	}); err != nil {
		printer.Warn("ADMIN_CHILDREN_REONLINE_NODE", true, err, "reonline node %s failed", mess.UUID)
		return
	}

	// 在父节点和此节点之间添加边
	edgeType := topology.TreeEdge
	// NodeReonline 只携带 parent/uuid/ip，并不包含边类型；但如果 parent<->child 已存在补边，
	// 这里的 ADDEDGE 不能把它覆盖成 TreeEdge，否则路由会丢失 "#supp"，转发端会优先走 primary，
	// 在 duty-cycled sleep 场景下容易触发连接抖动与 DTN ACK timeout。
	for _, peer := range supp.GetActiveSuppNeighbors(mess.UUID) {
		if peer == mess.ParentUUID {
			edgeType = topology.SupplementalEdge
			break
		}
	}
	if edgeType != topology.SupplementalEdge {
		// 回退到持久化的拓扑状态（例如重启后，或心跳暂时缺失时）。
		if topo != nil && (topo.IsSupplementalEdge(mess.ParentUUID, mess.UUID) || topo.IsSupplementalEdge(mess.UUID, mess.ParentUUID)) {
			edgeType = topology.SupplementalEdge
		}
	}
	if err := topoExecute(topo, &topology.TopoTask{
		Mode:         topology.ADDEDGE,
		UUID:         mess.ParentUUID,
		NeighborUUID: mess.UUID,
		EdgeType:     edgeType,
	}); err != nil {
		printer.Warn("ADMIN_CHILDREN_ADD_EDGE", true, err, "add edge for reonline node %s failed", mess.UUID)
	}

	// 重新计算路由（节流）
	if topo != nil {
		topo.ScheduleCalculate()
	}

	result, err := topoRequestDefault(topo, &topology.TopoTask{Mode: topology.GETUUIDNUM, UUID: mess.UUID})
	if err != nil || result == nil {
		printer.Warn("ADMIN_CHILDREN_FETCH_ID", true, err, "fetch id for reonline node %s failed", mess.UUID)
		return
	}

	printer.Success("\r\n[*] Node %d is reonline!", result.IDNum)
	supp.PublishNodeAdded(mess.UUID)
	if oldParentUnavailable {
		for _, fn := range cleanup {
			if fn != nil {
				fn(oldParent, mess.UUID, mess.ParentUUID)
			}
		}
	}
}

func DispatchChildrenMess(ctx context.Context, mgr *manager.Manager, topo *topology.Topology, onNodeReonline func(string), onFailoverCleanup ...failoverCleanupFunc) {
	for {
		var message interface{}
		select {
		case <-ctx.Done():
			return
		case message = <-mgr.ChildrenManager.ChildrenMessChan:
		}

		switch mess := message.(type) {
		case *protocol.NodeOffline:
			// 总是优先尝试挽救后代：当中间节点离线时
			// （无论是死亡还是 duty-cycled 睡眠），更深层的后代
			// 已经不能再依赖这个离线父节点回传上行 ACK，
			// 但它们可能仍然拥有可晋升的 supplemental 链路。
			salvageOfflineSubtree(mgr, topo, mess.UUID, onFailoverCleanup...)

			// duty-cycled 节点（sleepSeconds>0）会主动断开与父节点的连接；
			// 若试图通过 supplemental failover 为它们换父，
			// 往往只会造成抖动，并与睡眠窗口竞争。让它们按正常流程自行重连即可。
			if rt, ok := topo.NodeRuntime(mess.UUID); ok && rt.SleepSeconds > 0 {
				nodeOffline(mgr, topo, mess.UUID)
				break
			}

			if !attemptSupplementalFailover(mgr, topo, mess.UUID, onFailoverCleanup...) {
				nodeOffline(mgr, topo, mess.UUID)
			}
		case *protocol.NodeReonline:
			nodeReonline(mgr, topo, mess, onFailoverCleanup...)
			if onNodeReonline != nil {
				onNodeReonline(strings.TrimSpace(mess.UUID))
			}
		}
	}
}

func selectFailoverCandidate(topo *topology.Topology, uuid, parent string, candidates []string) string {
	best := ""
	bestDepth := math.MaxInt32
	nodeNetwork := networkForNode(topo, uuid)
	for _, peer := range candidates {
		if peer == "" || peer == uuid || peer == parent {
			diagSuppFailoverCandidate(uuid, parent, peer, "invalid_peer", "")
			continue
		}
		if !topo.IsSupplementalEdge(uuid, peer) {
			diagSuppFailoverCandidate(uuid, parent, peer, "not_supp_edge", "")
			continue
		}
		if nodeNetwork != "" {
			peerNetwork := networkForNode(topo, peer)
			if peerNetwork != "" && peerNetwork != nodeNetwork {
				diagSuppFailoverCandidate(uuid, parent, peer, "network_mismatch",
					fmt.Sprintf("node_network=%s peer_network=%s", nodeNetwork, peerNetwork))
				continue
			}
		}
		if parentChainIncludes(topo, peer, uuid) {
			diagSuppFailoverCandidate(uuid, parent, peer, "parent_chain_loops_back", "")
			continue
		}
		route := fetchRouteString(topo, peer)
		if route == "" {
			diagSuppFailoverCandidate(uuid, parent, peer, "empty_route", "")
			continue
		}
		if routeIncludesUUID(route, uuid) {
			diagSuppFailoverCandidate(uuid, parent, peer, "route_loops_back",
				fmt.Sprintf("route=%s", route))
			continue
		}
		depth := fetchNodeDepth(topo, peer)
		if depth <= 0 {
			parts := strings.Split(route, ":")
			depth = len(parts)
		}
		if depth < bestDepth {
			bestDepth = depth
			best = peer
			diagSuppFailoverCandidate(uuid, parent, peer, "selected",
				fmt.Sprintf("depth=%d route=%s", depth, route))
		}
	}
	if best == "" {
		diagSuppFailoverCandidate(uuid, parent, "", "no_valid_candidate", fmt.Sprintf("candidates=%s", shortIDList(candidates)))
	}
	return best
}

func parentChainIncludes(topo *topology.Topology, uuid, ancestor string) bool {
	uuid = strings.TrimSpace(uuid)
	ancestor = strings.TrimSpace(ancestor)
	if topo == nil || uuid == "" || ancestor == "" {
		return false
	}
	visited := make(map[string]struct{}, 8)
	current := uuid
	for current != "" && current != protocol.ADMIN_UUID {
		if current == ancestor {
			return true
		}
		if _, seen := visited[current]; seen {
			return false
		}
		visited[current] = struct{}{}
		current = fetchParentUUID(topo, current)
	}
	return false
}

func detachBrokenTreeEdge(topo *topology.Topology, parent, child string) {
	if parent == "" || child == "" {
		return
	}
	if topo.IsSupplementalEdge(parent, child) {
		return
	}
	if err := topoExecute(topo, &topology.TopoTask{
		Mode:         topology.REMOVEEDGE,
		UUID:         parent,
		NeighborUUID: child,
	}); err != nil {
		printer.Warn("ADMIN_CHILDREN_DETACH_EDGE", true, err, "detach tree edge %s-%s failed", parent, child)
	}
}

func reparentTree(topo *topology.Topology, child, newParent string) bool {
	if err := topoExecute(topo, &topology.TopoTask{
		Mode:       topology.REPARENTNODE,
		UUID:       child,
		ParentUUID: newParent,
	}); err != nil {
		printer.Warn("ADMIN_CHILDREN_REPARENT_NODE", true, err, "reparent node %s to %s failed", child, newParent)
		return false
	}
	return fetchParentUUID(topo, child) == newParent
}

func fetchParentUUID(topo *topology.Topology, uuid string) string {
	result, err := topoRequestDefault(topo, &topology.TopoTask{Mode: topology.GETNODEMETA, UUID: uuid})
	if err != nil || result == nil {
		printer.Warn("ADMIN_CHILDREN_FETCH_PARENT", true, err, "fetch parent for node %s failed", uuid)
		return ""
	}
	return result.Parent
}

func fetchNodeDepth(topo *topology.Topology, uuid string) int {
	result, err := topoRequestDefault(topo, &topology.TopoTask{Mode: topology.GETDEPTH, UUID: uuid})
	if err != nil || result == nil {
		printer.Warn("ADMIN_CHILDREN_FETCH_DEPTH", true, err, "fetch depth for node %s failed", uuid)
		return 0
	}
	return result.Depth
}

func fetchRouteString(topo *topology.Topology, uuid string) string {
	result, err := topoRequestDefault(topo, &topology.TopoTask{Mode: topology.GETROUTE, UUID: uuid})
	if err != nil || result == nil {
		printer.Warn("ADMIN_CHILDREN_FETCH_ROUTE", true, err, "fetch route for node %s failed", uuid)
		return ""
	}
	return result.Route
}

func shortIDList(ids []string) string {
	if len(ids) == 0 {
		return "-"
	}
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		out = append(out, shortID(id))
	}
	if len(out) == 0 {
		return "-"
	}
	return strings.Join(out, ",")
}

func diagSuppFailover(stage, uuid, parentUUID, targetUUID string, candidates []string, detail string) {
	if strings.TrimSpace(detail) == "" {
		detail = "-"
	}
	printer.Warning("\r\n[diag][supp_failover] stage=%s node=%s parent=%s target=%s candidates=%s detail=%s\r\n",
		stage,
		shortID(uuid),
		shortID(parentUUID),
		shortID(targetUUID),
		shortIDList(candidates),
		detail,
	)
}

func diagSuppFailoverCandidate(uuid, parentUUID, peerUUID, reason, detail string) {
	if strings.TrimSpace(detail) == "" {
		detail = "-"
	}
	printer.Warning("\r\n[diag][supp_failover_candidate] node=%s parent=%s peer=%s reason=%s detail=%s\r\n",
		shortID(uuid),
		shortID(parentUUID),
		shortID(peerUUID),
		reason,
		detail,
	)
}
