package process

import (
	"fmt"
	"sort"
	"strings"

	"codeberg.org/agnoie/shepherd/internal/kelpie/topology"
	"codeberg.org/agnoie/shepherd/protocol"
)

type NetworkInfo struct {
	ID      string
	Entries []string
}

type pendingRetiredEvent struct {
	link      string
	endpoints []string
	reason    string
}

type pendingPromotionEvent struct {
	link   string
	parent string
	child  string
}

func (admin *Admin) handleNodeAdded(uuid string) {
	if admin == nil || uuid == "" {
		return
	}
	if admin.suppPlanner != nil {
		admin.suppPlanner.OnNodeAdded(uuid)
		return
	}
	admin.suppPendingMu.Lock()
	admin.pendingNodeAdds = append(admin.pendingNodeAdds, uuid)
	admin.suppPendingMu.Unlock()
}

func (admin *Admin) handleNodeRemoved(uuid string) {
	if admin == nil || uuid == "" {
		return
	}
	if admin.suppPlanner != nil {
		admin.suppPlanner.OnNodeRemoved(uuid)
		return
	}
	admin.suppPendingMu.Lock()
	admin.pendingNodeRemovals = append(admin.pendingNodeRemovals, uuid)
	admin.suppPendingMu.Unlock()
}

func (admin *Admin) handleLinkRetired(linkUUID string, endpoints []string, reason string) {
	if admin == nil || linkUUID == "" {
		return
	}
	if admin.suppPlanner != nil {
		admin.suppPlanner.OnLinkRetired(linkUUID, endpoints, reason)
		return
	}
	admin.suppPendingMu.Lock()
	admin.pendingRetired = append(admin.pendingRetired, pendingRetiredEvent{
		link:      linkUUID,
		endpoints: append([]string(nil), endpoints...),
		reason:    reason,
	})
	admin.suppPendingMu.Unlock()
}

func (admin *Admin) handleLinkPromoted(linkUUID, parentUUID, childUUID string) {
	if admin == nil || linkUUID == "" {
		return
	}
	if admin.suppPlanner != nil {
		admin.suppPlanner.OnLinkPromoted(linkUUID, parentUUID, childUUID)
		return
	}
	admin.suppPendingMu.Lock()
	admin.pendingPromotions = append(admin.pendingPromotions, pendingPromotionEvent{
		link:   linkUUID,
		parent: parentUUID,
		child:  childUUID,
	})
	admin.suppPendingMu.Unlock()
}

func (admin *Admin) flushPendingSuppEvents() {
	if admin == nil || admin.suppPlanner == nil {
		return
	}
	admin.suppPendingMu.Lock()
	pending := append([]string(nil), admin.pendingNodeAdds...)
	admin.pendingNodeAdds = nil
	removed := append([]string(nil), admin.pendingNodeRemovals...)
	admin.pendingNodeRemovals = nil
	retired := append([]pendingRetiredEvent(nil), admin.pendingRetired...)
	admin.pendingRetired = nil
	promoted := append([]pendingPromotionEvent(nil), admin.pendingPromotions...)
	admin.pendingPromotions = nil
	admin.suppPendingMu.Unlock()
	for _, uuid := range pending {
		if uuid == "" {
			continue
		}
		admin.suppPlanner.OnNodeAdded(uuid)
	}
	for _, uuid := range removed {
		if uuid == "" {
			continue
		}
		admin.suppPlanner.OnNodeRemoved(uuid)
	}
	for _, evt := range retired {
		admin.suppPlanner.OnLinkRetired(evt.link, evt.endpoints, evt.reason)
	}
	for _, evt := range promoted {
		admin.suppPlanner.OnLinkPromoted(evt.link, evt.parent, evt.child)
	}
}

func (admin *Admin) nodeExists(uuid string) bool {
	task := &topology.TopoTask{
		Mode: topology.GETUUIDNUM,
		UUID: uuid,
	}
	res, err := admin.topoRequest(task)
	if err != nil || res == nil {
		return false
	}
	return res.IDNum >= 0
}

func (admin *Admin) markEntriesOffline() {
	if admin == nil || admin.mgr == nil || admin.topology == nil {
		return
	}
	roots := admin.topology.RootTargets()
	if len(roots) == 0 {
		return
	}
	for _, target := range roots {
		if target == "" {
			continue
		}
		nodeOffline(admin.mgr, admin.topology, target)
	}
}

func (admin *Admin) RequestRepair(uuid string) error {
	if admin == nil {
		return fmt.Errorf("admin unavailable")
	}
	uuid = strings.TrimSpace(uuid)
	if uuid == "" {
		return fmt.Errorf("invalid node uuid")
	}
	if !admin.nodeExists(uuid) {
		return fmt.Errorf("node %s not found", uuid)
	}
	if admin.suppPlanner == nil {
		return fmt.Errorf("supplemental planner unavailable")
	}
	return admin.suppPlanner.RequestManualRepair(uuid)
}

func (admin *Admin) SupplementalStatus() (SupplementalStatusSnapshot, error) {
	if admin == nil || admin.suppPlanner == nil {
		return SupplementalStatusSnapshot{}, fmt.Errorf("supplemental planner unavailable")
	}
	return admin.suppPlanner.StatusSnapshot(), nil
}

func (admin *Admin) SupplementalMetrics() (SupplementalMetricsSnapshot, error) {
	if admin == nil || admin.suppPlanner == nil {
		return SupplementalMetricsSnapshot{}, fmt.Errorf("supplemental planner unavailable")
	}
	return admin.suppPlanner.MetricsSnapshot(), nil
}

func (admin *Admin) SupplementalEvents(limit int) ([]SupplementalPlannerEvent, error) {
	if admin == nil || admin.suppPlanner == nil {
		return nil, fmt.Errorf("supplemental planner unavailable")
	}
	return admin.suppPlanner.EventLog(limit), nil
}

func (admin *Admin) SupplementalQuality(limit int, nodes []string) ([]SupplementalQualitySnapshot, error) {
	if admin == nil || admin.suppPlanner == nil {
		return nil, fmt.Errorf("supplemental planner unavailable")
	}
	return admin.suppPlanner.QualitySnapshot(limit, nodes), nil
}

func (admin *Admin) SupplementalRepairs() ([]RepairStatusSnapshot, error) {
	if admin == nil || admin.suppPlanner == nil {
		return nil, fmt.Errorf("supplemental planner unavailable")
	}
	return admin.suppPlanner.RepairStatuses(), nil
}

// RoutingStrategy 返回当前拓扑的路由策略。
func (admin *Admin) RoutingStrategy() topology.RoutingStrategy {
	if admin == nil || admin.topology == nil {
		return topology.RoutingByLatency
	}
	return admin.topology.RoutingStrategy()
}

// SetRoutingStrategy 在运行时切换拓扑路由策略。
func (admin *Admin) SetRoutingStrategy(strategy topology.RoutingStrategy) error {
	if admin == nil || admin.topology == nil {
		return fmt.Errorf("topology unavailable")
	}
	_, err := admin.topology.Execute(&topology.TopoTask{
		Mode:    topology.SETROUTINGSTRATEGY,
		UUIDNum: int(strategy),
	})
	return err
}

func (admin *Admin) ActiveNetworkID() string {
	if admin == nil {
		return ""
	}
	admin.networkMu.RLock()
	defer admin.networkMu.RUnlock()
	return admin.activeNetwork
}

func (admin *Admin) setActiveNetwork(id string) {
	admin.networkMu.Lock()
	admin.activeNetwork = strings.TrimSpace(id)
	admin.networkMu.Unlock()
}

func (admin *Admin) handleNetworkChange(id string) {
	admin.setActiveNetwork(id)
}

func (admin *Admin) DropEntrySession(entry string) error {
	return admin.DropSession(entry)
}

// DropSession 按节点 UUID 强制移除一个 session/component。
func (admin *Admin) DropSession(target string) error {
	if admin == nil || admin.store == nil {
		return fmt.Errorf("admin unavailable")
	}
	target = strings.TrimSpace(target)
	if target == "" {
		return fmt.Errorf("target uuid required")
	}
	if strings.EqualFold(target, protocol.ADMIN_UUID) {
		return fmt.Errorf("cannot drop admin session")
	}
	comp, ok := admin.store.Component(target)
	if !ok || comp == nil {
		return fmt.Errorf("session %s not found", shorten(target))
	}
	if comp.Conn != nil {
		_ = comp.Conn.Close()
	}
	admin.store.RemoveComponent(target)
	if admin.sessions != nil {
		admin.sessions.remove(target)
	}
	return nil
}

func (admin *Admin) ListNetworks() ([]NetworkInfo, string, error) {
	if admin == nil || admin.topology == nil {
		return nil, "", fmt.Errorf("topology unavailable")
	}
	ids := admin.topology.NetworkIDs()
	if len(ids) == 0 {
		return nil, admin.ActiveNetworkID(), nil
	}
	sort.Strings(ids)
	infos := make([]NetworkInfo, 0, len(ids))
	for _, id := range ids {
		entries := admin.topology.NetworkEntries(id)
		if len(entries) > 1 {
			sort.Strings(entries)
		}
		infos = append(infos, NetworkInfo{ID: id, Entries: entries})
	}
	return infos, admin.ActiveNetworkID(), nil
}

func (admin *Admin) SetActiveNetwork(networkID string) (string, error) {
	if admin == nil || admin.topology == nil {
		return "", fmt.Errorf("topology unavailable")
	}
	id := strings.TrimSpace(networkID)
	if id == "" {
		return "", fmt.Errorf("network id required")
	}
	matches := ""
	for _, candidate := range admin.topology.NetworkIDs() {
		if strings.EqualFold(candidate, id) {
			matches = candidate
			break
		}
	}
	if matches == "" {
		return "", fmt.Errorf("network %s not found", networkID)
	}
	admin.setActiveNetwork(matches)
	return matches, nil
}

func (admin *Admin) ResetActiveNetwork() string {
	if admin == nil {
		return ""
	}
	admin.setActiveNetwork("")
	return ""
}

func (admin *Admin) SetNodeNetwork(target, network string) error {
	if admin == nil || admin.topology == nil {
		return fmt.Errorf("topology unavailable")
	}
	target = strings.TrimSpace(target)
	network = strings.TrimSpace(network)
	if target == "" || network == "" {
		return fmt.Errorf("target and network required")
	}
	if err := admin.topology.SetNetwork(target, network); err != nil {
		return err
	}
	return nil
}

func (admin *Admin) PruneOffline() (int, error) {
	if admin == nil || admin.topology == nil {
		return 0, fmt.Errorf("topology unavailable")
	}
	res, err := admin.topology.Service().Request(admin.context(), &topology.TopoTask{Mode: topology.PRUNEOFFLINE})
	if err != nil {
		return 0, err
	}
	if res == nil || len(res.AllNodes) == 0 {
		return 0, nil
	}
	return len(res.AllNodes), nil
}

func (admin *Admin) messageAllowed(header *protocol.Header) bool {
	activeNetwork := admin.ActiveNetworkID()
	if activeNetwork == "" {
		return true
	}
	if admin == nil || admin.topology == nil || header == nil {
		return true
	}
	candidates := []string{header.Sender, header.Accepter}
	for _, uuid := range candidates {
		if uuid == "" || uuid == protocol.ADMIN_UUID {
			continue
		}
		if network := admin.topology.NetworkFor(uuid); network != "" {
			return strings.EqualFold(network, activeNetwork)
		}
	}
	if header.Route != "" {
		entry := header.Route
		if idx := strings.Index(entry, ":"); idx >= 0 {
			entry = entry[:idx]
		}
		entry = strings.TrimSuffix(entry, "#supp")
		if entry != "" && entry != protocol.ADMIN_UUID {
			if network := admin.topology.NetworkFor(entry); network != "" {
				return strings.EqualFold(network, activeNetwork)
			}
		}
	}
	return true
}
