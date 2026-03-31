package supp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"syscall"
	"time"

	"codeberg.org/agnoie/shepherd/internal/kelpie/logging"
	"codeberg.org/agnoie/shepherd/internal/kelpie/manager"
	"codeberg.org/agnoie/shepherd/internal/kelpie/printer"
	"codeberg.org/agnoie/shepherd/internal/kelpie/topology"
	"codeberg.org/agnoie/shepherd/pkg/session"
	"codeberg.org/agnoie/shepherd/pkg/utils"
	"codeberg.org/agnoie/shepherd/protocol"
)

const (
	suppHeartbeatInterval = 30 * time.Second
	suppHeartbeatTimeout  = 75 * time.Second
)

func heartbeatTimeoutForEndpoint(uuid string) time.Duration {
	timeout := suppHeartbeatTimeout
	uuid = strings.TrimSpace(uuid)
	if uuid == "" {
		return timeout
	}
	heartbeatMonitorTopoM.Lock()
	topo := heartbeatMonitorTopo
	heartbeatMonitorTopoM.Unlock()
	if topo == nil {
		return timeout
	}
	rt, ok := topo.NodeRuntime(uuid)
	if !ok || rt.SleepSeconds <= 0 {
		return timeout
	}
	// 睡眠节点会主动关闭自己的上游会话；而 supplemental 心跳正是通过这条上游上报给 admin。
	// 因此 admin 可能跨越多个睡眠周期都收不到心跳。这里使用更大的超时值，
	// 避免在 duty-cycled 睡眠期间误拆掉本来健康的 supplemental 边。
	cycle := time.Duration(rt.SleepSeconds+rt.WorkSeconds) * time.Second
	if cycle <= 0 {
		cycle = time.Duration(rt.SleepSeconds) * time.Second
	}
	// 预留多个周期，再额外加一点余量，覆盖调度抖动与重连延迟。
	sleepyTimeout := cycle*4 + suppHeartbeatInterval
	if sleepyTimeout > timeout {
		timeout = sleepyTimeout
	}
	return timeout
}

type SuppLinkSummary struct {
	LinkUUID  string
	EndpointA string
	EndpointB string
	Ready     bool
	HealthA   SuppLinkHealth
	HealthB   SuppLinkHealth
	LastA     time.Time
	LastB     time.Time
}

var (
	heartbeatMonitorOnce  sync.Once
	heartbeatMonitorTopo  *topology.Topology
	heartbeatMonitorMgr   *manager.Manager
	heartbeatMonitorTopoM sync.Mutex
	suppPersistMu         sync.Mutex
	suppPersistLast       = make(map[string]time.Time)
)

const supplementalPersistMinInterval = 5 * time.Second
const defaultTopoTimeout = 5 * time.Second

func SendSuppLinkPairRequest(topo *topology.Topology, mgr *manager.Manager, initiatorUUID, targetUUID string) error {
	if mgr == nil {
		return fmt.Errorf("manager unavailable")
	}
	initInfo, err := fetchNodeDetails(topo, initiatorUUID)
	if err != nil {
		return err
	}
	targetInfo, err := fetchNodeDetails(topo, targetUUID)
	if err != nil {
		return err
	}
	listener := initInfo
	dialer := targetInfo
	if initInfo.Depth < targetInfo.Depth {
		listener = targetInfo
		dialer = initInfo
	}
	if topo != nil {
		listenerNet := topo.NetworkFor(listener.UUID)
		dialerNet := topo.NetworkFor(dialer.UUID)
		if listenerNet != "" && dialerNet != "" && !strings.EqualFold(listenerNet, dialerNet) {
			return fmt.Errorf("listener %s and dialer %s belong to different networks", listener.UUID, dialer.UUID)
		}
	}
	linkUUID := utils.GenerateUUID()
	reqUUID := utils.GenerateUUID()
	if err := sendSuppLinkRequest(topo, mgr, listener.UUID, listener.UUID, dialer.UUID, linkUUID, reqUUID, protocol.SuppLinkRoleInitiator, protocol.SuppLinkActionListen, "", 0); err != nil {
		return err
	}
	registerSuppLink(topo, linkUUID, listener.UUID, dialer.UUID, listener.IP)
	printer.Success("\r\n[*] Supplemental link request dispatched (%s <-> %s)\r\n", initiatorUUID[:8], targetUUID[:8])
	return nil
}

func HandleSuppLinkResponse(topo *topology.Topology, mgr *manager.Manager, resp *protocol.SuppLinkResponse) {
	if resp == nil || mgr == nil {
		return
	}
	status := "FAILED"
	if resp.Status == 1 {
		status = "OK"
	}
	linkID := resp.LinkUUID
	if len(linkID) > 8 {
		linkID = linkID[:8]
	}
	printer.Success("\r\n[*] Link %s response from %s [%s]: %s\r\n", linkID, resp.AgentUUID, status, resp.Message)
	if resp.Status == 1 {
		if resp.ListenPort > 0 {
			dispatchDialAfterListen(topo, mgr, resp)
		}
		handleSuppLinkAck(topo, mgr, resp)
	} else {
		if record := getSuppLinkRecord(resp.LinkUUID); record != nil {
			sendSuppLinkTeardown(topo, mgr, record.Listener, resp.LinkUUID, resp.Message)
			sendSuppLinkTeardown(topo, mgr, record.Dialer, resp.LinkUUID, resp.Message)
		}
		updated := suppCtrl.setStatus(resp.LinkUUID, SuppLinkRetired, resp.Message)
		persistSuppRecord(topo, updated, true)
	}
}

func sendSuppLinkRequest(topo *topology.Topology, mgr *manager.Manager, recipientUUID, initiatorUUID, peerUUID, linkUUID, requestUUID string, role uint16, action uint16, peerIP string, peerPort uint16) error {
	route, err := fetchRoute(topo, recipientUUID)
	if err != nil {
		return err
	}
	sess := sessionForUUID(topo, mgr, recipientUUID)
	sMessage, ok := newDownMsg(sess)
	if !ok {
		return fmt.Errorf("no active session for supplemental request")
	}
	header := &protocol.Header{
		Sender:      sess.UUID(),
		Accepter:    protocol.TEMP_UUID,
		MessageType: uint16(protocol.SUPPLINKREQ),
		RouteLen:    uint32(len([]byte(route))),
		Route:       route,
	}
	req := &protocol.SuppLinkRequest{
		RequestUUIDLen:   uint16(len(requestUUID)),
		RequestUUID:      requestUUID,
		LinkUUIDLen:      uint16(len(linkUUID)),
		LinkUUID:         linkUUID,
		InitiatorUUIDLen: uint16(len(initiatorUUID)),
		InitiatorUUID:    initiatorUUID,
		TargetUUIDLen:    uint16(len(peerUUID)),
		TargetUUID:       peerUUID,
		PeerIPLen:        uint16(len(peerIP)),
		PeerIP:           peerIP,
		PeerPort:         peerPort,
		Action:           action,
		Role:             role,
		Flags:            0,
		Timeout:          30,
	}
	protocol.ConstructMessage(sMessage, header, req, false)
	sMessage.SendMessage()
	return nil
}

func fetchRoute(topo *topology.Topology, uuid string) (string, error) {
	if uuid == protocol.ADMIN_UUID {
		return "", nil
	}
	result, err := topoRequest(topo, &topology.TopoTask{
		Mode: topology.GETROUTE,
		UUID: uuid,
	})
	if err != nil || result == nil {
		return "", err
	}
	return result.Route, nil
}

type nodeDetails struct {
	UUID  string
	IP    string
	Port  int
	Depth int
}

func fetchNodeDetails(topo *topology.Topology, uuid string) (*nodeDetails, error) {
	result, err := topoRequest(topo, &topology.TopoTask{
		Mode: topology.GETNODEINFO,
		UUID: uuid,
	})
	if err != nil || result == nil {
		return nil, err
	}
	if result.UUID == "" {
		return nil, fmt.Errorf("node %s not found", uuid)
	}
	return &nodeDetails{
		UUID:  result.UUID,
		IP:    result.IP,
		Port:  result.Port,
		Depth: result.Depth,
	}, nil
}

func registerSuppLink(topo *topology.Topology, linkUUID, listener, dialer, defaultIP string) {
	record := suppCtrl.register(linkUUID, listener, dialer, defaultIP)
	persistSuppRecord(topo, record, true)
}

func getSuppLinkRecord(linkUUID string) *suppLinkRecord {
	return suppCtrl.get(linkUUID)
}

func persistSuppRecord(topo *topology.Topology, record *suppLinkRecord, force bool) {
	if topo == nil || record == nil || record.LinkUUID == "" {
		return
	}
	if record.Status == SuppLinkRetired {
		topo.DeleteSupplementalLink(record.LinkUUID)
		suppPersistMu.Lock()
		delete(suppPersistLast, record.LinkUUID)
		suppPersistMu.Unlock()
		return
	}
	now := time.Now()
	if !force && record.Status != SuppLinkHealthy {
		force = true
	}
	if !force {
		suppPersistMu.Lock()
		last := suppPersistLast[record.LinkUUID]
		if now.Sub(last) < supplementalPersistMinInterval {
			suppPersistMu.Unlock()
			return
		}
		suppPersistLast[record.LinkUUID] = now
		suppPersistMu.Unlock()
	} else {
		suppPersistMu.Lock()
		suppPersistLast[record.LinkUUID] = now
		suppPersistMu.Unlock()
	}
	snapshot := supplementalSnapshotFromRecord(record)
	topo.PersistSupplementalLink(snapshot)
}

func supplementalSnapshotFromRecord(record *suppLinkRecord) topology.SupplementalLinkSnapshot {
	if record == nil {
		return topology.SupplementalLinkSnapshot{}
	}
	last := record.LastTransition
	if last.IsZero() {
		if !record.LastStatusUpdate.IsZero() {
			last = record.LastStatusUpdate
		} else {
			last = time.Now()
		}
	}
	return topology.SupplementalLinkSnapshot{
		LinkUUID:       record.LinkUUID,
		Listener:       record.Listener,
		Dialer:         record.Dialer,
		Status:         int(record.Status),
		FailureReason:  record.FailureReason,
		LastTransition: last,
	}
}

// RestoreSupplementalLinks 根据持久化快照重建补链控制器状态。
func RestoreSupplementalLinks(snapshots []topology.SupplementalLinkSnapshot) {
	if len(snapshots) == 0 {
		return
	}
	now := time.Now()
	for _, snapshot := range snapshots {
		if snapshot.LinkUUID == "" {
			continue
		}
		status := SuppLinkStatus(snapshot.Status)
		record := &suppLinkRecord{
			LinkUUID:         snapshot.LinkUUID,
			Listener:         snapshot.Listener,
			Dialer:           snapshot.Dialer,
			Status:           status,
			FailureReason:    snapshot.FailureReason,
			LastTransition:   snapshot.LastTransition,
			RequestedAt:      snapshot.LastTransition,
			LastStatusUpdate: snapshot.LastTransition,
			Ack:              make(map[string]bool),
			LastHeartbeat:    make(map[string]time.Time),
			Health:           make(map[string]SuppLinkHealth),
		}
		if record.RequestedAt.IsZero() {
			record.RequestedAt = now
		}
		if record.LastTransition.IsZero() {
			record.LastTransition = record.RequestedAt
		}
		heartbeatStamp := record.LastTransition
		if heartbeatStamp.IsZero() {
			heartbeatStamp = now
		}
		switch status {
		case SuppLinkHealthy, SuppLinkPromoted:
			record.Ready = true
			if snapshot.Listener != "" {
				record.Ack[snapshot.Listener] = true
				record.LastHeartbeat[snapshot.Listener] = now
				record.Health[snapshot.Listener] = SuppLinkHealthAlive
			}
			if snapshot.Dialer != "" {
				record.Ack[snapshot.Dialer] = true
				record.LastHeartbeat[snapshot.Dialer] = now
				record.Health[snapshot.Dialer] = SuppLinkHealthAlive
			}
		case SuppLinkDegraded, SuppLinkFailoverCandidate:
			record.Ready = true
			if snapshot.Listener != "" {
				record.Ack[snapshot.Listener] = true
				record.LastHeartbeat[snapshot.Listener] = heartbeatStamp
				record.Health[snapshot.Listener] = SuppLinkHealthFailed
			}
			if snapshot.Dialer != "" {
				record.Ack[snapshot.Dialer] = true
				record.LastHeartbeat[snapshot.Dialer] = heartbeatStamp
				record.Health[snapshot.Dialer] = SuppLinkHealthFailed
			}
		}
		suppCtrl.restore(record)
		if !record.LastTransition.IsZero() {
			suppPersistMu.Lock()
			suppPersistLast[record.LinkUUID] = record.LastTransition
			suppPersistMu.Unlock()
		}
	}
}

// ListSupplementalLinks 返回当前补边记录（含历史）
func ListSupplementalLinks() []*SuppLinkSummary {
	entries := suppCtrl.snapshot()
	summaries := make([]*SuppLinkSummary, 0, len(entries))
	for _, entry := range entries {
		if entry == nil {
			continue
		}
		summary := &SuppLinkSummary{
			LinkUUID:  entry.LinkUUID,
			EndpointA: entry.Listener,
			EndpointB: entry.Dialer,
			Ready:     entry.Status == SuppLinkHealthy || entry.Status == SuppLinkPromoted,
		}
		if entry.Health != nil {
			summary.HealthA = entry.Health[entry.Listener]
			summary.HealthB = entry.Health[entry.Dialer]
		}
		if entry.LastHeartbeat != nil {
			summary.LastA = entry.LastHeartbeat[entry.Listener]
			summary.LastB = entry.LastHeartbeat[entry.Dialer]
		}
		summaries = append(summaries, summary)
	}
	return summaries
}

func handleSuppLinkAck(topo *topology.Topology, mgr *manager.Manager, resp *protocol.SuppLinkResponse) {
	if topo == nil || resp == nil {
		return
	}
	ready, record := suppCtrl.markAck(resp.LinkUUID, resp.AgentUUID)
	persistSuppRecord(topo, record, true)
	if !ready || record == nil {
		return
	}
	finalizeSupplementalEdge(topo, mgr, record)
}

func finalizeSupplementalEdge(topo *topology.Topology, mgr *manager.Manager, record *suppLinkRecord) {
	if topo == nil || record == nil {
		return
	}
	if err := topoExecute(topo, &topology.TopoTask{
		Mode:         topology.ADDEDGE,
		UUID:         record.Listener,
		NeighborUUID: record.Dialer,
		EdgeType:     topology.SupplementalEdge,
	}); err != nil {
		warnRuntime("ADMIN_SUPP_ADD_EDGE", true, err, "failed to add supplemental edge between %s and %s", record.Listener, record.Dialer)
		return
	}
	// 让 supplemental 边立刻可用起来（用于路由、failover 命令和 DTN 分发）。
	if err := topoExecute(topo, &topology.TopoTask{Mode: topology.CALCULATE}); err != nil {
		warnRuntime("ADMIN_SUPP_CALC", true, err, "failed to calculate routes after adding supplemental edge between %s and %s", record.Listener, record.Dialer)
	}

	updated := suppCtrl.markReady(record.LinkUUID)
	persistSuppRecord(topo, updated, true)
	ensureSuppHeartbeatMonitor(topo, mgr)

	printer.Success("\r\n[*] Supplemental edge ready between %s and %s\r\n", shortUUID(record.Listener), shortUUID(record.Dialer))
}

func shortUUID(id string) string {
	if len(id) <= 8 {
		return id
	}
	return id[:8]
}

// GetActiveSuppNeighbors 返回给定节点仍保持心跳的补边邻居
func GetActiveSuppNeighbors(uuid string) []string {
	if uuid == "" {
		return nil
	}
	now := time.Now()
	var neighbors []string
	for _, entry := range suppCtrl.activeEntries() {
		if entry == nil {
			continue
		}
		var peer string
		if uuid == entry.Listener {
			peer = entry.Dialer
		} else if uuid == entry.Dialer {
			peer = entry.Listener
		} else {
			continue
		}
		if peer == "" {
			continue
		}
		if !entryEndpointHealthy(entry, uuid, now) {
			continue
		}
		if !entryEndpointHealthy(entry, peer, now) {
			continue
		}
		if !containsNeighbor(neighbors, peer) {
			neighbors = append(neighbors, peer)
		}
	}
	return neighbors
}

// ActiveSupplementalLinkCount 返回当前活跃的补链数量。
func ActiveSupplementalLinkCount() int {
	if suppCtrl == nil {
		return 0
	}
	return suppCtrl.activeLinkCount()
}

func dispatchLinkFailureEvent(linkUUID string, record *suppLinkRecord, reason string) {
	if record == nil {
		return
	}
	endpoints := []string{record.Listener, record.Dialer}
	PublishSuppLinkFailed(linkUUID, endpoints)
}

func dispatchLinkRetiredEvent(linkUUID string, record *suppLinkRecord, reason string) {
	if record == nil {
		return
	}
	endpoints := []string{record.Listener, record.Dialer}
	PublishSuppLinkRetired(linkUUID, endpoints, reason)
}

func dispatchLinkPromotedEvent(linkUUID, parent, child string) {
	PublishSuppLinkPromoted(linkUUID, parent, child)
}

func dispatchHeartbeatEvent(linkUUID, endpoint string, status SuppLinkHealth, ts time.Time) {
	PublishSuppHeartbeat(linkUUID, endpoint, status, ts)
}

func FindSuppLinkUUID(a, b string) string {
	if a == "" || b == "" {
		return ""
	}
	for _, entry := range suppCtrl.snapshot() {
		if entry == nil {
			continue
		}
		if (entry.Listener == a && entry.Dialer == b) || (entry.Listener == b && entry.Dialer == a) {
			return entry.LinkUUID
		}
	}
	return ""
}

// FailLinksForEndpoint 会主动把所有涉及指定端点的 supplemental 链路标记为 failed。
//
// 这在常在线节点（sleepSeconds=0）离线时尤其有用：如果等心跳超时，
// 往往要拖几十秒，期间 DTN bundle 会被卡在已经失效的 "#supp" 边之后。
func FailLinksForEndpoint(topo *topology.Topology, mgr *manager.Manager, endpoint, reason string) {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return
	}
	if reason == "" {
		reason = fmt.Sprintf("%s offline", shortUUID(endpoint))
	}
	for _, entry := range suppCtrl.snapshot() {
		if entry == nil || entry.LinkUUID == "" {
			continue
		}
		if entry.Listener != endpoint && entry.Dialer != endpoint {
			continue
		}
		markSuppLinkFailed(topo, mgr, entry.LinkUUID, reason)
	}
}

func containsNeighbor(list []string, target string) bool {
	for _, elem := range list {
		if elem == target {
			return true
		}
	}
	return false
}

func entryEndpointHealthy(entry *suppLinkRecord, uuid string, now time.Time) bool {
	if entry == nil || uuid == "" {
		return false
	}
	if entry.Health != nil {
		if status, ok := entry.Health[uuid]; ok && status == SuppLinkHealthFailed {
			return false
		}
	}
	if entry.LastHeartbeat != nil {
		if last, ok := entry.LastHeartbeat[uuid]; ok && !last.IsZero() {
			if now.Sub(last) > heartbeatTimeoutForEndpoint(uuid) {
				return false
			}
		}
	}
	return true
}

type heartbeatEvent int

const (
	heartbeatIgnored heartbeatEvent = iota
	heartbeatUpdated
	heartbeatClosed
)

func ensureSuppHeartbeatMonitor(topo *topology.Topology, mgr *manager.Manager) {
	if topo == nil {
		return
	}
	heartbeatMonitorTopoM.Lock()
	heartbeatMonitorTopo = topo
	if mgr != nil {
		heartbeatMonitorMgr = mgr
	}
	heartbeatMonitorTopoM.Unlock()
	heartbeatMonitorOnce.Do(func() {
		go runSuppHeartbeatMonitor()
	})
}

func runSuppHeartbeatMonitor() {
	ticker := time.NewTicker(suppHeartbeatInterval)
	defer ticker.Stop()
	for range ticker.C {
		heartbeatMonitorTopoM.Lock()
		topo := heartbeatMonitorTopo
		mgr := heartbeatMonitorMgr
		heartbeatMonitorTopoM.Unlock()
		if topo == nil || mgr == nil {
			continue
		}
		checkSuppHeartbeatTimeouts(topo, mgr)
	}
}

func checkSuppHeartbeatTimeouts(topo *topology.Topology, mgr *manager.Manager) {
	now := time.Now()
	failures := make(map[string]string)
	for _, entry := range suppCtrl.activeEntries() {
		if entry == nil {
			continue
		}
		// 避免反复对已经 degraded 的链路重复判失败；
		// 第一次状态变化已经触发 teardown 与移除边，后续检查只会制造噪音。
		if entry.Status != SuppLinkHealthy && entry.Status != SuppLinkFailoverCandidate {
			continue
		}
		// 心跳是通过各端点自己的上游会话上报给 admin 的。
		// 当父节点故障时，某个端点可能暂时失去上游，但 supplemental TCP 连接本身
		// 仍是健康的（对端依然可以帮它转发流量）。
		//
		// 如果只要任意一端停止上报就立即判这条链路失败，
		// 就可能在最需要它的时候误拆一条仍有价值的边，
		// 从而在 trace replay 中制造抖动（如 DTN ACK 超时）。
		//
		// 因此只有当两端在各自预期超时窗口内都没有上报最近心跳时，
		// 才把这条链路视为超时。
		hasHeartbeat := false
		hasRecent := false
		var worstEndpoint string
		var worstAge time.Duration
		for endpoint, last := range entry.LastHeartbeat {
			if last.IsZero() {
				continue
			}
			hasHeartbeat = true
			age := now.Sub(last)
			if age <= heartbeatTimeoutForEndpoint(endpoint) {
				hasRecent = true
				break
			}
			if age > worstAge {
				worstAge = age
				worstEndpoint = endpoint
			}
		}
		if hasHeartbeat && !hasRecent {
			if worstEndpoint == "" {
				worstEndpoint = entry.Listener
			}
			failures[entry.LinkUUID] = fmt.Sprintf("%s heartbeat timeout", shortUUID(worstEndpoint))
		}
	}
	for linkUUID, reason := range failures {
		markSuppLinkFailed(topo, mgr, linkUUID, reason)
	}
}

func HandleSuppLinkHeartbeat(topo *topology.Topology, mgr *manager.Manager, sender string, hb *protocol.SuppLinkHeartbeat) {
	if topo == nil || mgr == nil || hb == nil {
		return
	}
	if hb.LinkUUID == "" {
		return
	}
	ts := time.Unix(hb.Timestamp, 0)
	entry, event := suppCtrl.recordHeartbeat(hb.LinkUUID, sender, ts, hb.Status != 0)
	persistSuppRecord(topo, entry, false)
	if event == heartbeatClosed {
		reason := fmt.Sprintf("%s closed link", shortUUID(sender))
		markSuppLinkFailed(topo, mgr, hb.LinkUUID, reason)
	} else if entry != nil && entry.Status == SuppLinkDegraded {
		reason := fmt.Sprintf("%s heartbeat timeout", shortUUID(sender))
		markSuppLinkFailed(topo, mgr, hb.LinkUUID, reason)
	}
	status := SuppLinkHealthUnknown
	if entry != nil && entry.Health != nil {
		if s, ok := entry.Health[sender]; ok {
			status = s
		}
	}
	dispatchHeartbeatEvent(hb.LinkUUID, sender, status, ts)
}

func markSuppLinkFailed(topo *topology.Topology, mgr *manager.Manager, linkUUID, reason string) {
	record, transitioned := suppCtrl.markFailed(linkUUID, reason)
	if reason == "" {
		reason = "link failure"
	}
	if record == nil {
		return
	}
	if !transitioned {
		// 已经标记为 degraded/retired：避免重复执行 teardown 或 remove-edge 循环。
		return
	}
	persistSuppRecord(topo, record, true)
	printer.Warning("\r\n[*] Supplemental link %s offline: %s\r\n", shortUUID(linkUUID), reason)
	if mgr != nil {
		sendSuppLinkTeardown(topo, mgr, record.Listener, linkUUID, reason)
		sendSuppLinkTeardown(topo, mgr, record.Dialer, linkUUID, reason)
	}
	removeEdgeBetween(topo, record.Listener, record.Dialer)
	triggerSuppFailover(record.Listener)
	triggerSuppFailover(record.Dialer)
	dispatchLinkFailureEvent(linkUUID, record, reason)
}

func sendSuppLinkTeardown(topo *topology.Topology, mgr *manager.Manager, recipientUUID, linkUUID, reason string) {
	if topo == nil || mgr == nil || recipientUUID == "" {
		return
	}
	route, err := fetchRoute(topo, recipientUUID)
	if err != nil {
		printer.Fail("\r\n[*] Failed to fetch route for teardown: %v\r\n", err)
		return
	}
	if len(reason) > 0xffff {
		reason = reason[:0xffff]
	}
	td := &protocol.SuppLinkTeardown{
		LinkUUIDLen: uint16(len(linkUUID)),
		LinkUUID:    linkUUID,
		ReasonLen:   uint16(len(reason)),
		Reason:      reason,
	}
	sess := sessionForUUID(topo, mgr, recipientUUID)
	if sess == nil || sess.Conn() == nil {
		return
	}
	header := &protocol.Header{
		Sender:      sess.UUID(),
		Accepter:    protocol.TEMP_UUID,
		MessageType: uint16(protocol.SUPPLINKTEARDOWN),
		RouteLen:    uint32(len([]byte(route))),
		Route:       route,
	}
	sMessage, ok := newDownMsg(sess)
	if !ok {
		return
	}
	protocol.ConstructMessage(sMessage, header, td, false)
	sMessage.SendMessage()
}

func triggerSuppFailover(uuid string) {
	if uuid == "" {
		return
	}
	PublishSuppFailover(uuid)
}

func removeEdgeBetween(topo *topology.Topology, a, b string) {
	if topo == nil || a == "" || b == "" {
		return
	}
	if err := topoExecute(topo, &topology.TopoTask{
		Mode:         topology.REMOVEEDGE,
		UUID:         a,
		NeighborUUID: b,
	}); err != nil {
		warnRuntime("ADMIN_SUPP_REMOVE_EDGE", true, err, "failed to remove edge between %s and %s", a, b)
		return
	}
	topo.ScheduleCalculate()
}

func AbortSupplementalLinkWithReason(topo *topology.Topology, mgr *manager.Manager, linkHint, reason string) error {
	if reason == "" {
		reason = "operator teardown"
	}
	linkUUID := resolveLinkUUID(linkHint)
	if linkUUID == "" {
		return fmt.Errorf("supplemental link %s not found", linkHint)
	}
	record := getSuppLinkRecord(linkUUID)
	if record == nil {
		return fmt.Errorf("supplemental link %s not found", linkHint)
	}
	listener := record.Listener
	dialer := record.Dialer
	if record.Status == SuppLinkHealthy || record.Status == SuppLinkDegraded || record.Status == SuppLinkFailoverCandidate || record.Status == SuppLinkPromoted {
		removeEdgeBetween(topo, listener, dialer)
	}
	sendSuppLinkTeardown(topo, mgr, listener, linkUUID, reason)
	sendSuppLinkTeardown(topo, mgr, dialer, linkUUID, reason)
	updated := suppCtrl.setStatus(linkUUID, SuppLinkRetired, reason)
	persistSuppRecord(topo, updated, true)
	dispatchLinkRetiredEvent(linkUUID, record, reason)
	return nil
}

func StartSuppFailover(topo *topology.Topology, mgr *manager.Manager, linkUUID, parentUUID, childUUID string) {
	if topo == nil || mgr == nil || linkUUID == "" || parentUUID == "" || childUUID == "" {
		return
	}
	parentNet := topo.NetworkFor(parentUUID)
	childNet := topo.NetworkFor(childUUID)
	if parentNet != "" && childNet != "" && !strings.EqualFold(parentNet, childNet) {
		printer.Warning("\r\n[*] Supp failover aborted: %s and %s are in different networks (%s vs %s)\r\n", shortUUID(parentUUID), shortUUID(childUUID), parentNet, childNet)
		return
	}
	candidate := suppCtrl.setStatus(linkUUID, SuppLinkFailoverCandidate, "failover")
	persistSuppRecord(topo, candidate, true)
	sendSuppFailoverCommand(topo, mgr, parentUUID, linkUUID, parentUUID, childUUID, protocol.SuppFailoverRoleParent, protocol.SuppFailoverFlagInitiator)
	sendSuppFailoverCommand(topo, mgr, childUUID, linkUUID, parentUUID, childUUID, protocol.SuppFailoverRoleChild, 0)
	convertEdgeToTree(topo, parentUUID, childUUID)
	updated := suppCtrl.promote(linkUUID)
	persistSuppRecord(topo, updated, true)
	dispatchLinkPromotedEvent(linkUUID, parentUUID, childUUID)
}

func convertEdgeToTree(topo *topology.Topology, parentUUID, childUUID string) {
	if topo == nil || parentUUID == "" || childUUID == "" {
		return
	}
	if err := topoExecute(topo, &topology.TopoTask{
		Mode:         topology.ADDEDGE,
		UUID:         parentUUID,
		NeighborUUID: childUUID,
		EdgeType:     topology.TreeEdge,
	}); err != nil {
		warnRuntime("ADMIN_SUPP_CONVERT_ADD_TREE", true, err, "failed to add tree edge between %s and %s during failover", parentUUID, childUUID)
		return
	}
	if err := topoExecute(topo, &topology.TopoTask{
		Mode:         topology.SETEDGEWEIGHT,
		UUID:         parentUUID,
		NeighborUUID: childUUID,
		Weight:       1,
		Force:        true,
	}); err != nil {
		warnRuntime("ADMIN_SUPP_CONVERT_SET_WEIGHT", true, err, "failed to set edge weight for %s-%s during failover", parentUUID, childUUID)
		return
	}
	topo.ScheduleCalculate()
}

func sendSuppFailoverCommand(topo *topology.Topology, mgr *manager.Manager, recipient, linkUUID, parentUUID, childUUID string, role, flags uint16) {
	route, err := fetchRoute(topo, recipient)
	if err != nil {
		printer.Fail("\r\n[*] Failed to fetch route for failover: %v\r\n", err)
		return
	}
	sess := sessionForUUID(topo, mgr, recipient)
	sMessage, ok := newDownMsg(sess)
	if !ok {
		return
	}
	header := &protocol.Header{
		Sender:      sess.UUID(),
		Accepter:    protocol.TEMP_UUID,
		MessageType: uint16(protocol.SUPPFAILOVER),
		RouteLen:    uint32(len([]byte(route))),
		Route:       route,
	}
	cmd := &protocol.SuppFailoverCommand{
		LinkUUIDLen:   uint16(len(linkUUID)),
		LinkUUID:      linkUUID,
		ParentUUIDLen: uint16(len(parentUUID)),
		ParentUUID:    parentUUID,
		ChildUUIDLen:  uint16(len(childUUID)),
		ChildUUID:     childUUID,
		Role:          role,
		Flags:         flags,
	}
	protocol.ConstructMessage(sMessage, header, cmd, false)
	sMessage.SendMessage()
}

func resolveLinkUUID(hint string) string {
	if hint == "" {
		return ""
	}
	if record := suppCtrl.findByPrefix(hint); record != nil {
		return record.LinkUUID
	}
	return ""
}

func dispatchDialAfterListen(topo *topology.Topology, mgr *manager.Manager, resp *protocol.SuppLinkResponse) {
	if mgr == nil {
		return
	}
	record := getSuppLinkRecord(resp.LinkUUID)
	if record == nil {
		return
	}
	if resp.AgentUUID != record.Listener {
		return
	}
	listenerIP := resp.ListenIP
	if listenerIP == "" {
		listenerIP = record.ListenIP
	}
	if listenerIP == "" {
		if info, err := fetchNodeDetails(topo, record.Listener); err == nil {
			listenerIP = info.IP
		}
	}
	if listenerIP == "" || resp.ListenPort == 0 {
		return
	}
	reqUUID := utils.GenerateUUID()
	if err := sendSuppLinkRequest(topo, mgr, record.Dialer, record.Listener, record.Listener, resp.LinkUUID, reqUUID, protocol.SuppLinkRoleResponder, protocol.SuppLinkActionDial, listenerIP, resp.ListenPort); err != nil {
		printer.Fail("\r\n[*] Failed to dispatch dial request: %v\r\n", err)
		return
	}
	suppCtrl.markDialDispatched(resp.LinkUUID, listenerIP, resp.ListenPort)
	persistSuppRecord(topo, getSuppLinkRecord(resp.LinkUUID), true)
}

func topoRequestWithTimeout(topo *topology.Topology, task *topology.TopoTask, timeout time.Duration) (*topology.Result, error) {
	if topo == nil {
		return nil, topology.ErrNilTopology
	}
	if task == nil {
		return nil, topology.ErrNilTask
	}
	if timeout <= 0 {
		timeout = defaultTopoTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return topo.Request(ctx, task)
}

func topoRequest(topo *topology.Topology, task *topology.TopoTask) (*topology.Result, error) {
	return topoRequestWithTimeout(topo, task, defaultTopoTimeout)
}

func topoExecute(topo *topology.Topology, task *topology.TopoTask) error {
	_, err := topoRequest(topo, task)
	return err
}

func warnRuntime(code string, retryable bool, err error, format string, args ...interface{}) {
	if isBenignNetClose(err) {
		return
	}
	logging.Warn(code, retryable, err, format, args...)
}

func isBenignNetClose(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return true
	}
	if errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNRESET) {
		return true
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "use of closed network connection") ||
		strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "connection reset by peer") {
		return true
	}
	return false
}

func newDownMsg(sess session.Session) (protocol.Message, bool) {
	if sess == nil {
		return nil, false
	}
	conn := sess.Conn()
	if conn == nil {
		return nil, false
	}
	msg := protocol.NewDownMsg(conn, sess.Secret(), sess.UUID())
	protocol.SetMessageMeta(msg, sess.ProtocolVersion(), sess.ProtocolFlags())
	return msg, true
}

func sessionForUUID(_ *topology.Topology, mgr *manager.Manager, uuid string) session.Session {
	if mgr == nil {
		return nil
	}
	if sess := mgr.SessionForTarget(uuid); sess != nil {
		return sess
	}
	return mgr.ActiveSession()
}
