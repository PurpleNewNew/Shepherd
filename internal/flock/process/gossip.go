package process

import (
	"encoding/json"
	"math"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"time"

	"codeberg.org/agnoie/shepherd/internal/flock/gossip"
	"codeberg.org/agnoie/shepherd/internal/flock/state"
	"codeberg.org/agnoie/shepherd/pkg/utils"
	"codeberg.org/agnoie/shepherd/protocol"
)

const (
	minAdaptiveFanout     = 1
	maxAdaptiveFanout     = 10
	baseAdaptiveTTL       = 3
	adaptiveTTLUpperBound = 8
	gossipRateCapacity    = 12.0
	gossipRatePerSecond   = 2.5
)

func (agent *Agent) initGossip() {
	ip, port := agent.currentGossipEndpoint()
	agent.gossipMgr = gossip.NewGossipManager(agent.UUID, ip, port, agent.gossipConfig, agent.currentSession)
	if agent.gossipMgr != nil {
		// 透传短连接参数（若配置）：sleep_seconds；next_wake 可后续由调度器提供
		cfg := agent.loadSleepConfig()
		agent.gossipMgr.SetSleepSeconds(int32(cfg.sleepSeconds))
		agent.gossipMgr.SetNextWakeProvider(agent.nextWakeEstimate)
		agent.refreshGossipEndpoint()
		agent.gossipMgr.Start()
	}
	agent.gossipTrigger = make(chan struct{}, 1)
	agent.gossipStop = make(chan struct{})
	go agent.gossipLoop()
	go agent.gossipDiscoveryLoop()
	agent.triggerGossipUpdate()
}

// sendBootstrapSleepReport 将自身的 NodeInfo（含 sleep/work/next_wake）直接发给 Admin，
// 避免等待首个 gossip 周期，尽快让 Kelpie 更新离线阈值。
func (agent *Agent) sendBootstrapSleepReport() {
	if agent == nil {
		return
	}
	sess := agent.currentSession()
	if sess == nil || sess.Conn() == nil {
		return
	}
	nodeInfo := agent.buildNodeInfo()
	nodeData, err := json.Marshal(nodeInfo)
	if err != nil {
		logger.Warnf("bootstrap sleep report marshal failed: %v", err)
		return
	}
	update := &protocol.GossipUpdate{
		TTL:           1,
		NodeDataLen:   uint64(len(nodeData)),
		NodeData:      nodeData,
		SenderUUIDLen: uint16(len(nodeInfo.UUID)),
		SenderUUID:    nodeInfo.UUID,
		Timestamp:     time.Now().Unix(),
	}
	agent.storeKnownNode(nodeInfo)
	agent.sendUpdateDirectToAdmin(update)
}

func (agent *Agent) gossipLoop() {
	ctx := agent.context()
	ticker := time.NewTicker(agent.gossipConfig.GossipInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			agent.emitGossipUpdate()
			// Opportunistic DTN pull (small batch)
			agent.requestDTNPull(2)
		case <-agent.gossipTrigger:
			agent.emitGossipUpdate()
		case <-agent.gossipStop:
			return
		}
	}
}

func (agent *Agent) gossipDiscoveryLoop() {
	ctx := agent.context()
	ticker := time.NewTicker(agent.gossipConfig.GossipInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			agent.performDiscovery()
		case <-agent.gossipStop:
			return
		}
	}
}

func (agent *Agent) performDiscovery() {
	expired := agent.expirePendingRequests()
	for _, target := range expired {
		agent.removeDynamicNeighbor(target)
	}

	candidates := agent.collectDiscoveryTargets()
	if len(candidates) == 0 {
		return
	}

	shuffler := rand.New(rand.NewSource(time.Now().UnixNano()))
	shuffler.Shuffle(len(candidates), func(i, j int) {
		candidates[i], candidates[j] = candidates[j], candidates[i]
	})

	fanout := agent.dynamicFanout()
	if fanout > len(candidates) {
		fanout = len(candidates)
	}

	for i := 0; i < fanout; i++ {
		agent.sendDiscoveryRequest(candidates[i])
	}
}

func (agent *Agent) requestDTNPull(limit int) {
	if agent == nil || limit <= 0 {
		return
	}
	sess := agent.currentSession()
	if sess == nil || sess.Conn() == nil {
		return
	}
	up := protocol.NewUpMsg(sess.Conn(), sess.Secret(), agent.UUID)
	header := &protocol.Header{
		Sender:      agent.UUID,
		Accepter:    protocol.ADMIN_UUID,
		MessageType: uint16(protocol.DTN_PULL),
		RouteLen:    uint32(len([]byte(protocol.TEMP_ROUTE))),
		Route:       protocol.TEMP_ROUTE,
	}
	req := &protocol.DTNPull{Limit: uint16(limit)}
	protocol.ConstructMessage(up, header, req, false)
	up.SendMessage()
}

// nextWakeEstimate 返回当前节点的粗略下次唤醒时间（Unix 秒）。
// 目前简单使用 sleep 周期：now + sleepSeconds。
func (agent *Agent) nextWakeEstimate() int64 {
	if agent == nil {
		return 0
	}
	predictor := agent.ensureSleepPredictor()
	if predictor == nil {
		return 0
	}
	// 确保使用当前 UUID 生成确定性的相位与抖动
	predictor.UpdateUUID(agent.UUID)
	next := predictor.NextWake(time.Now())
	if next.IsZero() {
		return 0
	}
	return next.Unix()
}

func (agent *Agent) ensureSleepPredictor() *sleepPredictor {
	cfg := agent.loadSleepConfig()
	if agent == nil || cfg.sleepSeconds <= 0 {
		return nil
	}
	agent.sleepPredMu.Lock()
	defer agent.sleepPredMu.Unlock()
	if agent.sleepPredictor == nil {
		agent.sleepPredictor = newSleepPredictor(agent.UUID, cfg.sleepSeconds)
	} else {
		agent.sleepPredictor.UpdateSleepSeconds(cfg.sleepSeconds)
	}
	agent.sleepPredictor.SetJitterRatio(cfg.jitter)
	agent.sleepPredictor.UpdateUUID(agent.UUID)
	return agent.sleepPredictor
}

func (agent *Agent) collectDiscoveryTargets() []string {
	agent.knownMu.RLock()
	known := make([]*protocol.NodeInfo, 0, len(agent.knownNodes))
	for _, info := range agent.knownNodes {
		known = append(known, info)
	}
	agent.knownMu.RUnlock()

	now := time.Now()

	agent.neighborsMu.Lock()
	direct := make(map[string]struct{}, len(agent.neighbors)+len(agent.extraNeighbors)+2)
	for uuid := range agent.neighbors {
		direct[uuid] = struct{}{}
	}
	if parent := agent.ParentUUID(); parent != "" {
		direct[parent] = struct{}{}
	}
	if agent.pruneExpiredNeighborsLocked(now) {
		defer agent.triggerGossipUpdate()
	}
	for uuid := range agent.extraNeighbors {
		direct[uuid] = struct{}{}
	}
	agent.neighborsMu.Unlock()

	candidates := make([]string, 0, len(known))
	for _, info := range known {
		if info == nil || info.UUID == "" {
			continue
		}
		if info.UUID == agent.UUID {
			continue
		}
		if _, isDirect := direct[info.UUID]; isDirect {
			continue
		}
		if agent.isDiscoveryPending(info.UUID) {
			continue
		}
		if info.Status == 0 {
			continue
		}
		if info.LastSeen > 0 {
			last := time.Unix(info.LastSeen, 0)
			if now.Sub(last) > dynamicNeighborTTL*2 {
				continue
			}
		}
		candidates = append(candidates, info.UUID)
	}
	return candidates
}

func (agent *Agent) sendDiscoveryRequest(target string) {
	if target == "" {
		return
	}
	if agent.isDiscoveryPending(target) {
		return
	}

	reqID := utils.GenerateUUID()
	request := &protocol.GossipRequest{
		RequestUUIDLen:   uint16(len(reqID)),
		RequestUUID:      reqID,
		RequesterUUIDLen: uint16(len(agent.UUID)),
		RequesterUUID:    agent.UUID,
		TargetUUIDLen:    uint16(len(target)),
		TargetUUID:       target,
		Timestamp:        time.Now().UnixNano(),
	}

	agent.registerPendingRequest(reqID, target)

	header := &protocol.Header{
		Sender:      agent.UUID,
		Accepter:    protocol.ADMIN_UUID,
		MessageType: uint16(protocol.GOSSIP_REQUEST),
		RouteLen:    uint32(len([]byte(protocol.TEMP_ROUTE))),
		Route:       protocol.TEMP_ROUTE,
	}

	sMessage, _, ok := agent.newUpMsg()
	if !ok {
		return
	}
	protocol.ConstructMessage(sMessage, header, request, false)
	sMessage.SendMessage()
}

func (agent *Agent) registerPendingRequest(requestID, target string) {
	now := time.Now()
	agent.pendingMu.Lock()
	agent.pendingByID[requestID] = &pendingDiscovery{
		target:   target,
		initTime: now,
		request:  requestID,
	}
	agent.pendingByTarget[target] = requestID
	agent.pendingMu.Unlock()
}

func (agent *Agent) isDiscoveryPending(target string) bool {
	agent.pendingMu.Lock()
	_, exists := agent.pendingByTarget[target]
	agent.pendingMu.Unlock()
	return exists
}

func (agent *Agent) expirePendingRequests() []string {
	now := time.Now()
	var expired []string
	agent.pendingMu.Lock()
	for id, pending := range agent.pendingByID {
		if now.Sub(pending.initTime) > discoveryTimeout {
			expired = append(expired, pending.target)
			delete(agent.pendingByID, id)
			delete(agent.pendingByTarget, pending.target)
		}
	}
	agent.pendingMu.Unlock()
	return expired
}

func (agent *Agent) completePendingRequest(requestID string) (string, bool) {
	agent.pendingMu.Lock()
	defer agent.pendingMu.Unlock()
	pending, ok := agent.pendingByID[requestID]
	if !ok {
		return "", false
	}
	delete(agent.pendingByID, requestID)
	delete(agent.pendingByTarget, pending.target)
	return pending.target, true
}

func (agent *Agent) triggerGossipUpdate() {
	if agent.gossipTrigger == nil {
		return
	}
	select {
	case agent.gossipTrigger <- struct{}{}:
	default:
	}
}

func (agent *Agent) currentChildren() []string {
	agent.neighborsMu.RLock()
	defer agent.neighborsMu.RUnlock()
	neighbors := make([]string, 0, len(agent.neighbors))
	for uuid := range agent.neighbors {
		neighbors = append(neighbors, uuid)
	}
	return neighbors
}

func (agent *Agent) currentNeighbors() []string {
	agent.neighborsMu.Lock()
	now := time.Now()
	changed := agent.pruneExpiredNeighborsLocked(now)
	parent := agent.ParentUUID()

	total := len(agent.neighbors)
	if parent != "" {
		total++
	}
	neighbors := make([]string, 0, total)
	for uuid := range agent.neighbors {
		neighbors = append(neighbors, uuid)
	}
	if parent != "" {
		neighbors = append(neighbors, parent)
	}
	agent.neighborsMu.Unlock()

	if changed {
		agent.triggerGossipUpdate()
	}

	return neighbors
}

func (agent *Agent) addNeighbor(uuid string) {
	if uuid == "" {
		return
	}
	agent.neighborsMu.Lock()
	if agent.neighbors == nil {
		agent.neighbors = make(map[string]struct{})
	}
	agent.neighbors[uuid] = struct{}{}
	agent.neighborsMu.Unlock()
	agent.triggerGossipUpdate()
}

func (agent *Agent) addDynamicNeighbor(uuid string) {
	if parent := agent.ParentUUID(); uuid == "" || uuid == agent.UUID || uuid == parent {
		return
	}
	now := time.Now()
	agent.neighborsMu.Lock()
	changed := agent.pruneExpiredNeighborsLocked(now)
	if _, exists := agent.extraNeighbors[uuid]; !exists {
		changed = true
	}
	agent.extraNeighbors[uuid] = now
	agent.neighborsMu.Unlock()
	if changed {
		agent.triggerGossipUpdate()
	}
}

func (agent *Agent) removeNeighbor(uuid string) {
	if uuid == "" {
		return
	}
	agent.neighborsMu.Lock()
	delete(agent.neighbors, uuid)
	agent.neighborsMu.Unlock()
	agent.triggerGossipUpdate()
}

func (agent *Agent) removeDynamicNeighbor(uuid string) {
	if uuid == "" {
		return
	}
	agent.neighborsMu.Lock()
	removed := false
	if _, exists := agent.extraNeighbors[uuid]; exists {
		delete(agent.extraNeighbors, uuid)
		removed = true
	}
	agent.neighborsMu.Unlock()
	if removed {
		agent.triggerGossipUpdate()
	}
}

func (agent *Agent) pruneExpiredNeighborsLocked(now time.Time) bool {
	changed := false
	for uuid, ts := range agent.extraNeighbors {
		if now.Sub(ts) > dynamicNeighborTTL {
			delete(agent.extraNeighbors, uuid)
			changed = true
		}
	}
	return changed
}

func (agent *Agent) setParentUUID(uuid string) {
	if agent == nil || uuid == "" || uuid == agent.UUID {
		return
	}
	if agent.identity.SetParent(uuid) {
		agent.triggerGossipUpdate()
	}
}

func (agent *Agent) currentGossipEndpoint() (string, uint16) {
	ip := agent.localSessionIP()
	if isUnspecifiedIP(ip) {
		if fallback := agent.optionPreferredIP(); fallback != "" {
			ip = fallback
		}
	}
	port := agent.localListenPort()
	return ip, port
}

func (agent *Agent) refreshGossipEndpoint() {
	if agent == nil || agent.gossipMgr == nil {
		return
	}
	ip, port := agent.currentGossipEndpoint()
	agent.gossipMgr.SetLocalEndpoint(ip, port)
}

func (agent *Agent) localSessionIP() string {
	sess := agent.currentSession()
	if sess == nil {
		return ""
	}
	conn := sess.Conn()
	if conn == nil {
		return ""
	}
	addr := conn.LocalAddr()
	if addr == nil {
		return ""
	}
	if tcpAddr, ok := addr.(*net.TCPAddr); ok {
		if len(tcpAddr.IP) > 0 && !tcpAddr.IP.IsUnspecified() {
			return tcpAddr.IP.String()
		}
		return ""
	}
	if host, _, err := net.SplitHostPort(addr.String()); err == nil {
		if host == "" {
			return ""
		}
		if parsed := net.ParseIP(host); parsed != nil && parsed.IsUnspecified() {
			return ""
		}
		return host
	}
	return ""
}

func (agent *Agent) optionPreferredIP() string {
	if agent == nil || agent.options == nil {
		return ""
	}
	candidates := []string{
		strings.TrimSpace(agent.options.ReuseHost),
		extractHost(agent.options.Listen),
	}
	for _, candidate := range candidates {
		if candidate == "" {
			continue
		}
		if isUnspecifiedIP(candidate) {
			continue
		}
		return candidate
	}
	return ""
}

func (agent *Agent) localListenPort() uint16 {
	if port := parsePort(state.GetCurrentListenAddr()); port != 0 {
		return port
	}
	if agent == nil || agent.options == nil {
		return 0
	}
	if port := parsePort(agent.options.Listen); port != 0 {
		return port
	}
	if port := parsePort(agent.options.ReusePort); port != 0 {
		return port
	}
	return 0
}

func isUnspecifiedIP(value string) bool {
	if value == "" {
		return true
	}
	if parsed := net.ParseIP(value); parsed != nil {
		return parsed.IsUnspecified()
	}
	return false
}

func extractHost(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if host, _, err := net.SplitHostPort(value); err == nil {
		return host
	}
	if parsed := net.ParseIP(value); parsed != nil {
		return value
	}
	return ""
}

func parsePort(raw string) uint16 {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0
	}
	if _, portStr, err := net.SplitHostPort(raw); err == nil {
		if port := parseUint16(portStr); port != 0 {
			return port
		}
	}
	return parseUint16(raw)
}

func parseUint16(value string) uint16 {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	parsed, err := strconv.ParseUint(value, 10, 16)
	if err != nil {
		return 0
	}
	return uint16(parsed)
}
func (agent *Agent) buildNodeInfo() *protocol.NodeInfo {
	ip, port := agent.currentGossipEndpoint()
	if agent.gossipMgr != nil {
		agent.gossipMgr.SetLocalEndpoint(ip, port)
	}

	children := agent.currentChildren()
	cfg := agent.loadSleepConfig()

	info := &protocol.NodeInfo{
		UUID:      agent.UUID,
		IP:        ip,
		Port:      int(port),
		Username:  agent.Username(),
		Hostname:  agent.Hostname(),
		Memo:      agent.Memo,
		Status:    1,
		LastSeen:  time.Now().Unix(),
		Neighbors: agent.currentNeighbors(),
		Health:    uint32(len(children)),
	}
	if cfg.sleepSeconds > 0 {
		info.SleepSeconds = int32(cfg.sleepSeconds)
		if next := agent.nextWakeEstimate(); next > 0 {
			info.NextWake = next
		}
	}
	if cfg.workSeconds > 0 {
		info.WorkSeconds = int32(cfg.workSeconds)
	}
	return info
}

func (agent *Agent) storeKnownNode(info *protocol.NodeInfo) {
	if info == nil || info.UUID == "" {
		return
	}
	agent.knownMu.Lock()
	agent.knownNodes[info.UUID] = info
	agent.knownMu.Unlock()
}

func (agent *Agent) knownNodeMemo(uuid string) string {
	uuid = strings.TrimSpace(uuid)
	if agent == nil || uuid == "" {
		return ""
	}
	agent.knownMu.RLock()
	defer agent.knownMu.RUnlock()
	info := agent.knownNodes[uuid]
	if info == nil {
		return ""
	}
	return strings.TrimSpace(info.Memo)
}

func decodeGossipMemo(update *protocol.GossipUpdate) (sourceUUID, memo string) {
	if update == nil || len(update.NodeData) == 0 {
		return "", ""
	}
	var info protocol.NodeInfo
	if err := json.Unmarshal(update.NodeData, &info); err != nil {
		return "", ""
	}
	return strings.TrimSpace(info.UUID), strings.TrimSpace(info.Memo)
}

func (agent *Agent) emitGossipUpdate() {
	sess := agent.currentSession()
	if sess == nil {
		return
	}
	conn := sess.Conn()
	if conn == nil {
		return
	}

	nodeInfo := agent.buildNodeInfo()
	nodeData, err := json.Marshal(nodeInfo)
	if err != nil {
		logger.Errorf("failed to marshal node info: %v", err)
		return
	}

	senderUUID := nodeInfo.UUID
	nodeDataLen := len(nodeData)

	ttl := agent.dynamicTTL()

	update := &protocol.GossipUpdate{
		TTL:           ttl,
		NodeDataLen:   uint64(nodeDataLen),
		NodeData:      nodeData,
		SenderUUIDLen: uint16(len(senderUUID)),
		SenderUUID:    senderUUID,
		Timestamp:     time.Now().Unix(),
	}

	agent.storeKnownNode(nodeInfo)
	if memo := strings.TrimSpace(nodeInfo.Memo); memo != "" {
		logger.Infof("[diag/memo_emit] self=%s source=%s memo=%q ttl=%d parent=%s neighbors=%d",
			agent.UUID, nodeInfo.UUID, memo, ttl, agent.ParentUUID(), len(nodeInfo.Neighbors))
	}

	if agent.ParentUUID() == "" {
		agent.sendUpdateDirectToAdmin(update)
	}

	agent.forwardGossipUpdate(update, "", ttl)
}

func (agent *Agent) sendUpdateDirectToAdmin(update *protocol.GossipUpdate) {
	sess := agent.currentSession()
	if sess == nil {
		return
	}
	if sess.Conn() == nil {
		return
	}

	header := &protocol.Header{
		Sender:      agent.UUID,
		Accepter:    protocol.ADMIN_UUID,
		MessageType: uint16(protocol.GOSSIP_UPDATE),
		RouteLen:    uint32(len([]byte(protocol.TEMP_ROUTE))),
		Route:       protocol.TEMP_ROUTE,
	}

	clone := *update
	if sourceUUID, memo := decodeGossipMemo(update); memo != "" {
		logger.Infof("[diag/memo_admin] self=%s source=%s memo=%q ttl=%d", agent.UUID, sourceUUID, memo, update.TTL)
	}

	sMessage, _, ok := agent.newUpMsg()
	if !ok {
		return
	}
	protocol.ConstructMessage(sMessage, header, &clone, false)
	sMessage.SendMessage()
}

func (agent *Agent) selectNeighborTargets(sender string) []string {
	candidates := agent.currentNeighbors()
	others := make([]string, 0, len(candidates))
	parentTarget := ""

	parent := agent.ParentUUID()
	for _, uuid := range candidates {
		if uuid == "" || uuid == sender || uuid == agent.UUID {
			continue
		}
		if uuid == parent {
			parentTarget = uuid
			continue
		}
		others = append(others, uuid)
	}

	selected := make([]string, 0, len(others)+1)
	fanout := agent.dynamicFanout()

	if fanout == 0 || len(others) <= fanout {
		selected = append(selected, others...)
	} else {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		r.Shuffle(len(others), func(i, j int) {
			others[i], others[j] = others[j], others[i]
		})
		selected = append(selected, others[:fanout]...)
	}

	if parentTarget != "" {
		selected = append(selected, parentTarget)
	}

	return selected
}

func (agent *Agent) forwardGossipUpdate(update *protocol.GossipUpdate, sender string, ttl int32) {
	if update == nil || ttl <= 0 {
		return
	}

	parent := agent.ParentUUID()
	targets := agent.selectNeighborTargets(sender)
	sourceUUID, memo := decodeGossipMemo(update)
	for _, target := range targets {
		clone := *update
		clone.TTL = ttl
		if memo != "" {
			via := "child"
			if target == parent {
				via = "parent"
			}
			logger.Infof("[diag/memo_forward] self=%s sender=%s source=%s memo=%q ttl=%d target=%s via=%s",
				agent.UUID, sender, sourceUUID, memo, ttl, target, via)
		}
		if target == parent {
			agent.sendGossipToParent(&clone)
		} else {
			agent.sendGossipToChild(target, &clone)
		}
	}
}

func (agent *Agent) sendGossipToParent(update *protocol.GossipUpdate) {
	parent := agent.ParentUUID()
	if update == nil || parent == "" {
		return
	}
	if sess := agent.currentSession(); sess == nil || sess.Conn() == nil {
		return
	}

	header := &protocol.Header{
		Sender:      agent.UUID,
		Accepter:    parent,
		MessageType: uint16(protocol.GOSSIP_UPDATE),
	}

	sMessage, _, ok := agent.newUpMsg()
	if !ok {
		return
	}
	protocol.ConstructMessage(sMessage, header, update, false)
	sMessage.SendMessage()
}

func (agent *Agent) sendGossipToChild(childUUID string, update *protocol.GossipUpdate) {
	if update == nil || childUUID == "" || agent.mgr == nil {
		return
	}

	conn, ok := agent.mgr.ChildrenManager.GetConn(childUUID)
	if !ok || conn == nil {
		return
	}

	header := &protocol.Header{
		Sender:      agent.UUID,
		Accepter:    childUUID,
		MessageType: uint16(protocol.GOSSIP_UPDATE),
	}

	sess := agent.currentSession()
	if sess == nil {
		return
	}
	sMessage := protocol.NewDownMsg(conn, sess.Secret(), sess.UUID())
	protocol.SetMessageMeta(sMessage, sess.ProtocolVersion(), sess.ProtocolFlags())
	protocol.ConstructMessage(sMessage, header, update, false)
	sMessage.SendMessage()
}

func (agent *Agent) handleIncomingGossip(update *protocol.GossipUpdate, sender string) {
	info, err := agent.decodeNodeInfo(update)
	if err != nil {
		logger.Errorf("failed to decode gossip payload: %v", err)
	}

	if info != nil {
		oldMemo := agent.knownNodeMemo(info.UUID)
		agent.storeKnownNode(info)
		newMemo := strings.TrimSpace(info.Memo)
		if newMemo != "" {
			logger.Infof("[diag/memo_receive] self=%s sender=%s source=%s memo=%q ttl=%d old_memo=%q",
				agent.UUID, sender, info.UUID, newMemo, update.TTL, oldMemo)
		}
		if info.UUID == agent.UUID {
			agent.syncLocalNeighbors(info)
		}
	}

	if update == nil {
		return
	}

	// Root nodes may not have ParentUUID populated (they receive admin commands via routed TEMP messages).
	// In that case, child-originated gossip would never reach Kelpie unless we explicitly uplink it.
	if agent.ParentUUID() == "" && sender != protocol.ADMIN_UUID {
		agent.sendUpdateDirectToAdmin(update)
	}

	// TTL should limit fanout and redundant flooding, but it must not prevent gossip
	// updates from reaching the upstream parent/admin in deep topologies (e.g. long
	// chains). If TTL is exhausted, forward *only* to the parent to guarantee
	// eventual convergence at the root without creating cycles.
	if update.TTL <= 1 {
		parent := agent.ParentUUID()
		if parent != "" && parent != sender {
			clone := *update
			clone.TTL = 1
			if sourceUUID, memo := decodeGossipMemo(update); memo != "" {
				logger.Infof("[diag/memo_ttl_guard] self=%s sender=%s source=%s memo=%q ttl=%d forward_parent=%s",
					agent.UUID, sender, sourceUUID, memo, update.TTL, parent)
			}
			agent.sendGossipToParent(&clone)
		}
		return
	}

	agent.forwardGossipUpdate(update, sender, update.TTL-1)
}

func (agent *Agent) handleGossipRequest(req *protocol.GossipRequest) {
	if req == nil {
		return
	}

	if req.TargetUUID != "" && req.TargetUUID != agent.UUID {
		agent.sendGossipResponse(&protocol.GossipResponse{
			RequestUUIDLen:   uint16(len(req.RequestUUID)),
			RequestUUID:      req.RequestUUID,
			RequesterUUIDLen: uint16(len(req.RequesterUUID)),
			RequesterUUID:    req.RequesterUUID,
			ResponderUUIDLen: uint16(len(agent.UUID)),
			ResponderUUID:    agent.UUID,
			TargetUUIDLen:    uint16(len(req.TargetUUID)),
			TargetUUID:       req.TargetUUID,
			Reachable:        false,
			Timestamp:        time.Now().UnixNano(),
		})
		return
	}

	agent.addDynamicNeighbor(req.RequesterUUID)

	response := &protocol.GossipResponse{
		RequestUUIDLen:   uint16(len(req.RequestUUID)),
		RequestUUID:      req.RequestUUID,
		RequesterUUIDLen: uint16(len(req.RequesterUUID)),
		RequesterUUID:    req.RequesterUUID,
		ResponderUUIDLen: uint16(len(agent.UUID)),
		ResponderUUID:    agent.UUID,
		TargetUUIDLen:    uint16(len(agent.UUID)),
		TargetUUID:       agent.UUID,
		Reachable:        true,
		Timestamp:        time.Now().UnixNano(),
	}

	if req.Timestamp > 0 {
		latency := time.Now().UnixNano() - req.Timestamp
		if latency < 0 {
			latency = 0
		}
		response.Latency = latency / int64(time.Millisecond)
	}

	if info := agent.buildNodeInfo(); info != nil {
		if data, err := json.Marshal(info); err == nil {
			response.NodeDataLen = uint64(len(data))
			response.NodeData = data
		}
	}

	agent.sendGossipResponse(response)
}

func (agent *Agent) handleGossipResponse(resp *protocol.GossipResponse) {
	if resp == nil || resp.RequesterUUID != agent.UUID {
		return
	}

	if len(resp.NodeData) > 0 {
		var nodeInfo protocol.NodeInfo
		if err := json.Unmarshal(resp.NodeData, &nodeInfo); err == nil {
			agent.storeKnownNode(&nodeInfo)
		}
	}

	target, ok := agent.completePendingRequest(resp.RequestUUID)
	if !ok {
		return
	}
	if !resp.Reachable {
		if target != "" {
			agent.removeDynamicNeighbor(target)
		}
		return
	}

	neighbor := resp.ResponderUUID
	if neighbor == "" {
		neighbor = target
	}
	agent.addDynamicNeighbor(neighbor)
}

func (agent *Agent) sendGossipResponse(resp *protocol.GossipResponse) {
	if resp == nil {
		return
	}
	conn, secret, uuid := agent.connectionTriple()
	if conn == nil {
		return
	}

	resp.RequestUUIDLen = uint16(len(resp.RequestUUID))
	resp.RequesterUUIDLen = uint16(len(resp.RequesterUUID))
	resp.ResponderUUIDLen = uint16(len(resp.ResponderUUID))
	resp.TargetUUIDLen = uint16(len(resp.TargetUUID))
	resp.NodeDataLen = uint64(len(resp.NodeData))

	header := &protocol.Header{
		Sender:      agent.UUID,
		Accepter:    protocol.ADMIN_UUID,
		MessageType: uint16(protocol.GOSSIP_RESPONSE),
		RouteLen:    uint32(len([]byte(protocol.TEMP_ROUTE))),
		Route:       protocol.TEMP_ROUTE,
	}
	sMessage := protocol.NewUpMsg(conn, secret, uuid)
	protocol.ConstructMessage(sMessage, header, resp, false)
	sMessage.SendMessage()
}

func (agent *Agent) handleGossipPayload(message interface{}, origin string) {
	switch payload := message.(type) {
	case *protocol.GossipUpdate:
		agent.handleIncomingGossip(payload, origin)
	case []byte:
		var update protocol.GossipUpdate
		if err := json.Unmarshal(payload, &update); err != nil {
			logger.Errorf("failed to decode forwarded gossip payload: %v", err)
			return
		}
		agent.handleIncomingGossip(&update, origin)
	default:
		logger.Warnf("unexpected gossip payload type %T", message)
	}
}

func (agent *Agent) decodeNodeInfo(update *protocol.GossipUpdate) (*protocol.NodeInfo, error) {
	if update == nil {
		return nil, nil
	}
	var nodeInfo protocol.NodeInfo
	if err := json.Unmarshal(update.NodeData, &nodeInfo); err != nil {
		return nil, err
	}
	return &nodeInfo, nil
}

func (agent *Agent) syncLocalNeighbors(info *protocol.NodeInfo) {
	if info == nil {
		return
	}
	agent.neighborsMu.Lock()
	agent.neighbors = make(map[string]struct{})
	parent := agent.ParentUUID()
	for _, neighbor := range info.Neighbors {
		if neighbor == "" || neighbor == agent.UUID || neighbor == parent {
			continue
		}
		agent.neighbors[neighbor] = struct{}{}
	}
	agent.neighborsMu.Unlock()
	agent.triggerGossipUpdate()
}

func (agent *Agent) knownNodeCount() int {
	if agent == nil {
		return 0
	}
	agent.knownMu.RLock()
	defer agent.knownMu.RUnlock()
	return len(agent.knownNodes)
}

func (agent *Agent) pendingFailoverCount() int {
	if agent == nil {
		return 0
	}
	agent.failoverMu.Lock()
	defer agent.failoverMu.Unlock()
	return len(agent.pendingFailovers)
}

func (agent *Agent) dynamicFanout() int {
	base := int(agent.gossipConfig.Fanout)
	if base <= 0 {
		base = discoveryFanout
	}
	size := agent.knownNodeCount()
	fanout := base
	if size > base {
		extra := int(math.Ceil(math.Log2(float64(size))))
		if extra > 0 {
			fanout += extra
		}
	}
	if agent.pendingFailoverCount() > 0 && fanout < maxAdaptiveFanout {
		fanout++
	}
	if fanout > maxAdaptiveFanout {
		fanout = maxAdaptiveFanout
	}
	if fanout < minAdaptiveFanout {
		fanout = minAdaptiveFanout
	}
	metrics := agent.gossipHealthSummary()
	if metrics.unhealthyRatio > 0.35 && fanout > minAdaptiveFanout {
		fanout--
	}
	if metrics.queuePressure && fanout > minAdaptiveFanout {
		fanout--
	}
	if metrics.sleepyRatio > 0.5 && fanout < maxAdaptiveFanout {
		fanout++
	}
	return agent.consumeGossipBudget(fanout)
}

func (agent *Agent) dynamicTTL() int32 {
	ttl := int(agent.gossipConfig.MaxTTL)
	if ttl <= 0 {
		ttl = baseAdaptiveTTL
	}
	size := agent.knownNodeCount()
	if size > 1 {
		increment := int(math.Ceil(math.Log10(float64(size) + 1)))
		if increment > 0 {
			ttl += increment
		}
	}
	cfg := agent.loadSleepConfig()
	if cfg.sleepSeconds > 0 {
		ttl++
	}
	if agent.pendingFailoverCount() > 0 {
		ttl += 2
	}
	if ttl > adaptiveTTLUpperBound {
		ttl = adaptiveTTLUpperBound
	}
	if ttl < baseAdaptiveTTL {
		ttl = baseAdaptiveTTL
	}
	metrics := agent.gossipHealthSummary()
	if metrics.unhealthyRatio > 0.25 {
		ttl++
	}
	if metrics.sleepyRatio > 0.4 {
		ttl++
	}
	if metrics.queuePressure && ttl > baseAdaptiveTTL {
		ttl--
	}
	return int32(ttl)
}

type gossipHealthMetrics struct {
	avgHealth      float64
	unhealthyRatio float64
	avgQueue       float64
	queuePressure  bool
	sleepyRatio    float64
}

func (agent *Agent) gossipHealthSummary() gossipHealthMetrics {
	metrics := gossipHealthMetrics{}
	if agent == nil {
		return metrics
	}
	agent.knownMu.RLock()
	defer agent.knownMu.RUnlock()
	count := 0
	unhealthy := 0
	queueCount := 0
	sleepy := 0
	for _, info := range agent.knownNodes {
		if info == nil || info.UUID == "" {
			continue
		}
		if info.Health > 0 {
			metrics.avgHealth += float64(info.Health)
			count++
			if info.Health > 50 {
				unhealthy++
			}
		}
		if info.QueueDepth > 0 {
			metrics.avgQueue += float64(info.QueueDepth)
			queueCount++
			if info.QueueDepth > 32 {
				metrics.queuePressure = true
			}
		}
		if info.SleepSeconds > 0 {
			sleepy++
		}
	}
	if count > 0 {
		metrics.avgHealth /= float64(count)
		metrics.unhealthyRatio = float64(unhealthy) / float64(count)
	}
	if queueCount > 0 {
		metrics.avgQueue /= float64(queueCount)
	}
	totalNodes := len(agent.knownNodes)
	if totalNodes > 0 {
		metrics.sleepyRatio = float64(sleepy) / float64(totalNodes)
	}
	return metrics
}

func (agent *Agent) consumeGossipBudget(request int) int {
	if agent == nil || request <= 0 {
		return request
	}
	agent.gossipRateMu.Lock()
	defer agent.gossipRateMu.Unlock()
	now := time.Now()
	if agent.gossipRefill.IsZero() {
		agent.gossipRefill = now
	}
	elapsed := now.Sub(agent.gossipRefill).Seconds()
	if elapsed > 0 {
		agent.gossipTokens += elapsed * gossipRatePerSecond
		if agent.gossipTokens > gossipRateCapacity {
			agent.gossipTokens = gossipRateCapacity
		}
		agent.gossipRefill = now
	}
	if agent.gossipTokens <= 0 {
		return 0
	}
	needed := float64(request)
	if needed > agent.gossipTokens {
		allowed := int(math.Floor(agent.gossipTokens))
		if allowed <= 0 {
			return 0
		}
		agent.gossipTokens -= float64(allowed)
		return allowed
	}
	agent.gossipTokens -= needed
	return request
}
