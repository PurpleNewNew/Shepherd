package gossip

import (
	"context"
	crand "crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"math/rand"
	"sync"
	"time"

	"codeberg.org/agnoie/shepherd/pkg/session"
	"codeberg.org/agnoie/shepherd/protocol"
)

// GossipManager 处理代理节点的gossip协议操作
type GossipManager struct {
	localUUID       string
	localIP         string
	localPort       uint16
	neighbors       map[string]*NeighborInfo
	knownNodes      map[string]*protocol.NodeInfo
	gossipConfig    *protocol.GossipConfig
	messageBuffer   map[string]time.Time // 用于消息去重
	routingTable    map[string]string    // UUID -> 下一跳
	lastHeartbeat   map[string]time.Time
	mutex           sync.RWMutex
	sessionProvider func() session.Session

	// 消息处理通道
	messageChan   chan *GossipMessage
	heartbeatChan chan *protocol.GossipHeartbeat
	routeChan     chan *protocol.GossipRouteUpdate
	joinChan      chan *protocol.GossipNodeJoin
	leaveChan     chan *protocol.GossipNodeLeave
	rnd           *rand.Rand

	ctx    context.Context
	cancel context.CancelFunc

	// 短连接/DTN 相关（可选）
	sleepSeconds int32
	nextWakeFn   func() int64 // 返回预计下次唤醒的 Unix 秒（0 表示未知）
}

// NeighborInfo 表示邻居节点的信息
type NeighborInfo struct {
	UUID     string
	IP       string
	Port     uint16
	LastSeen time.Time
	Status   uint8
	Latency  uint32 // 用于权重计算的测量延迟
}

// GossipMessage 表示用于处理的内部gossip消息
type GossipMessage struct {
	Type      protocol.GossipMessageType
	Data      interface{}
	Sender    string
	Timestamp time.Time
	TTL       uint16
	ID        string // 用于去重的唯一消息ID
}

// NewGossipManager 创建一个新的gossip管理器
func NewGossipManager(uuid, ip string, port uint16, config *protocol.GossipConfig, provider ...func() session.Session) *GossipManager {
	ctx, cancel := context.WithCancel(context.Background())
	var sessionProvider func() session.Session
	if len(provider) > 0 && provider[0] != nil {
		sessionProvider = provider[0]
	} else {
		sessionProvider = func() session.Session { return nil }
	}

	if config == nil {
		config = &protocol.GossipConfig{
			GossipInterval:    30 * time.Second,
			HeartbeatInterval: 10 * time.Second,
			MaxTTL:            5,
			Fanout:            3,
		}
	}

	manager := &GossipManager{
		localUUID:       uuid,
		localIP:         ip,
		localPort:       port,
		neighbors:       make(map[string]*NeighborInfo),
		knownNodes:      make(map[string]*protocol.NodeInfo),
		gossipConfig:    config,
		messageBuffer:   make(map[string]time.Time),
		routingTable:    make(map[string]string),
		lastHeartbeat:   make(map[string]time.Time),
		messageChan:     make(chan *GossipMessage, 1000),
		heartbeatChan:   make(chan *protocol.GossipHeartbeat, 100),
		routeChan:       make(chan *protocol.GossipRouteUpdate, 100),
		joinChan:        make(chan *protocol.GossipNodeJoin, 50),
		leaveChan:       make(chan *protocol.GossipNodeLeave, 50),
		ctx:             ctx,
		cancel:          cancel,
		sessionProvider: sessionProvider,
		rnd:             newSecureRand(),
	}

	return manager
}

// SetSleepSeconds 配置本节点预期的短连接唤醒周期（秒），0 表示长连接。
func (gm *GossipManager) SetSleepSeconds(sec int32) {
	gm.mutex.Lock()
	gm.sleepSeconds = sec
	gm.mutex.Unlock()
}

// SetNextWakeProvider 设置获取预计下次唤醒时间戳（Unix 秒）的提供者。
func (gm *GossipManager) SetNextWakeProvider(fn func() int64) {
	gm.mutex.Lock()
	gm.nextWakeFn = fn
	gm.mutex.Unlock()
}

// Start 开始gossip协议操作
func (gm *GossipManager) Start() {
	// 启动消息处理协程
	go gm.processMessages()

	// 启动心跳发送协程
	go gm.sendHeartbeats()

	// 启动定期 gossip 传播协程
	go gm.gossipPeriodic()

	// 启动节点健康监视协程
	go gm.monitorNodeHealth()
}

// Stop 停止gossip管理器
func (gm *GossipManager) Stop() {
	gm.cancel()
}

// AddNeighbor 向gossip网络添加新邻居
func (gm *GossipManager) AddNeighbor(uuid, ip string, port uint16) {
	gm.mutex.Lock()
	defer gm.mutex.Unlock()

	neighbor := &NeighborInfo{
		UUID:     uuid,
		IP:       ip,
		Port:     port,
		LastSeen: time.Now(),
		Status:   1,   // 在线
		Latency:  100, // 默认延迟
	}

	gm.neighbors[uuid] = neighbor
	gm.updateRoutingTable(uuid, uuid) // 直接邻居：路由到自身

	logger.Infof("added neighbor: %s (%s:%d)", uuid[:8], ip, port)
}

// RemoveNeighbor 从gossip网络中移除邻居
func (gm *GossipManager) RemoveNeighbor(uuid string) {
	gm.mutex.Lock()
	defer gm.mutex.Unlock()

	delete(gm.neighbors, uuid)
	delete(gm.routingTable, uuid)

	// 从路由表中移除相关条目
	for targetUUID, nextHop := range gm.routingTable {
		if nextHop == uuid {
			delete(gm.routingTable, targetUUID)
		}
	}

	logger.Infof("removed neighbor: %s", uuid[:8])
}

// ProcessIncomingMessage 处理传入的gossip消息
func (gm *GossipManager) ProcessIncomingMessage(msgType protocol.GossipMessageType, data []byte, sender string) {
	// 检查消息是否已处理过
	messageID := gm.messageID(msgType, sender, data)
	if gm.isMessageProcessed(messageID) {
		return
	}

	gm.markMessageProcessed(messageID)

	var gossipMsg *GossipMessage

	switch msgType {
	case protocol.GOSSIP_DISCOVER:
		var discover protocol.GossipDiscover
		if err := json.Unmarshal(data, &discover); err == nil {
			gossipMsg = &GossipMessage{
				Type:      msgType,
				Data:      &discover,
				Sender:    sender,
				Timestamp: time.Now(),
				TTL:       gm.gossipConfig.MaxTTL,
				ID:        messageID,
			}
		}

	case protocol.GOSSIP_HEARTBEAT:
		var heartbeat protocol.GossipHeartbeat
		if err := json.Unmarshal(data, &heartbeat); err == nil {
			gossipMsg = &GossipMessage{
				Type:      msgType,
				Data:      &heartbeat,
				Sender:    sender,
				Timestamp: time.Now(),
				TTL:       1, // 心跳消息不会传播太远
				ID:        messageID,
			}
		}

	case protocol.GOSSIP_NODE_JOIN:
		var join protocol.GossipNodeJoin
		if err := json.Unmarshal(data, &join); err == nil {
			gossipMsg = &GossipMessage{
				Type:      msgType,
				Data:      &join,
				Sender:    sender,
				Timestamp: time.Now(),
				TTL:       gm.gossipConfig.MaxTTL,
				ID:        messageID,
			}
		}

	case protocol.GOSSIP_NODE_LEAVE:
		var leave protocol.GossipNodeLeave
		if err := json.Unmarshal(data, &leave); err == nil {
			gossipMsg = &GossipMessage{
				Type:      msgType,
				Data:      &leave,
				Sender:    sender,
				Timestamp: time.Now(),
				TTL:       gm.gossipConfig.MaxTTL,
				ID:        messageID,
			}
		}

	case protocol.GOSSIP_ROUTE_UPDATE:
		var routeUpdate protocol.GossipRouteUpdate
		if err := json.Unmarshal(data, &routeUpdate); err == nil {
			gossipMsg = &GossipMessage{
				Type:      msgType,
				Data:      &routeUpdate,
				Sender:    sender,
				Timestamp: time.Now(),
				TTL:       gm.gossipConfig.MaxTTL,
				ID:        messageID,
			}
		}
	}

	if gossipMsg != nil {
		select {
		case gm.messageChan <- gossipMsg:
		case <-gm.ctx.Done():
			return
		}
	}
}

// processMessages 处理所有gossip消息
func (gm *GossipManager) processMessages() {
	for {
		select {
		case <-gm.ctx.Done():
			return
		case msg := <-gm.messageChan:
			gm.handleGossipMessage(msg)
		case hb := <-gm.heartbeatChan:
			gm.handleHeartbeat(hb)
		case route := <-gm.routeChan:
			gm.handleRouteUpdate(route)
		case join := <-gm.joinChan:
			gm.handleNodeJoin(join)
		case leave := <-gm.leaveChan:
			gm.handleNodeLeave(leave)
		}
	}
}

// handleGossipMessage 处理不同类型的gossip消息
func (gm *GossipManager) handleGossipMessage(msg *GossipMessage) {
	switch msg.Type {
	case protocol.GOSSIP_DISCOVER:
		if discover, ok := msg.Data.(*protocol.GossipDiscover); ok {
			gm.handleDiscover(discover, msg.Sender)
		}
	case protocol.GOSSIP_NODE_JOIN:
		if join, ok := msg.Data.(*protocol.GossipNodeJoin); ok {
			gm.handleNodeJoin(join)
		}
	}

	// 在 TTL 大于 0 时继续向外传播消息
	if msg.TTL > 1 {
		go gm.propagateMessage(msg)
	}
}

// handleDiscover 处理发现消息
func (gm *GossipManager) handleDiscover(discover *protocol.GossipDiscover, sender string) {
	if discover == nil {
		return
	}

	now := time.Now().Unix()
	var added bool

	gm.mutex.Lock()
	if _, exists := gm.knownNodes[discover.SenderUUID]; !exists {
		gm.knownNodes[discover.SenderUUID] = &protocol.NodeInfo{
			UUID:     discover.SenderUUID,
			IP:       discover.SenderIP,
			Port:     int(discover.SenderPort), // 类型转换：uint16 -> int
			Status:   1,                        // 在线
			LastSeen: now,
		}
		added = true
	}
	gm.mutex.Unlock()

	if added {
		logger.Infof("discovered new node: %s (%s:%d)",
			discover.SenderUUID[:8], discover.SenderIP, discover.SenderPort)
		// 发送一条自己的加入消息作为回应
		go gm.sendNodeJoin()
	}
}

// handleNodeJoin 处理节点加入消息
func (gm *GossipManager) handleNodeJoin(join *protocol.GossipNodeJoin) {
	if join == nil {
		return
	}

	info := &protocol.NodeInfo{
		UUID:     join.NodeUUID,
		IP:       join.NodeIP,
		Port:     int(join.NodePort), // 类型转换：uint16 -> int
		Status:   1,                  // 在线
		LastSeen: join.Timestamp,
		IsFirst:  join.ParentUUID == "",
	}

	gm.mutex.Lock()
	gm.knownNodes[join.NodeUUID] = info
	gm.mutex.Unlock()

	logger.Infof("node joined network: %s (%s:%d)",
		join.NodeUUID[:8], join.NodeIP, join.NodePort)
}

// handleNodeLeave 处理节点离开消息
func (gm *GossipManager) handleNodeLeave(leave *protocol.GossipNodeLeave) {
	if leave == nil {
		return
	}

	gm.mutex.Lock()
	// 从已知节点及邻居列表中移除
	delete(gm.knownNodes, leave.NodeUUID)
	delete(gm.lastHeartbeat, leave.NodeUUID)
	gm.mutex.Unlock()

	gm.RemoveNeighbor(leave.NodeUUID)

	logger.Infof("node left network: %s (reason: %d)",
		leave.NodeUUID[:8], leave.Reason)
}

// handleHeartbeat 处理心跳消息
func (gm *GossipManager) handleHeartbeat(hb *protocol.GossipHeartbeat) {
	gm.mutex.Lock()
	defer gm.mutex.Unlock()

	// 更新最近一次心跳时间
	gm.lastHeartbeat[hb.SenderUUID] = time.Now()

	// 更新节点状态
	if node, exists := gm.knownNodes[hb.SenderUUID]; exists {
		node.Status = uint8(hb.Status) // 类型转换：int32 -> uint8
		if hb.Load > 0 {
			node.Health = uint32(hb.Load) // 类型转换：int32 -> uint32
		} else {
			node.Health = 0
		}
		node.LastSeen = hb.Timestamp
	}

	// 更新邻居信息
	if neighbor, exists := gm.neighbors[hb.SenderUUID]; exists {
		neighbor.LastSeen = time.Now()
		neighbor.Status = uint8(hb.Status) // 类型转换：int32 -> uint8
	}
}

// updateRoutingTable 更新路由表
func (gm *GossipManager) updateRoutingTable(targetUUID, nextHopUUID string) {
	gm.mutex.Lock()
	defer gm.mutex.Unlock()
	gm.routingTable[targetUUID] = nextHopUUID
}

// handleRouteUpdate 处理路由更新消息
func (gm *GossipManager) handleRouteUpdate(route *protocol.GossipRouteUpdate) {
	gm.mutex.Lock()
	defer gm.mutex.Unlock()

	// 若是更优路径则更新路由表
	_, exists := gm.routingTable[route.TargetUUID]
	if !exists {
		// 如果没有现有路由，使用新路由
		gm.routingTable[route.TargetUUID] = route.NextHopUUID
		logger.Infof("added route to %s via %s (hops: %d, weight: %d)",
			route.TargetUUID[:8], route.NextHopUUID[:8], route.HopCount, route.Weight)
	} else {
		// 如果有现有路由，只有在新路由的跳数更少时才更新
		// 这里简化处理：直接比较hop count
		if route.HopCount < 10 { // 简化：接受任何合理的路由
			gm.routingTable[route.TargetUUID] = route.NextHopUUID
			logger.Infof("updated route to %s via %s (hops: %d, weight: %d)",
				route.TargetUUID[:8], route.NextHopUUID[:8], route.HopCount, route.Weight)
		}
	}
}

// sendHeartbeats 定期发送心跳消息
func (gm *GossipManager) sendHeartbeats() {
	ticker := time.NewTicker(gm.gossipConfig.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-gm.ctx.Done():
			return
		case <-ticker.C:
			heartbeat := &protocol.GossipHeartbeat{
				SenderUUIDLen: uint16(len(gm.localUUID)),
				SenderUUID:    gm.localUUID,
				Status:        1, // 在线
				Timestamp:     time.Now().Unix(),
				Load:          int32(gm.calculateLoad()), // 类型转换：uint32 -> int32
			}

			gm.broadcastToNeighbors(protocol.GOSSIP_HEARTBEAT, heartbeat)
		}
	}
}

// gossipPeriodic 执行定期的gossip传播
func (gm *GossipManager) gossipPeriodic() {
	ticker := time.NewTicker(gm.gossipConfig.GossipInterval)
	defer ticker.Stop()

	for {
		select {
		case <-gm.ctx.Done():
			return
		case <-ticker.C:
			gm.performPeriodicGossip()
		}
	}
}

// performPeriodicGossip 执行实际的gossip交换
func (gm *GossipManager) performPeriodicGossip() {
	gm.mutex.RLock()
	defer gm.mutex.RUnlock()

	healthScore := gm.calculateLoad()
	queueDepth := gm.queueDepth()

	// 构建节点信息更新包
	// 复制当前配置，避免长锁
	sleepSec := gm.sleepSeconds
	var nextWake int64
	if gm.nextWakeFn != nil {
		nextWake = gm.nextWakeFn()
	}

	nodeInfo := &protocol.NodeInfo{
		UUID:         gm.localUUID,
		IP:           gm.localIP,
		Port:         int(gm.localPort), // 类型转换：uint16 -> int
		Neighbors:    gm.getNeighborUUIDs(),
		Status:       1, // 在线
		Health:       healthScore,
		LastSeen:     time.Now().Unix(),
		LatencyMs:    0,
		FailRate:     0,
		QueueDepth:   queueDepth,
		SleepSeconds: sleepSec,
		NextWake:     nextWake,
	}

	// 随机挑选部分邻居进行广播（扇出）
	neighbors := gm.selectRandomNeighbors()
	for _, neighborUUID := range neighbors {
		go gm.sendToNeighbor(neighborUUID, protocol.GOSSIP_UPDATE, nodeInfo)
	}
}

// monitorNodeHealth 监控已知节点的健康状况
func (gm *GossipManager) monitorNodeHealth() {
	ticker := time.NewTicker(30 * time.Second) // 每 30 秒检查一次
	defer ticker.Stop()

	for {
		select {
		case <-gm.ctx.Done():
			return
		case <-ticker.C:
			gm.checkNodeHealth()
		}
	}
}

// checkNodeHealth 检查节点是否仍然响应
func (gm *GossipManager) checkNodeHealth() {
	gm.mutex.Lock()
	defer gm.mutex.Unlock()

	now := time.Now()
	timeout := 60 * time.Second // 超过 60 秒未响应视为离线

	for uuid, lastSeen := range gm.lastHeartbeat {
		if now.Sub(lastSeen) > timeout {
			// 将节点标记为离线
			if node, exists := gm.knownNodes[uuid]; exists {
				node.Status = 0 // 离线
			}

			if neighbor, exists := gm.neighbors[uuid]; exists {
				neighbor.Status = 0 // 离线
			}

			// 从路由表移除记录
			delete(gm.routingTable, uuid)

			logger.Warnf("node %s marked as offline (timeout)", uuid[:8])
		}
	}

	// 清理过期的去重缓冲
	gm.cleanupMessageBuffer()
}

// propagateMessage 将gossip消息传播给邻居
func (gm *GossipManager) propagateMessage(msg *GossipMessage) {
	if msg.TTL <= 1 {
		return
	}

	// 创建 TTL 已递减的新消息副本
	newMsg := *msg
	newMsg.TTL--

	// 向随机邻居子集发送
	neighbors := gm.selectRandomNeighbors()
	for _, neighborUUID := range neighbors {
		if neighborUUID != msg.Sender { // 避免回发给原发送者
			go gm.sendGossipMessage(neighborUUID, &newMsg)
		}
	}
}

// 辅助方法

func (gm *GossipManager) isMessageProcessed(messageID string) bool {
	gm.mutex.RLock()
	defer gm.mutex.RUnlock()
	_, ok := gm.messageBuffer[messageID]
	return ok
}

func (gm *GossipManager) markMessageProcessed(messageID string) {
	gm.mutex.Lock()
	defer gm.mutex.Unlock()
	if messageID == "" {
		return
	}
	gm.messageBuffer[messageID] = time.Now()
}

func (gm *GossipManager) messageID(msgType protocol.GossipMessageType, sender string, data []byte) string {
	if gm == nil {
		return ""
	}
	hasher := sha256.New()
	hasher.Write([]byte(sender))
	var typeBuf [2]byte
	binary.BigEndian.PutUint16(typeBuf[:], uint16(msgType))
	hasher.Write(typeBuf[:])
	if len(data) > 0 {
		hasher.Write(data)
	}
	return hex.EncodeToString(hasher.Sum(nil))
}

func (gm *GossipManager) cleanupMessageBuffer() {
	// 移除超过保留时间的去重记录。调用方应确保已持有 gm.mutex。
	cutoff := time.Now().Add(-5 * time.Minute)
	for id, ts := range gm.messageBuffer {
		if ts.IsZero() || ts.Before(cutoff) {
			delete(gm.messageBuffer, id)
		}
	}
}

func (gm *GossipManager) selectRandomNeighbors() []string {
	gm.mutex.RLock()
	defer gm.mutex.RUnlock()

	var neighbors []string
	for uuid := range gm.neighbors {
		neighbors = append(neighbors, uuid)
	}

	if len(neighbors) == 0 {
		return neighbors
	}

	gm.shuffleNeighbors(neighbors)

	fanout := 0
	if gm.gossipConfig != nil {
		fanout = int(gm.gossipConfig.Fanout)
	}
	if fanout <= 0 || fanout >= len(neighbors) {
		return neighbors
	}
	return neighbors[:fanout]
}

func (gm *GossipManager) getNeighborUUIDs() []string {
	gm.mutex.RLock()
	defer gm.mutex.RUnlock()

	var uuids []string
	for uuid := range gm.neighbors {
		uuids = append(uuids, uuid)
	}
	return uuids
}

func (gm *GossipManager) calculateLoad() uint32 {
	if gm == nil {
		return 0
	}

	now := time.Now()
	threshold := 2 * time.Minute
	if gm.gossipConfig != nil && gm.gossipConfig.HeartbeatInterval > 0 {
		threshold = gm.gossipConfig.HeartbeatInterval * 2
	}

	neighborCount := 0
	staleNeighbors := 0
	stalenessScore := 0

	gm.mutex.RLock()
	for uuid := range gm.neighbors {
		neighborCount++
		ts := gm.lastHeartbeat[uuid]
		if ts.IsZero() {
			staleNeighbors++
			continue
		}
		if delta := now.Sub(ts); delta > threshold {
			staleNeighbors++
			excess := (delta - threshold) / time.Second
			if excess > 0 {
				stalenessScore += int(excess)
			}
		}
	}
	gm.mutex.RUnlock()

	queueLoad := len(gm.messageChan) + len(gm.heartbeatChan) + len(gm.routeChan) + len(gm.joinChan) + len(gm.leaveChan)
	load := neighborCount*10 + staleNeighbors*20 + stalenessScore + queueLoad
	if load > 100 {
		load = 100
	}
	if load < 0 {
		load = 0
	}
	return uint32(load)
}

// GetRoutingTable 返回当前路由表
func (gm *GossipManager) GetRoutingTable() map[string]string {
	gm.mutex.RLock()
	defer gm.mutex.RUnlock()

	// 创建副本以避免并发访问问题
	table := make(map[string]string)
	for k, v := range gm.routingTable {
		table[k] = v
	}
	return table
}

// GetKnownNodes 返回所有已知节点
func (gm *GossipManager) GetKnownNodes() map[string]*protocol.NodeInfo {
	gm.mutex.RLock()
	defer gm.mutex.RUnlock()

	// 创建副本以避免并发访问问题
	nodes := make(map[string]*protocol.NodeInfo)
	for k, v := range gm.knownNodes {
		nodes[k] = v
	}
	return nodes
}

// SetLocalEndpoint 更新本地节点的地址信息，供后续消息使用。
func (gm *GossipManager) SetLocalEndpoint(ip string, port uint16) {
	if gm == nil {
		return
	}
	gm.mutex.Lock()
	if ip != "" {
		gm.localIP = ip
	}
	gm.localPort = port
	gm.mutex.Unlock()
}

func (gm *GossipManager) shuffleNeighbors(values []string) {
	if len(values) < 2 {
		return
	}
	if gm != nil && gm.rnd != nil {
		gm.rnd.Shuffle(len(values), func(i, j int) {
			values[i], values[j] = values[j], values[i]
		})
		return
	}
	rand.Shuffle(len(values), func(i, j int) {
		values[i], values[j] = values[j], values[i]
	})
}

func newSecureRand() *rand.Rand {
	seed := make([]byte, 8)
	if _, err := crand.Read(seed); err == nil {
		return rand.New(rand.NewSource(int64(binary.LittleEndian.Uint64(seed))))
	}
	return rand.New(rand.NewSource(time.Now().UnixNano()))
}

func (gm *GossipManager) queueDepth() uint32 {
	if gm == nil {
		return 0
	}
	return uint32(len(gm.messageChan) + len(gm.heartbeatChan) + len(gm.routeChan) + len(gm.joinChan) + len(gm.leaveChan))
}

// 消息发送方法（将使用实际的网络传输实现）

func (gm *GossipManager) sendNodeJoin() {
	join := &protocol.GossipNodeJoin{
		NodeUUIDLen:   uint16(len(gm.localUUID)),
		NodeUUID:      gm.localUUID,
		NodeIPLen:     uint16(len(gm.localIP)),
		NodeIP:        gm.localIP,
		NodePort:      gm.localPort,
		ParentUUIDLen: 0, // mesh 网络没有父节点
		ParentUUID:    "",
		Timestamp:     time.Now().Unix(),
	}

	gm.broadcastToNeighbors(protocol.GOSSIP_NODE_JOIN, join)
}

func (gm *GossipManager) broadcastToNeighbors(msgType protocol.GossipMessageType, data interface{}) {
	neighbors := gm.getNeighborUUIDs()
	for _, neighborUUID := range neighbors {
		go gm.sendToNeighbor(neighborUUID, msgType, data)
	}
}

func (gm *GossipManager) sendToNeighbor(neighborUUID string, msgType protocol.GossipMessageType, data interface{}) {
	// 获取邻居连接信息
	gm.mutex.RLock()
	_, exists := gm.neighbors[neighborUUID]
	gm.mutex.RUnlock()

	if !exists {
		logger.Warnf("neighbor %s not found", neighborUUID[:8])
		return
	}

	switch payload := data.(type) {
	case *protocol.GossipUpdate:
		payload.NodeDataLen = uint64(len(payload.NodeData))
		payload.SenderUUIDLen = uint16(len(payload.SenderUUID))
	case *protocol.GossipHeartbeat:
		payload.SenderUUIDLen = uint16(len(payload.SenderUUID))
	case *protocol.GossipRouteUpdate:
		payload.SenderUUIDLen = uint16(len(payload.SenderUUID))
		payload.TargetUUIDLen = uint16(len(payload.TargetUUID))
		payload.NextHopUUIDLen = uint16(len(payload.NextHopUUID))
	case *protocol.GossipRequest:
		payload.RequestUUIDLen = uint16(len(payload.RequestUUID))
		payload.RequesterUUIDLen = uint16(len(payload.RequesterUUID))
		payload.TargetUUIDLen = uint16(len(payload.TargetUUID))
	case *protocol.GossipResponse:
		payload.RequestUUIDLen = uint16(len(payload.RequestUUID))
		payload.RequesterUUIDLen = uint16(len(payload.RequesterUUID))
		payload.ResponderUUIDLen = uint16(len(payload.ResponderUUID))
		payload.TargetUUIDLen = uint16(len(payload.TargetUUID))
		payload.NodeDataLen = uint64(len(payload.NodeData))
	case *protocol.GossipDiscover:
		payload.SenderUUIDLen = uint16(len(payload.SenderUUID))
		payload.SenderIPLen = uint16(len(payload.SenderIP))
	case *protocol.GossipNodeJoin:
		payload.NodeUUIDLen = uint16(len(payload.NodeUUID))
		payload.NodeIPLen = uint16(len(payload.NodeIP))
		payload.ParentUUIDLen = uint16(len(payload.ParentUUID))
	case *protocol.GossipNodeLeave:
		payload.NodeUUIDLen = uint16(len(payload.NodeUUID))
	}

	// 构造消息头。消息实际通过上游连接发往管理端，由管理端根据路由信息转发。
	header := &protocol.Header{
		Sender:      gm.localUUID,
		Accepter:    protocol.ADMIN_UUID,
		MessageType: uint16(msgType),
		RouteLen:    uint32(len([]byte(neighborUUID))),
		Route:       neighborUUID,
	}

	// 发送消息到邻居节点
	// 由于GossipManager没有ChildrenManager的直接引用，我们需要通过全局组件发送
	// 实际上，应该通过 agent 的子节点 dispatcher 来发送消息
	var sess session.Session
	if gm.sessionProvider != nil {
		sess = gm.sessionProvider()
	}
	if sess != nil && sess.Conn() != nil {
		msg := protocol.NewUpMsg(sess.Conn(), sess.Secret(), sess.UUID())
		protocol.SetMessageMeta(msg, sess.ProtocolVersion(), sess.ProtocolFlags())
		protocol.ConstructMessage(msg, header, data, false)
		msg.SendMessage()
		logger.Debugf("sent %d message to neighbor %s", msgType, neighborUUID[:8])
	} else {
		logger.Warnf("no connection available to send message to %s", neighborUUID[:8])
	}
}

func (gm *GossipManager) sendGossipMessage(neighborUUID string, msg *GossipMessage) {
	// 检查TTL是否已耗尽
	if msg.TTL <= 0 {
		logger.Debugf("message TTL expired, not forwarding")
		return
	}

	// 构造一份 TTL 已减少的新消息
	newMsg := *msg
	newMsg.TTL--

	// 根据消息类型构造相应的协议消息
	switch msg.Type {
	case protocol.GOSSIP_DISCOVER:
		if discover, ok := msg.Data.(*protocol.GossipDiscover); ok {
			go gm.sendToNeighbor(neighborUUID, msg.Type, discover)
		}
	case protocol.GOSSIP_HEARTBEAT:
		if heartbeat, ok := msg.Data.(*protocol.GossipHeartbeat); ok {
			go gm.sendToNeighbor(neighborUUID, msg.Type, heartbeat)
		}
	case protocol.GOSSIP_NODE_JOIN:
		if join, ok := msg.Data.(*protocol.GossipNodeJoin); ok {
			go gm.sendToNeighbor(neighborUUID, msg.Type, join)
		}
	case protocol.GOSSIP_NODE_LEAVE:
		if leave, ok := msg.Data.(*protocol.GossipNodeLeave); ok {
			go gm.sendToNeighbor(neighborUUID, msg.Type, leave)
		}
	case protocol.GOSSIP_ROUTE_UPDATE:
		if route, ok := msg.Data.(*protocol.GossipRouteUpdate); ok {
			go gm.sendToNeighbor(neighborUUID, msg.Type, route)
		}
	case protocol.GOSSIP_UPDATE:
		if nodeInfo, ok := msg.Data.(*protocol.NodeInfo); ok {
			// 序列化NodeInfo为字节数据
			nodeData, err := json.Marshal(nodeInfo)
			if err != nil {
				logger.Errorf("failed to marshal node info: %v", err)
				return
			}

			update := &protocol.GossipUpdate{
				TTL:           int32(newMsg.TTL),
				NodeDataLen:   uint64(len(nodeData)),
				NodeData:      nodeData,
				SenderUUIDLen: uint16(len(newMsg.Sender)),
				SenderUUID:    newMsg.Sender,
				Timestamp:     time.Now().Unix(),
			}
			go gm.sendToNeighbor(neighborUUID, msg.Type, update)
		}
	default:
		logger.Warnf("unknown message type: %d", msg.Type)
	}
}

// SendToChildren 将Gossip消息发送到所有子节点
func (gm *GossipManager) SendToChildren(msgType protocol.GossipMessageType, data interface{}) {
	neighbors := gm.getNeighborUUIDs()
	for _, neighborUUID := range neighbors {
		go gm.sendToNeighbor(neighborUUID, msgType, data)
	}
}

// ForwardGossipMessage 将接收到的Gossip消息转发给邻居节点
func (gm *GossipManager) ForwardGossipMessage(msgType protocol.GossipMessageType, data []byte, sender string) {
	// 检查消息是否已被处理
	messageID := gm.messageID(msgType, sender, data)
	if gm.isMessageProcessed(messageID) {
		return
	}

	gm.markMessageProcessed(messageID)

	// 递减 TTL 后继续转发
	var ttl int32 = 1 // 默认TTL

	switch msgType {
	case protocol.GOSSIP_UPDATE:
		var update protocol.GossipUpdate
		if err := json.Unmarshal(data, &update); err == nil {
			ttl = update.TTL

			// 如果TTL已耗尽，则不再转发
			if ttl <= 1 {
				logger.Debugf("message TTL expired, not forwarding")
				return
			}

			// 减少TTL并重新构造消息
			update.TTL = ttl - 1

			// 转发给所有邻居（除了发送者）
			neighbors := gm.getNeighborUUIDs()
			for _, neighborUUID := range neighbors {
				if neighborUUID != sender {
					// 为每个邻居复制一份，避免并发修改
					forwardCopy := update
					go gm.sendToNeighbor(neighborUUID, msgType, &forwardCopy)
				}
			}
		}
	case protocol.GOSSIP_HEARTBEAT:
		// 心跳消息TTL为1，不转发
		return
	default:
		// 其他消息类型按最大TTL转发
		neighbors := gm.getNeighborUUIDs()
		for _, neighborUUID := range neighbors {
			if neighborUUID != sender {
				go gm.sendToNeighbor(neighborUUID, msgType, data)
			}
		}
	}
}
