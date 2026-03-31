package process

import (
	"context"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"codeberg.org/agnoie/shepherd/internal/flock/gossip"
	"codeberg.org/agnoie/shepherd/internal/flock/initial"
	"codeberg.org/agnoie/shepherd/internal/flock/manager"
	"codeberg.org/agnoie/shepherd/pkg/bus"
	"codeberg.org/agnoie/shepherd/pkg/global"
	"codeberg.org/agnoie/shepherd/pkg/session"
	"codeberg.org/agnoie/shepherd/protocol"
)

const (
	dynamicNeighborTTL       = 2 * time.Minute
	discoveryFanout          = 3
	discoveryTimeout         = 45 * time.Second
	failoverInitialWait      = 45 * time.Second
	failoverCommandWait      = 30 * time.Second
	failoverPollInterval     = 500 * time.Millisecond
	childDispatcherQueueSize = 64
	dtnInboxCapacity         = 64
)

type contextKey string

const (
	originContextKey     contextKey = "message-origin"
	originConnContextKey contextKey = "message-origin-conn"
)

type Agent struct {
	ctx    context.Context
	cancel context.CancelFunc
	UUID   string
	Memo   string

	identity agentIdentity

	options     *initial.Options
	mgr         *manager.Manager
	session     session.Session
	store       *global.Store
	router      *bus.Router
	routerReady bool

	gossipConfig *protocol.GossipConfig
	knownNodes   map[string]*protocol.NodeInfo
	knownMu      sync.RWMutex

	neighborsMu sync.RWMutex
	neighbors   map[string]struct{}

	gossipTrigger chan struct{}
	gossipStop    chan struct{}
	gossipRateMu  sync.Mutex
	gossipTokens  float64
	gossipRefill  time.Time

	extraNeighbors   map[string]time.Time
	pendingMu        sync.Mutex
	pendingByID      map[string]*pendingDiscovery
	pendingByTarget  map[string]string
	failoverConnChan chan *failoverCandidate
	pendingFailovers map[string]*protocol.SuppFailoverCommand
	failoverMu       sync.Mutex
	failoverDeadline time.Time
	gossipMgr        *gossip.GossipManager
	sleepPredictor   *sleepPredictor
	sleepPredMu      sync.Mutex

	dispatcherMu     sync.Mutex
	childDispatchers map[string]*childDispatcher

	repairBind     string
	repairPort     int
	repairListener net.Listener

	repairConnHook func(net.Conn)

	// DTN inbox (simple in-memory for diagnostics)
	dtnInboxMu sync.Mutex
	dtnInbox   []*protocol.DTNData
	// DTN carry-forward queue (local store-carry-forward)
	carryMu    sync.Mutex
	carryQueue map[string][]*carryItem
	// Upstream carry-forward queue: ADMIN_UUID-bound pass-through messages received
	// while the upstream session is temporarily unavailable (sleep/failover/reconnect).
	//
	// This avoids losing DTN_ACK / RuntimeLog and other small control-plane messages
	// during churn in duty-cycled/self-heal traces.
	upCarryMu    sync.Mutex
	upCarryQueue []*upCarryItem
	// Stream sessions
	streamMu sync.Mutex
	streams  map[uint32]*streamState
	// File streams
	fileMu   sync.Mutex
	fileByID map[uint32]*fileStream
	// Proxy streams (CONNECT bridge)
	fwdMu   sync.Mutex
	fwdByID map[uint32]net.Conn
	// SOCKS streams (SOCKS5 CONNECT over DTN-Stream)
	socksByID map[uint32]*socksStream
	// Sleep controller
	sleepMu          sync.Mutex
	lastActivity     time.Time
	sleepingUntil    time.Time
	sleepGraceUntil  time.Time
	sleepManagerOnce sync.Once
	sleepCfg         atomic.Value
	// connChanged is a best-effort notification channel used to interrupt long-running
	// reconnect/sleep waits when a repair/rescue connection has already replaced the
	// active upstream session.
	connChanged chan struct{}
	workDir     string
}

func (agent *Agent) ParentUUID() string {
	if agent == nil {
		return ""
	}
	return agent.identity.Parent()
}

func (agent *Agent) Username() string {
	if agent == nil {
		return ""
	}
	return agent.identity.Username()
}

func (agent *Agent) Hostname() string {
	if agent == nil {
		return ""
	}
	return agent.identity.Hostname()
}

func (agent *Agent) upstreamTransport() string {
	if agent != nil && agent.options != nil {
		value := strings.ToLower(strings.TrimSpace(agent.options.Upstream))
		if value != "" {
			return value
		}
	}
	return protocol.DefaultTransports().Upstream()
}

func (agent *Agent) updateSystemInfo(hostname, username string) {
	if agent == nil {
		return
	}
	agent.identity.SetSystemInfo(hostname, username)
}

func (agent *Agent) dispatcherFor(uuid string) *childDispatcher {
	if agent == nil || uuid == "" {
		return nil
	}
	agent.dispatcherMu.Lock()
	defer agent.dispatcherMu.Unlock()
	if dispatcher, ok := agent.childDispatchers[uuid]; ok {
		return dispatcher
	}
	dispatcher := newChildDispatcher(agent, uuid)
	agent.childDispatchers[uuid] = dispatcher
	return dispatcher
}

func (agent *Agent) currentDispatcher(uuid string) *childDispatcher {
	if agent == nil || uuid == "" {
		return nil
	}
	agent.dispatcherMu.Lock()
	dispatcher := agent.childDispatchers[uuid]
	agent.dispatcherMu.Unlock()
	return dispatcher
}

func (agent *Agent) enqueueChildMessage(msg *ChildrenMess) {
	if agent == nil || msg == nil || msg.targetUUID == "" {
		return
	}
	dispatcher := agent.dispatcherFor(msg.targetUUID)
	if dispatcher == nil {
		logger.Warnf("failed to acquire dispatcher for child %s", msg.targetUUID)
		return
	}
	dispatcher.enqueue(msg)
}

func (agent *Agent) removeDispatcher(uuid string, expected *childDispatcher) {
	if agent == nil || uuid == "" || expected == nil {
		return
	}
	agent.dispatcherMu.Lock()
	dispatcher, ok := agent.childDispatchers[uuid]
	if ok && dispatcher == expected {
		delete(agent.childDispatchers, uuid)
	}
	agent.dispatcherMu.Unlock()
	if ok && dispatcher == expected {
		dispatcher.stopDispatcher()
	}
}

func (agent *Agent) removeAllDispatchers() {
	if agent == nil {
		return
	}
	agent.dispatcherMu.Lock()
	dispatchers := make([]*childDispatcher, 0, len(agent.childDispatchers))
	for uuid, dispatcher := range agent.childDispatchers {
		dispatchers = append(dispatchers, dispatcher)
		delete(agent.childDispatchers, uuid)
	}
	agent.dispatcherMu.Unlock()
	for _, dispatcher := range dispatchers {
		if dispatcher != nil {
			dispatcher.stopDispatcher()
		}
	}
}

func (agent *Agent) currentSession() session.Session {
	if agent == nil {
		return nil
	}
	if agent.session != nil {
		return agent.session
	}
	if agent.store != nil {
		return agent.store.ActiveSession()
	}
	return nil
}

func (agent *Agent) context() context.Context {
	if agent == nil || agent.ctx == nil {
		return context.Background()
	}
	return agent.ctx
}

func (agent *Agent) Stop() {
	if agent == nil || agent.cancel == nil {
		return
	}
	agent.removeAllDispatchers()
	if agent.gossipMgr != nil {
		agent.gossipMgr.Stop()
	}
	if agent.gossipStop != nil {
		close(agent.gossipStop)
		agent.gossipStop = nil
	}
	if agent.repairListener != nil {
		_ = agent.repairListener.Close()
	}
	if agent.mgr != nil {
		agent.mgr.Close()
	}
	agent.cancel()
}

func (agent *Agent) BindSession(sess session.Session) {
	agent.session = sess
}

func (agent *Agent) SetStore(store *global.Store) {
	agent.store = store
	if agent.session == nil && store != nil {
		agent.session = store.ActiveSession()
	}
}

func (agent *Agent) preAuthToken() string {
	if agent == nil || agent.store == nil {
		return ""
	}
	return agent.store.PreAuthToken()
}

func (agent *Agent) updateSessionConn(conn net.Conn) {
	sess := agent.currentSession()
	if sess != nil {
		sess.UpdateConn(conn)
	}
	// If upstream connectivity was restored, try to flush any buffered admin-bound
	// pass-through messages (DTN_ACK / RuntimeLog, etc).
	if conn != nil {
		agent.flushUpCarryQueueForce()
	}
}

func (agent *Agent) connectionTriple() (net.Conn, string, string) {
	sess := agent.currentSession()
	if sess == nil {
		return nil, "", ""
	}
	if conn := sess.Conn(); conn != nil {
		return conn, sess.Secret(), sess.UUID()
	}
	return nil, "", ""
}

func (agent *Agent) newUpMsg() (protocol.Message, session.Session, bool) {
	sess := agent.currentSession()
	if sess == nil {
		return nil, nil, false
	}
	conn := sess.Conn()
	if conn == nil {
		return nil, sess, false
	}
	return protocol.NewUpMsg(conn, sess.Secret(), sess.UUID()), sess, true
}

func (agent *Agent) recordDTNMessage(msg *protocol.DTNData) {
	if agent == nil || msg == nil {
		return
	}
	agent.dtnInboxMu.Lock()
	defer agent.dtnInboxMu.Unlock()
	if agent.dtnInbox == nil {
		agent.dtnInbox = make([]*protocol.DTNData, 0, dtnInboxCapacity)
	}
	if len(agent.dtnInbox) >= dtnInboxCapacity {
		copy(agent.dtnInbox, agent.dtnInbox[1:])
		agent.dtnInbox[len(agent.dtnInbox)-1] = msg
		return
	}
	agent.dtnInbox = append(agent.dtnInbox, msg)
}

func (agent *Agent) watchConn(ctx context.Context, conn net.Conn) {
	if ctx == nil || conn == nil {
		return
	}
	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()
}

type ChildrenMess struct {
	cHeader    *protocol.Header
	cMessage   []byte
	targetUUID string
	preferSupp bool
}

type failoverCandidate struct {
	conn       net.Conn
	parentUUID string
	linkUUID   string
}

type pendingDiscovery struct {
	target   string
	initTime time.Time
	request  string
}

func NewAgent(ctx context.Context, options *initial.Options, store *global.Store, sess session.Session) *Agent {
	derivedCtx, cancel := context.WithCancel(ctx)
	agent := new(Agent)
	agent.ctx = derivedCtx
	agent.cancel = cancel
	agent.UUID = protocol.TEMP_UUID
	agent.options = options
	agent.session = sess
	agent.store = store
	if agent.session == nil && store != nil {
		agent.session = store.ActiveSession()
	}
	agent.neighbors = make(map[string]struct{})
	agent.knownNodes = make(map[string]*protocol.NodeInfo)
	agent.extraNeighbors = make(map[string]time.Time)
	agent.pendingByID = make(map[string]*pendingDiscovery)
	agent.pendingByTarget = make(map[string]string)
	agent.childDispatchers = make(map[string]*childDispatcher)
	agent.gossipConfig = &protocol.GossipConfig{
		GossipInterval:    30 * time.Second,
		HeartbeatInterval: 10 * time.Second,
		MaxTTL:            5,
		Fanout:            3,
	}
	agent.failoverConnChan = make(chan *failoverCandidate, 1)
	agent.pendingFailovers = make(map[string]*protocol.SuppFailoverCommand)
	agent.connChanged = make(chan struct{}, 1)
	agent.router = bus.NewRouter()
	agent.routerReady = false
	if wd, err := os.Getwd(); err == nil {
		agent.workDir = wd
	} else {
		agent.workDir = "."
	}
	agent.gossipTokens = gossipRateCapacity
	agent.gossipRefill = time.Now()
	if options != nil {
		agent.repairBind = options.RepairBind
		agent.repairPort = options.RepairPort
		if options.SleepSeconds > 0 {
			agent.sleepPredictor = newSleepPredictor(agent.UUID, options.SleepSeconds)
		}
	}
	agent.publishSleepConfig()
	return agent
}

func (agent *Agent) Run() {
	ctx := agent.context()

	agent.mgr = manager.NewManager(agent.store)
	if sess := agent.currentSession(); sess != nil {
		agent.mgr.SetSession(sess)
	}
	go agent.mgr.Run(ctx)
	agent.setupRouter()
	agent.startRepairListener()
	agent.sendMyInfo()
	agent.sendBootstrapSleepReport()
	agent.initGossip()
	agent.startSleepManager()
	agent.startCarryForward()
	// 运行调度器以分发各种消息
	go DispatchListenMess(ctx, agent.mgr, agent.options)
	// SSH/Shell 管理器保留本地会话生命周期管理。
	go DispatchSSHMess(ctx, agent.mgr)
	go DispatchShellMess(ctx, agent.mgr, agent.options)
	go DispatchOfflineMess(agent)
	// 等待子节点
	go agent.waitingChild()
	// 等待补边连接
	go agent.waitingSupplemental()
	// 处理来自上游的数据
	agent.handleDataFromUpstream()
}

func withOrigin(ctx context.Context, origin string) context.Context {
	if origin == "" {
		return ctx
	}
	return context.WithValue(ctx, originContextKey, origin)
}

func withOriginConn(ctx context.Context, conn net.Conn) context.Context {
	if conn == nil {
		return ctx
	}
	return context.WithValue(ctx, originConnContextKey, conn)
}

func originFromContext(ctx context.Context, header *protocol.Header) string {
	if ctx != nil {
		if v, ok := ctx.Value(originContextKey).(string); ok && v != "" {
			return v
		}
	}
	if header != nil {
		return header.Sender
	}
	return ""
}

func originConnFromContext(ctx context.Context) net.Conn {
	if ctx != nil {
		if v, ok := ctx.Value(originConnContextKey).(net.Conn); ok && v != nil {
			return v
		}
	}
	return nil
}
