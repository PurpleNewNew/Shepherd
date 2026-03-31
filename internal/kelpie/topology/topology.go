package topology

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"codeberg.org/agnoie/shepherd/protocol"
)

const (
	// 拓扑操作
	ADDNODE = iota
	GETUUID
	GETUUIDNUM
	CHECKNODE
	CALCULATE
	GETROUTE
	DELNODE
	REONLINENODE
	// 图特定操作
	ADDEDGE
	REMOVEEDGE
	SETEDGEWEIGHT
	GETNEIGHBORS
	GETNODEMETA
	GETNODEINFO
	GETDEPTH
	SETROUTINGSTRATEGY
	// 用户友好
	UPDATEDETAIL
	SHOWDETAIL
	SHOWTOPO
	UPDATEMEMO
	UPDATECONNINFO
	GETCONNINFO
	// 自动SOCKS操作
	GETALLNODES
	REPARENTNODE
	CHECKSUPPEDGE
	GETSUPPCANDIDATES
	// 维护类
	PRUNEOFFLINE
	// 标记久未更新的节点为离线（不删除，仅置位）
	MARKSTALEOFFLINE
	// 标记节点（及其子树）离线（不删除，仅置位）
	MARKNODEOFFLINE
)

const calcDebounceInterval = 250 * time.Millisecond

const topoRequestTimeout = 5 * time.Second

// RoutingStrategy 表示路由算法的策略。
type RoutingStrategy int

const (
	RoutingByHops    RoutingStrategy = iota // 基于最小跳数 (BFS)
	RoutingByWeight                         // 基于最小权重 (Dijkstra)
	RoutingByLatency                        // 基于延迟测量
)

// Topology 表示基于图的网络拓扑。
type Topology struct {
	mu               sync.RWMutex
	currentIDNum     int
	nodes            map[int]*node                  // 网络中的节点
	parentByChild    map[string]string              // childUUID -> parentUUID
	childrenByParent map[string]map[string]struct{} // parentUUID -> childUUID set
	edges            map[string][]string            // 邻接表: nodeUUID -> 邻居UUID列表
	edgeWeights      map[string]map[string]uint32   // 边权重: [从][到] = 权重
	edgeLatencies    map[string]map[string]latencySample
	edgeTypes        map[string]map[string]EdgeType
	routeInfo        map[string]*RouteInfo
	routingStrategy  atomic.Int32 // 当前路由策略
	history          map[string]int
	uuidIndex        map[string]int
	lastUpdateTime   time.Time
	depthCache       map[string]int
	nodeLatency      map[string]uint32
	networkMu        sync.RWMutex
	targetNetworks   map[string]string
	networks         map[string]*Network
	persist          Persistence

	// Gossip协议字段
	gossipConfig *protocol.GossipConfig
	nodeInfo     map[string]*protocol.NodeInfo // 本地节点信息缓存
	gossipCache  map[string]bool               // 防止gossip消息重复

	TaskChan      chan *TopoTask
	ResultChan    chan *topoResult
	service       *Service
	stopCh        chan struct{}
	stopOnce      sync.Once
	calcMu        sync.Mutex
	calcPending   bool
	calcScheduled bool

	// 离线判定策略（DTN/短连接友好）：
	// staleTimeout>0 表示预期唤醒周期；为 0 则视为长连接。
	// zeroSleepGrace 为 sleep=0 时的容忍窗口（避免瞬时抖动报警）。
	staleTimeout   time.Duration
	zeroSleepGrace time.Duration
}

type Network struct {
	ID      string
	Targets map[string]struct{}
	Created time.Time
}

type RouteInfo struct {
	Entry   string
	Path    []string
	Display string
	Depth   int
}

// SuppCandidate 表示携带评分信息的补链候选节点。
type SuppCandidate struct {
	UUID        string
	Path        []string
	Display     string
	Depth       int
	Overlap     int
	SleepBudget time.Duration
	Redundancy  float64
	WorkSeconds int
}

type EdgeType int

const (
	TreeEdge EdgeType = iota
	SupplementalEdge
)

type Edge struct {
	From    string
	To      string
	Type    EdgeType
	Weight  uint32
	Active  bool
	Updated time.Time
}

type latencySample struct {
	value   uint32
	updated time.Time
}

// node 表示网络中的一个节点
type node struct {
	uuid            string
	currentUser     string
	currentHostname string
	currentIP       string
	listenPort      int
	dialAddress     string
	fallbackPort    int
	transport       string
	tlsEnabled      bool
	lastSuccess     time.Time
	repairFailures  int
	repairUpdated   time.Time
	memo            string
	lastSeen        time.Time
	isAlive         bool
	// DTN/短连接属性（仅内存使用，初期不持久化）
	sleepSeconds int       // 预期睡眠周期（秒），0=长连接
	workSeconds  int       // 唤醒窗口长度（秒）
	nextWake     time.Time // 预计下次唤醒时间（可选）
}

// NodeRuntime 记录节点或会话的轻量级运行时元数据。
type NodeRuntime struct {
	UUID         string
	Memo         string
	NetworkID    string
	LastSeen     time.Time
	SleepSeconds int
	WorkSeconds  int
	NextWake     time.Time
}

// TopoTask 表示拓扑操作的任务
type TopoTask struct {
	Mode           int
	UUID           string
	UUIDNum        int
	ParentUUID     string
	Target         *node
	HostName       string
	UserName       string
	Memo           string
	Port           int
	IsFirst        bool
	DialAddress    string
	FallbackPort   int
	Transport      string
	LastSuccess    time.Time
	RepairAttempts int
	TlsEnabled     bool
	// 图特定字段
	NeighborUUID string
	Weight       uint32
	Force        bool
	EdgeType     EdgeType
	Response     chan *topoResult
	FilterID     string
	Count        int
	NetworkID    string
	LatencyMs    int64
	// 短连接/DTN（可选）
	SleepSeconds int
	WorkSeconds  int
	NextWakeUnix int64
	// SkipLiveness 用于阻止更新任务（例如配置变更）
	// 改动节点活性字段（LastSeen/IsAlive）。
	SkipLiveness bool
}

// NodeConnectionMeta 描述节点连接相关的补充信息。
type NodeConnectionMeta struct {
	ListenPort     int
	DialAddress    string
	FallbackPort   int
	Transport      string
	LastSuccess    time.Time
	RepairAttempts int
	TLSEnabled     bool
}

// topoResult 表示拓扑操作的结果。
type topoResult struct {
	IsExist        bool
	UUID           string
	Route          string
	IDNum          int
	AllNodes       []string
	Neighbors      []string
	Parent         string
	Children       []string
	Edges          []*Edge
	IP             string
	Port           int
	Depth          int
	IsSupplemental bool
	Candidates     []*SuppCandidate
	DialAddress    string
	FallbackPort   int
	Transport      string
	LastSuccess    time.Time
	RepairFailures int
	TLSEnabled     bool
}

// Result 是 topoResult 的导出别名，供更高层的服务使用。
type Result = topoResult

// NewTopology 创建一个新的基于图的拓扑。
func NewTopology() *Topology {
	topology := new(Topology)
	topology.nodes = make(map[int]*node)
	topology.parentByChild = make(map[string]string)
	topology.childrenByParent = make(map[string]map[string]struct{})
	topology.edges = make(map[string][]string)                // 图的邻接表
	topology.edgeWeights = make(map[string]map[string]uint32) // 边权重
	topology.edgeLatencies = make(map[string]map[string]latencySample)
	topology.edgeTypes = make(map[string]map[string]EdgeType)
	topology.routeInfo = make(map[string]*RouteInfo)
	topology.targetNetworks = make(map[string]string)
	topology.networks = make(map[string]*Network)
	topology.routingStrategy.Store(int32(RoutingByHops)) // 默认使用BFS路由
	topology.history = make(map[string]int)
	topology.uuidIndex = make(map[string]int)
	topology.lastUpdateTime = time.Now()
	topology.depthCache = make(map[string]int)
	topology.nodeLatency = make(map[string]uint32)

	// 初始化 gossip 协议字段
	topology.gossipConfig = &protocol.GossipConfig{
		GossipInterval:    30 * time.Second,
		HeartbeatInterval: 10 * time.Second,
		MaxTTL:            5,
		Fanout:            3,
	}
	topology.nodeInfo = make(map[string]*protocol.NodeInfo)
	topology.gossipCache = make(map[string]bool)

	topology.TaskChan = make(chan *TopoTask)
	topology.ResultChan = make(chan *topoResult)
	topology.service = &Service{topo: topology}
	topology.stopCh = make(chan struct{})
	return topology
}

// Service 返回拓扑的共享服务实例。
func (topology *Topology) Service() *Service {
	if topology == nil {
		return nil
	}
	if topology.service == nil {
		topology.service = &Service{topo: topology}
	}
	return topology.service
}

// RoutingStrategy 返回当前的路由策略。
func (topology *Topology) RoutingStrategy() RoutingStrategy {
	if topology == nil {
		return RoutingByHops
	}
	return RoutingStrategy(topology.routingStrategy.Load())
}

// ScheduleCalculate 会在一个短窗口内合并路由重算请求。
func (topology *Topology) ScheduleCalculate() {
	if topology == nil {
		return
	}
	topology.calcMu.Lock()
	if topology.calcScheduled {
		topology.calcPending = true
		topology.calcMu.Unlock()
		return
	}
	topology.calcScheduled = true
	topology.calcMu.Unlock()

	time.AfterFunc(calcDebounceInterval, func() {
		_, _ = topology.Execute(&TopoTask{Mode: CALCULATE})
		topology.calcMu.Lock()
		if topology.calcPending {
			topology.calcPending = false
			topology.calcScheduled = false
			topology.calcMu.Unlock()
			topology.ScheduleCalculate()
			return
		}
		topology.calcScheduled = false
		topology.calcMu.Unlock()
	})
}

// Request 使用给定上下文执行任务并返回结果。
func (topology *Topology) Request(ctx context.Context, task *TopoTask) (*Result, error) {
	svc := topology.Service()
	if svc == nil {
		return nil, ErrNilTopology
	}
	return svc.Request(ctx, task)
}

// Submit 异步入队任务并返回一个可等待的 future。
func (topology *Topology) Submit(ctx context.Context, task *TopoTask) (*Future, error) {
	svc := topology.Service()
	if svc == nil {
		return nil, ErrNilTopology
	}
	return svc.Submit(ctx, task)
}

// Execute 在后台上下文中执行任务。
func (topology *Topology) Execute(task *TopoTask) (*Result, error) {
	return topology.Request(context.Background(), task)
}

// NewNode 创建一个新节点。
func NewNode(uuid string, ip string) *node {
	node := new(node)
	node.uuid = uuid
	node.currentIP = ip
	node.lastSeen = time.Now()
	node.lastSuccess = time.Now()
	node.isAlive = true
	return node
}

// Stop 向拓扑调度器发出退出信号。
func (topology *Topology) Stop() {
	if topology == nil {
		return
	}
	topology.stopOnce.Do(func() {
		close(topology.stopCh)
	})
}

// Done 返回一个通道，在拓扑停止后会被关闭。
func (topology *Topology) Done() <-chan struct{} {
	if topology == nil {
		ch := make(chan struct{})
		close(ch)
		return ch
	}
	return topology.stopCh
}

// ConfigureStalePolicy 配置基于 sleep 的离线判定策略。
// sleep 为预期唤醒周期（0 表示长连接）；zeroGrace 为 sleep=0 时离线置位的容忍窗口。
func (topology *Topology) ConfigureStalePolicy(sleep, zeroGrace time.Duration) {
	if topology == nil {
		return
	}
	topology.mu.Lock()
	topology.staleTimeout = sleep
	topology.zeroSleepGrace = zeroGrace
	topology.mu.Unlock()
}

// StalePolicy 返回当前的离线判定策略参数（sleep, zeroGrace）。
func (topology *Topology) StalePolicy() (time.Duration, time.Duration) {
	if topology == nil {
		return 0, 0
	}
	topology.mu.RLock()
	sleep := topology.staleTimeout
	zero := topology.zeroSleepGrace
	topology.mu.RUnlock()
	return sleep, zero
}

// Enqueue 使用后台上下文将任务入队。
func (topology *Topology) Enqueue(task *TopoTask) error {
	return topology.EnqueueContext(context.Background(), task)
}

// EnqueueContext 在遵循上下文和停止信号的前提下入队任务。
func (topology *Topology) EnqueueContext(ctx context.Context, task *TopoTask) error {
	if topology == nil {
		return ErrNilTopology
	}
	if task == nil {
		return ErrNilTask
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-topology.stopCh:
		return ErrStopped
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	select {
	case topology.TaskChan <- task:
		return nil
	case <-topology.stopCh:
		return ErrStopped
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Run 启动拓扑管理器。
func (topology *Topology) Run() {
	for {
		select {
		case <-topology.stopCh:
			return
		case task := <-topology.TaskChan:
			if task == nil {
				continue
			}
			original := topology.ResultChan
			if task.Response != nil {
				topology.ResultChan = task.Response
			}
			topology.mu.Lock()
			topology.handleTask(task)
			topology.mu.Unlock()
			if task.Response != nil {
				topology.ResultChan = original
			}
		}
	}
}

func (topology *Topology) handleTask(task *TopoTask) {
	switch task.Mode {
	case ADDNODE:
		topology.addNode(task)
	case GETUUID:
		topology.getUUID(task)
	case GETUUIDNUM:
		topology.getUUIDNum(task)
	case CHECKNODE:
		topology.checkNode(task)
	case UPDATEDETAIL:
		topology.updateDetail(task)
	case SHOWDETAIL:
		topology.showDetail(task)
	case SHOWTOPO:
		topology.showTopo(task)
	case UPDATEMEMO:
		topology.updateMemo(task)
	case UPDATECONNINFO:
		topology.updateConnInfo(task)
	case GETCONNINFO:
		topology.getConnInfo(task)
	case CALCULATE:
		topology.calculate()
	case GETROUTE:
		topology.getRoute(task)
	case DELNODE:
		topology.delNode(task)
	case REONLINENODE:
		topology.reonlineNode(task)
	case ADDEDGE:
		topology.addEdge(task)
	case REMOVEEDGE:
		topology.removeEdge(task)
	case SETEDGEWEIGHT:
		topology.setEdgeWeight(task)
	case GETNEIGHBORS:
		topology.getNeighbors(task)
	case GETNODEMETA:
		topology.getNodeMeta(task)
	case SETROUTINGSTRATEGY:
		topology.setRoutingStrategy(RoutingStrategy(task.UUIDNum))
	case GETALLNODES:
		topology.getAllNodes(task)
	case GETDEPTH:
		topology.getDepthTask(task)
	case GETNODEINFO:
		topology.getNodeInfo(task)
	case REPARENTNODE:
		topology.reparentNode(task)
	case CHECKSUPPEDGE:
		topology.checkSupplementalEdge(task)
	case GETSUPPCANDIDATES:
		topology.getSuppCandidates(task)
	case PRUNEOFFLINE:
		topology.pruneOffline(task)
	case MARKSTALEOFFLINE:
		topology.markStaleOffline()
	case MARKNODEOFFLINE:
		topology.markNodeOffline(task)
	}
}
