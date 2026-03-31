package process

import (
	"context"
	"io"
	"sync"
	"time"

	"codeberg.org/agnoie/shepherd/internal/kelpie/dtn"
	"codeberg.org/agnoie/shepherd/internal/kelpie/initial"
	"codeberg.org/agnoie/shepherd/internal/kelpie/manager"
	"codeberg.org/agnoie/shepherd/internal/kelpie/planner"
	"codeberg.org/agnoie/shepherd/internal/kelpie/stream"
	"codeberg.org/agnoie/shepherd/internal/kelpie/topology"
	"codeberg.org/agnoie/shepherd/pkg/global"
	"codeberg.org/agnoie/shepherd/pkg/session"
	"codeberg.org/agnoie/shepherd/protocol"
)

type adminRuntime struct {
	mgr           *manager.Manager
	routerCore    *routerCore
	reconnector   *Reconnector
	reconnMu      sync.Mutex
	startOnce     sync.Once
	hookCancelers []func()
}

type adminSessionState struct {
	session       session.Session
	sessions      *sessionRegistry
	sessionMetaMu sync.RWMutex
	sessionMeta   map[string]sessionState
}

type adminSupplementalState struct {
	gossipUpdateChan    chan *protocol.GossipUpdate
	suppPlanner         *planner.SupplementalPlanner
	suppPendingMu       sync.Mutex
	pendingSleepMu      sync.Mutex
	pendingSleepUpdates map[string]pendingSleepUpdate
	pendingNodeAdds     []string
	pendingNodeRemovals []string
	pendingRetired      []pendingRetiredEvent
	pendingPromotions   []pendingPromotionEvent
	plannerMetricsSeed  topology.PlannerMetricsSnapshot
}

type adminDTNState struct {
	dtnManager    *dtn.Manager
	dtnPersistor  dtn.Persistor
	dtnInflightMu sync.Mutex
	dtnInflight   map[string]*dtnInflightRecord
	dtnEnqueued   uint64
	dtnDelivered  uint64
	dtnFailed     uint64
	dtnRetried    uint64
	dtnSuppMu     sync.Mutex
	dtnSuppLast   map[string]time.Time

	dtnMaxInflightPerTarget int
	dtnSprayThreshold       float64
	dtnFocusThreshold       float64
	dtnFocusHold            time.Duration
	dtnMetricsMu            sync.RWMutex
	dtnMetrics              dtnMetricSnapshot
}

type adminStreamState struct {
	streamEngine        *stream.Engine
	streamOpenOverride  func(ctx context.Context, target, sessionID string, meta map[string]string) (io.ReadWriteCloser, error)
	streamStats         streamStats
	streamReasonMu      sync.Mutex
	streamCloseReasons  map[uint32]string
	shutdownOverride    func(route, uuid string)
	routeOverride       func(uuid string) (string, bool)
	enqueueDiagOverride func(target, data string, priority dtn.Priority, ttl time.Duration) (string, error)
	portProxies         *portProxyManager
}

type adminListenerState struct {
	listenerStore           ListenerPersistence
	listeners               *listenerRegistry
	lootContentStore        LootContentStore
	listenerStartOverride   func(targetUUID, bind string, mode int, listenerID string) (string, error)
	listenerStopOverride    func(targetUUID, listenerID string) error
	controllerListeners     *controllerListenerRegistry
	controllerListenerStore ControllerListenerPersistence
	controllerListenerRuns  map[string]context.CancelFunc
	controllerListenerRunMu sync.Mutex
	lootStore               LootPersistence
}

type adminNetworkState struct {
	networkMu     sync.RWMutex
	activeNetwork string
}

// Admin 负责装配并协调 Kelpie 的子系统；具体能力按子域拆分为嵌入状态块。
type Admin struct {
	ctx         context.Context
	cancel      context.CancelFunc
	options     *initial.Options
	topology    *topology.Topology
	topoService *topology.Service
	store       *global.Store

	adminRuntime
	adminSessionState
	adminSupplementalState
	adminDTNState
	adminStreamState
	adminListenerState
	adminNetworkState
}
