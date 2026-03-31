package process

import (
	"context"
	"io"
	"sync"
	"time"

	"codeberg.org/agnoie/shepherd/internal/kelpie/dtn"
	"codeberg.org/agnoie/shepherd/internal/kelpie/initial"
	"codeberg.org/agnoie/shepherd/internal/kelpie/manager"
	"codeberg.org/agnoie/shepherd/internal/kelpie/stream"
	"codeberg.org/agnoie/shepherd/internal/kelpie/topology"
	"codeberg.org/agnoie/shepherd/pkg/global"
	"codeberg.org/agnoie/shepherd/pkg/session"
	"codeberg.org/agnoie/shepherd/protocol"
)

type Admin struct {
	ctx                 context.Context
	cancel              context.CancelFunc
	mgr                 *manager.Manager
	options             *initial.Options
	topology            *topology.Topology
	topoService         *topology.Service
	reconnMu            sync.Mutex
	gossipUpdateChan    chan *protocol.GossipUpdate
	session             session.Session
	routerCore          *routerCore
	reconnector         *Reconnector
	store               *global.Store
	sessions            *sessionRegistry
	sessionMetaMu       sync.RWMutex
	sessionMeta         map[string]sessionState
	pendingSleepMu      sync.Mutex
	pendingSleepUpdates map[string]pendingSleepUpdate
	suppPlanner         *SupplementalPlanner
	suppPendingMu       sync.Mutex
	pendingNodeAdds     []string
	pendingNodeRemovals []string
	pendingRetired      []pendingRetiredEvent
	pendingPromotions   []pendingPromotionEvent
	plannerMetricsSeed  topology.PlannerMetricsSnapshot
	hookCancelers       []func()
	dtnManager          *dtn.Manager
	dtnPersistor        dtn.Persistor
	dtnInflightMu       sync.Mutex
	dtnInflight         map[string]*dtnInflightRecord
	// DTN 计数器
	dtnEnqueued  uint64
	dtnDelivered uint64
	dtnFailed    uint64
	dtnRetried   uint64
	dtnSuppMu    sync.Mutex
	dtnSuppLast  map[string]time.Time
	// 策略参数
	dtnMaxInflightPerTarget int
	dtnSprayThreshold       float64
	dtnFocusThreshold       float64
	dtnFocusHold            time.Duration
	dtnMetricsMu            sync.RWMutex
	dtnMetrics              dtnMetricSnapshot
	streamEngine            *stream.Engine
	streamOpenOverride      func(ctx context.Context, target, sessionID string, meta map[string]string) (io.ReadWriteCloser, error)
	streamStats             streamStats
	streamReasonMu          sync.Mutex
	streamCloseReasons      map[uint32]string
	shutdownOverride        func(route, uuid string)
	routeOverride           func(uuid string) (string, bool)
	enqueueDiagOverride     func(target, data string, priority dtn.Priority, ttl time.Duration) (string, error)
	startOnce               sync.Once
	listenerStore           ListenerPersistence
	listeners               *listenerRegistry
	lootContentStore        LootContentStore
	listenerStartOverride   func(targetUUID, bind string, mode int, listenerID string) (string, error)
	listenerStopOverride    func(targetUUID, listenerID string) error
	controllerListeners     *controllerListenerRegistry
	controllerListenerStore ControllerListenerPersistence
	controllerListenerRuns  map[string]context.CancelFunc
	controllerListenerRunMu sync.Mutex
	portProxies             *portProxyManager
	networkMu               sync.RWMutex
	activeNetwork           string
	lootStore               LootPersistence
}
