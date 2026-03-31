package process

import (
	"context"
	"time"

	"codeberg.org/agnoie/shepherd/internal/kelpie/initial"
	"codeberg.org/agnoie/shepherd/internal/kelpie/manager"
	"codeberg.org/agnoie/shepherd/internal/kelpie/printer"
	"codeberg.org/agnoie/shepherd/internal/kelpie/supp"
	"codeberg.org/agnoie/shepherd/internal/kelpie/topology"
	"codeberg.org/agnoie/shepherd/pkg/global"
	"codeberg.org/agnoie/shepherd/pkg/session"
	"codeberg.org/agnoie/shepherd/protocol"
)

type adminBuilder struct {
	ctx             context.Context
	options         *initial.Options
	topo            *topology.Topology
	store           *global.Store
	sess            session.Session
	metrics         topology.PlannerMetricsSnapshot
	listenerStore   ListenerPersistence
	controllerStore ControllerListenerPersistence
	lootStore       LootPersistence
	lootContent     LootContentStore
}

func newAdminBuilder(ctx context.Context, opt *initial.Options, topo *topology.Topology, store *global.Store) *adminBuilder {
	return &adminBuilder{ctx: ctx, options: opt, topo: topo, store: store}
}

func (b *adminBuilder) withSession(sess session.Session) *adminBuilder {
	b.sess = sess
	return b
}

func (b *adminBuilder) withMetrics(metrics topology.PlannerMetricsSnapshot) *adminBuilder {
	b.metrics = metrics
	return b
}

func (b *adminBuilder) withListenerStore(store ListenerPersistence) *adminBuilder {
	b.listenerStore = store
	return b
}

func (b *adminBuilder) withControllerListenerStore(store ControllerListenerPersistence) *adminBuilder {
	b.controllerStore = store
	return b
}

func (b *adminBuilder) withLootStore(store LootPersistence) *adminBuilder {
	b.lootStore = store
	return b
}

func (b *adminBuilder) withLootContentStore(store LootContentStore) *adminBuilder {
	b.lootContent = store
	return b
}

func (b *adminBuilder) build() *Admin {
	ctx := b.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	derived, cancel := context.WithCancel(ctx)
	admin := &Admin{
		ctx:                derived,
		cancel:             cancel,
		options:            b.options,
		topology:           b.topo,
		topoService:        topology.NewService(b.topo),
		gossipUpdateChan:   make(chan *protocol.GossipUpdate, 32),
		store:              b.store,
		sessions:           newSessionRegistry(b.store, b.topo),
		plannerMetricsSeed: b.metrics,
	}
	if b.store != nil {
		admin.session = b.store.ActiveSession()
	}
	if b.sess != nil {
		admin.session = b.sess
	}
	admin.decorateSupplementalHooks()
	admin.initListeners(b.listenerStore)
	admin.initControllerListeners(b.controllerStore)
	admin.lootStore = b.lootStore
	admin.lootContentStore = b.lootContent
	return admin
}

func (admin *Admin) decorateSupplementalHooks() {
	admin.unregisterHooks()
	admin.hookCancelers = append(admin.hookCancelers, supp.RegisterSuppFailoverHook(func(uuid string) {
		if uuid == "" {
			return
		}
		attemptSupplementalFailover(admin.mgr, admin.topology, uuid)
	}))
	admin.hookCancelers = append(admin.hookCancelers, supp.RegisterNodeAddedHook(admin.handleNodeAdded))
	admin.hookCancelers = append(admin.hookCancelers, supp.RegisterSuppLinkFailedHook(func(linkUUID string, endpoints []string) {
		if admin.suppPlanner == nil {
			return
		}
		admin.suppPlanner.OnLinkFailed(linkUUID, endpoints)
	}))
	admin.hookCancelers = append(admin.hookCancelers, supp.RegisterSuppLinkRetiredHook(func(linkUUID string, endpoints []string, reason string) {
		admin.handleLinkRetired(linkUUID, endpoints, reason)
	}))
	admin.hookCancelers = append(admin.hookCancelers, supp.RegisterSuppLinkPromotedHook(func(linkUUID, parentUUID, childUUID string) {
		admin.handleLinkPromoted(linkUUID, parentUUID, childUUID)
	}))
	admin.hookCancelers = append(admin.hookCancelers, supp.RegisterSuppHeartbeatHook(func(linkUUID, endpoint string, status supp.SuppLinkHealth, ts time.Time) {
		if admin.suppPlanner == nil {
			return
		}
		admin.suppPlanner.OnHeartbeat(linkUUID, endpoint, status, ts)
	}))
	admin.hookCancelers = append(admin.hookCancelers, supp.RegisterNodeRemovedHook(admin.handleNodeRemoved))
}

func (admin *Admin) unregisterHooks() {
	if admin == nil {
		return
	}
	for _, cancel := range admin.hookCancelers {
		if cancel != nil {
			cancel()
		}
	}
	admin.hookCancelers = nil
}

func (admin *Admin) initManager(ctx context.Context) {
	admin.mgr = manager.NewManager(admin.store)
	if sess := admin.currentSession(); sess != nil {
		admin.mgr.SetSession(sess)
	}
	if admin.sessions != nil {
		admin.sessions.SetPrimaryCallback(admin.mgr.SetSession)
	}
	go admin.mgr.Run(ctx)
}

func (admin *Admin) initSupplementalPlanner(ctx context.Context) {
	admin.suppPlanner = NewSupplementalPlanner(admin.topology, admin.topoService, admin.mgr, admin.store)
	if admin.suppPlanner == nil {
		return
	}
	admin.suppPlanner.SetBaseOptions(admin.options)
	admin.suppPlanner.RestoreMetrics(admin.plannerMetricsSeed)
	admin.suppPlanner.Start(ctx)
	admin.flushPendingSuppEvents()
}

func (admin *Admin) initControllerListeners(store ControllerListenerPersistence) {
	if admin == nil {
		return
	}
	var seed []ControllerListenerRecord
	if store != nil {
		loaded, err := store.LoadControllerListeners()
		if err != nil {
			printer.Warning("[*] Failed to load controller listeners: %v\r\n", err)
		} else {
			seed = loaded
		}
	}
	admin.controllerListenerStore = store
	admin.controllerListeners = newControllerListenerRegistry(store, seed)
}

func (admin *Admin) startManagers(ctx context.Context) {
	go DispatchListenMess(ctx, admin.mgr, admin.topology)
	go DispatchConnectMess(ctx, admin.mgr)
	go DispatchInfoMess(ctx, admin.mgr, admin.topology)
	go DispatchChildrenMess(ctx, admin.mgr, admin.topology, admin.onNodeReonline)
	if admin.options != nil && admin.options.Heartbeat {
		go admin.runHeartbeat(ctx)
	}
}
