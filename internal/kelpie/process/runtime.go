package process

import (
	"context"
	"net"
	"time"

	"codeberg.org/agnoie/shepherd/internal/kelpie/dtn"
	"codeberg.org/agnoie/shepherd/internal/kelpie/initial"
	"codeberg.org/agnoie/shepherd/internal/kelpie/printer"
	"codeberg.org/agnoie/shepherd/internal/kelpie/topology"
	"codeberg.org/agnoie/shepherd/pkg/bus"
	"codeberg.org/agnoie/shepherd/pkg/config/defaults"
	"codeberg.org/agnoie/shepherd/pkg/global"
	"codeberg.org/agnoie/shepherd/pkg/session"
	"codeberg.org/agnoie/shepherd/protocol"
)

type ReconnectStatsView struct {
	Attempts  uint64
	Success   uint64
	Failures  uint64
	LastError string
}

func (admin *Admin) context() context.Context {
	if admin == nil || admin.ctx == nil {
		return context.Background()
	}
	return admin.ctx
}

func (admin *Admin) Stop() {
	if admin == nil || admin.cancel == nil {
		return
	}
	admin.unregisterHooks()
	if admin.topology != nil {
		admin.topology.Stop()
	}
	if admin.sessions != nil {
		admin.sessions.stop()
	}
	if admin.reconnector != nil {
		admin.reconnector.Close()
	}
	if admin.mgr != nil {
		admin.mgr.Close()
	}
	if admin.suppPlanner != nil {
		admin.suppPlanner.Stop()
	}
	if admin.portProxies != nil {
		admin.portProxies.StopAll()
	}
	admin.cancel()
}

func (admin *Admin) updateSessionConn(conn net.Conn) {
	sess := admin.currentSession()
	if sess != nil {
		sess.UpdateConn(conn)
	}
}

func (admin *Admin) connectionTriple() (net.Conn, string, string) {
	sess := admin.currentSession()
	if sess != nil {
		return sess.Conn(), sess.Secret(), sess.UUID()
	}
	if admin.store != nil {
		return admin.store.ActiveConn(), admin.store.ActiveSecret(), admin.store.ActiveUUID()
	}
	return nil, "", ""
}

func (admin *Admin) ensureReconnector() *Reconnector {
	if admin == nil {
		return nil
	}
	if admin.reconnector != nil {
		return admin.reconnector
	}
	rec := NewReconnector(admin.context(), admin.options, admin.topology)
	rec.WithConnUpdater(func(conn net.Conn) {
		if admin.store != nil {
			admin.store.UpdateActiveConn(conn)
		}
		admin.updateSessionConn(conn)
		admin.watchConn(admin.context(), conn)
	}).WithTopoUpdater(func() error {
		if admin.topoService == nil {
			return nil
		}
		_, err := admin.topoRequest(&topology.TopoTask{Mode: topology.CALCULATE})
		return err
	}).WithProtocolUpdater(func(nego *protocol.ProtocolMeta) {
		if admin.store != nil && nego != nil {
			admin.store.UpdateProtocolFlags(protocol.ADMIN_UUID, nego.Flags)
		}
	})
	admin.reconnector = rec
	return rec
}

func (admin *Admin) reconnectUpstream() bool {
	if admin == nil {
		return false
	}
	admin.reconnMu.Lock()
	defer admin.reconnMu.Unlock()

	printer.Warning("\r\n[*] Upstream connection lost, attempting to reconnect...\r\n")
	rec := admin.ensureReconnector()
	if rec == nil {
		printer.Fail("\r\n[*] Reconnector unavailable\r\n")
		return false
	}
	if _, _, err := rec.Attempt(admin.context(), admin.options); err != nil {
		printer.Fail("\r\n[*] Reconnect aborted: %v\r\n", err)
		return false
	}
	printer.Success("\r\n[*] Reconnected to upstream node\r\n")
	return true
}

func (admin *Admin) watchConn(ctx context.Context, conn net.Conn) {
	if ctx == nil || conn == nil {
		return
	}
	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()
}

func NewAdmin(ctx context.Context, opt *initial.Options, topo *topology.Topology, store *global.Store, sess session.Session, metrics topology.PlannerMetricsSnapshot, dtnPersistor dtn.Persistor, listenerStore ListenerPersistence, controllerStore ControllerListenerPersistence, lootStore LootPersistence, lootContent LootContentStore) *Admin {
	builder := newAdminBuilder(ctx, opt, topo, store).
		withMetrics(metrics).
		withListenerStore(listenerStore).
		withControllerListenerStore(controllerStore).
		withLootStore(lootStore).
		withLootContentStore(lootContent)
	if sess != nil {
		builder = builder.withSession(sess)
	}
	admin := builder.build()
	admin.dtnPersistor = dtnPersistor
	return admin
}

func (admin *Admin) Start() {
	if admin == nil {
		return
	}
	admin.startOnce.Do(func() {
		admin.bootstrapRuntime()
	})
}

func (admin *Admin) bootstrapRuntime() {
	ctx := admin.context()

	admin.initManager(ctx)
	admin.routerCore = newRouterCore(ctx, admin.mgr, admin.topology)
	if router := admin.routerCore.Router(); router != nil {
		router.Register(uint16(protocol.GOSSIP_UPDATE), admin.dispatchGossipUpdate())
		router.Register(uint16(protocol.GOSSIP_REQUEST), admin.dispatchGossipRequest())
		router.Register(uint16(protocol.GOSSIP_RESPONSE), admin.dispatchGossipResponse())
		router.Register(uint16(protocol.DTN_PULL), admin.dispatchDTNPull())
	}
	admin.initSupplementalPlanner(ctx)
	if admin.routerCore != nil {
		admin.routerCore.setPlanner(admin.suppPlanner)
		admin.routerCore.setStreamHandlers(admin.handleStreamOpen, admin.handleStreamData, admin.handleStreamAck, admin.handleStreamClose)
	}
	admin.initDTN(ctx)
	admin.initStreamEngine()
	admin.handleNetworkChange("")
	if admin.routerCore != nil {
		go admin.routerCore.handleFromDownstream(admin)
	}
	admin.startManagers(ctx)
	admin.startListenerReconciler(ctx)
	admin.startPendingControllerListeners()
	go admin.processGossipUpdates(ctx)
	go startStaleNodeMonitor(ctx, admin.topology)
	go admin.runPendingSleepUpdateFlusher(ctx)
}

// startStaleNodeMonitor 周期性触发拓扑任务，将超过阈值未更新的节点标记为离线。
func startStaleNodeMonitor(ctx context.Context, topo *topology.Topology) {
	if topo == nil {
		return
	}
	// 根据策略选择更快的扫描频率：
	// - sleep=0：使用更快的 500ms 轮询，贴近“立即”置离线
	// - sleep>0：选择 min(StalenessSweepInterval, sleep/2) 的频率
	sleep, zeroGrace := topo.StalePolicy()
	interval := defaults.StalenessSweepInterval
	if sleep > 0 {
		if d := sleep / 2; d > 0 && d < interval {
			interval = d
		}
	} else if zeroGrace > 0 {
		if d := zeroGrace / 2; d > 0 && d < interval {
			interval = d
		}
		if interval > 500*time.Millisecond {
			interval = 500 * time.Millisecond
		}
	}
	if interval <= 0 {
		interval = 1 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			svc := topo.Service()
			if svc == nil {
				continue
			}
			c, cancel := context.WithTimeout(ctx, defaults.TopologyRequestTimeout)
			_, _ = svc.Request(c, &topology.TopoTask{Mode: topology.MARKSTALEOFFLINE})
			cancel()
		}
	}
}

func (admin *Admin) RouterStats() map[uint16]bus.RouterCounter {
	if admin == nil || admin.routerCore == nil {
		return nil
	}
	if router := admin.routerCore.Router(); router != nil {
		return router.Stats()
	}
	return nil
}

func (admin *Admin) ReconnectStatsView() ReconnectStatsView {
	if admin == nil || admin.reconnector == nil {
		return ReconnectStatsView{}
	}
	stats := admin.reconnector.Stats()
	return ReconnectStatsView(stats)
}
