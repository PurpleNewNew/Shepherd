package process

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"codeberg.org/agnoie/shepherd/internal/kelpie/dtn"
	"codeberg.org/agnoie/shepherd/internal/kelpie/initial"
	"codeberg.org/agnoie/shepherd/internal/kelpie/manager"
	"codeberg.org/agnoie/shepherd/internal/kelpie/printer"
	"codeberg.org/agnoie/shepherd/internal/kelpie/stream"
	"codeberg.org/agnoie/shepherd/internal/kelpie/topology"
	"codeberg.org/agnoie/shepherd/internal/kelpie/uipb"
	"codeberg.org/agnoie/shepherd/pkg/bus"
	"codeberg.org/agnoie/shepherd/pkg/config/defaults"
	"codeberg.org/agnoie/shepherd/pkg/global"
	"codeberg.org/agnoie/shepherd/pkg/session"
	"codeberg.org/agnoie/shepherd/pkg/share/streamopts"
	"codeberg.org/agnoie/shepherd/pkg/streamid"
	"codeberg.org/agnoie/shepherd/pkg/utils"
	"codeberg.org/agnoie/shepherd/protocol"
)

func parseStreamOptions(opt string) map[string]string {
	return streamopts.Parse(opt)
}

func normalizeKind(kind string) string {
	kind = strings.TrimSpace(strings.ToLower(kind))
	if kind == "" {
		return "unknown"
	}
	return kind
}

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
	// DTN counters
	dtnEnqueued  uint64
	dtnDelivered uint64
	dtnFailed    uint64
	dtnRetried   uint64
	dtnSuppMu    sync.Mutex
	dtnSuppLast  map[string]time.Time
	// Policy
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

type dtnInflightRecord struct {
	bundle *dtn.Bundle
	sentAt time.Time
}

type streamStats struct {
	mu    sync.Mutex
	kinds map[string]*streamKindStats
}

type streamKindStats struct {
	opened     uint64
	closed     uint64
	lastReason string
	lastClosed time.Time
}

type StreamStat struct {
	Kind       string
	Opened     uint64
	Closed     uint64
	Active     uint64
	LastReason string
	LastClosed time.Time
}

type ReconnectStatsView struct {
	Attempts  uint64
	Success   uint64
	Failures  uint64
	LastError string
}

type NetworkInfo struct {
	ID      string
	Entries []string
}

// SleepUpdateParams captures optional fields for runtime sleep updates.
type SleepUpdateParams struct {
	SleepSeconds  *int
	WorkSeconds   *int
	JitterPercent *float64
}

type pendingSleepUpdate struct {
	params      SleepUpdateParams
	enqueuedAt  time.Time
	nextAttempt time.Time
	backoff     time.Duration
	attempts    int
	lastError   string
}

const (
	pendingSleepFlushInterval  = 250 * time.Millisecond
	pendingSleepInitialBackoff = 250 * time.Millisecond
	pendingSleepMaxBackoff     = 5 * time.Second
	pendingSleepMaxAge         = 30 * time.Minute
)

type dtnMetricSnapshot struct {
	Stats    dtn.QueueStats
	Captured time.Time
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

func (admin *Admin) currentSession() session.Session {
	if admin == nil {
		return nil
	}
	if admin.sessions != nil {
		if sess := admin.sessions.primary(); sess != nil {
			return sess
		}
	}
	if admin.store != nil {
		if sess := admin.store.ActiveSession(); sess != nil {
			return sess
		}
	}
	// bootstrap fallback (may be stale after failover/reconnect)
	if admin.session != nil {
		return admin.session
	}
	return nil
}

func (admin *Admin) context() context.Context {
	if admin == nil || admin.ctx == nil {
		return context.Background()
	}
	return admin.ctx
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

const (
	dtnSuppTriggerAge      = 2 * time.Minute
	dtnSuppTriggerCooldown = 2 * time.Minute
)

func (admin *Admin) initDTN(ctx context.Context) {
	if admin == nil {
		return
	}
	cfg := dtn.DefaultConfig()
	// DTN default pacing is tuned for control-plane diagnostics. For stream/dataplane
	// traffic we need a tighter dispatch cadence, otherwise large transfers can stall
	// behind long scheduler ticks and hit upper-layer timeouts.
	cfg.DispatchBatch = 64
	cfg.DispatchInterval = 500 * time.Millisecond
	admin.dtnManager = dtn.NewManager(cfg)
	if admin.dtnPersistor != nil {
		admin.dtnManager.SetPersistor(admin.dtnPersistor)
		if bundles, err := admin.dtnPersistor.LoadDTNBundles(); err != nil {
			printer.Warning("[*] Failed to load persisted DTN bundles: %v\r\n", err)
		} else if len(bundles) > 0 {
			restored := admin.dtnManager.Restore(bundles)
			if restored > 0 {
				printer.Warning("[*] Restored %d DTN bundle(s) from persistence\r\n", restored)
			}
		}
	}
	admin.dtnInflight = make(map[string]*dtnInflightRecord)
	// Default inflight should be high enough to sustain stream/dataplane throughput.
	// Traces can tighten this via dtn_policy when testing edge cases.
	admin.dtnMaxInflightPerTarget = 16
	admin.dtnSprayThreshold = 0.6
	admin.dtnFocusThreshold = 0.85
	admin.dtnFocusHold = 20 * time.Second
	admin.snapshotDTNMetrics()
	if ctx == nil {
		return
	}
	if admin.routerCore != nil {
		admin.routerCore.setDTNAckHandler(admin.onDTNAck)
	}
	go admin.runDTNDispatcher(ctx)
}

func (admin *Admin) initStreamEngine() {
	if admin == nil {
		return
	}
	send := func(target string, payload []byte) error {
		if admin == nil {
			return fmt.Errorf("admin unavailable")
		}
		// STREAM_* is latency-sensitive control/data-plane traffic; when DTN queues are under
		// pressure (e.g., rescue storms), a single dropped STREAM_OPEN / STREAM_CLOSE can
		// strand the whole transfer. Prefer higher DTN priority for control frames.
		prio := dtn.PriorityNormal
		env := string(payload)
		if strings.HasPrefix(env, "proto:") && len(env) >= len("proto:0000:") {
			if v, err := strconv.ParseUint(env[5:9], 16, 16); err == nil {
				switch uint16(v) {
				case protocol.STREAM_OPEN, protocol.STREAM_CLOSE, protocol.STREAM_ACK:
					prio = dtn.PriorityHigh
				}
			}
		}
		_, err := admin.EnqueueDiagnostic(target, env, prio, 0)
		return err
	}
	logFn := func(format string, args ...interface{}) {
		printer.Warning(format, args...)
	}
	// DTN-Stream rides on top of the DTN scheduler (sleep windows, repair, focus holds).
	// Use a DTN-friendly retransmission timeout budget; "LAN TCP-like" RTOs (e.g. 1-3s)
	// create false losses under churn and can wedge higher-level systems like dataplane.
	cfg := stream.DefaultConfig()
	// Larger chunks drastically reduce DTN bundle count for file transfers, making dataplane
	// "big file" scenarios complete within bounded timeouts under DTN scheduling.
	cfg.ChunkBytes = 8 * 1024
	cfg.MinWindow = 8
	cfg.InitialWindow = 12
	cfg.BaseRTO = 10 * time.Second
	cfg.MinRTO = 3 * time.Second
	cfg.MaxRTO = 2 * time.Minute
	cfg.IdleTimeout = 2 * time.Minute
	cfg.RetransLimit = 12
	admin.streamEngine = stream.New(cfg, send, logFn)
}

func (admin *Admin) recordStreamOpen(kind string) {
	if admin == nil {
		return
	}
	kind = normalizeKind(kind)
	admin.streamStats.mu.Lock()
	if admin.streamStats.kinds == nil {
		admin.streamStats.kinds = make(map[string]*streamKindStats)
	}
	stat := admin.streamStats.kinds[kind]
	if stat == nil {
		stat = &streamKindStats{}
		admin.streamStats.kinds[kind] = stat
	}
	stat.opened++
	admin.streamStats.mu.Unlock()
}

func (admin *Admin) recordStreamClose(kind, reason string) {
	if admin == nil {
		return
	}
	kind = normalizeKind(kind)
	admin.streamStats.mu.Lock()
	if admin.streamStats.kinds == nil {
		admin.streamStats.kinds = make(map[string]*streamKindStats)
	}
	stat := admin.streamStats.kinds[kind]
	if stat == nil {
		stat = &streamKindStats{}
		admin.streamStats.kinds[kind] = stat
	}
	stat.closed++
	if reason != "" {
		stat.lastReason = reason
	}
	stat.lastClosed = time.Now()
	admin.streamStats.mu.Unlock()
}

func (admin *Admin) rememberStreamReason(streamID uint32, reason string) {
	if admin == nil || streamID == 0 {
		return
	}
	admin.streamReasonMu.Lock()
	if admin.streamCloseReasons == nil {
		admin.streamCloseReasons = make(map[uint32]string)
	}
	if reason == "" {
		delete(admin.streamCloseReasons, streamID)
	} else {
		admin.streamCloseReasons[streamID] = reason
	}
	admin.streamReasonMu.Unlock()
}

// StreamCloseReason returns and clears the recorded close reason for a stream ID.
func (admin *Admin) StreamCloseReason(streamID uint32) string {
	if admin == nil || streamID == 0 {
		return ""
	}
	admin.streamReasonMu.Lock()
	reason := admin.streamCloseReasons[streamID]
	delete(admin.streamCloseReasons, streamID)
	admin.streamReasonMu.Unlock()
	return reason
}

func (admin *Admin) ensurePortProxyManager() *portProxyManager {
	if admin == nil {
		return nil
	}
	if admin.portProxies == nil {
		admin.portProxies = newPortProxyManager(admin.context, func(ctx context.Context, target, sessionID string, meta map[string]string) (io.ReadWriteCloser, error) {
			return admin.OpenStream(ctx, target, sessionID, meta)
		})
	}
	return admin.portProxies
}

func (admin *Admin) handleStreamOpen(header *protocol.Header, msg *protocol.StreamOpen) {
	if admin == nil || admin.streamEngine == nil || msg == nil {
		return
	}
	opts := parseStreamOptions(msg.Options)
	kind := strings.ToLower(opts["kind"])
	target := ""
	if header != nil {
		target = strings.TrimSpace(header.Sender)
	}
	if target == "" {
		printer.Fail("\r\n[*] Stream open missing sender, stream=%d\r\n", msg.StreamID)
		return
	}
	sessionStream := admin.streamEngine.Accept(msg.StreamID, stream.Options{Target: target, Meta: opts})
	if sessionStream == nil {
		return
	}
	switch kind {
	case "backward-conn":
		lport := strings.TrimSpace(opts["lport"])
		if lport == "" {
			sessionStream.Close()
			return
		}
		admin.recordStreamOpen(kind)
		go func() {
			defer sessionStream.Close()
			conn, err := net.Dial("tcp", net.JoinHostPort("127.0.0.1", lport))
			if err != nil {
				printer.Fail("\r\n[*] backward-conn dial %s failed: %v\r\n", lport, err)
				return
			}
			defer conn.Close()
			done := make(chan struct{}, 2)
			go func() { io.Copy(sessionStream, conn); done <- struct{}{} }()
			go func() { io.Copy(conn, sessionStream); done <- struct{}{} }()
			<-done
		}()
	default:
		admin.recordStreamOpen(kind)
		sessionStream.Close()
	}
}

func (admin *Admin) handleStreamData(_ *protocol.Header, msg *protocol.StreamData) {
	if admin == nil || admin.streamEngine == nil || msg == nil {
		return
	}
	admin.streamEngine.HandleData(msg)
}

func (admin *Admin) handleStreamAck(_ *protocol.Header, msg *protocol.StreamAck) {
	if admin == nil || admin.streamEngine == nil || msg == nil {
		return
	}
	admin.streamEngine.HandleAck(msg)
}

func (admin *Admin) handleStreamClose(hdr *protocol.Header, msg *protocol.StreamClose) {
	if admin == nil || admin.streamEngine == nil || msg == nil {
		return
	}
	meta := admin.streamEngine.SessionMeta(msg.StreamID)
	kind := ""
	if meta != nil {
		kind = meta["kind"]
	}
	target := ""
	if hdr != nil {
		target = strings.TrimSpace(hdr.Sender)
	}
	admin.recordStreamClose(kind, msg.Reason)
	admin.rememberStreamReason(msg.StreamID, msg.Reason)
	if kind == "" {
		kind = "unknown"
	}
	if msg.Reason != "" {
		printer.Warning("\r\n[*] Stream(%s) closed: %s\r\n", kind, msg.Reason)
	} else {
		printer.Warning("\r\n[*] Stream(%s) closed.\r\n", kind)
	}
	// 针对 file-put/file-get，记录完成的 loot（仅成功时）。
	if (kind == "file-put" || kind == "file-get") && admin.lootStore != nil {
		path := ""
		if meta != nil {
			path = strings.TrimSpace(meta["path"])
		}
		rec := LootRecord{
			TargetUUID: target,
			Category:   LootCategoryFile,
			Name:       path,
			OriginPath: path,
			Tags:       []string{kind, "completed"},
		}
		if parsed := parseStreamReason(msg.Reason); parsed != nil {
			if parsed.size > 0 {
				rec.Size = uint64(parsed.size)
			}
			if parsed.hash != "" {
				rec.Hash = parsed.hash
			}
			if parsed.mime != "" {
				rec.Mime = parsed.mime
			}
		}
		_, _ = admin.SubmitLoot(rec, nil)
	}
	admin.streamEngine.HandleClose(msg)
}

type streamCloseMeta struct {
	size int64
	hash string
	mime string
}

func parseStreamReason(reason string) *streamCloseMeta {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return nil
	}
	parts := strings.Fields(reason)
	if len(parts) == 0 {
		return nil
	}
	meta := &streamCloseMeta{}
	for _, p := range parts[1:] {
		kv := strings.SplitN(p, "=", 2)
		if len(kv) != 2 {
			continue
		}
		switch kv[0] {
		case "size":
			if sz, err := strconv.ParseInt(kv[1], 10, 64); err == nil {
				meta.size = sz
			}
		case "hash":
			meta.hash = kv[1]
		case "mime":
			meta.mime = kv[1]
		}
	}
	if meta.size == 0 && meta.hash == "" && meta.mime == "" {
		return nil
	}
	return meta
}

func (admin *Admin) runDTNDispatcher(ctx context.Context) {
	if admin == nil || admin.dtnManager == nil {
		return
	}
	interval := admin.dtnManager.Config().DispatchInterval
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			admin.flushDTNBundles(ctx)
		}
	}
}

func (admin *Admin) flushDTNBundles(ctx context.Context) {
	if admin == nil || admin.dtnManager == nil {
		return
	}
	now := time.Now()
	admin.requeueExpiredInflight(now)
	cfg := admin.dtnManager.Config()
	ready := admin.dtnManager.Ready(now, cfg.DispatchBatch)
	if len(ready) == 0 {
		admin.snapshotDTNMetrics()
		return
	}
	for _, bundle := range ready {
		if delay := admin.preDispatchDelay(bundle); delay > 0 {
			admin.dtnManager.Requeue(bundle, delay)
			continue
		}
		if admin.attemptBundleDelivery(ctx, bundle) {
			continue
		}
		// 基于窗口的重排队：若首跳处于睡眠，按预计唤醒设置延后；否则退避。
		delay := admin.nextSendDelay(bundle.Target)
		if delay <= 0 {
			backoff := cfg.DispatchInterval * time.Duration(bundle.Attempts+1)
			if backoff > time.Minute {
				backoff = time.Minute
			}
			delay = backoff
		}
		admin.dtnManager.Requeue(bundle, delay)
	}
	admin.snapshotDTNMetrics()
}

func (admin *Admin) dispatchDTNPull() bus.Handler {
	return func(ctx context.Context, header *protocol.Header, payload interface{}) error {
		req, ok := payload.(*protocol.DTNPull)
		if !ok {
			return fmt.Errorf("expected *protocol.DTNPull, got %T", payload)
		}
		if admin == nil || admin.dtnManager == nil || header == nil || header.Sender == "" {
			return nil
		}
		limit := int(req.Limit)
		if limit <= 0 {
			limit = admin.dtnManager.Config().DispatchBatch
		}
		now := time.Now()
		ready := admin.dtnManager.ReadyFor(header.Sender, now, limit)
		for _, b := range ready {
			if delay := admin.preDispatchDelay(b); delay > 0 {
				admin.dtnManager.Requeue(b, delay)
				continue
			}
			if !admin.attemptBundleDelivery(ctx, b) {
				// push back if failed
				admin.dtnManager.Requeue(b, admin.nextSendDelay(b.Target))
			}
		}
		admin.snapshotDTNMetrics()
		return nil
	}
}

func (admin *Admin) preDispatchDelay(bundle *dtn.Bundle) time.Duration {
	if admin == nil || admin.dtnManager == nil || bundle == nil {
		return 0
	}
	stats := admin.dtnManager.Stats(bundle.Target)
	cap := stats.Capacity
	if cap <= 0 {
		cap = admin.dtnManager.Config().PerNodeCapacity
	}
	if cap <= 0 {
		return 0
	}
	load := float64(stats.Total) / float64(cap)
	if load < admin.dtnSprayThreshold {
		return 0
	}
	inflight := admin.inflightForTarget(bundle.Target)
	top := admin.dtnFocusThreshold
	if top <= 0 {
		top = 0.85
	}
	var heldRatio float64
	if stats.Total > 0 {
		heldRatio = float64(stats.Held) / float64(stats.Total)
	}
	if (load >= top || heldRatio >= 0.5) && inflight > 0 {
		delay := time.Duration(load * load * float64(admin.dtnFocusHold))
		if delay < admin.dtnManager.Config().DispatchInterval {
			delay = admin.dtnManager.Config().DispatchInterval
		}
		if delay > time.Minute {
			delay = time.Minute
		}
		return delay
	}
	if admin.dtnMaxInflightPerTarget > 0 && inflight >= admin.dtnMaxInflightPerTarget {
		delay := time.Duration(load * float64(admin.dtnManager.Config().DispatchInterval))
		if delay < admin.dtnManager.Config().DispatchInterval {
			delay = admin.dtnManager.Config().DispatchInterval
		}
		return delay
	}
	return 0
}

// nextSendDelay 估算从现在到“首跳可发送”的等待时长；若不可达或无需等待则返回 0。
func (admin *Admin) nextSendDelay(target string) time.Duration {
	if admin == nil || admin.topology == nil || strings.TrimSpace(target) == "" {
		return 0
	}
	delay := admin.topology.RecommendSendDelay(target, time.Now())
	if delay <= 0 {
		return 0
	}
	sleepBudget := admin.topology.PathSleepBudget(target)
	if sleepBudget > 0 && delay > sleepBudget {
		return sleepBudget
	}
	return delay
}

func (admin *Admin) attemptBundleDelivery(ctx context.Context, bundle *dtn.Bundle) bool {
	if admin == nil || bundle == nil {
		return false
	}
	// per-target inflight limit
	if admin.inflightForTarget(bundle.Target) >= admin.dtnMaxInflightPerTarget {
		return false
	}
	route, ok := admin.fetchRoute(bundle.Target)
	if !ok || route == "" {
		return false
	}
	firstHop := dtnRouteFirstHop(route)
	if firstHop == "" {
		printer.Warning("\r\n[diag][dtn_dispatch] stage=invalid_route bundle=%s target=%s route=%s\r\n",
			shortUUID(bundle.ID), shortUUID(bundle.Target), route)
		return false
	}
	sess := admin.sessionForRoute(route)
	if sess == nil || sess.Conn() == nil {
		printer.Warning("\r\n[diag][dtn_dispatch] stage=session_unavailable bundle=%s target=%s route=%s first_hop=%s session_key=%s\r\n",
			shortUUID(bundle.ID), shortUUID(bundle.Target), route, shortUUID(firstHop), shortUUID(firstHop))
		return false
	}
	if firstHop != "" && sess.UUID() != "" && sess.UUID() != firstHop {
		printer.Warning("\r\n[diag][dtn_dispatch] stage=session_fallback bundle=%s target=%s route=%s first_hop=%s session=%s conn=%s\r\n",
			shortUUID(bundle.ID), shortUUID(bundle.Target), route, shortUUID(firstHop), shortUUID(sess.UUID()), connEndpoints(sess.Conn()))
	}
	if firstHop != "" && sess.UUID() != "" &&
		sess.UUID() != firstHop &&
		sess.UUID() != protocol.ADMIN_UUID &&
		sess.UUID() != protocol.TEMP_UUID {
		printer.Warning("\r\n[diag][dtn_dispatch] stage=session_mismatch bundle=%s target=%s route=%s first_hop=%s session=%s conn=%s\r\n",
			shortUUID(bundle.ID), shortUUID(bundle.Target), route, shortUUID(firstHop), shortUUID(sess.UUID()), connEndpoints(sess.Conn()))
	}
	msg := protocol.NewDownMsg(sess.Conn(), sess.Secret(), sess.UUID())
	protocol.SetMessageMeta(msg, sess.ProtocolVersion(), sess.ProtocolFlags())
	header := &protocol.Header{
		Sender:      sess.UUID(),
		Accepter:    protocol.TEMP_UUID,
		MessageType: uint16(protocol.DTN_DATA),
		RouteLen:    uint32(len([]byte(route))),
		Route:       route,
	}
	payload := &protocol.DTNData{BundleIDLen: uint16(len(bundle.ID)), BundleID: bundle.ID, Payload: append([]byte(nil), bundle.Payload...)}
	protocol.ConstructMessage(msg, header, payload, false)
	msg.SendMessage()
	// Track inflight awaiting DTN_ACK
	admin.rememberInflightAt(bundle, time.Now())
	printer.Success("\r\n[*] DTN delivered bundle %s -> %s (route=%s via=%s session=%s conn=%s) [memo len=%d]\r\n",
		shortUUID(bundle.ID), shortUUID(bundle.Target), route, shortUUID(firstHop), shortUUID(sess.UUID()), connEndpoints(sess.Conn()), len(bundle.Payload))
	return true
}

func (admin *Admin) sessionForComponent(uuid string) session.Session {
	if admin == nil {
		return nil
	}
	uuid = strings.TrimSpace(uuid)
	if uuid == "" {
		return nil
	}
	if admin.sessions != nil {
		if sess := admin.sessions.sessionForComponent(uuid); sess != nil {
			return sess
		}
	}
	if admin.store != nil {
		if sess := admin.store.SessionFor(uuid); sess != nil {
			return sess
		}
	}
	return nil
}

func dtnRouteFirstHop(route string) string {
	route = strings.TrimSpace(route)
	if route == "" {
		return ""
	}
	parts := strings.Split(route, ":")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		hop, _ := stripRouteSegment(part)
		return hop
	}
	return ""
}

func connEndpoints(conn net.Conn) string {
	if conn == nil {
		return "-"
	}
	local := "-"
	if addr := conn.LocalAddr(); addr != nil {
		local = addr.String()
	}
	remote := "-"
	if addr := conn.RemoteAddr(); addr != nil {
		remote = addr.String()
	}
	return local + "->" + remote
}

func (admin *Admin) onDTNAck(ack *protocol.DTNAck) {
	if admin == nil || ack == nil || admin.dtnManager == nil {
		return
	}
	// Fetch and forget inflight record
	bundle := admin.forgetInflight(ack.BundleID)
	if ack.OK != 0 {
		// Finalize: ensure queue cleaned up (in case it was requeued after a timeout).
		_, removed := admin.dtnManager.Remove(ack.BundleID)
		// Count delivery only if this ACK corresponds to an inflight send, or it removed a queued retry.
		// Otherwise it's a stale/duplicate ACK.
		if bundle != nil || removed {
			admin.dtnDelivered++
		}
		if bundle != nil {
			admin.recalculateHoldForTarget(bundle.Target)
			admin.flushDTNBundles(admin.context())
		}
		return
	}
	// Error path: re-enqueue if TTL still valid
	if bundle == nil {
		return
	}
	admin.dtnFailed++
	now := time.Now()
	if !bundle.DeliverBy.IsZero() {
		if !bundle.DeliverBy.After(now) {
			// expired, drop
			return
		}
	}
	// Requeue the same bundle to preserve BundleID across retries (important for restart recovery).
	// Try to align with next expected send window (sleep), otherwise use a bounded backoff.
	delay := admin.nextSendDelay(bundle.Target)
	if delay <= 0 && admin.dtnManager != nil {
		cfg := admin.dtnManager.Config()
		backoff := cfg.DispatchInterval * time.Duration(bundle.Attempts+1)
		if backoff > time.Minute {
			backoff = time.Minute
		}
		delay = backoff
	}
	admin.dtnManager.Requeue(bundle, delay)
	admin.flushDTNBundles(admin.context())
	admin.dtnRetried++
}

func (admin *Admin) recalculateHoldForTarget(target string) {
	if admin == nil || admin.topology == nil || admin.dtnManager == nil {
		return
	}
	delay := admin.topology.RecommendSendDelay(target, time.Now())
	var hold time.Time
	if delay > 0 {
		hold = time.Now().Add(delay)
	}
	_ = admin.dtnManager.RecalculateHoldForTarget(target, hold)
}

func (admin *Admin) onNodeReonline(uuid string) {
	if admin == nil || admin.dtnManager == nil {
		return
	}
	uuid = strings.TrimSpace(uuid)
	if uuid == "" {
		return
	}
	// A node can come back online while its bundles are still waiting on a stale
	// HoldUntil derived from an older sleep estimate. Release holds immediately so
	// short work windows are not missed.
	if changed := admin.dtnManager.RecalculateHoldForTarget(uuid, time.Time{}); changed > 0 {
		printer.Warning("\r\n[*] DTN hold released on node reonline: %s bundles=%d\r\n", shortUUID(uuid), changed)
	}
	admin.flushDTNBundles(admin.context())
}

const (
	dtnMinAckTimeout  = 10 * time.Second
	dtnBaseAckTimeout = 15 * time.Second
	dtnMaxAckTimeout  = 10 * time.Minute
)

func (admin *Admin) dtnAckTimeout(target string) time.Duration {
	if admin == nil {
		return dtnBaseAckTimeout
	}
	timeout := dtnBaseAckTimeout
	if admin.topology != nil && strings.TrimSpace(target) != "" {
		// Budget is per-direction expected wait. Use a single budget unit here; using 2x
		// can make retries too sluggish in short traces when intermediate hops lose route
		// during self-heal (star/chain flake cases).
		if budget := admin.topology.PathSleepBudget(target); budget > 0 {
			timeout += budget
		}
	}
	if admin.dtnManager != nil {
		if interval := admin.dtnManager.Config().DispatchInterval; interval > 0 {
			timeout += interval
		}
	}
	if timeout < dtnMinAckTimeout {
		timeout = dtnMinAckTimeout
	}
	if timeout > dtnMaxAckTimeout {
		timeout = dtnMaxAckTimeout
	}
	return timeout
}

func (admin *Admin) requeueExpiredInflight(now time.Time) {
	if admin == nil || admin.dtnManager == nil {
		return
	}
	var timedOut []*dtn.Bundle
	admin.dtnInflightMu.Lock()
	for id, rec := range admin.dtnInflight {
		if rec == nil || rec.bundle == nil {
			delete(admin.dtnInflight, id)
			continue
		}
		timeout := admin.dtnAckTimeout(rec.bundle.Target)
		if rec.sentAt.IsZero() || now.Sub(rec.sentAt) <= timeout {
			continue
		}
		timedOut = append(timedOut, rec.bundle)
		delete(admin.dtnInflight, id)
	}
	admin.dtnInflightMu.Unlock()

	cfg := admin.dtnManager.Config()
	for _, b := range timedOut {
		if b == nil {
			continue
		}
		if !b.DeliverBy.IsZero() && now.After(b.DeliverBy) {
			// expired, drop
			admin.dtnFailed++
			continue
		}
		delay := admin.nextSendDelay(b.Target)
		if delay <= 0 {
			backoff := cfg.DispatchInterval * time.Duration(b.Attempts+1)
			if backoff < cfg.DispatchInterval {
				backoff = cfg.DispatchInterval
			}
			if backoff > time.Minute {
				backoff = time.Minute
			}
			delay = backoff
		}
		admin.dtnManager.Requeue(b, delay)
		admin.dtnRetried++
		printer.Warning("\r\n[*] DTN bundle %s ACK timeout; requeued (delay=%s attempts=%d)\r\n",
			shortUUID(b.ID), delay, b.Attempts)
	}
}

func (admin *Admin) rememberInflightAt(b *dtn.Bundle, sentAt time.Time) {
	if admin == nil || b == nil {
		return
	}
	admin.dtnInflightMu.Lock()
	admin.dtnInflight[b.ID] = &dtnInflightRecord{bundle: b, sentAt: sentAt}
	admin.dtnInflightMu.Unlock()
}

func (admin *Admin) forgetInflight(id string) *dtn.Bundle {
	if admin == nil || id == "" {
		return nil
	}
	admin.dtnInflightMu.Lock()
	rec := admin.dtnInflight[id]
	delete(admin.dtnInflight, id)
	admin.dtnInflightMu.Unlock()
	if rec == nil {
		return nil
	}
	return rec.bundle
}

func (admin *Admin) inflightForTarget(target string) int {
	if admin == nil || target == "" {
		return 0
	}
	admin.dtnInflightMu.Lock()
	defer admin.dtnInflightMu.Unlock()
	count := 0
	for _, rec := range admin.dtnInflight {
		if rec != nil && rec.bundle != nil && rec.bundle.Target == target {
			count++
		}
	}
	return count
}

func (admin *Admin) EnqueueDiagnostic(target, data string, priority dtn.Priority, ttl time.Duration) (string, error) {
	if admin == nil {
		return "", fmt.Errorf("dtn manager unavailable")
	}
	if admin.enqueueDiagOverride != nil {
		return admin.enqueueDiagOverride(target, data, priority, ttl)
	}
	if admin.dtnManager == nil {
		return "", fmt.Errorf("dtn manager unavailable")
	}
	opts := []dtn.EnqueueOptions{dtn.WithPriority(priority)}
	if ttl > 0 {
		opts = append(opts, dtn.WithTTL(ttl))
	}
	if admin.topology != nil {
		if delay := admin.topology.RecommendSendDelay(target, time.Now()); delay > 0 {
			opts = append(opts, dtn.WithHoldUntil(time.Now().Add(delay)))
		}
	}
	bundle, err := admin.dtnManager.Enqueue(target, []byte(data), opts...)
	if err != nil {
		return "", err
	}
	admin.dtnEnqueued++
	return bundle.ID, nil
}

func (admin *Admin) QueueStats(target string) (dtn.QueueStats, error) {
	if admin == nil || admin.dtnManager == nil {
		return dtn.QueueStats{}, fmt.Errorf("dtn manager unavailable")
	}
	return admin.dtnManager.Stats(target), nil
}

func (admin *Admin) ListBundles(target string, limit int) ([]dtn.BundleSummary, error) {
	if admin == nil || admin.dtnManager == nil {
		return nil, fmt.Errorf("dtn manager unavailable")
	}
	return admin.dtnManager.List(target, limit), nil
}

func (admin *Admin) OpenStream(ctx context.Context, target, sessionID string, meta map[string]string) (io.ReadWriteCloser, error) {
	if admin == nil || admin.streamEngine == nil {
		return nil, fmt.Errorf("stream engine unavailable")
	}
	opts := map[string]string{}
	for k, v := range meta {
		opts[k] = v
	}
	if sessionID != "" {
		opts["session"] = sessionID
	}
	stream, err := admin.streamEngine.Open(ctx, target, stream.Options{Target: target, Meta: opts})
	if err != nil {
		return nil, err
	}
	admin.recordStreamOpen(opts["kind"])
	return stream, nil
}

// ShellSessionID returns the current/desired shell session identifier for a node.
func (admin *Admin) ShellSessionID(uuid string) string {
	if admin == nil || admin.mgr == nil || admin.mgr.ShellManager == nil {
		return uuid
	}
	sessionID := admin.mgr.ShellManager.SessionID(uuid)
	if sessionID == "" {
		return uuid
	}
	return sessionID
}

// SendShellWindowSize forwards shell resize hints to the active session.
// RequestPinAwake asks a node to stay awake for duration seconds.
// Pin-awake retired in V2; no RequestPinAwake.

// DTNStats implements DTNController
func (admin *Admin) DTNStats() (uint64, uint64, uint64, uint64) {
	if admin == nil {
		return 0, 0, 0, 0
	}
	return admin.dtnEnqueued, admin.dtnDelivered, admin.dtnFailed, admin.dtnRetried
}

func (admin *Admin) snapshotDTNMetrics() {
	if admin == nil || admin.dtnManager == nil {
		return
	}
	stats := admin.dtnManager.Stats("")
	admin.dtnMetricsMu.Lock()
	admin.dtnMetrics = dtnMetricSnapshot{Stats: stats, Captured: time.Now()}
	admin.dtnMetricsMu.Unlock()
	admin.maybeTriggerSupplementalFromDTN(stats)
}

func (admin *Admin) maybeTriggerSupplementalFromDTN(stats dtn.QueueStats) {
	if admin == nil || admin.suppPlanner == nil || !admin.suppPlanner.Enabled() {
		return
	}
	target := strings.TrimSpace(stats.OldestTarget)
	if target == "" || stats.OldestAge < dtnSuppTriggerAge {
		return
	}
	now := time.Now()
	admin.dtnSuppMu.Lock()
	if admin.dtnSuppLast == nil {
		admin.dtnSuppLast = make(map[string]time.Time)
	}
	last := admin.dtnSuppLast[target]
	if now.Sub(last) < dtnSuppTriggerCooldown {
		admin.dtnSuppMu.Unlock()
		return
	}
	admin.dtnSuppLast[target] = now
	admin.dtnSuppMu.Unlock()
	_ = admin.suppPlanner.RequestManualRepair(target)
}

func (admin *Admin) DTNMetricsSnapshot() (dtn.QueueStats, time.Time) {
	admin.dtnMetricsMu.RLock()
	snapshot := admin.dtnMetrics
	admin.dtnMetricsMu.RUnlock()
	return snapshot.Stats, snapshot.Captured
}

// TopologySnapshot returns a UI-friendly snapshot of current nodes and edges.
func (admin *Admin) TopologySnapshot(entry, network string) topology.UISnapshot {
	if admin == nil || admin.topology == nil {
		return topology.UISnapshot{}
	}
	return admin.topology.UISnapshot(entry, network)
}

// StreamDiagnostics exposes current stream engine diagnostics.
func (admin *Admin) StreamDiagnostics() []stream.SessionDiag {
	if admin == nil || admin.streamEngine == nil {
		return nil
	}
	return admin.streamEngine.Diagnostics()
}

func (admin *Admin) CloseStream(sessionID uint32, reason string) error {
	if admin == nil || admin.streamEngine == nil {
		return fmt.Errorf("stream engine unavailable")
	}
	if sessionID == 0 {
		return fmt.Errorf("stream id required")
	}
	if ok := admin.streamEngine.Close(sessionID, reason); !ok {
		return fmt.Errorf("stream %d not found", sessionID)
	}
	admin.rememberStreamReason(sessionID, reason)
	return nil
}

func (admin *Admin) StreamPing(targetUUID string, count, payloadSize int) error {
	if admin == nil {
		return fmt.Errorf("admin unavailable")
	}
	targetUUID = strings.TrimSpace(targetUUID)
	if targetUUID == "" {
		return ErrStreamPingMissingTarget
	}
	if count <= 0 {
		return ErrStreamPingInvalidCount
	}
	if payloadSize < 0 {
		return ErrStreamPingInvalidSize
	}
	streamID := streamid.Next()
	open := &protocol.StreamOpen{StreamID: streamID, Options: "mode=ping"}
	openBytes, _ := protocol.EncodePayload(open)
	openEnvelope := fmt.Sprintf("proto:%04x:%s", uint16(protocol.STREAM_OPEN), hex.EncodeToString(openBytes))
	if _, err := admin.EnqueueDiagnostic(targetUUID, openEnvelope, dtn.PriorityNormal, 0); err != nil {
		return fmt.Errorf("enqueue open failed: %w", err)
	}
	for i := 1; i <= count; i++ {
		data := &protocol.StreamData{StreamID: streamID, Seq: uint32(i)}
		if payloadSize > 0 {
			data.Payload = make([]byte, payloadSize)
		}
		payload, _ := protocol.EncodePayload(data)
		env := fmt.Sprintf("proto:%04x:%s", uint16(protocol.STREAM_DATA), hex.EncodeToString(payload))
		if _, err := admin.EnqueueDiagnostic(targetUUID, env, dtn.PriorityNormal, 0); err != nil {
			return fmt.Errorf("enqueue data #%d failed: %w", i, err)
		}
	}
	close := &protocol.StreamClose{StreamID: streamID, Code: 0, Reason: "ok"}
	closeBytes, _ := protocol.EncodePayload(close)
	closeEnv := fmt.Sprintf("proto:%04x:%s", uint16(protocol.STREAM_CLOSE), hex.EncodeToString(closeBytes))
	if _, err := admin.EnqueueDiagnostic(targetUUID, closeEnv, dtn.PriorityNormal, 0); err != nil {
		return fmt.Errorf("enqueue close failed: %w", err)
	}
	return nil
}

// DTNPolicy implements DTNController
func (admin *Admin) DTNPolicy() map[string]string {
	if admin == nil {
		return map[string]string{}
	}
	enq, drp, exp := admin.dtnManager.Metrics()
	return map[string]string{
		"max_inflight_per_target": fmt.Sprintf("%d", admin.dtnMaxInflightPerTarget),
		"queue_enqueued":          fmt.Sprintf("%d", enq),
		"queue_dropped":           fmt.Sprintf("%d", drp),
		"queue_expired":           fmt.Sprintf("%d", exp),
	}
}

// SetDTNPolicy implements DTNController
func (admin *Admin) SetDTNPolicy(key, value string) error {
	if admin == nil {
		return fmt.Errorf("admin unavailable")
	}
	switch strings.ToLower(strings.TrimSpace(key)) {
	case "max_inflight_per_target":
		v, err := strconv.Atoi(value)
		if err != nil || v < 0 || v > 64 {
			return fmt.Errorf("invalid value: %s", value)
		}
		admin.dtnMaxInflightPerTarget = v
		return nil
	default:
		return fmt.Errorf("unknown key: %s", key)
	}
}

func (admin *Admin) StreamStats() []StreamStat {
	if admin == nil {
		return nil
	}
	admin.streamStats.mu.Lock()
	defer admin.streamStats.mu.Unlock()
	snapshot := make([]StreamStat, 0, len(admin.streamStats.kinds))
	for kind, stat := range admin.streamStats.kinds {
		active := uint64(0)
		if stat.opened > stat.closed {
			active = stat.opened - stat.closed
		}
		snapshot = append(snapshot, StreamStat{
			Kind:       kind,
			Opened:     stat.opened,
			Closed:     stat.closed,
			Active:     active,
			LastReason: stat.lastReason,
			LastClosed: stat.lastClosed,
		})
	}
	sort.Slice(snapshot, func(i, j int) bool {
		return snapshot[i].Kind < snapshot[j].Kind
	})
	return snapshot
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

func (admin *Admin) sessionForUUID(uuid string) session.Session {
	if admin == nil {
		return nil
	}
	uuid = strings.TrimSpace(uuid)
	if uuid == "" {
		return admin.currentSession()
	}
	if sess := admin.sessionForComponent(uuid); sess != nil && sess.Conn() != nil {
		return sess
	}
	var (
		route string
		ok    bool
	)
	if fn := admin.routeOverride; fn != nil {
		route, ok = fn(uuid)
	} else {
		route, ok = admin.fetchRoute(uuid)
	}
	if ok && strings.TrimSpace(route) != "" {
		return admin.sessionForRoute(route)
	}
	return admin.currentSession()
}

func (admin *Admin) sessionForRoute(route string) session.Session {
	if admin == nil {
		return nil
	}
	route = strings.TrimSpace(route)
	if route == "" {
		return admin.currentSession()
	}
	firstHop := dtnRouteFirstHop(route)
	if firstHop != "" {
		if sess := admin.sessionForComponent(firstHop); sess != nil && sess.Conn() != nil {
			return sess
		}
	}
	sess := admin.currentSession()
	if sess == nil || sess.Conn() == nil {
		return nil
	}
	if firstHop == "" {
		return sess
	}
	sessUUID := strings.TrimSpace(sess.UUID())
	if sessUUID == firstHop || sessUUID == protocol.ADMIN_UUID || sessUUID == protocol.TEMP_UUID {
		return sess
	}
	return nil
}

func (admin *Admin) newDownstreamMessageForRoute(targetUUID, route string) (protocol.Message, error) {
	var sess session.Session
	if strings.TrimSpace(route) != "" {
		sess = admin.sessionForRoute(route)
	} else {
		sess = admin.sessionForUUID(targetUUID)
	}
	if sess == nil {
		return nil, fmt.Errorf("session unavailable for %s", targetUUID)
	}
	conn := sess.Conn()
	if conn == nil {
		return nil, fmt.Errorf("connection unavailable for %s", targetUUID)
	}
	msg := protocol.NewDownMsg(conn, sess.Secret(), sess.UUID())
	protocol.SetMessageMeta(msg, sess.ProtocolVersion(), sess.ProtocolFlags())
	return msg, nil
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
	}).WithProtocolUpdater(func(nego *protocol.Negotiation) {
		if admin.store != nil && nego != nil {
			admin.store.UpdateProtocol(protocol.ADMIN_UUID, nego.Version, nego.Flags)
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

func (admin *Admin) fetchRoute(uuid string) (string, bool) {
	if uuid == "" {
		return "", false
	}
	if uuid == protocol.ADMIN_UUID {
		return "", true
	}
	if !admin.nodeExists(uuid) {
		return "", false
	}
	task := &topology.TopoTask{
		Mode: topology.GETROUTE,
		UUID: uuid,
	}
	res, err := admin.topoRequest(task)
	if err != nil || res == nil {
		return "", false
	}
	return res.Route, true
}

// UpdateNodeMemo sets memo field for the given node and notifies the agent.
func (admin *Admin) UpdateNodeMemo(targetUUID, memo string) error {
	if admin == nil || admin.mgr == nil {
		return fmt.Errorf("admin unavailable")
	}
	targetUUID = strings.TrimSpace(targetUUID)
	if targetUUID == "" {
		return fmt.Errorf("missing target uuid")
	}
	task := &topology.TopoTask{Mode: topology.UPDATEMEMO, UUID: targetUUID, Memo: memo}
	if _, err := admin.topoRequest(task); err != nil {
		return err
	}
	route, ok := admin.fetchRoute(targetUUID)
	if !ok || route == "" {
		return fmt.Errorf("route unavailable for %s", targetUUID)
	}
	sess := admin.sessionForRoute(route)
	if sess == nil || sess.Conn() == nil {
		return fmt.Errorf("session unavailable for %s", targetUUID)
	}
	msg := protocol.NewDownMsg(sess.Conn(), sess.Secret(), sess.UUID())
	protocol.SetMessageMeta(msg, sess.ProtocolVersion(), sess.ProtocolFlags())
	header := &protocol.Header{
		Sender:      protocol.ADMIN_UUID,
		Accepter:    protocol.TEMP_UUID,
		MessageType: protocol.MYMEMO,
		RouteLen:    uint32(len([]byte(route))),
		Route:       route,
	}
	payload := &protocol.MyMemo{MemoLen: uint64(len(memo)), Memo: memo}
	protocol.ConstructMessage(msg, header, payload, false)
	msg.SendMessage()
	return nil
}

// ConnectNode requests the target node to connect to a downstream address.
func (admin *Admin) ConnectNode(ctx context.Context, targetUUID, addr string) error {
	if admin == nil || admin.mgr == nil {
		return fmt.Errorf("admin unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	targetUUID = strings.TrimSpace(targetUUID)
	addr = strings.TrimSpace(addr)
	if targetUUID == "" {
		return fmt.Errorf("missing target uuid")
	}
	if addr == "" {
		return fmt.Errorf("missing connect address")
	}
	normalized, _, err := utils.CheckIPPort(addr)
	if err != nil {
		return err
	}
	route, ok := admin.fetchRoute(targetUUID)
	if !ok || route == "" {
		return fmt.Errorf("route unavailable for %s", targetUUID)
	}
	ackCh := admin.mgr.ConnectManager.RegisterAck()
	if err := admin.sendConnectStart(route, targetUUID, normalized); err != nil {
		admin.mgr.ConnectManager.UnregisterAck(ackCh)
		return err
	}
	timer := time.NewTimer(defaults.ConnectAckTimeout)
	defer timer.Stop()
	consumed := false
	defer func() {
		if !consumed {
			admin.mgr.ConnectManager.UnregisterAck(ackCh)
		}
	}()
	select {
	case ok, open := <-ackCh:
		consumed = true
		if !open {
			return fmt.Errorf("connect acknowledgement channel closed")
		}
		if !ok {
			return fmt.Errorf("connect request rejected by %s", targetUUID)
		}
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return fmt.Errorf("connect request timeout for %s", targetUUID)
	}
	return nil
}

var (
	ErrSSHTunnelMissingTarget   = errors.New("missing target uuid")
	ErrSSHTunnelMissingAddr     = errors.New("missing server address")
	ErrSSHTunnelMissingPort     = errors.New("missing agent port")
	ErrSSHTunnelMissingUser     = errors.New("missing username")
	ErrSSHTunnelMissingPass     = errors.New("missing password")
	ErrSSHTunnelMissingCert     = errors.New("missing certificate payload")
	ErrSSHTunnelUnsupportedAuth = errors.New("unsupported auth method")
	ErrSleepUpdateMissingTarget = errors.New("missing target uuid")
	ErrSleepUpdateNoFields      = errors.New("at least one sleep field required")
	ErrSleepUpdateInvalidSleep  = errors.New("sleep seconds must be >= 0")
	ErrSleepUpdateInvalidWork   = errors.New("work seconds must be >= 0")
	ErrShutdownMissingTarget    = errors.New("missing target uuid")
	ErrStreamPingMissingTarget  = errors.New("missing target uuid")
	ErrStreamPingInvalidCount   = errors.New("ping count must be > 0")
	ErrStreamPingInvalidSize    = errors.New("payload size must be >= 0")
)

func (admin *Admin) StartSSHTunnel(ctx context.Context, targetUUID, serverAddr, agentPort string, method uipb.SshTunnelAuthMethod, username, password string, privateKey []byte) error {
	if admin == nil {
		return fmt.Errorf("admin unavailable")
	}
	targetUUID = strings.TrimSpace(targetUUID)
	if targetUUID == "" {
		return ErrSSHTunnelMissingTarget
	}
	serverAddr = strings.TrimSpace(serverAddr)
	if serverAddr == "" {
		return ErrSSHTunnelMissingAddr
	}
	agentPort = strings.TrimSpace(agentPort)
	if agentPort == "" {
		return ErrSSHTunnelMissingPort
	}
	username = strings.TrimSpace(username)
	if username == "" {
		return ErrSSHTunnelMissingUser
	}
	authMethod := method
	switch authMethod {
	case uipb.SshTunnelAuthMethod_SSH_TUNNEL_AUTH_METHOD_PASSWORD:
		if strings.TrimSpace(password) == "" {
			return ErrSSHTunnelMissingPass
		}
	case uipb.SshTunnelAuthMethod_SSH_TUNNEL_AUTH_METHOD_CERT:
		if len(privateKey) == 0 {
			return ErrSSHTunnelMissingCert
		}
	default:
		return ErrSSHTunnelUnsupportedAuth
	}
	opts := map[string]string{
		"kind":     "ssh-tunnel",
		"addr":     serverAddr,
		"port":     agentPort,
		"method":   "1",
		"username": username,
	}
	if authMethod == uipb.SshTunnelAuthMethod_SSH_TUNNEL_AUTH_METHOD_CERT {
		opts["method"] = "2"
	} else {
		opts["password"] = password
	}
	openFn := admin.streamOpenOverride
	if openFn == nil {
		openFn = admin.OpenStream
	}
	stream, err := openFn(ctx, targetUUID, "", opts)
	if err != nil {
		return err
	}
	defer stream.Close()
	if authMethod == uipb.SshTunnelAuthMethod_SSH_TUNNEL_AUTH_METHOD_CERT {
		if _, err := stream.Write(privateKey); err != nil {
			return fmt.Errorf("send cert failed: %w", err)
		}
	}
	return nil
}

func (admin *Admin) UpdateSleep(targetUUID string, params SleepUpdateParams) error {
	if admin == nil || admin.topology == nil || admin.mgr == nil {
		return fmt.Errorf("admin unavailable")
	}
	targetUUID = strings.TrimSpace(targetUUID)
	if targetUUID == "" {
		return ErrSleepUpdateMissingTarget
	}
	if params.SleepSeconds == nil && params.WorkSeconds == nil && params.JitterPercent == nil {
		return ErrSleepUpdateNoFields
	}
	// Validate payload early so we don't enqueue invalid updates for later dispatch.
	if _, err := buildSleepUpdatePayload(params); err != nil {
		return err
	}

	// Update desired sleep/work settings in topology even if the node is temporarily
	// unreachable. This prevents Kelpie from misclassifying duty-cycle nodes as
	// "always-on" and triggering unnecessary self-heal churn.
	admin.updateSleepTopology(targetUUID, params, false)

	// Session metadata is "desired state": update it regardless of immediate dispatch.
	state := admin.sessionState(targetUUID)
	now := time.Now().UTC()
	if params.JitterPercent != nil {
		val := *params.JitterPercent
		state.JitterPercent = &val
	}
	state.LastCommand = "sleep_update"
	state.LastCommandAt = now
	admin.setSessionState(targetUUID, state)

	var (
		route string
		ok    bool
	)
	if fn := admin.routeOverride; fn != nil {
		route, ok = fn(targetUUID)
	} else {
		route, ok = admin.fetchRoute(targetUUID)
	}
	if ok && strings.TrimSpace(route) != "" {
		if err := admin.sendSleepUpdate(targetUUID, route, params); err == nil {
			return nil
		} else {
			admin.enqueuePendingSleepUpdate(targetUUID, params, err)
			return nil
		}
	}
	admin.enqueuePendingSleepUpdate(targetUUID, params, fmt.Errorf("route unavailable for %s", targetUUID))
	return nil
}

func (admin *Admin) sendSleepUpdate(targetUUID, route string, params SleepUpdateParams) error {
	payload, err := buildSleepUpdatePayload(params)
	if err != nil {
		return err
	}
	msg, err := admin.newDownstreamMessageForRoute(targetUUID, route)
	if err != nil {
		return err
	}
	header := &protocol.Header{
		Sender:      protocol.ADMIN_UUID,
		Accepter:    protocol.TEMP_UUID,
		MessageType: protocol.SLEEP_UPDATE,
		RouteLen:    uint32(len([]byte(route))),
		Route:       route,
	}
	protocol.ConstructMessage(msg, header, payload, false)
	msg.SendMessage()
	admin.updateSleepTopology(targetUUID, params, true)
	printer.Success("\r\n[*] Sleep update dispatched.\r\n")
	return nil
}

func buildSleepUpdatePayload(params SleepUpdateParams) (*protocol.SleepUpdate, error) {
	update := &protocol.SleepUpdate{}
	if params.SleepSeconds != nil {
		if *params.SleepSeconds < 0 {
			return nil, ErrSleepUpdateInvalidSleep
		}
		update.Flags |= protocol.SleepUpdateFlagSleepSeconds
		update.SleepSeconds = int32(*params.SleepSeconds)
	}
	if params.WorkSeconds != nil {
		if *params.WorkSeconds < 0 {
			return nil, ErrSleepUpdateInvalidWork
		}
		update.Flags |= protocol.SleepUpdateFlagWorkSeconds
		update.WorkSeconds = int32(*params.WorkSeconds)
	}
	if params.JitterPercent != nil {
		jitter := *params.JitterPercent
		if jitter < 0 {
			jitter = 0
		}
		if jitter > 50 {
			jitter = 50
		}
		update.Flags |= protocol.SleepUpdateFlagJitter
		update.JitterPermille = uint16(math.Round(jitter * 10))
	}
	if update.Flags == 0 {
		return nil, ErrSleepUpdateNoFields
	}
	return update, nil
}

func (admin *Admin) updateSleepTopology(targetUUID string, params SleepUpdateParams, touchLiveness bool) {
	if admin == nil || admin.topology == nil {
		return
	}
	if params.SleepSeconds == nil && params.WorkSeconds == nil {
		return
	}
	task := &topology.TopoTask{
		Mode:         topology.UPDATEDETAIL,
		UUID:         targetUUID,
		SleepSeconds: -1,
		WorkSeconds:  -1,
		SkipLiveness: !touchLiveness,
	}
	if params.SleepSeconds != nil {
		task.SleepSeconds = *params.SleepSeconds
	}
	if params.WorkSeconds != nil {
		task.WorkSeconds = *params.WorkSeconds
	}
	admin.topology.Execute(task)
}

func copySleepUpdateParams(params SleepUpdateParams) SleepUpdateParams {
	out := SleepUpdateParams{}
	if params.SleepSeconds != nil {
		v := *params.SleepSeconds
		out.SleepSeconds = &v
	}
	if params.WorkSeconds != nil {
		v := *params.WorkSeconds
		out.WorkSeconds = &v
	}
	if params.JitterPercent != nil {
		v := *params.JitterPercent
		out.JitterPercent = &v
	}
	return out
}

func mergeSleepUpdateParams(base SleepUpdateParams, override SleepUpdateParams) SleepUpdateParams {
	out := copySleepUpdateParams(base)
	if override.SleepSeconds != nil {
		v := *override.SleepSeconds
		out.SleepSeconds = &v
	}
	if override.WorkSeconds != nil {
		v := *override.WorkSeconds
		out.WorkSeconds = &v
	}
	if override.JitterPercent != nil {
		v := *override.JitterPercent
		out.JitterPercent = &v
	}
	return out
}

func (admin *Admin) enqueuePendingSleepUpdate(targetUUID string, params SleepUpdateParams, lastErr error) {
	if admin == nil {
		return
	}
	key := strings.ToLower(strings.TrimSpace(targetUUID))
	if key == "" {
		return
	}
	now := time.Now().UTC()
	rec := pendingSleepUpdate{
		params:      copySleepUpdateParams(params),
		enqueuedAt:  now,
		nextAttempt: now,
		backoff:     pendingSleepInitialBackoff,
	}
	if lastErr != nil {
		rec.lastError = lastErr.Error()
	}
	admin.pendingSleepMu.Lock()
	if admin.pendingSleepUpdates == nil {
		admin.pendingSleepUpdates = make(map[string]pendingSleepUpdate)
	}
	if existing, ok := admin.pendingSleepUpdates[key]; ok {
		existing.params = mergeSleepUpdateParams(existing.params, params)
		if existing.enqueuedAt.IsZero() {
			existing.enqueuedAt = rec.enqueuedAt
		}
		if existing.backoff <= 0 {
			existing.backoff = pendingSleepInitialBackoff
		}
		existing.nextAttempt = now
		if rec.lastError != "" {
			existing.lastError = rec.lastError
		}
		admin.pendingSleepUpdates[key] = existing
	} else {
		admin.pendingSleepUpdates[key] = rec
	}
	admin.pendingSleepMu.Unlock()
}

func (admin *Admin) runPendingSleepUpdateFlusher(ctx context.Context) {
	if admin == nil || ctx == nil {
		return
	}
	ticker := time.NewTicker(pendingSleepFlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			admin.flushPendingSleepUpdates()
		}
	}
}

func (admin *Admin) flushPendingSleepUpdates() {
	if admin == nil {
		return
	}
	now := time.Now().UTC()

	type dueUpdate struct {
		target string
		rec    pendingSleepUpdate
	}

	var due []dueUpdate
	admin.pendingSleepMu.Lock()
	for target, rec := range admin.pendingSleepUpdates {
		if !rec.enqueuedAt.IsZero() && pendingSleepMaxAge > 0 && now.Sub(rec.enqueuedAt) > pendingSleepMaxAge {
			delete(admin.pendingSleepUpdates, target)
			continue
		}
		if !rec.nextAttempt.IsZero() && now.Before(rec.nextAttempt) {
			continue
		}
		delete(admin.pendingSleepUpdates, target)
		due = append(due, dueUpdate{target: target, rec: rec})
	}
	admin.pendingSleepMu.Unlock()

	for _, item := range due {
		targetUUID := item.target
		params := item.rec.params
		var (
			route string
			ok    bool
		)
		if fn := admin.routeOverride; fn != nil {
			route, ok = fn(targetUUID)
		} else {
			route, ok = admin.fetchRoute(targetUUID)
		}
		if ok && strings.TrimSpace(route) != "" {
			if err := admin.sendSleepUpdate(targetUUID, route, params); err == nil {
				continue
			} else {
				item.rec.lastError = err.Error()
			}
		} else {
			item.rec.lastError = fmt.Sprintf("route unavailable for %s", targetUUID)
		}

		item.rec.attempts++
		if item.rec.backoff <= 0 {
			item.rec.backoff = pendingSleepInitialBackoff
		} else {
			item.rec.backoff *= 2
		}
		if item.rec.backoff > pendingSleepMaxBackoff {
			item.rec.backoff = pendingSleepMaxBackoff
		}
		item.rec.nextAttempt = now.Add(item.rec.backoff)
		if item.rec.enqueuedAt.IsZero() {
			item.rec.enqueuedAt = now
		}

		admin.pendingSleepMu.Lock()
		if existing, ok := admin.pendingSleepUpdates[targetUUID]; ok {
			// Newer updates win; preserve fields already queued after we popped this attempt.
			existing.params = mergeSleepUpdateParams(item.rec.params, existing.params)
			if existing.enqueuedAt.IsZero() || (!item.rec.enqueuedAt.IsZero() && item.rec.enqueuedAt.Before(existing.enqueuedAt)) {
				existing.enqueuedAt = item.rec.enqueuedAt
			}
			if existing.backoff < item.rec.backoff {
				existing.backoff = item.rec.backoff
			}
			if existing.nextAttempt.Before(item.rec.nextAttempt) {
				existing.nextAttempt = item.rec.nextAttempt
			}
			existing.lastError = item.rec.lastError
			existing.attempts += item.rec.attempts
			admin.pendingSleepUpdates[targetUUID] = existing
		} else {
			admin.pendingSleepUpdates[targetUUID] = item.rec
		}
		admin.pendingSleepMu.Unlock()
	}
}

func (admin *Admin) ShutdownNode(targetUUID string) error {
	if admin == nil || admin.mgr == nil {
		return fmt.Errorf("admin unavailable")
	}
	targetUUID = strings.TrimSpace(targetUUID)
	if targetUUID == "" {
		return ErrShutdownMissingTarget
	}
	var (
		route string
		ok    bool
	)
	if fn := admin.routeOverride; fn != nil {
		route, ok = fn(targetUUID)
	} else {
		route, ok = admin.fetchRoute(targetUUID)
	}
	if !ok || strings.TrimSpace(route) == "" {
		return fmt.Errorf("route unavailable for %s", targetUUID)
	}
	if fn := admin.shutdownOverride; fn != nil {
		fn(route, targetUUID)
		return nil
	}
	msg, err := admin.newDownstreamMessageForRoute(targetUUID, route)
	if err != nil {
		return err
	}
	header := &protocol.Header{
		Sender:      protocol.ADMIN_UUID,
		Accepter:    protocol.TEMP_UUID,
		MessageType: protocol.SHUTDOWN,
		RouteLen:    uint32(len([]byte(route))),
		Route:       route,
	}
	payload := &protocol.Shutdown{OK: 1}
	protocol.ConstructMessage(msg, header, payload, false)
	msg.SendMessage()
	return nil
}

func (admin *Admin) StartForwardProxy(ctx context.Context, targetUUID, bindAddr, remoteAddr string) (*ProxyDescriptor, error) {
	if admin == nil {
		return nil, fmt.Errorf("admin unavailable")
	}
	mgr := admin.ensurePortProxyManager()
	if mgr == nil {
		return nil, fmt.Errorf("proxy manager unavailable")
	}
	return mgr.StartForward(ctx, targetUUID, bindAddr, remoteAddr)
}

func (admin *Admin) StopForwardProxy(targetUUID, proxyID string) ([]*ProxyDescriptor, error) {
	if admin == nil {
		return nil, fmt.Errorf("admin unavailable")
	}
	mgr := admin.ensurePortProxyManager()
	if mgr == nil {
		return nil, fmt.Errorf("proxy manager unavailable")
	}
	return mgr.StopForward(targetUUID, proxyID)
}

func (admin *Admin) StartBackwardProxy(ctx context.Context, targetUUID, remotePort, localPort string) (*ProxyDescriptor, error) {
	if admin == nil {
		return nil, fmt.Errorf("admin unavailable")
	}
	mgr := admin.ensurePortProxyManager()
	if mgr == nil {
		return nil, fmt.Errorf("proxy manager unavailable")
	}
	return mgr.StartBackward(ctx, targetUUID, remotePort, localPort)
}

func (admin *Admin) StopBackwardProxy(targetUUID, proxyID string) ([]*ProxyDescriptor, error) {
	if admin == nil {
		return nil, fmt.Errorf("admin unavailable")
	}
	mgr := admin.ensurePortProxyManager()
	if mgr == nil {
		return nil, fmt.Errorf("proxy manager unavailable")
	}
	return mgr.StopBackward(targetUUID, proxyID)
}

func (admin *Admin) ListProxies() []*ProxyDescriptor {
	if admin == nil {
		return nil
	}
	mgr := admin.ensurePortProxyManager()
	if mgr == nil {
		return nil
	}
	return mgr.List()
}

// StartListener triggers a passive listener on the given node via the current DTN control path.
func (admin *Admin) StartListener(targetUUID, bind string, mode int, listenerID string) (string, error) {
	if admin == nil || admin.mgr == nil {
		return "", fmt.Errorf("admin unavailable")
	}
	targetUUID = strings.TrimSpace(targetUUID)
	if targetUUID == "" {
		return "", fmt.Errorf("missing target uuid")
	}
	listenerID = strings.TrimSpace(listenerID)
	if listenerID == "" {
		return "", fmt.Errorf("listener id required")
	}
	bind = strings.TrimSpace(bind)
	if mode == listenerModeNormal {
		if bind == "" {
			return "", fmt.Errorf("missing bind address")
		}
		validated, _, err := utils.CheckIPPort(bind)
		if err != nil {
			return "", err
		}
		bind = validated
	}
	route, ok := admin.fetchRoute(targetUUID)
	if !ok || route == "" {
		return "", fmt.Errorf("route unavailable for %s", targetUUID)
	}
	ackCh := admin.mgr.ListenManager.RegisterAck(listenerID)
	if err := admin.sendListenerStart(route, targetUUID, listenerID, mode, bind); err != nil {
		return "", err
	}
	select {
	case ok := <-ackCh:
		if !ok {
			return "", fmt.Errorf("listener %s rejected", shorten(listenerID))
		}
	case <-time.After(defaults.ListenerAckTimeout):
		return "", fmt.Errorf("listener %s start timeout", shorten(listenerID))
	}
	return route, nil
}

func (admin *Admin) StopListener(targetUUID, listenerID string) error {
	if admin == nil || admin.mgr == nil {
		return fmt.Errorf("admin unavailable")
	}
	targetUUID = strings.TrimSpace(targetUUID)
	listenerID = strings.TrimSpace(listenerID)
	if targetUUID == "" || listenerID == "" {
		return fmt.Errorf("target uuid and listener id required")
	}
	route, ok := admin.fetchRoute(targetUUID)
	if !ok || strings.TrimSpace(route) == "" {
		return fmt.Errorf("route unavailable for %s", targetUUID)
	}
	ackCh := admin.mgr.ListenManager.RegisterAck(listenerID)
	if err := admin.sendListenerStop(route, targetUUID, listenerID); err != nil {
		return err
	}
	select {
	case ok := <-ackCh:
		if !ok {
			return fmt.Errorf("listener %s stop rejected", shorten(listenerID))
		}
	case <-time.After(defaults.ListenerAckTimeout):
		return fmt.Errorf("listener %s stop timeout", shorten(listenerID))
	}
	return nil
}

func (admin *Admin) sendListenerStart(route, targetUUID, listenerID string, mode int, addr string) error {
	msg, err := admin.newDownstreamMessageForRoute(targetUUID, route)
	if err != nil {
		return err
	}
	header := &protocol.Header{
		Sender:      protocol.ADMIN_UUID,
		Accepter:    protocol.TEMP_UUID,
		MessageType: protocol.LISTENREQ,
		RouteLen:    uint32(len([]byte(route))),
		Route:       route,
	}
	listenReq := &protocol.ListenReq{
		Method:     uint16(mode),
		AddrLen:    uint64(len(addr)),
		Addr:       addr,
		ListenerID: listenerID,
	}
	protocol.ConstructMessage(msg, header, listenReq, false)
	msg.SendMessage()
	return nil
}

func (admin *Admin) sendListenerStop(route, targetUUID, listenerID string) error {
	msg, err := admin.newDownstreamMessageForRoute(targetUUID, route)
	if err != nil {
		return err
	}
	header := &protocol.Header{
		Sender:      protocol.ADMIN_UUID,
		Accepter:    protocol.TEMP_UUID,
		MessageType: protocol.LISTENSTOP,
		RouteLen:    uint32(len([]byte(route))),
		Route:       route,
	}
	stopReq := &protocol.ListenStop{ListenerID: listenerID}
	protocol.ConstructMessage(msg, header, stopReq, false)
	msg.SendMessage()
	return nil
}

func (admin *Admin) sendConnectStart(route, targetUUID, addr string) error {
	msg, err := admin.newDownstreamMessageForRoute(targetUUID, route)
	if err != nil {
		return err
	}
	header := &protocol.Header{
		Sender:      protocol.ADMIN_UUID,
		Accepter:    protocol.TEMP_UUID,
		MessageType: protocol.CONNECTSTART,
		RouteLen:    uint32(len([]byte(route))),
		Route:       route,
	}
	req := &protocol.ConnectStart{
		AddrLen: uint16(len(addr)),
		Addr:    addr,
	}
	protocol.ConstructMessage(msg, header, req, false)
	msg.SendMessage()
	return nil
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

func (admin *Admin) topoRequest(task *topology.TopoTask) (*topology.Result, error) {
	if admin == nil || admin.topoService == nil {
		return nil, fmt.Errorf("topology service unavailable")
	}
	ctx, cancel := context.WithTimeout(context.Background(), defaults.TopologyRequestTimeout)
	defer cancel()
	return admin.topoService.Request(ctx, task)
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
		// DTN pull (experimental)
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

// RoutingStrategy returns the current topology routing strategy.
func (admin *Admin) RoutingStrategy() topology.RoutingStrategy {
	if admin == nil || admin.topology == nil {
		return topology.RoutingByHops
	}
	return admin.topology.RoutingStrategy()
}

// SetRoutingStrategy switches the topology routing strategy at runtime.
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

// DropSession forcibly removes a session/component by node UUID.
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

func (admin *Admin) ReconnectStatsView() ReconnectStatsView {
	if admin == nil || admin.reconnector == nil {
		return ReconnectStatsView{}
	}
	stats := admin.reconnector.Stats()
	return ReconnectStatsView(stats)
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
