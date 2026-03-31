package process

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"codeberg.org/agnoie/shepherd/internal/kelpie/dtn"
	"codeberg.org/agnoie/shepherd/internal/kelpie/printer"
	"codeberg.org/agnoie/shepherd/pkg/bus"
	"codeberg.org/agnoie/shepherd/pkg/session"
	"codeberg.org/agnoie/shepherd/protocol"
)

type dtnInflightRecord struct {
	bundle *dtn.Bundle
	sentAt time.Time
}

type dtnMetricSnapshot struct {
	Stats    dtn.QueueStats
	Captured time.Time
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
	// DTN 默认节奏偏向控制面诊断。对于 stream/dataplane 流量，
	// 需要更紧凑的调度频率，否则大传输可能被长调度周期拖住，
	// 进而触发上层超时。
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
	// 默认 inflight 数应足以维持 stream/dataplane 吞吐。
	// 在测试边界场景时，trace 可以通过 dtn_policy 收紧该值。
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
				// 若失败则重新放回队列
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
	// 按目标限制 inflight 数量。
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
	// 记录正在等待 DTN_ACK 的 inflight 项。
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
	// 取出并删除对应的 inflight 记录。
	bundle := admin.forgetInflight(ack.BundleID)
	if ack.OK != 0 {
		// 收尾：确保队列里的同一 bundle 已被清理（防止它在超时后被重新入队）。
		_, removed := admin.dtnManager.Remove(ack.BundleID)
		// 只有当 ACK 对应一次 inflight 发送，或它确实移除了队列里的重试项时，
		// 才计入 delivered；否则它只是陈旧或重复的 ACK。
		if bundle != nil || removed {
			admin.dtnDelivered++
		}
		if bundle != nil {
			admin.recalculateHoldForTarget(bundle.Target)
			admin.flushDTNBundles(admin.context())
		}
		return
	}
	// 错误路径：如果 TTL 仍有效，则重新入队。
	if bundle == nil {
		return
	}
	admin.dtnFailed++
	now := time.Now()
	if !bundle.DeliverBy.IsZero() {
		if !bundle.DeliverBy.After(now) {
			// 已过期，直接丢弃。
			return
		}
	}
	// 重试时复用同一个 bundle，以保留 BundleID（这对重启恢复很重要）。
	// 优先对齐到下一个预计可发送窗口（sleep），否则使用有界退避。
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
	// 节点重新上线时，它的 bundle 可能还在等待一个由旧 sleep 估计推导出的
	// 过期 HoldUntil。这里立即释放 hold，避免错过短暂的 work 窗口。
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
		// 预算表示单向的预期等待时长。这里只加一次预算；
		// 如果乘以 2，在短 trace 中遇到中间跳点于自愈期间丢路由时，
		// 重试会变得过于迟缓（典型是 star/chain flake 场景）。
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
			// 已过期，直接丢弃。
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

// DTNStats 实现 DTNController。
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

// DTNPolicy 实现 DTNController。
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

// SetDTNPolicy 实现 DTNController。
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
