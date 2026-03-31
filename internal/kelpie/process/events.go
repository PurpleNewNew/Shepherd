package process

import (
	"fmt"
	"strings"
	"time"

	"codeberg.org/agnoie/shepherd/internal/kelpie/supp"
	"codeberg.org/agnoie/shepherd/pkg/config/defaults"
	"codeberg.org/agnoie/shepherd/protocol"
)

func (p *SupplementalPlanner) cancelOfflineProbe(uuid string) {
	if p == nil || uuid == "" {
		return
	}
	p.offlineMu.Lock()
	timer := (*time.Timer)(nil)
	if p.offlineProbe != nil {
		timer = p.offlineProbe[uuid]
		delete(p.offlineProbe, uuid)
	}
	p.offlineMu.Unlock()
	if timer != nil {
		timer.Stop()
	}
}

func (p *SupplementalPlanner) scheduleOfflineProbe(uuid string) {
	if p == nil || uuid == "" || uuid == protocol.ADMIN_UUID {
		return
	}
	if !p.Enabled() {
		return
	}
	delay := p.offlineProbeDelay(uuid)
	if delay <= 0 {
		delay = 2 * time.Second
	}
	p.scheduleOfflineProbeAfter(uuid, delay)
}

func (p *SupplementalPlanner) scheduleOfflineProbeAfter(uuid string, delay time.Duration) {
	if p == nil || uuid == "" || uuid == protocol.ADMIN_UUID {
		return
	}
	if !p.Enabled() {
		return
	}
	if delay <= 0 {
		delay = 2 * time.Second
	}
	// 避免在异常时间戳下陷入紧循环；这里只是一条尽力而为的探测路径。
	if delay < 2*time.Second {
		delay = 2 * time.Second
	}

	p.offlineMu.Lock()
	if p.offlineProbe == nil {
		p.offlineProbe = make(map[string]*time.Timer)
	}
	if _, exists := p.offlineProbe[uuid]; exists {
		p.offlineMu.Unlock()
		return
	}
	timer := time.AfterFunc(delay, func() {
		p.fireOfflineProbe(uuid)
	})
	p.offlineProbe[uuid] = timer
	p.offlineMu.Unlock()
}

func (p *SupplementalPlanner) offlineProbeDelay(uuid string) time.Duration {
	// 对于 sleep/work 节点，要等错过若干次预期的 duty-cycle 刷新后，
	// 才延后发起 repair 尝试。这样既能避免在正常睡眠窗口内抖动，
	// 也仍能识别“在睡眠期间被 kill”这类情况。
	if p == nil || p.topo == nil || uuid == "" {
		return defaults.NodeStaleTimeout
	}
	rt, ok := p.topo.NodeRuntime(uuid)
	if !ok {
		return defaults.NodeStaleTimeout
	}
	if rt.SleepSeconds <= 0 {
		// 常在线节点：NodeOffline 通常意味着硬故障，应尽快探测。
		return 2 * time.Second
	}
	work := rt.WorkSeconds
	if work <= 0 {
		work = offlineProbeDefaultWorkSeconds
	}
	cycle := time.Duration(rt.SleepSeconds+work) * time.Second
	if cycle <= 0 {
		return defaults.NodeStaleTimeout
	}
	delay := cycle*3 + 5*time.Second
	if !rt.NextWake.IsZero() {
		if until := time.Until(rt.NextWake); until > 0 {
			// 确保至少等到上报的唤醒时刻之后，再额外留一小段余量。
			slack := time.Duration(work)*time.Second + 5*time.Second
			if d := until + slack; d > delay {
				delay = d
			}
		}
	}
	if delay < 15*time.Second {
		delay = 15 * time.Second
	}
	if delay > 3*time.Minute {
		delay = 3 * time.Minute
	}
	return delay
}

func (p *SupplementalPlanner) offlineProbeThreshold(uuid string) time.Duration {
	if p == nil || p.topo == nil || uuid == "" {
		return defaults.NodeStaleTimeout
	}
	rt, ok := p.topo.NodeRuntime(uuid)
	if !ok {
		return defaults.NodeStaleTimeout
	}
	if rt.SleepSeconds <= 0 {
		return 0
	}
	work := rt.WorkSeconds
	if work <= 0 {
		work = offlineProbeDefaultWorkSeconds
	}
	cycle := time.Duration(rt.SleepSeconds+work) * time.Second
	if cycle <= 0 {
		return defaults.NodeStaleTimeout
	}
	threshold := cycle*3 + 5*time.Second
	if threshold < 15*time.Second {
		threshold = 15 * time.Second
	}
	if threshold > 3*time.Minute {
		threshold = 3 * time.Minute
	}
	return threshold
}

func (p *SupplementalPlanner) fireOfflineProbe(uuid string) {
	if p == nil || uuid == "" {
		return
	}

	// 先移除 timer 句柄，后续新的 offline 变化才能再次调度。
	p.offlineMu.Lock()
	if p.offlineProbe != nil {
		delete(p.offlineProbe, uuid)
	}
	p.offlineMu.Unlock()

	if !p.Enabled() {
		return
	}
	if p.ctx != nil {
		select {
		case <-p.ctx.Done():
			return
		default:
		}
	}

	// 如果节点当前已经重新在线，就不再处理。
	if nodes, err := p.allNodeUUIDs(); err == nil {
		for _, id := range nodes {
			if id == uuid {
				return
			}
		}
	}

	threshold := p.offlineProbeThreshold(uuid)
	if threshold > 0 && p.topo != nil {
		if rt, ok := p.topo.NodeRuntime(uuid); ok {
			if !rt.LastSeen.IsZero() {
				staleness := time.Since(rt.LastSeen)
				if staleness < threshold {
					// 节点最近仍被观测到，说明这更可能只是一次正常的 duty-cycled
					// 睡眠窗口。这里重新安排后续探测，以便当节点持续陈旧
					// （例如在睡眠中被 kill）时，系统仍能最终收敛到 rescue。
					wait := threshold - staleness + 2*time.Second
					if wait < 5*time.Second {
						wait = 5 * time.Second
					}
					if wait > 30*time.Second {
						wait = 30 * time.Second
					}
					p.scheduleOfflineProbeAfter(uuid, wait)
					return
				}
			}
		}
	}

	// 入队一个具备 repair 能力的动作。只有在节点经 GETALLNODES
	// 确认确实离线时，planForNode() 才会把它路由到 tryRepair()。
	p.Enqueue(PlanAction{
		Reason:      reasonLinkFailed,
		TargetUUID:  uuid,
		RequestedAt: time.Now(),
		Attempt:     1,
	})
}

// OnNodeAdded 在新节点加入时触发规划。
func (p *SupplementalPlanner) OnNodeAdded(uuid string) {
	if uuid == "" {
		return
	}
	p.cancelOfflineProbe(uuid)
	// 在过于简单的拓扑上跳过规划（此时还没有可用的对等节点）。
	if nodes, err := p.allNodeUUIDs(); err == nil {
		if len(nodes) < 2 {
			return
		}
	}
	p.Enqueue(PlanAction{
		Reason:      reasonNodeAdded,
		TargetUUID:  uuid,
		RequestedAt: time.Now(),
		Attempt:     1,
	})
}

// OnLinkFailed 在补链失效时触发规划。
func (p *SupplementalPlanner) OnLinkFailed(linkUUID string, endpoints []string) {
	if linkUUID == "" {
		return
	}
	now := time.Now()
	p.recordPlannerEvent("link", "failed", linkUUID, strings.Join(endpoints, ","), "")
	if len(endpoints) == 0 {
		return
	}
	first := endpoints[0]
	second := ""
	if len(endpoints) > 1 {
		second = endpoints[1]
	}

	if first != "" {
		p.Enqueue(PlanAction{
			Reason:      reasonLinkFailed,
			TargetUUID:  first,
			SourceUUID:  second,
			LinkUUID:    linkUUID,
			RequestedAt: now,
			Attempt:     1,
		})
	}
	if second != "" && second != first {
		p.Enqueue(PlanAction{
			Reason:      reasonLinkFailed,
			TargetUUID:  second,
			SourceUUID:  first,
			LinkUUID:    linkUUID,
			RequestedAt: now,
			Attempt:     1,
		})
	}
}

// OnLinkRetired 处理人工或调度器触发的补链下线。
func (p *SupplementalPlanner) OnLinkRetired(linkUUID string, endpoints []string, reason string) {
	if p == nil || linkUUID == "" {
		return
	}
	if reason == "" {
		reason = "retired"
	}
	p.recordPlannerEvent("link", reason, linkUUID, strings.Join(endpoints, ","), "")
	if !p.Enabled() {
		return
	}
	for _, endpoint := range endpoints {
		if endpoint == "" {
			continue
		}
		p.Enqueue(PlanAction{
			Reason:      "link-retired",
			TargetUUID:  endpoint,
			SourceUUID:  linkUUID,
			RequestedAt: time.Now(),
		})
	}
}

// OnLinkPromoted 处理故障切换期间被晋升的补链。
func (p *SupplementalPlanner) OnLinkPromoted(linkUUID, parentUUID, childUUID string) {
	if p == nil || linkUUID == "" {
		return
	}
	detail := fmt.Sprintf("parent=%s child=%s", short(parentUUID), short(childUUID))
	p.recordPlannerEvent("link", "promoted", linkUUID, parentUUID, detail)
	if !p.Enabled() {
		return
	}
	for _, endpoint := range []string{parentUUID, childUUID} {
		if endpoint == "" {
			continue
		}
		p.Enqueue(PlanAction{
			Reason:      "link-promoted",
			TargetUUID:  endpoint,
			SourceUUID:  linkUUID,
			RequestedAt: time.Now(),
		})
	}
}

// OnNodeRemoved 通知调度器节点已退出拓扑。
func (p *SupplementalPlanner) OnNodeRemoved(uuid string) {
	if p == nil || uuid == "" {
		return
	}
	p.recordPlannerEvent("node", "removed", eventSourceSystem, uuid, "")
	// 对常在线节点（sleepSeconds=0）而言，“offline”通常意味着硬故障（kill/crash）。
	// 因此这里主动将其 supplemental 链路判为失效，让路由尽快停止使用过期的
	// "#supp" 边，也让 planner 能立即创建替代链路，而不是继续等待心跳超时。
	if p.topo != nil {
		if rt, ok := p.topo.NodeRuntime(uuid); ok && rt.SleepSeconds <= 0 {
			supp.FailLinksForEndpoint(p.topo, p.mgr, uuid, fmt.Sprintf("%s offline", short(uuid)))
		}
	}
	// 清理失败记录，以便新的尝试使用全新的退避窗口。
	p.failuresMu.Lock()
	delete(p.failures, uuid)
	p.failuresMu.Unlock()
	p.qualityMu.Lock()
	delete(p.nodeQuality, uuid)
	p.qualityMu.Unlock()

	// Sleep/work 节点会主动断开上游；除非它在多个 duty-cycle 窗口内都不再刷新 LastSeen，
	// 否则应把“offline”视为预期行为。也正是这条探测路径，
	// 会把“在睡眠期间被 kill”最终转化为一次 rescue 尝试。
	p.scheduleOfflineProbe(uuid)

	// 在后台触发一次快速巡检。
	if p.Enabled() {
		go p.inspectTopology()
	}
}

// RequestManualRepair 手动触发节点修复尝试。
func (p *SupplementalPlanner) RequestManualRepair(uuid string) error {
	if p == nil {
		return fmt.Errorf("supplemental planner unavailable")
	}
	uuid = strings.TrimSpace(uuid)
	if uuid == "" {
		return fmt.Errorf("invalid node uuid")
	}
	if !p.Enabled() {
		return fmt.Errorf("supplemental planner is disabled")
	}
	action := PlanAction{
		Reason:      reasonManual,
		TargetUUID:  uuid,
		RequestedAt: time.Now(),
	}
	p.Enqueue(action)
	p.recordPlannerEvent("node", "manual-repair", eventSourcePlanner, uuid, "")
	return nil
}

// OnHeartbeat 接收补链心跳信息用于评估链路质量。
func (p *SupplementalPlanner) OnHeartbeat(linkUUID, endpoint string, status supp.SuppLinkHealth, ts time.Time) {
	if p == nil || endpoint == "" {
		return
	}
	alive := strings.EqualFold(status.String(), "alive")
	p.updateNodeQuality(endpoint, alive, ts)
	if !alive {
		detail := fmt.Sprintf("link=%s endpoint=%s", short(linkUUID), short(endpoint))
		p.recordPlannerEvent("heartbeat", "degraded", eventSourceSystem, endpoint, detail)
	}
}

// OnGossipTelemetry 根据 Gossip 节点信息更新节点质量评分。
func (p *SupplementalPlanner) OnGossipTelemetry(info *protocol.NodeInfo) {
	if p == nil || info == nil || info.UUID == "" {
		return
	}
	now := time.Now()

	score := info.Health
	if score > uint32(qualityHealthDefaultMax) {
		score = uint32(qualityHealthDefaultMax)
	}

	lastSeen := now
	if info.LastSeen > 0 {
		lastSeen = time.Unix(info.LastSeen, 0)
	}

	latencyDuration := time.Duration(info.LatencyMs) * time.Millisecond
	queueSample := normalizeQueue(info.QueueDepth)
	failSample := clamp01(float64(info.FailRate))
	healthSample := -1.0
	if score > 0 {
		healthSample = clamp01(float64(score) / qualityHealthDefaultMax)
	}
	stalenessSample := -1.0
	if !lastSeen.IsZero() {
		if lastSeen.After(now) {
			lastSeen = now
		}
		stalenessSample = normalizeDuration(now.Sub(lastSeen), qualityStalenessRef)
	}

	p.qualityMu.Lock()
	state := p.nodeQuality[info.UUID]
	if state == nil {
		state = newNodeQualityState()
		p.nodeQuality[info.UUID] = state
	}
	state.sampleCount++
	state.HealthScore = score
	state.QueueDepth = info.QueueDepth
	if info.LatencyMs > 0 {
		state.AvgLatency = time.Duration(info.LatencyMs) * time.Millisecond
		state.latencyEMA = applyEMA(state.latencyEMA, normalizeDuration(latencyDuration, qualityLatencyRef))
	}
	if healthSample >= 0 {
		state.healthEMA = applyEMA(state.healthEMA, healthSample)
	}
	state.queueEMA = applyEMA(state.queueEMA, queueSample)
	if failSample >= 0 {
		state.failEMA = applyEMA(state.failEMA, failSample)
	}
	if stalenessSample >= 0 {
		state.stalenessEMA = applyEMA(state.stalenessEMA, stalenessSample)
	}
	if !lastSeen.IsZero() {
		state.LastHeartbeat = lastSeen
	}
	state.UpdatedAt = now
	p.qualityMu.Unlock()
}
