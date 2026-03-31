package planner

import (
	"context"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"codeberg.org/agnoie/shepherd/internal/kelpie/initial"
	"codeberg.org/agnoie/shepherd/internal/kelpie/manager"
	"codeberg.org/agnoie/shepherd/internal/kelpie/printer"
	"codeberg.org/agnoie/shepherd/internal/kelpie/supp"
	"codeberg.org/agnoie/shepherd/internal/kelpie/topology"
	"codeberg.org/agnoie/shepherd/pkg/config/defaults"
	"codeberg.org/agnoie/shepherd/pkg/global"
	"codeberg.org/agnoie/shepherd/pkg/share"
	"codeberg.org/agnoie/shepherd/pkg/share/transport"
	"codeberg.org/agnoie/shepherd/protocol"
)

// PlanAction 描述等待执行的补链调度任务。
type PlanAction struct {
	Reason      string
	SourceUUID  string
	TargetUUID  string
	RequestedAt time.Time
	LinkUUID    string
	Attempt     int
}

// PlannerPolicy 记录补链调度器在运行时可调的配置项。
type PlannerPolicy struct {
	DesiredLinks     int
	RetryBase        time.Duration
	RetryMax         time.Duration
	RetryMaxAttempts int
	RetryCooldown    time.Duration
	MaxLinksPerNode  int
	AllowNodes       []string
	DenyNodes        []string
	AllowNetworks    []string
	DenyNetworks     []string
}

// SupplementalMetricsSnapshot 描述调度器的累计指标。
type SupplementalMetricsSnapshot struct {
	Dispatched      uint64
	Success         uint64
	Failures        uint64
	Dropped         uint64
	Recycled        uint64
	RepairAttempts  uint64
	RepairSuccess   uint64
	RepairFailures  uint64
	QueueHigh       int
	QueueDepth      int
	LastFailure     string
	EventSeq        uint64
	LastGraphReport []string
}

// SupplementalPlannerEvent 反映调度器最近的事件记录。
type SupplementalPlannerEvent struct {
	Seq        uint64
	Kind       string
	Action     string
	SourceUUID string
	TargetUUID string
	Detail     string
	Timestamp  time.Time
}

// SupplementalQualitySnapshot 描述每个节点的质量评分。
type SupplementalQualitySnapshot struct {
	NodeUUID       string
	Score          float64
	HealthScore    float64
	LatencyScore   float64
	FailureScore   float64
	QueueScore     float64
	StalenessScore float64
	TotalSuccess   uint64
	TotalFailures  uint64
	LastHeartbeat  time.Time
}

// SupplementalStatusSnapshot 描述补链调度器的整体状态。
type SupplementalStatusSnapshot struct {
	Enabled        bool
	QueueLength    int
	PendingActions int
	ActiveLinks    int
}

type RepairStatusSnapshot struct {
	TargetUUID  string
	Attempts    int
	NextAttempt time.Time
	Broken      bool
}

type failureState struct {
	attempts       int
	next           time.Time
	timer          *time.Timer
	reason         string
	repairAttempts int
	repairNext     time.Time
	repairTimer    *time.Timer
	broken         bool
}

type nodeLink struct {
	linkUUID string
	peer     string
	overlap  int
	last     time.Time
}

// SupplementalPlanner 负责协调自动化的补链规划。
type SupplementalPlanner struct {
	topo    *topology.Topology
	service *topology.Service
	mgr     *manager.Manager
	store   *global.Store

	queue     chan PlanAction
	queueMu   sync.Mutex
	queueKeys map[string]struct{}
	ctx       context.Context
	cancel    context.CancelFunc
	startOnce sync.Once
	wg        sync.WaitGroup

	rescue *RescueCoordinator

	enabled atomic.Bool
	pending atomic.Int64

	failuresMu sync.Mutex
	failures   map[string]*failureState

	offlineMu    sync.Mutex
	offlineProbe map[string]*time.Timer

	policyMu sync.RWMutex
	policy   PlannerPolicy

	metricsMu sync.Mutex
	metrics   plannerMetrics

	qualityMu        sync.RWMutex
	nodeQuality      map[string]*nodeQualityState
	baseOptions      *initial.Options
	qualityWeightsMu sync.RWMutex
	qualityWeights   qualityWeightSet
	qualityAdjustCnt uint64
	qualityStatsMu   sync.Mutex
	qualityStats     qualityOutcomeStats

	dialerMu     sync.RWMutex
	repairDialer func(*initial.Options) (net.Conn, *protocol.Negotiation, string, error)
}

type nodeQualityState struct {
	LastHeartbeat       time.Time
	TotalSuccess        uint64
	TotalFailures       uint64
	ConsecutiveFailures uint32
	AvgLatency          time.Duration
	HealthScore         uint32
	QueueDepth          uint32
	UpdatedAt           time.Time
	healthEMA           float64
	latencyEMA          float64
	failEMA             float64
	queueEMA            float64
	stalenessEMA        float64
	sampleCount         uint64
}

type qualityOutcomeStats struct {
	Attempts       uint64
	Success        uint64
	SumSuccess     float64
	SumFailure     float64
	LastUpdate     time.Time
	LastAdjustTime time.Time
}

type plannerMetrics struct {
	dispatched      uint64
	success         uint64
	failures        uint64
	dropped         uint64
	recycled        uint64
	queueHigh       int
	lastFailure     string
	lastEventSeq    uint64
	eventLog        []plannerEvent
	repairAttempts  uint64
	repairSuccess   uint64
	repairFailures  uint64
	lastGraphReport []string
}

// plannerEvent 用于记录调度器的重要事件，便于观测。
type plannerEvent struct {
	Seq       uint64
	Kind      string
	Action    string
	Source    string
	Target    string
	Detail    string
	Timestamp time.Time
}

const (
	plannerQueueSize        = 256
	plannerRequestTimeout   = 3 * time.Second
	plannerInspectInterval  = 90 * time.Second
	plannerRecycleInterval  = 2 * time.Minute
	plannerRetryBase        = 15 * time.Second
	plannerRetryMax         = 10 * time.Minute
	plannerRetryMaxAttempts = 6
	plannerRetryCooldown    = 5 * time.Minute
	repairMaxAttempts       = 3
	repairRetryBase         = 10 * time.Second
	repairRetryMax          = 5 * time.Minute
	repairDialTimeout       = 15 * time.Second
	repairHandshakeTimeout  = 10 * time.Second

	reasonNodeAdded  = "node-added"
	reasonLinkFailed = "link-failed"
	reasonPeriodic   = "periodic-inspect"
	reasonRetry      = "auto-retry"
	reasonManual     = "manual-repair"
)

const (
	plannerEventLogSize  = 64
	plannerIdleThreshold = 10 * time.Minute
	eventSourcePlanner   = "planner"
	eventSourceSystem    = "system"
)

const offlineProbeDefaultWorkSeconds = 10

const (
	qualityEMAAlpha             = 0.3
	qualityDefaultHealthWeight  = 0.4
	qualityDefaultLatencyWeight = 0.25
	qualityDefaultFailureWeight = 0.2
	qualityDefaultQueueWeight   = 0.1
	qualityDefaultStaleWeight   = 0.05
	qualityConsecPenalty        = 8.0
	qualityFailurePenalty       = 15.0
	qualityScoreScale           = 100.0
	qualityLatencyRef           = 2 * time.Second
	qualityStalenessRef         = 2 * time.Minute
	qualityQueueRef             = 128.0
	qualityHealthDefaultMax     = 100.0
	qualityMinWeight            = 0.05
	qualityAdjustmentLowThresh  = 0.35
	qualityAdjustmentHighThresh = 0.65
)

const (
	candidateQualityWeight    = 0.4
	candidateSleepWeight      = 0.2
	candidateOverlapWeight    = 0.15
	candidateDepthWeight      = 0.1
	candidateRedundancyWeight = 0.15
	candidateWorkWeight       = 0.1
	candidateWorkRef          = 20.0
)

var supplementalSleepRef = 5 * defaults.NodeStaleTimeout

type qualityWeightSet struct {
	Health    float64
	Latency   float64
	Failure   float64
	Queue     float64
	Staleness float64
}

func defaultPlannerPolicy() PlannerPolicy {
	return PlannerPolicy{
		DesiredLinks:     1,
		RetryBase:        plannerRetryBase,
		RetryMax:         plannerRetryMax,
		RetryMaxAttempts: plannerRetryMaxAttempts,
		RetryCooldown:    plannerRetryCooldown,
		MaxLinksPerNode:  0,
	}
}

// NewSupplementalPlanner 创建补链调度器，默认启用自动模式。
func NewSupplementalPlanner(topo *topology.Topology, svc *topology.Service, mgr *manager.Manager, store *global.Store) *SupplementalPlanner {
	planner := &SupplementalPlanner{
		topo:         topo,
		service:      svc,
		mgr:          mgr,
		store:        store,
		queue:        make(chan PlanAction, plannerQueueSize),
		queueKeys:    make(map[string]struct{}),
		failures:     make(map[string]*failureState),
		offlineProbe: make(map[string]*time.Timer),
		policy:       defaultPlannerPolicy(),
	}
	planner.metrics.eventLog = make([]plannerEvent, 0, plannerEventLogSize)
	planner.nodeQuality = make(map[string]*nodeQualityState)
	planner.enabled.Store(true)
	planner.repairDialer = planner.directConnect
	planner.qualityWeights = newQualityWeightSet()
	planner.qualityStats = qualityOutcomeStats{}
	planner.rescue = NewRescueCoordinator(topo, svc, mgr, store)
	if planner.rescue != nil {
		planner.rescue.SetCandidateGuard(planner.rescueCandidateAllowed)
	}
	return planner
}

// RestoreMetrics 使用持久化数据恢复调度器的计数器。
func (p *SupplementalPlanner) RestoreMetrics(snapshot topology.PlannerMetricsSnapshot) {
	if p == nil {
		return
	}
	p.metricsMu.Lock()
	p.metrics.dispatched = snapshot.Dispatched
	p.metrics.success = snapshot.Success
	p.metrics.failures = snapshot.Failures
	p.metrics.dropped = snapshot.Dropped
	p.metrics.recycled = snapshot.Recycled
	p.metrics.queueHigh = snapshot.QueueHigh
	p.metrics.lastFailure = snapshot.LastFailure
	p.metrics.repairAttempts = snapshot.RepairAttempts
	p.metrics.repairSuccess = snapshot.RepairSuccess
	p.metrics.repairFailures = snapshot.RepairFailures
	if cap(p.metrics.eventLog) < plannerEventLogSize {
		p.metrics.eventLog = make([]plannerEvent, 0, plannerEventLogSize)
	} else {
		p.metrics.eventLog = p.metrics.eventLog[:0]
	}
	p.metrics.lastEventSeq = 0
	p.metrics.lastGraphReport = nil
	p.metricsMu.Unlock()
}

func (p *SupplementalPlanner) persistMetricsLocked(m *plannerMetrics) {
	if p == nil || p.topo == nil || m == nil {
		return
	}
	snapshot := topology.PlannerMetricsSnapshot{
		Dispatched:     m.dispatched,
		Success:        m.success,
		Failures:       m.failures,
		Dropped:        m.dropped,
		Recycled:       m.recycled,
		QueueHigh:      m.queueHigh,
		LastFailure:    m.lastFailure,
		RepairAttempts: m.repairAttempts,
		RepairSuccess:  m.repairSuccess,
		RepairFailures: m.repairFailures,
	}
	p.topo.PersistPlannerMetrics(snapshot)
}

// Policy 返回当前调度策略的副本。
func (p *SupplementalPlanner) Policy() PlannerPolicy {
	if p == nil {
		return defaultPlannerPolicy()
	}
	p.policyMu.RLock()
	defer p.policyMu.RUnlock()
	return p.policy
}

// SetBaseOptions 设置建立直接连接时的基础拨号参数。
func (p *SupplementalPlanner) SetBaseOptions(opt *initial.Options) {
	if p == nil || opt == nil {
		return
	}
	clone := cloneInitialOptions(opt)
	p.policyMu.Lock()
	p.baseOptions = clone
	p.policyMu.Unlock()
}

// SetRepairDialer 覆盖默认的补链拨号实现，主要用于测试模拟。
func (p *SupplementalPlanner) SetRepairDialer(dialer func(*initial.Options) (net.Conn, *protocol.Negotiation, string, error)) {
	if p == nil {
		return
	}
	p.dialerMu.Lock()
	defer p.dialerMu.Unlock()
	if dialer == nil {
		p.repairDialer = p.directConnect
		return
	}
	p.repairDialer = dialer
}

// Start 在指定上下文中启动后台协程。
func (p *SupplementalPlanner) Start(ctx context.Context) {
	if p == nil {
		return
	}
	p.startOnce.Do(func() {
		if ctx == nil {
			ctx = context.Background()
		}
		p.ctx, p.cancel = context.WithCancel(ctx)
		p.wg.Add(1)
		go p.run()
		if p.rescue != nil {
			p.rescue.Start(p.ctx)
		}
	})
}

// Stop 停止后台协程并等待其退出。
func (p *SupplementalPlanner) Stop() {
	if p == nil {
		return
	}
	if p.cancel != nil {
		p.cancel()
	}
	p.offlineMu.Lock()
	for _, timer := range p.offlineProbe {
		if timer != nil {
			timer.Stop()
		}
	}
	p.offlineProbe = nil
	p.offlineMu.Unlock()
	if p.rescue != nil {
		p.rescue.Stop()
	}
	p.wg.Wait()
}

// Enabled 返回自动模式是否开启。
func (p *SupplementalPlanner) Enabled() bool {
	if p == nil {
		return false
	}
	return p.enabled.Load()
}

// SetEnabled 开启或关闭自动模式。
func (p *SupplementalPlanner) SetEnabled(enabled bool) {
	if p == nil {
		return
	}
	prev := p.enabled.Swap(enabled)
	if prev == enabled {
		return
	}
	state := "disabled"
	if enabled {
		state = "enabled"
	}
	printer.Success("\r\n[*] Supplemental planner now %s\r\n", state)
}

// HandleRescueResponse 处理来自救援节点的反馈。
func (p *SupplementalPlanner) HandleRescueResponse(resp *protocol.RescueResponse) {
	if p == nil || resp == nil || p.rescue == nil {
		return
	}
	p.rescue.HandleResponse(resp)
}

// QueueLength 返回当前排队任务数量。
func (p *SupplementalPlanner) QueueLength() int {
	if p == nil || p.queue == nil {
		return 0
	}
	return len(p.queue)
}

// PendingActions 返回已取出但尚在处理的任务数量。
func (p *SupplementalPlanner) PendingActions() int {
	if p == nil {
		return 0
	}
	value := p.pending.Load()
	if value < 0 {
		return 0
	}
	return int(value)
}

// StatusSnapshot 汇总当前启用状态与排队概况。
func (p *SupplementalPlanner) StatusSnapshot() SupplementalStatusSnapshot {
	if p == nil {
		return SupplementalStatusSnapshot{}
	}
	return SupplementalStatusSnapshot{
		Enabled:        p.Enabled(),
		QueueLength:    p.QueueLength(),
		PendingActions: p.PendingActions(),
		ActiveLinks:    supp.ActiveSupplementalLinkCount(),
	}
}

func queueActionKey(action PlanAction) string {
	target := strings.TrimSpace(action.TargetUUID)
	if target == "" {
		return ""
	}
	reason := strings.TrimSpace(action.Reason)
	if reason == "" {
		reason = "unknown"
	}
	return reason + "|" + target
}

func (p *SupplementalPlanner) markQueuedAction(action PlanAction) bool {
	if p == nil {
		return false
	}
	key := queueActionKey(action)
	if key == "" {
		return true
	}
	p.queueMu.Lock()
	defer p.queueMu.Unlock()
	if p.queueKeys == nil {
		p.queueKeys = make(map[string]struct{})
	}
	if _, exists := p.queueKeys[key]; exists {
		return false
	}
	p.queueKeys[key] = struct{}{}
	return true
}

func (p *SupplementalPlanner) unmarkQueuedAction(action PlanAction) {
	if p == nil {
		return
	}
	key := queueActionKey(action)
	if key == "" {
		return
	}
	p.queueMu.Lock()
	delete(p.queueKeys, key)
	p.queueMu.Unlock()
}

// Enqueue 在调度器启用时入队一个规划任务。
func (p *SupplementalPlanner) Enqueue(action PlanAction) {
	if p == nil || p.queue == nil {
		return
	}
	if !p.Enabled() {
		return
	}
	if action.RequestedAt.IsZero() {
		action.RequestedAt = time.Now()
	}
	if action.Attempt <= 0 {
		action.Attempt = 1
	}
	if !p.markQueuedAction(action) {
		return
	}
	releaseMark := true
	defer func() {
		if releaseMark {
			p.unmarkQueuedAction(action)
		}
	}()
	if skip, wait := p.shouldDelay(action); skip {
		return
	} else if wait > 0 {
		go p.deferEnqueue(action, wait)
		return
	}
	select {
	case p.queue <- action:
		p.noteQueueLength(len(p.queue))
		releaseMark = false
	default:
		printer.Warning("\r\n[*] Supplemental planner queue full; dropping action targeting %s\r\n", short(action.TargetUUID))
		p.metricsRecordDropped(action)
	}
}

func (p *SupplementalPlanner) run() {
	defer p.wg.Done()

	inspectTicker := time.NewTicker(plannerInspectInterval)
	recycleTicker := time.NewTicker(plannerRecycleInterval)
	defer inspectTicker.Stop()
	defer recycleTicker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-inspectTicker.C:
			if p.Enabled() {
				p.inspectTopology()
			}
		case <-recycleTicker.C:
			if p.Enabled() {
				p.recycleSupplementalLinks()
			}
		case action := <-p.queue:
			p.unmarkQueuedAction(action)
			p.pending.Add(1)
			p.process(action)
			p.pending.Add(-1)
		}
	}
}

func (p *SupplementalPlanner) process(action PlanAction) {
	switch action.Reason {
	case reasonNodeAdded, reasonLinkFailed, reasonPeriodic, reasonRetry, reasonManual, "link-retired", "link-promoted":
		p.planForNode(action)
	default:
		printer.Warning("\r\n[*] Supplemental planner received action targeting %s reason=%s\r\n",
			short(action.TargetUUID), action.Reason)
	}
}

func short(id string) string {
	if len(id) <= 8 {
		return id
	}
	return id[:8]
}

func (p *SupplementalPlanner) planForNode(action PlanAction) {
	if p == nil {
		return
	}
	uuid := action.TargetUUID
	if uuid == "" || !p.Enabled() {
		return
	}

	if !p.allowNode(uuid) {
		return
	}

	targetNetwork := p.networkFor(uuid)
	if !p.allowNetwork(targetNetwork) {
		return
	}

	// 只有当目标确实离线或不可达时，才尝试“repair dial”。
	// 否则一些通用重试（例如 bootstrap 早期的“no candidates”）
	// 可能会陷入 repair 退避循环，反而饿死正常的 supplemental 规划。
	if p.shouldAttemptRepair(action.Reason) {
		// 手动 repair 属于操作者的显式动作：无论当前 online/offline 标记如何，
		// 都应尝试进入 repair 路径。
		if action.Reason == reasonManual {
			if p.tryRepair(action) {
				return
			}
		}
		online := true
		if nodes, err := p.allNodeUUIDs(); err == nil {
			online = false
			for _, id := range nodes {
				if id == uuid {
					online = true
					break
				}
			}
		}
		if !online && p.tryRepair(action) {
			return
		}
	}

	policy := p.Policy()
	desired := p.desiredTarget()
	existing := len(supp.GetActiveSuppNeighbors(uuid))
	if policy.MaxLinksPerNode > 0 && existing >= policy.MaxLinksPerNode {
		return
	}
	remaining := desired - existing
	if remaining <= 0 {
		p.clearFailure(uuid)
		return
	}

	candidates, err := p.candidatesFor(uuid, remaining*2)
	if err != nil {
		p.metricsRecordFailure(uuid, action.SourceUUID, err.Error())
		p.recordAndRetry(action, uuid, fmt.Sprintf("candidate fetch failed: %v", err))
		return
	}
	if len(candidates) == 0 {
		p.metricsRecordFailure(uuid, action.SourceUUID, "no candidates")
		p.recordAndRetry(action, uuid, "no candidates available")
		return
	}
	p.sortCandidates(uuid, candidates)

	sameNetwork := make([]*topology.SuppCandidate, 0, len(candidates))
	skippedCross := 0
	for _, candidate := range candidates {
		if candidate == nil || candidate.UUID == "" {
			continue
		}
		candidateNetwork := p.networkFor(candidate.UUID)
		if isCrossNetwork(targetNetwork, candidateNetwork) {
			skippedCross++
			continue
		}
		sameNetwork = append(sameNetwork, candidate)
	}
	if skippedCross > 0 {
		detail := fmt.Sprintf("skipped=%d target=%s", skippedCross, short(uuid))
		p.recordPlannerEvent("planner", "skip-cross-network", eventSourcePlanner, uuid, detail)
	}
	if len(sameNetwork) == 0 && skippedCross > 0 {
		return
	}
	if len(sameNetwork) > 0 {
		candidates = sameNetwork
	}

	created := 0
	attempted := 0
	used := make(map[string]struct{}, len(candidates))
	for _, candidate := range candidates {
		if candidate == nil || candidate.UUID == "" || candidate.UUID == uuid {
			continue
		}
		if !p.allowNode(candidate.UUID) {
			continue
		}

		candidateNetwork := p.networkFor(candidate.UUID)
		if !p.allowNetwork(candidateNetwork) {
			continue
		}

		if _, ok := used[candidate.UUID]; ok {
			continue
		}
		used[candidate.UUID] = struct{}{}

		if policy.MaxLinksPerNode > 0 && existing+created >= policy.MaxLinksPerNode {
			break
		}

		predictedScore := p.nodeQualityScore(candidate.UUID)
		if err := supp.SendSuppLinkPairRequest(p.topo, p.mgr, uuid, candidate.UUID); err != nil {
			attempted++
			p.metricsRecordDispatch(uuid, candidate.UUID, false, err)
			p.debugDispatchError(uuid, candidate.UUID, err)
			p.adjustQualityWeights(predictedScore, false)
			continue
		}
		attempted++
		p.metricsRecordDispatch(uuid, candidate.UUID, true, nil)
		p.adjustQualityWeights(predictedScore, true)
		created++
		if created >= remaining {
			break
		}
	}

	stillMissing := remaining - created
	if created == 0 {
		if attempted == 0 {
			p.metricsRecordFailure(uuid, action.SourceUUID, "no candidates usable")
		}
		p.recordAndRetry(action, uuid, "dispatch failed for all candidates")
		return
	}

	if stillMissing > 0 {
		p.metricsRecordFailure(uuid, action.SourceUUID, fmt.Sprintf("partial success, missing %d", stillMissing))
		printer.Warning("\r\n[*] Supplemental planner dispatched %d link(s) but still lacks %d for %s\r\n",
			created, stillMissing, short(uuid))
		p.recordAndRetry(action, uuid, fmt.Sprintf("missing %d link(s)", stillMissing))
		return
	}

	printer.Success("\r\n[*] Supplemental planner dispatched %d supplemental link(s) for %s\r\n",
		created, short(uuid))
	p.metricsRecordSuccess(uuid, created, attempted)
	p.clearFailure(uuid)
}

func (p *SupplementalPlanner) shouldAttemptRepair(reason string) bool {
	return reason != reasonNodeAdded
}

func (p *SupplementalPlanner) tryRepair(action PlanAction) bool {
	if p == nil {
		return false
	}
	uuid := strings.TrimSpace(action.TargetUUID)
	if uuid == "" {
		return false
	}
	manual := action.Reason == reasonManual

	p.failuresMu.Lock()
	state, ok := p.failures[uuid]
	if !ok {
		state = &failureState{}
		if p.failures == nil {
			p.failures = make(map[string]*failureState)
		}
		p.failures[uuid] = state
	}
	if !manual && state.broken {
		p.failuresMu.Unlock()
		return false
	}
	now := time.Now()
	if !manual && !state.repairNext.IsZero() && now.Before(state.repairNext) {
		wait := state.repairNext.Sub(now)
		if wait > 0 {
			p.scheduleRepair(uuid, action, wait)
			p.failuresMu.Unlock()
			return true
		}
	}
	state.repairAttempts++
	attempt := state.repairAttempts
	p.failuresMu.Unlock()

	detail := fmt.Sprintf("attempt=%d reason=%s", attempt, action.Reason)
	p.recordPlannerEvent("repair", "start", eventSourcePlanner, uuid, detail)

	handleFailure := func(err error) bool {
		p.metricsRecordRepairFailure(uuid, err)
		p.noteRepairFailure(uuid, attempt)
		failDetail := fmt.Sprintf("attempt=%d err=%v", attempt, err)
		p.recordPlannerEvent("repair", "failure", eventSourcePlanner, uuid, failDetail)

		if manual {
			return true
		}

		if attempt >= repairMaxAttempts {
			p.markRepairBroken(uuid, failDetail)
			return false
		}

		delay := backoffDuration(repairRetryBase, repairRetryMax, attempt)
		p.scheduleRepair(uuid, PlanAction{
			Reason:      reasonRetry,
			TargetUUID:  uuid,
			RequestedAt: time.Now().Add(delay),
		}, delay)
		return true
	}

	meta, fetchErr := p.fetchConnectionInfo(uuid)
	if fetchErr != nil {
		return handleFailure(fetchErr)
	}

	if p.rescue != nil {
		if handled := p.rescue.Schedule(uuid, meta); handled {
			p.metricsRecordRepairSuccess(uuid)
			p.noteRepairSuccess(uuid)
			p.recordPlannerEvent("repair", "delegated", eventSourcePlanner, uuid, detail)
			p.clearFailure(uuid)
			return true
		}
	}

	if err := p.performRepair(uuid, meta); err == nil {
		p.metricsRecordRepairSuccess(uuid)
		p.noteRepairSuccess(uuid)
		p.recordPlannerEvent("repair", "success", eventSourcePlanner, uuid, detail)
		p.clearFailure(uuid)
		return true
	} else {
		return handleFailure(err)
	}
}

func (p *SupplementalPlanner) candidatesFor(uuid string, limit int) ([]*topology.SuppCandidate, error) {
	if p == nil {
		return nil, fmt.Errorf("planner unavailable")
	}
	if limit <= 0 {
		limit = 1
	}
	result, err := p.requestTopology(&topology.TopoTask{Mode: topology.GETSUPPCANDIDATES, UUID: uuid, Count: limit})
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, fmt.Errorf("nil topology result")
	}
	return result.Candidates, nil
}

func (p *SupplementalPlanner) recordAndRetry(action PlanAction, uuid, reason string) {
	attempts, delay, schedule, exhausted := p.recordFailure(uuid, reason)
	if exhausted {
		p.metricsRecordFailure(uuid, action.SourceUUID, reason)
		printer.Warning("\r\n[*] Supplemental planner exhausted retries for %s after %d attempts (%s)\r\n",
			short(uuid), attempts, reason)
		return
	}

	if !schedule {
		printer.Warning("\r\n[*] Supplemental planner retry pending for %s (attempts=%d): %s\r\n",
			short(uuid), attempts, reason)
		return
	}

	next := action
	next.Reason = reasonRetry
	next.Attempt = attempts + 1
	next.RequestedAt = time.Now().Add(delay)
	p.scheduleRetry(uuid, next, delay)
	printer.Warning("\r\n[*] Supplemental planner will retry %s in %s (attempt %d): %s\r\n",
		short(uuid), delay.Round(time.Second), attempts+1, reason)
}

func (p *SupplementalPlanner) scheduleRepair(uuid string, action PlanAction, delay time.Duration) {
	if delay <= 0 {
		delay = time.Second
	}
	timer := time.AfterFunc(delay, func() {
		action.RequestedAt = time.Now()
		p.Enqueue(action)
	})
	p.failuresMu.Lock()
	if state, ok := p.failures[uuid]; ok {
		if state.repairTimer != nil {
			state.repairTimer.Stop()
		}
		state.repairTimer = timer
		state.repairNext = time.Now().Add(delay)
	}
	p.failuresMu.Unlock()
}

func (p *SupplementalPlanner) performRepair(uuid string, meta *topology.Result) error {
	if p == nil {
		return fmt.Errorf("planner unavailable")
	}
	if meta == nil {
		return fmt.Errorf("missing connection metadata")
	}
	opt, err := p.buildRepairOptions(meta)
	if err != nil {
		return err
	}
	conn, nego, nodeUUID, err := p.dialRepair(opt)
	if err != nil {
		return err
	}
	if nodeUUID != "" && nodeUUID != uuid {
		defer conn.Close()
		return fmt.Errorf("repair connected to unexpected node %s (expect %s)", nodeUUID, uuid)
	}
	if p.store != nil {
		p.store.RegisterComponent(conn, opt.Secret, uuid, protocol.DefaultTransports().Upstream(), opt.Downstream)
		p.store.ActivateComponent(uuid)
		if nego != nil {
			p.store.UpdateProtocol(uuid, nego.Version, nego.Flags)
		}
	}
	return nil
}

func (p *SupplementalPlanner) dialRepair(opt *initial.Options) (net.Conn, *protocol.Negotiation, string, error) {
	p.dialerMu.RLock()
	dialer := p.repairDialer
	p.dialerMu.RUnlock()
	if dialer == nil {
		dialer = p.directConnect
	}
	return dialer(opt)
}

func (p *SupplementalPlanner) fetchConnectionInfo(uuid string) (*topology.Result, error) {
	result, err := p.requestTopology(&topology.TopoTask{Mode: topology.GETCONNINFO, UUID: uuid})
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, fmt.Errorf("nil connection info for %s", uuid)
	}
	return result, nil
}

func (p *SupplementalPlanner) buildRepairOptions(meta *topology.Result) (*initial.Options, error) {
	if meta == nil {
		return nil, fmt.Errorf("missing connection metadata")
	}
	p.policyMu.RLock()
	base := cloneInitialOptions(p.baseOptions)
	p.policyMu.RUnlock()
	if base == nil {
		return nil, fmt.Errorf("base dial options unavailable")
	}
	host := strings.TrimSpace(meta.DialAddress)
	if host == "" {
		host = strings.TrimSpace(meta.IP)
	}
	if host == "" {
		return nil, fmt.Errorf("no dial address recorded for %s", meta.UUID)
	}
	port := meta.FallbackPort
	if port <= 0 {
		port = meta.Port
	}
	if port <= 0 {
		return nil, fmt.Errorf("no dial port recorded for %s", meta.UUID)
	}
	base.Mode = initial.NORMAL_ACTIVE
	base.Connect = net.JoinHostPort(host, strconv.Itoa(port))
	if meta.Transport != "" {
		base.Downstream = meta.Transport
	}
	base.TlsEnable = meta.TLSEnabled
	return cloneInitialOptions(base), nil
}

func (p *SupplementalPlanner) directConnect(opt *initial.Options) (net.Conn, *protocol.Negotiation, string, error) {
	if opt == nil {
		return nil, nil, "", fmt.Errorf("options unavailable")
	}
	ctx, cancel := context.WithTimeout(context.Background(), repairDialTimeout)
	defer cancel()
	dialer := &net.Dialer{}
	rawConn, err := dialer.DialContext(ctx, "tcp", opt.Connect)
	if err != nil {
		return nil, nil, "", err
	}
	deadline := time.Now().Add(repairHandshakeTimeout)
	if err := rawConn.SetDeadline(deadline); err != nil {
		_ = rawConn.Close()
		return nil, nil, "", err
	}
	conn := rawConn
	downstream := opt.Downstream
	if downstream == "" {
		downstream = protocol.DefaultTransports().Downstream()
	}
	if opt.TlsEnable {
		tlsConfig, tlsErr := transport.NewClientTLSConfig(opt.Domain, opt.PreAuthToken)
		if tlsErr != nil {
			_ = rawConn.Close()
			return nil, nil, "", tlsErr
		}
		conn = transport.WrapTLSClientConn(rawConn, tlsConfig)
		if err := conn.SetDeadline(deadline); err != nil {
			_ = conn.Close()
			return nil, nil, "", err
		}
	}
	param := new(protocol.NegParam)
	param.Conn = conn
	param.Domain = opt.Domain
	proto := protocol.NewDownProto(param)
	if err := proto.CNegotiate(); err != nil {
		conn.Close()
		return nil, nil, "", err
	}
	conn = param.Conn
	if err := share.ActivePreAuth(conn, opt.PreAuthToken); err != nil {
		conn.Close()
		return nil, nil, "", err
	}
	if err := conn.SetDeadline(deadline); err != nil {
		conn.Close()
		return nil, nil, "", err
	}
	handshakeSecret := opt.BaseSecret()
	if strings.TrimSpace(handshakeSecret) == "" {
		handshakeSecret = opt.Secret
	}
	localVersion := protocol.CurrentProtocolVersion
	localFlags := protocol.DefaultProtocolFlags
	if downstream != "http" {
		localFlags &^= protocol.FlagSupportChunked
	}
	hiMess := &protocol.HIMess{
		GreetingLen:  uint16(len("Shhh...")),
		Greeting:     "Shhh...",
		UUIDLen:      uint16(len(protocol.ADMIN_UUID)),
		UUID:         protocol.ADMIN_UUID,
		IsAdmin:      1,
		IsReconnect:  0,
		ProtoVersion: localVersion,
		ProtoFlags:   localFlags,
	}
	header := &protocol.Header{
		Version:     localVersion,
		Flags:       localFlags,
		Sender:      protocol.ADMIN_UUID,
		Accepter:    protocol.TEMP_UUID,
		MessageType: protocol.HI,
		RouteLen:    uint32(len([]byte(protocol.TEMP_ROUTE))),
		Route:       protocol.TEMP_ROUTE,
	}
	sMessage := protocol.NewDownMsgWithTransport(conn, handshakeSecret, protocol.ADMIN_UUID, downstream)
	protocol.SetMessageMeta(sMessage, localVersion, localFlags)
	protocol.ConstructMessage(sMessage, header, hiMess, false)
	sMessage.SendMessage()
	rMessage := protocol.NewDownMsgWithTransport(conn, handshakeSecret, protocol.ADMIN_UUID, downstream)
	if err := conn.SetDeadline(deadline); err != nil {
		conn.Close()
		return nil, nil, "", err
	}
	fHeader, fMessage, err := protocol.DestructMessage(rMessage)
	if err != nil {
		conn.Close()
		return nil, nil, "", err
	}
	if err := conn.SetDeadline(time.Time{}); err != nil {
		conn.Close()
		return nil, nil, "", err
	}
	if fHeader.MessageType != protocol.HI {
		conn.Close()
		return nil, nil, "", fmt.Errorf("unexpected message type %d", fHeader.MessageType)
	}
	mmess, ok := fMessage.(*protocol.HIMess)
	if !ok {
		conn.Close()
		return nil, nil, "", fmt.Errorf("unexpected HI payload %T", fMessage)
	}
	negotiation := protocol.Negotiate(localVersion, localFlags, mmess.ProtoVersion, mmess.ProtoFlags)
	if !negotiation.IsV1() {
		conn.Close()
		return nil, nil, "", fmt.Errorf("peer protocol version %d unsupported", mmess.ProtoVersion)
	}
	if downstream == "http" && negotiation.Flags&protocol.FlagSupportChunked == 0 {
		conn.Close()
		return nil, nil, "", fmt.Errorf("peer does not support HTTP chunked transfer")
	}
	return conn, &negotiation, strings.TrimSpace(mmess.UUID), nil
}

func (p *SupplementalPlanner) recordFailure(uuid, reason string) (attempts int, delay time.Duration, schedule bool, exhausted bool) {
	if uuid == "" {
		return 0, 0, false, false
	}
	policy := p.Policy()
	if policy.RetryBase <= 0 {
		policy.RetryBase = plannerRetryBase
	}
	if policy.RetryMax <= 0 {
		policy.RetryMax = plannerRetryMax
	}
	if policy.RetryMaxAttempts <= 0 {
		policy.RetryMaxAttempts = plannerRetryMaxAttempts
	}
	if policy.RetryCooldown <= 0 {
		policy.RetryCooldown = plannerRetryCooldown
	}

	p.failuresMu.Lock()
	defer p.failuresMu.Unlock()

	if p.failures == nil {
		p.failures = make(map[string]*failureState)
	}
	state, ok := p.failures[uuid]
	if !ok {
		state = &failureState{}
		p.failures[uuid] = state
	}
	state.attempts++
	state.reason = reason
	attempts = state.attempts

	if state.timer != nil {
		if !state.timer.Stop() {
			// 计时器已触发，允许回调继续执行但清空引用
		}
		state.timer = nil
	}

	if attempts >= policy.RetryMaxAttempts {
		exhausted = true
		state.next = time.Now().Add(policy.RetryCooldown)
		return
	}

	delay = backoffDuration(policy.RetryBase, policy.RetryMax, attempts)
	state.next = time.Now().Add(delay)
	schedule = true
	return
}

func (p *SupplementalPlanner) scheduleRetry(uuid string, action PlanAction, delay time.Duration) {
	timer := time.AfterFunc(delay, func() {
		p.failuresMu.Lock()
		if state, ok := p.failures[uuid]; ok {
			state.timer = nil
			state.next = time.Now()
		}
		p.failuresMu.Unlock()
		action.RequestedAt = time.Now()
		p.Enqueue(action)
	})

	p.failuresMu.Lock()
	if state, ok := p.failures[uuid]; ok {
		state.timer = timer
		state.next = time.Now().Add(delay)
	}
	p.failuresMu.Unlock()
}

func (p *SupplementalPlanner) clearFailure(uuid string) {
	if uuid == "" {
		return
	}
	p.failuresMu.Lock()
	defer p.failuresMu.Unlock()
	if state, ok := p.failures[uuid]; ok {
		if state.timer != nil {
			state.timer.Stop()
		}
		if state.repairTimer != nil {
			state.repairTimer.Stop()
		}
		delete(p.failures, uuid)
	}
}

func (p *SupplementalPlanner) debugDispatchError(src, dst string, err error) {
	if err == nil {
		return
	}
	printer.Warning("\r\n[*] Auto supplemental request %s->%s failed: %v\r\n",
		short(src), short(dst), err)
}

func (p *SupplementalPlanner) desiredTarget() int {
	policy := p.Policy()
	desired := policy.DesiredLinks
	if desired <= 0 {
		return 1
	}
	return desired
}

func (p *SupplementalPlanner) shouldDelay(action PlanAction) (bool, time.Duration) {
	key := action.TargetUUID
	if key == "" {
		return false, 0
	}
	p.failuresMu.Lock()
	state, ok := p.failures[key]
	timer := (*time.Timer)(nil)
	next := time.Time{}
	if ok && state != nil {
		timer = state.timer
		next = state.next
	}
	p.failuresMu.Unlock()
	if !ok || state == nil {
		return false, 0
	}
	if action.Reason != reasonRetry && timer != nil {
		return true, 0
	}
	if action.Reason == reasonNodeAdded {
		return false, 0
	}
	wait := time.Until(next)
	if wait <= 0 {
		return false, 0
	}
	return false, wait
}

func (p *SupplementalPlanner) deferEnqueue(action PlanAction, delay time.Duration) {
	if delay <= 0 {
		p.Enqueue(action)
		return
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-p.ctx.Done():
		return
	case <-timer.C:
		p.Enqueue(action)
	}
}

func (p *SupplementalPlanner) inspectTopology() {
	nodes, err := p.allNodeUUIDs()
	if err != nil {
		printer.Warning("\r\n[*] Supplemental planner inspect failed: %v\r\n", err)
		return
	}
	if len(nodes) < 2 {
		return
	}
	desired := p.desiredTarget()
	policy := p.Policy()
	now := time.Now()
	reportCandidates := make([]string, 0, len(nodes))
	for _, uuid := range nodes {
		if uuid == "" || uuid == protocol.ADMIN_UUID {
			continue
		}
		if !p.allowNode(uuid) {
			continue
		}
		networkID := p.networkFor(uuid)
		if !p.allowNetwork(networkID) {
			continue
		}
		existing := len(supp.GetActiveSuppNeighbors(uuid))
		if existing >= desired {
			continue
		}
		if policy.MaxLinksPerNode > 0 && existing >= policy.MaxLinksPerNode {
			continue
		}
		reportCandidates = append(reportCandidates, uuid)
		p.Enqueue(PlanAction{
			Reason:      reasonPeriodic,
			TargetUUID:  uuid,
			RequestedAt: now,
			Attempt:     1,
		})
	}
	if len(reportCandidates) > 0 {
		p.generateRedundancyReport(reportCandidates, desired)
	}
}

type redundancyEntry struct {
	node        string
	baseCost    float64
	deficit     int
	bestPeer    string
	bestScore   float64
	bestOverlap int
	sleepBudget time.Duration
}

func (p *SupplementalPlanner) generateRedundancyReport(nodes []string, desired int) {
	if p == nil || len(nodes) == 0 {
		return
	}
	entries := make([]redundancyEntry, 0, len(nodes))
	for _, uuid := range nodes {
		if uuid == "" || uuid == protocol.ADMIN_UUID {
			continue
		}
		existing := len(supp.GetActiveSuppNeighbors(uuid))
		base := p.redundancyBaseCost(uuid, existing, desired)
		if base <= 0 {
			continue
		}
		entry := redundancyEntry{
			node:        uuid,
			baseCost:    base,
			deficit:     maxInt(0, desired-existing),
			sleepBudget: p.topo.PathSleepBudget(uuid),
		}
		if cands, err := p.candidatesFor(uuid, 1); err == nil && len(cands) > 0 {
			entry.bestPeer = cands[0].UUID
			entry.bestScore = p.candidateScore(uuid, cands[0])
			entry.bestOverlap = cands[0].Overlap
		}
		entries = append(entries, entry)
	}
	if len(entries) == 0 {
		return
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].baseCost != entries[j].baseCost {
			return entries[i].baseCost > entries[j].baseCost
		}
		return entries[i].node < entries[j].node
	})
	limit := 5
	if len(entries) < limit {
		limit = len(entries)
	}
	lines := make([]string, 0, limit)
	for idx := 0; idx < limit; idx++ {
		entry := entries[idx]
		line := fmt.Sprintf("node=%s cost=%.2f deficit=%d candidate=%s gain=%.2f overlap=%d sleep=%s",
			short(entry.node), entry.baseCost, entry.deficit, short(entry.bestPeer), entry.bestScore, entry.bestOverlap,
			entry.sleepBudget.Truncate(time.Second))
		printer.Success("\r\n[*] Supplemental graph: %s\r\n", line)
		lines = append(lines, line)
	}
	p.metricsRecordGraph(lines)
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (p *SupplementalPlanner) redundancyBaseCost(uuid string, existing, desired int) float64 {
	if p == nil || uuid == "" {
		return 0
	}
	info := p.topo.RouteInfo(uuid)
	cost := 1.0
	if info != nil {
		if info.Depth > 0 {
			cost += float64(info.Depth)
		}
	}
	budget := p.topo.PathSleepBudget(uuid)
	if budget > 0 {
		cost += budget.Seconds() / 10
	}
	deficit := desired - existing
	if deficit > 0 {
		cost += float64(deficit) * 1.5
	}
	if work := p.topo.WorkSeconds(uuid); work > 0 {
		cost -= float64(work) / candidateWorkRef
	}
	if cost < 0.5 {
		cost = 0.5
	}
	return cost
}

func (p *SupplementalPlanner) recycleSupplementalLinks() {
	summaries := supp.ListSupplementalLinks()
	if len(summaries) == 0 {
		return
	}

	desired := p.desiredTarget()
	stale := make(map[string]string)
	linksByNode := make(map[string][]nodeLink)

	for _, summary := range summaries {
		if summary == nil || summary.LinkUUID == "" {
			continue
		}
		if !summary.Ready ||
			summary.HealthA.String() == "failed" ||
			summary.HealthB.String() == "failed" ||
			summary.EndpointA == "" ||
			summary.EndpointB == "" {
			stale[summary.LinkUUID] = "link unhealthy"
			continue
		}
		if summary.LastA.IsZero() && summary.LastB.IsZero() {
			// 尚未收到心跳，留待下次处理
		} else {
			idleA := summary.LastA.IsZero() || time.Since(summary.LastA) > plannerIdleThreshold
			idleB := summary.LastB.IsZero() || time.Since(summary.LastB) > plannerIdleThreshold
			if idleA && idleB {
				stale[summary.LinkUUID] = "idle timeout"
				continue
			}
		}
		networkA := p.networkFor(summary.EndpointA)
		networkB := p.networkFor(summary.EndpointB)
		if isCrossNetwork(networkA, networkB) {
			stale[summary.LinkUUID] = "cross-network"
			continue
		}
		overlap := p.pathOverlap(summary.EndpointA, summary.EndpointB)
		linksByNode[summary.EndpointA] = append(linksByNode[summary.EndpointA], nodeLink{
			linkUUID: summary.LinkUUID,
			peer:     summary.EndpointB,
			overlap:  overlap,
			last:     summary.LastA,
		})
		linksByNode[summary.EndpointB] = append(linksByNode[summary.EndpointB], nodeLink{
			linkUUID: summary.LinkUUID,
			peer:     summary.EndpointA,
			overlap:  overlap,
			last:     summary.LastB,
		})
	}

	for linkUUID, reason := range stale {
		p.retireLink(linkUUID, reason)
	}

	if len(linksByNode) == 0 {
		return
	}

	remaining := make(map[string]int, len(linksByNode))
	for node, links := range linksByNode {
		remaining[node] = len(links)
		sortNodeLinks(links)
	}

	removeSet := make(map[string]string)
	for node, links := range linksByNode {
		extra := remaining[node] - desired
		if extra <= 0 {
			continue
		}
		left := p.applyRemoval(node, links, removeSet, remaining, desired, true, extra)
		if left > 0 {
			printer.Warning("\r\n[*] Supplemental planner could not recycle %d extra link(s) for %s without breaking peer quotas\r\n",
				left, short(node))
		}
	}

	policy := p.Policy()
	if policy.MaxLinksPerNode > 0 {
		for node, links := range linksByNode {
			count := remaining[node]
			extra := count - policy.MaxLinksPerNode
			if extra <= 0 {
				continue
			}
			_ = p.applyRemoval(node, links, removeSet, remaining, policy.MaxLinksPerNode, false, extra)
		}
	}

	for linkUUID, reason := range removeSet {
		p.retireLink(linkUUID, reason)
	}
}

func sortNodeLinks(links []nodeLink) {
	sort.SliceStable(links, func(i, j int) bool {
		if links[i].overlap != links[j].overlap {
			return links[i].overlap > links[j].overlap
		}
		zeroI := links[i].last.IsZero()
		zeroJ := links[j].last.IsZero()
		if zeroI != zeroJ {
			return zeroI
		}
		if !links[i].last.Equal(links[j].last) {
			return links[i].last.Before(links[j].last)
		}
		if links[i].peer != links[j].peer {
			return links[i].peer < links[j].peer
		}
		return links[i].linkUUID < links[j].linkUUID
	})
}

func (p *SupplementalPlanner) applyRemoval(node string, links []nodeLink, removeSet map[string]string, remaining map[string]int, desired int, requirePeer bool, budget int) int {
	for _, link := range links {
		if budget <= 0 {
			break
		}
		if link.linkUUID == "" {
			continue
		}
		if _, exists := removeSet[link.linkUUID]; exists {
			continue
		}
		peer := link.peer
		if requirePeer {
			if count := remaining[peer]; count <= desired {
				continue
			}
		}
		reason := fmt.Sprintf("auto recycle %s-%s overlap=%d", short(node), short(peer), link.overlap)
		removeSet[link.linkUUID] = reason
		remaining[node]--
		if peer != "" {
			remaining[peer]--
		}
		budget--
	}
	return budget
}

func (p *SupplementalPlanner) retireLink(linkUUID, reason string) {
	if linkUUID == "" {
		return
	}
	if err := supp.AbortSupplementalLinkWithReason(p.topo, p.mgr, linkUUID, reason); err != nil {
		printer.Warning("\r\n[*] Supplemental planner failed to recycle %s: %v\r\n", short(linkUUID), err)
		return
	}
	printer.Warning("\r\n[*] Supplemental planner recycled supplemental link %s (%s)\r\n",
		short(linkUUID), reason)
	p.metricsRecordRecycle(linkUUID, reason)
}

func (p *SupplementalPlanner) pathOverlap(a, b string) int {
	if p == nil || p.topo == nil || a == "" || b == "" {
		return 0
	}
	infoA := p.topo.RouteInfo(a)
	infoB := p.topo.RouteInfo(b)
	if infoA == nil || infoB == nil {
		return 0
	}
	return countOverlap(infoA.Path, infoB.Path)
}

func countOverlap(a, b []string) int {
	if len(a) == 0 || len(b) == 0 {
		return 0
	}
	set := make(map[string]struct{}, len(a))
	for _, id := range a {
		set[id] = struct{}{}
	}
	count := 0
	for _, id := range b {
		if _, ok := set[id]; ok {
			count++
		}
	}
	return count
}

func (p *SupplementalPlanner) allNodeUUIDs() ([]string, error) {
	result, err := p.requestTopology(&topology.TopoTask{Mode: topology.GETALLNODES})
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, fmt.Errorf("nil topology result")
	}
	return result.AllNodes, nil
}

func (p *SupplementalPlanner) requestTopology(task *topology.TopoTask) (*topology.Result, error) {
	if p == nil || p.service == nil {
		return nil, fmt.Errorf("topology service unavailable")
	}
	base := p.ctx
	if base == nil {
		base = context.Background()
	}
	ctx, cancel := context.WithTimeout(base, plannerRequestTimeout)
	defer cancel()
	return p.service.Request(ctx, task)
}

func backoffDuration(base, max time.Duration, attempt int) time.Duration {
	if attempt <= 0 {
		attempt = 1
	}
	if base <= 0 {
		base = plannerRetryBase
	}
	if max <= 0 {
		max = plannerRetryMax
	}
	delay := base * time.Duration(1<<(attempt-1))
	if delay <= 0 {
		delay = base
	}
	if delay > max {
		return max
	}
	return delay
}

func (p *SupplementalPlanner) allowNode(uuid string) bool {
	if uuid == "" {
		return false
	}
	policy := p.Policy()
	if containsIgnoreCase(policy.DenyNodes, uuid) {
		return false
	}
	if len(policy.AllowNodes) > 0 && !containsIgnoreCase(policy.AllowNodes, uuid) {
		return false
	}
	return true
}

func (p *SupplementalPlanner) allowNetwork(network string) bool {
	policy := p.Policy()
	if network == "" {
		return !containsIgnoreCase(policy.DenyNetworks, "")
	}
	if containsIgnoreCase(policy.DenyNetworks, network) {
		return false
	}
	if len(policy.AllowNetworks) > 0 && !containsIgnoreCase(policy.AllowNetworks, network) {
		return false
	}
	return true
}

func (p *SupplementalPlanner) rescueCandidateAllowed(target, rescuer string) bool {
	if p == nil {
		return false
	}
	if target == "" || rescuer == "" {
		return false
	}
	if !p.allowNode(rescuer) {
		return false
	}
	targetNet := p.networkFor(target)
	rescuerNet := p.networkFor(rescuer)
	if !p.allowNetwork(targetNet) || !p.allowNetwork(rescuerNet) {
		return false
	}
	if isCrossNetwork(targetNet, rescuerNet) {
		return false
	}
	policy := p.Policy()
	if policy.MaxLinksPerNode > 0 {
		if len(supp.GetActiveSuppNeighbors(rescuer)) >= policy.MaxLinksPerNode {
			return false
		}
	}
	return true
}

func (p *SupplementalPlanner) networkFor(uuid string) string {
	if p == nil || p.topo == nil || uuid == "" {
		return ""
	}
	return p.topo.NetworkFor(uuid)
}

func containsIgnoreCase(list []string, value string) bool {
	if len(list) == 0 {
		return false
	}
	value = strings.TrimSpace(value)
	for _, entry := range list {
		if strings.EqualFold(strings.TrimSpace(entry), value) {
			return true
		}
	}
	return false
}

func isCrossNetwork(a, b string) bool {
	a = strings.TrimSpace(a)
	b = strings.TrimSpace(b)
	if a == "" && b == "" {
		return false
	}
	if a == "" || b == "" {
		return true
	}
	return !strings.EqualFold(a, b)
}

func (p *SupplementalPlanner) noteRepairSuccess(uuid string) {
	if p == nil || uuid == "" {
		return
	}
	now := time.Now()
	if _, err := p.requestTopology(&topology.TopoTask{
		Mode:           topology.UPDATECONNINFO,
		UUID:           uuid,
		LastSuccess:    now,
		RepairAttempts: 0,
	}); err != nil {
		printer.Warning("\r\n[*] Supplemental planner failed to record repair success for %s: %v\r\n",
			short(uuid), err)
	}
	if p.topo != nil {
		p.topo.ScheduleCalculate()
	}
}

func (p *SupplementalPlanner) noteRepairFailure(uuid string, attempt int) {
	if p == nil || uuid == "" {
		return
	}
	if attempt < 0 {
		attempt = 0
	}
	if _, err := p.requestTopology(&topology.TopoTask{
		Mode:           topology.UPDATECONNINFO,
		UUID:           uuid,
		RepairAttempts: attempt,
	}); err != nil {
		printer.Warning("\r\n[*] Supplemental planner failed to record repair failure for %s: %v\r\n",
			short(uuid), err)
	}
}

func (p *SupplementalPlanner) markRepairBroken(uuid, detail string) {
	if p == nil || uuid == "" {
		return
	}
	p.failuresMu.Lock()
	if state, ok := p.failures[uuid]; ok {
		state.broken = true
		state.repairNext = time.Time{}
		if state.repairTimer != nil {
			state.repairTimer.Stop()
			state.repairTimer = nil
		}
	}
	p.failuresMu.Unlock()
	if detail == "" {
		detail = "repair attempts exhausted"
	}
	p.recordPlannerEvent("repair", "abandon", eventSourcePlanner, uuid, detail)
	printer.Warning("\r\n[*] Supplemental planner abandoned repair for %s: %s\r\n",
		short(uuid), detail)
}
