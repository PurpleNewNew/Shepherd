package process

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

	"slices"

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

func newNodeQualityState() *nodeQualityState {
	return &nodeQualityState{
		healthEMA:    -1,
		latencyEMA:   -1,
		failEMA:      -1,
		queueEMA:     -1,
		stalenessEMA: -1,
	}
}

type qualityOutcomeStats struct {
	Attempts       uint64
	Success        uint64
	SumSuccess     float64
	SumFailure     float64
	LastUpdate     time.Time
	LastAdjustTime time.Time
}

func (s *qualityOutcomeStats) record(predicted float64, success bool) {
	s.Attempts++
	s.LastUpdate = time.Now()
	if success {
		s.Success++
		s.SumSuccess += predicted
	} else {
		s.SumFailure += predicted
	}
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

func clamp01(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}

func applyEMA(prev float64, sample float64) float64 {
	if sample < 0 {
		return prev
	}
	if prev < 0 {
		return sample
	}
	return qualityEMAAlpha*sample + (1-qualityEMAAlpha)*prev
}

func normalizeDuration(value, reference time.Duration) float64 {
	if reference <= 0 {
		return 0
	}
	return clamp01(float64(value) / float64(reference))
}

func normalizeQueue(depth uint32) float64 {
	return clamp01(float64(depth) / qualityQueueRef)
}

type qualityWeightSet struct {
	Health    float64
	Latency   float64
	Failure   float64
	Queue     float64
	Staleness float64
}

func newQualityWeightSet() qualityWeightSet {
	w := qualityWeightSet{
		Health:    qualityDefaultHealthWeight,
		Latency:   qualityDefaultLatencyWeight,
		Failure:   qualityDefaultFailureWeight,
		Queue:     qualityDefaultQueueWeight,
		Staleness: qualityDefaultStaleWeight,
	}
	w.normalize()
	return w
}

func (w *qualityWeightSet) normalize() {
	sum := w.Health + w.Latency + w.Failure + w.Queue + w.Staleness
	if sum == 0 {
		*w = newQualityWeightSet()
		return
	}
	if w.Health < qualityMinWeight {
		w.Health = qualityMinWeight
	}
	if w.Latency < qualityMinWeight {
		w.Latency = qualityMinWeight
	}
	if w.Failure < qualityMinWeight {
		w.Failure = qualityMinWeight
	}
	if w.Queue < qualityMinWeight {
		w.Queue = qualityMinWeight
	}
	if w.Staleness < qualityMinWeight {
		w.Staleness = qualityMinWeight
	}
	sum = w.Health + w.Latency + w.Failure + w.Queue + w.Staleness
	w.Health /= sum
	w.Latency /= sum
	w.Failure /= sum
	w.Queue /= sum
	w.Staleness /= sum
}

func (w qualityWeightSet) clone() qualityWeightSet {
	return qualityWeightSet{
		Health:    w.Health,
		Latency:   w.Latency,
		Failure:   w.Failure,
		Queue:     w.Queue,
		Staleness: w.Staleness,
	}
}

func (w *qualityWeightSet) adjust(predicted float64, success bool, factor float64) {
	if factor <= 0 {
		factor = 1
	}
	normalized := clamp01(predicted / qualityScoreScale)
	delta := 0.02 * factor
	if success {
		if normalized > qualityAdjustmentHighThresh {
			w.Health *= 1 - delta
			w.Latency *= 1 - delta
			w.Failure *= 1 - delta
		} else if normalized < qualityAdjustmentLowThresh {
			w.Queue *= 1 - delta/2
		}
	} else {
		if normalized < qualityAdjustmentLowThresh {
			w.Failure *= 1 + delta*2
			w.Staleness *= 1 + delta
		} else {
			w.Failure *= 1 + delta
			w.Health *= 1 + delta/2
		}
	}
	w.normalize()
}

func (p *SupplementalPlanner) weightSnapshot() qualityWeightSet {
	p.qualityWeightsMu.RLock()
	ws := p.qualityWeights.clone()
	p.qualityWeightsMu.RUnlock()
	return ws
}

func (p *SupplementalPlanner) adjustQualityWeights(predicted float64, success bool) {
	if p == nil {
		return
	}
	p.qualityStatsMu.Lock()
	p.qualityStats.record(predicted, success)
	p.qualityStatsMu.Unlock()
	p.qualityWeightsMu.Lock()
	defer p.qualityWeightsMu.Unlock()
	p.qualityAdjustCnt++
	factor := clamp01(float64(p.qualityAdjustCnt)/20.0) + 0.5
	p.qualityWeights.adjust(predicted, success, factor)
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

func (p *SupplementalPlanner) metricsRecordGraph(lines []string) {
	if p == nil || len(lines) == 0 {
		return
	}
	p.metricsMu.Lock()
	defer p.metricsMu.Unlock()
	limit := 5
	if len(lines) < limit {
		limit = len(lines)
	}
	copyLines := make([]string, limit)
	copy(copyLines, lines[:limit])
	p.metrics.lastGraphReport = copyLines
	p.persistMetricsLocked(&p.metrics)
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
	// Avoid tight loops on pathological timestamps; this is a best-effort probe path.
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
	// For sleep/work nodes, delay repair attempts until we've missed several expected
	// duty-cycle refreshes. This avoids thrashing during normal sleep windows, but
	// still detects "killed while sleeping" cases.
	if p == nil || p.topo == nil || uuid == "" {
		return defaults.NodeStaleTimeout
	}
	rt, ok := p.topo.NodeRuntime(uuid)
	if !ok {
		return defaults.NodeStaleTimeout
	}
	if rt.SleepSeconds <= 0 {
		// Always-on nodes: a NodeOffline usually means hard failure; probe quickly.
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
			// Ensure we wait until after the reported wake time + a small slack window.
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

	// Drop the timer handle first so subsequent offline transitions can schedule again.
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

	// If the node is currently online again, do nothing.
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
					// The node has been seen recently enough that this is likely a normal
					// duty-cycled sleep window. Reschedule a follow-up probe so we still
					// converge to rescue when the node remains stale (killed while sleeping).
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

	// Enqueue a repair-capable action. planForNode() will route it into tryRepair()
	// only when the node is actually offline (per GETALLNODES).
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
	// Skip planning on trivial topologies (no peers available yet)
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
	// For always-on nodes (sleepSeconds=0), "offline" is usually a hard failure (kill/crash).
	// Proactively fail their supplemental links so routing stops using stale "#supp" edges and the
	// planner can immediately create replacements, instead of waiting for heartbeat timeouts.
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

	// Sleep/work nodes intentionally drop their upstream; treat "offline" as expected unless
	// the node stops refreshing LastSeen for multiple duty-cycle windows. This probe is what
	// turns "killed while sleeping" into an eventual rescue attempt.
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

	// Only attempt "repair dial" when the target is actually offline/unreachable.
	// Otherwise, generic retries (e.g. "no candidates" during early bootstrap) can
	// get stuck in repair backoff loops and starve normal supplemental planning.
	if p.shouldAttemptRepair(action.Reason) {
		// Manual repairs are an explicit operator action: always attempt the repair
		// path regardless of current online/offline marking.
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

func (p *SupplementalPlanner) metricsRecordDispatch(source, target string, success bool, dispatchErr error) {
	p.metricsMu.Lock()
	m := &p.metrics
	m.dispatched++
	var detail string
	var extra string
	if success {
		m.success++
		detail = "dispatch-ok"
	} else {
		m.failures++
		if dispatchErr != nil {
			m.lastFailure = dispatchErr.Error()
			extra = dispatchErr.Error()
		}
		detail = "dispatch-failed"
	}
	p.appendEventLocked(m, plannerEvent{
		Kind:      "dispatch",
		Action:    detail,
		Source:    source,
		Target:    target,
		Detail:    extra,
		Timestamp: time.Now(),
	})
	p.persistMetricsLocked(m)
	p.metricsMu.Unlock()
}

func (p *SupplementalPlanner) metricsRecordSuccess(target string, created, attempted int) {
	p.metricsMu.Lock()
	m := &p.metrics
	info := fmt.Sprintf("created=%d attempted=%d", created, attempted)
	p.appendEventLocked(m, plannerEvent{
		Kind:      "success",
		Action:    "dispatch",
		Source:    eventSourcePlanner,
		Target:    target,
		Detail:    info,
		Timestamp: time.Now(),
	})
	p.persistMetricsLocked(m)
	p.metricsMu.Unlock()
}

func (p *SupplementalPlanner) metricsRecordFailure(target, source, reason string) {
	p.metricsMu.Lock()
	m := &p.metrics
	m.lastFailure = reason
	p.appendEventLocked(m, plannerEvent{
		Kind:      "failure",
		Action:    "retry",
		Source:    source,
		Target:    target,
		Detail:    reason,
		Timestamp: time.Now(),
	})
	p.persistMetricsLocked(m)
	p.metricsMu.Unlock()
}

func (p *SupplementalPlanner) metricsRecordDropped(action PlanAction) {
	p.metricsMu.Lock()
	m := &p.metrics
	m.dropped++
	p.appendEventLocked(m, plannerEvent{
		Kind:      "drop",
		Action:    action.Reason,
		Source:    action.SourceUUID,
		Target:    action.TargetUUID,
		Detail:    "queue-full",
		Timestamp: time.Now(),
	})
	p.persistMetricsLocked(m)
	p.metricsMu.Unlock()
}

func (p *SupplementalPlanner) metricsRecordRecycle(linkUUID, reason string) {
	p.metricsMu.Lock()
	m := &p.metrics
	m.recycled++
	p.appendEventLocked(m, plannerEvent{
		Kind:      "recycle",
		Action:    reason,
		Source:    eventSourcePlanner,
		Target:    linkUUID,
		Detail:    "teardown",
		Timestamp: time.Now(),
	})
	p.persistMetricsLocked(m)
	p.metricsMu.Unlock()
}

func (p *SupplementalPlanner) metricsRecordRepairSuccess(uuid string) {
	p.metricsMu.Lock()
	m := &p.metrics
	m.repairSuccess++
	m.repairAttempts++
	p.appendEventLocked(m, plannerEvent{
		Kind:      "repair",
		Action:    "success",
		Source:    eventSourcePlanner,
		Target:    uuid,
		Detail:    "repair-success",
		Timestamp: time.Now(),
	})
	p.persistMetricsLocked(m)
	p.metricsMu.Unlock()
}

func (p *SupplementalPlanner) metricsRecordRepairFailure(uuid string, err error) {
	p.metricsMu.Lock()
	m := &p.metrics
	m.repairAttempts++
	m.repairFailures++
	var detail string
	if err != nil {
		detail = err.Error()
		m.lastFailure = detail
	}
	p.appendEventLocked(m, plannerEvent{
		Kind:      "repair",
		Action:    "failure",
		Source:    eventSourcePlanner,
		Target:    uuid,
		Detail:    detail,
		Timestamp: time.Now(),
	})
	p.persistMetricsLocked(m)
	p.metricsMu.Unlock()
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

func (p *SupplementalPlanner) noteQueueLength(length int) {
	p.metricsMu.Lock()
	if length > p.metrics.queueHigh {
		p.metrics.queueHigh = length
	}
	p.persistMetricsLocked(&p.metrics)
	p.metricsMu.Unlock()
}

// RepairStatuses returns a snapshot of nodes currently scheduled for repair.
func (p *SupplementalPlanner) RepairStatuses() []RepairStatusSnapshot {
	if p == nil {
		return nil
	}
	p.failuresMu.Lock()
	defer p.failuresMu.Unlock()
	if len(p.failures) == 0 {
		return nil
	}
	result := make([]RepairStatusSnapshot, 0, len(p.failures))
	for uuid, state := range p.failures {
		if state == nil {
			continue
		}
		result = append(result, RepairStatusSnapshot{
			TargetUUID:  uuid,
			Attempts:    state.repairAttempts,
			NextAttempt: state.repairNext,
			Broken:      state.broken,
		})
	}
	slices.SortFunc(result, func(a, b RepairStatusSnapshot) int {
		return strings.Compare(a.TargetUUID, b.TargetUUID)
	})
	return result
}

// MetricsSnapshot 返回聚合计数器概况。
func (p *SupplementalPlanner) MetricsSnapshot() SupplementalMetricsSnapshot {
	if p == nil {
		return SupplementalMetricsSnapshot{}
	}
	p.metricsMu.Lock()
	snapshot := SupplementalMetricsSnapshot{
		Dispatched:     p.metrics.dispatched,
		Success:        p.metrics.success,
		Failures:       p.metrics.failures,
		Dropped:        p.metrics.dropped,
		Recycled:       p.metrics.recycled,
		RepairAttempts: p.metrics.repairAttempts,
		RepairSuccess:  p.metrics.repairSuccess,
		RepairFailures: p.metrics.repairFailures,
		QueueHigh:      p.metrics.queueHigh,
		LastFailure:    p.metrics.lastFailure,
		EventSeq:       p.metrics.lastEventSeq,
	}
	if len(p.metrics.lastGraphReport) > 0 {
		snapshot.LastGraphReport = append([]string(nil), p.metrics.lastGraphReport...)
	}
	p.metricsMu.Unlock()
	snapshot.QueueDepth = p.QueueLength()
	return snapshot
}

// EventLog 返回最新的调度事件，默认最新在前。
func (p *SupplementalPlanner) EventLog(limit int) []SupplementalPlannerEvent {
	if p == nil {
		return nil
	}
	p.metricsMu.Lock()
	logCopy := append([]plannerEvent(nil), p.metrics.eventLog...)
	p.metricsMu.Unlock()
	if len(logCopy) == 0 {
		return nil
	}
	if limit <= 0 || limit > len(logCopy) {
		limit = len(logCopy)
	}
	result := make([]SupplementalPlannerEvent, 0, limit)
	for i := len(logCopy) - 1; i >= 0 && len(result) < limit; i-- {
		entry := logCopy[i]
		result = append(result, SupplementalPlannerEvent{
			Seq:        entry.Seq,
			Kind:       entry.Kind,
			Action:     entry.Action,
			SourceUUID: entry.Source,
			TargetUUID: entry.Target,
			Detail:     entry.Detail,
			Timestamp:  entry.Timestamp,
		})
	}
	return result
}

func (p *SupplementalPlanner) appendEventLocked(m *plannerMetrics, event plannerEvent) {
	m.lastEventSeq++
	event.Seq = m.lastEventSeq
	if len(m.eventLog) >= plannerEventLogSize {
		copy(m.eventLog, m.eventLog[1:])
		m.eventLog = m.eventLog[:plannerEventLogSize-1]
	}
	m.eventLog = append(m.eventLog, event)
}

func (p *SupplementalPlanner) recordPlannerEvent(kind, action, source, target, detail string) {
	p.metricsMu.Lock()
	m := &p.metrics
	p.appendEventLocked(m, plannerEvent{
		Kind:      kind,
		Action:    action,
		Source:    source,
		Target:    target,
		Detail:    detail,
		Timestamp: time.Now(),
	})
	p.persistMetricsLocked(m)
	p.metricsMu.Unlock()
}

func (p *SupplementalPlanner) sortCandidates(target string, candidates []*topology.SuppCandidate) {
	if len(candidates) <= 1 {
		return
	}
	scores := make(map[string]float64, len(candidates))
	for _, candidate := range candidates {
		scores[candidate.UUID] = p.candidateScore(target, candidate)
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		uuidI := candidates[i].UUID
		uuidJ := candidates[j].UUID
		scoreI := scores[uuidI]
		scoreJ := scores[uuidJ]
		if scoreI != scoreJ {
			return scoreI < scoreJ
		}
		if candidates[i].Redundancy != candidates[j].Redundancy {
			return candidates[i].Redundancy > candidates[j].Redundancy
		}
		if candidates[i].Overlap != candidates[j].Overlap {
			return candidates[i].Overlap < candidates[j].Overlap
		}
		if candidates[i].Depth != candidates[j].Depth {
			return candidates[i].Depth < candidates[j].Depth
		}
		return uuidI < uuidJ
	})
}

func (p *SupplementalPlanner) candidateScore(target string, candidate *topology.SuppCandidate) float64 {
	if candidate == nil {
		return 1.0
	}
	qualityNorm := p.nodeQualityScore(candidate.UUID) / qualityScoreScale
	if qualityNorm <= 0 {
		qualityNorm = 0.5
	}
	sleepNorm := 0.0
	if candidate.SleepBudget > 0 {
		sleepNorm = normalizeDuration(candidate.SleepBudget, supplementalSleepRef)
	}
	overlapNorm := 0.0
	if len(candidate.Path) > 0 {
		overlapNorm = clamp01(float64(candidate.Overlap) / float64(len(candidate.Path)))
	}
	depthNorm := 0.0
	if candidate.Depth > 0 {
		depthNorm = clamp01(float64(candidate.Depth) / float64(len(candidate.Path)+1))
	}
	redundancyNorm := clamp01(candidate.Redundancy)
	workNorm := 1 - clamp01(float64(candidate.WorkSeconds)/candidateWorkRef)
	score := candidateQualityWeight*qualityNorm +
		candidateSleepWeight*sleepNorm +
		candidateOverlapWeight*overlapNorm +
		candidateDepthWeight*depthNorm +
		candidateRedundancyWeight*redundancyNorm +
		candidateWorkWeight*workNorm
	return score
}

func (p *SupplementalPlanner) computeQualityComponents(state *nodeQualityState, now time.Time) (health, latency, failure, queue, staleness float64) {
	if state == nil {
		return 0.5, 0.3, 0.1, 0.0, 0.5
	}
	health = state.healthEMA
	if health < 0 {
		if state.HealthScore > 0 {
			health = clamp01(float64(state.HealthScore) / qualityHealthDefaultMax)
		} else {
			health = 0.5
		}
	}
	latency = state.latencyEMA
	if latency < 0 {
		if state.AvgLatency > 0 {
			latency = normalizeDuration(state.AvgLatency, qualityLatencyRef)
		} else {
			latency = 0.3
		}
	}
	failure = state.failEMA
	if failure < 0 {
		total := state.TotalSuccess + state.TotalFailures
		if total > 0 {
			failure = clamp01(float64(state.TotalFailures) / float64(total))
		} else {
			failure = 0.1
		}
	}
	queue = state.queueEMA
	if queue < 0 {
		queue = normalizeQueue(state.QueueDepth)
	}
	staleness = state.stalenessEMA
	if staleness < 0 {
		if !state.LastHeartbeat.IsZero() {
			staleness = normalizeDuration(now.Sub(state.LastHeartbeat), qualityStalenessRef)
		} else {
			staleness = 0.5
		}
	}
	if !state.UpdatedAt.IsZero() {
		age := now.Sub(state.UpdatedAt)
		switch {
		case age > 15*time.Minute:
			staleness = clamp01(staleness + 0.4)
		case age > 10*time.Minute:
			staleness = clamp01(staleness + 0.25)
		case age > 5*time.Minute:
			staleness = clamp01(staleness + 0.1)
		}
	}
	return
}

func (p *SupplementalPlanner) nodeQualityScore(uuid string) float64 {
	if uuid == "" {
		return qualityScoreScale / 2
	}
	p.qualityMu.RLock()
	state, ok := p.nodeQuality[uuid]
	p.qualityMu.RUnlock()
	if !ok || state == nil {
		return qualityScoreScale / 2
	}

	now := time.Now()
	health, latency, failure, queue, staleness := p.computeQualityComponents(state, now)
	weights := p.weightSnapshot()
	composite := weights.Health*health +
		weights.Latency*latency +
		weights.Failure*failure +
		weights.Queue*queue +
		weights.Staleness*staleness
	composite = clamp01(composite)

	score := composite * qualityScoreScale
	if state.ConsecutiveFailures > 0 {
		score += float64(state.ConsecutiveFailures) * qualityConsecPenalty
	}
	if state.TotalFailures > 0 {
		total := float64(state.TotalSuccess + state.TotalFailures)
		score += clamp01(float64(state.TotalFailures)/total) * qualityFailurePenalty
	}
	return score
}

func cloneInitialOptions(opt *initial.Options) *initial.Options {
	if opt == nil {
		return nil
	}
	clone := *opt
	return &clone
}

func (p *SupplementalPlanner) updateNodeQuality(uuid string, alive bool, ts time.Time) {
	if uuid == "" {
		return
	}
	p.qualityMu.Lock()
	state := p.nodeQuality[uuid]
	if state == nil {
		state = newNodeQualityState()
		p.nodeQuality[uuid] = state
	}
	now := time.Now()
	state.sampleCount++
	if alive {
		if ts.IsZero() {
			ts = now
		}
		if ts.After(now) {
			ts = now
		}
		state.LastHeartbeat = ts
		state.ConsecutiveFailures = 0
		state.TotalSuccess++
		latency := now.Sub(ts)
		if latency < 0 {
			latency = 0
		}
		if state.AvgLatency == 0 {
			state.AvgLatency = latency
		} else {
			state.AvgLatency = time.Duration((state.AvgLatency*3 + latency) / 4)
		}
		state.latencyEMA = applyEMA(state.latencyEMA, normalizeDuration(latency, qualityLatencyRef))
		state.failEMA = applyEMA(state.failEMA, 0)
		state.stalenessEMA = applyEMA(state.stalenessEMA, 0)
	} else {
		state.TotalFailures++
		state.ConsecutiveFailures++
		state.failEMA = applyEMA(state.failEMA, 1)
		state.stalenessEMA = applyEMA(state.stalenessEMA, 1)
	}
	state.healthEMA = applyEMA(state.healthEMA, clamp01(float64(state.HealthScore)/qualityHealthDefaultMax))
	state.queueEMA = applyEMA(state.queueEMA, normalizeQueue(state.QueueDepth))
	state.UpdatedAt = now
	p.qualityMu.Unlock()
}

// QualitySnapshot 返回指定节点的质量评分（按风险降序）。
func (p *SupplementalPlanner) QualitySnapshot(limit int, nodes []string) []SupplementalQualitySnapshot {
	if p == nil {
		return nil
	}
	var filter map[string]struct{}
	if len(nodes) > 0 {
		filter = make(map[string]struct{}, len(nodes))
		for _, id := range nodes {
			trimmed := strings.TrimSpace(id)
			if trimmed == "" {
				continue
			}
			filter[strings.ToLower(trimmed)] = struct{}{}
		}
	}
	now := time.Now()
	type qualityItem struct {
		uuid  string
		state *nodeQualityState
	}
	items := make([]qualityItem, 0)
	p.qualityMu.RLock()
	for uuid, state := range p.nodeQuality {
		if state == nil {
			continue
		}
		if filter != nil {
			if _, ok := filter[strings.ToLower(uuid)]; !ok {
				continue
			}
		}
		items = append(items, qualityItem{uuid: uuid, state: state})
	}
	p.qualityMu.RUnlock()
	if len(items) == 0 {
		return nil
	}
	sort.Slice(items, func(i, j int) bool {
		scoreI := p.nodeQualityScore(items[i].uuid)
		scoreJ := p.nodeQualityScore(items[j].uuid)
		if scoreI != scoreJ {
			return scoreI > scoreJ
		}
		return items[i].uuid < items[j].uuid
	})
	if limit > 0 && limit < len(items) {
		items = items[:limit]
	}
	result := make([]SupplementalQualitySnapshot, 0, len(items))
	for _, item := range items {
		health, latency, failure, queue, staleness := p.computeQualityComponents(item.state, now)
		snapshot := SupplementalQualitySnapshot{
			NodeUUID:       item.uuid,
			Score:          p.nodeQualityScore(item.uuid),
			HealthScore:    health,
			LatencyScore:   latency,
			FailureScore:   failure,
			QueueScore:     queue,
			StalenessScore: staleness,
			TotalSuccess:   item.state.TotalSuccess,
			TotalFailures:  item.state.TotalFailures,
			LastHeartbeat:  item.state.LastHeartbeat,
		}
		result = append(result, snapshot)
	}
	return result
}
