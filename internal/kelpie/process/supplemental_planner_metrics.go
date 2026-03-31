package process

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"slices"

	"codeberg.org/agnoie/shepherd/internal/kelpie/initial"
	"codeberg.org/agnoie/shepherd/internal/kelpie/topology"
)

func newNodeQualityState() *nodeQualityState {
	return &nodeQualityState{
		healthEMA:    -1,
		latencyEMA:   -1,
		failEMA:      -1,
		queueEMA:     -1,
		stalenessEMA: -1,
	}
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

func (p *SupplementalPlanner) noteQueueLength(length int) {
	p.metricsMu.Lock()
	if length > p.metrics.queueHigh {
		p.metrics.queueHigh = length
	}
	p.persistMetricsLocked(&p.metrics)
	p.metricsMu.Unlock()
}

// RepairStatuses 返回当前已排入 repair 计划的节点快照。
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
