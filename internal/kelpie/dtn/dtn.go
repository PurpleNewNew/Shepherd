package dtn

import (
	"encoding/hex"
	"errors"
	"sort"
	"sync"
	"time"

	"codeberg.org/agnoie/shepherd/pkg/utils"
)

type Priority int

const (
	PriorityHigh Priority = iota
	PriorityNormal
	PriorityLow
)

type Config struct {
	PerNodeCapacity  int
	DefaultTTL       time.Duration
	DispatchBatch    int
	DispatchInterval time.Duration
}

func DefaultConfig() Config {
	return Config{
		// NOTE: DTN also carries STREAM_/dataplane traffic (bulk payloads via chunking).
		// A tiny per-node capacity can cause steady-state drops under normal stream windows,
		// leading to false stream timeouts even on localhost. Keep this comfortably above
		// typical stream windows to avoid "drop as flow-control".
		PerNodeCapacity:  256,
		DefaultTTL:       30 * time.Minute,
		DispatchBatch:    8,
		DispatchInterval: 5 * time.Second,
	}
}

type Bundle struct {
	ID         string
	Target     string
	Payload    []byte
	Priority   Priority
	EnqueuedAt time.Time
	HoldUntil  time.Time
	DeliverBy  time.Time
	Attempts   int
	Meta       map[string]string
}

func (b *Bundle) cloneShallow() *Bundle {
	if b == nil {
		return nil
	}
	cp := *b
	if len(b.Payload) > 0 {
		cp.Payload = append([]byte(nil), b.Payload...)
	}
	if len(b.Meta) > 0 {
		cp.Meta = make(map[string]string, len(b.Meta))
		for k, v := range b.Meta {
			cp.Meta[k] = v
		}
	}
	return &cp
}

func (b *Bundle) expired(now time.Time) bool {
	return !b.DeliverBy.IsZero() && now.After(b.DeliverBy)
}

func (b *Bundle) ready(now time.Time) bool {
	if b.expired(now) {
		return false
	}
	if b.HoldUntil.IsZero() {
		return true
	}
	return !b.HoldUntil.After(now)
}

type enqueueOptions struct {
	Priority  Priority
	TTL       time.Duration
	HoldUntil time.Time
	Meta      map[string]string
}

type EnqueueOptions func(*enqueueOptions)

func WithPriority(p Priority) EnqueueOptions {
	return func(opt *enqueueOptions) { opt.Priority = p }
}

func WithTTL(ttl time.Duration) EnqueueOptions {
	return func(opt *enqueueOptions) { opt.TTL = ttl }
}

func WithHoldUntil(ts time.Time) EnqueueOptions {
	return func(opt *enqueueOptions) { opt.HoldUntil = ts }
}

type QueueStats struct {
	Total          int
	Ready          int
	Held           int
	Capacity       int
	HighWatermark  int
	AverageWait    time.Duration
	ByPriority     map[Priority]int
	DropByPriority map[Priority]uint64
	OldestAge      time.Duration
	OldestID       string
	OldestTarget   string
	DroppedTotal   uint64
	ExpiredTotal   uint64
}

type BundleSummary struct {
	ID        string
	Target    string
	Priority  Priority
	Attempts  int
	Age       time.Duration
	HoldUntil time.Time
	DeliverBy time.Time
	Preview   string
}

type Manager struct {
	cfg     Config
	clock   func() time.Time
	mu      sync.Mutex
	queues  map[string]*nodeQueue
	persist Persistor
	// metrics
	mEnqueued      uint64
	mDropped       uint64
	mExpired       uint64
	dropByPriority map[Priority]uint64
}

type nodeQueue struct {
	items     []*Bundle
	watermark int
}

func NewManager(cfg Config) *Manager {
	if cfg.PerNodeCapacity <= 0 {
		cfg.PerNodeCapacity = DefaultConfig().PerNodeCapacity
	}
	if cfg.DefaultTTL <= 0 {
		cfg.DefaultTTL = DefaultConfig().DefaultTTL
	}
	if cfg.DispatchBatch <= 0 {
		cfg.DispatchBatch = DefaultConfig().DispatchBatch
	}
	if cfg.DispatchInterval <= 0 {
		cfg.DispatchInterval = DefaultConfig().DispatchInterval
	}
	return &Manager{
		cfg:            cfg,
		clock:          time.Now,
		queues:         make(map[string]*nodeQueue),
		dropByPriority: make(map[Priority]uint64),
	}
}

func (m *Manager) SetPersistor(p Persistor) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.persist = p
	m.mu.Unlock()
}

func (m *Manager) Config() Config {
	return m.cfg
}

func (m *Manager) Now() time.Time {
	if m.clock != nil {
		return m.clock()
	}
	return time.Now()
}

// Restore re-inserts previously persisted bundles into the in-memory queues.
//
// It is intended to be used at Kelpie startup to recover DTN state across restarts.
// Expired bundles are skipped (and deleted from persistence when available).
func (m *Manager) Restore(bundles []*Bundle) int {
	if m == nil || len(bundles) == 0 {
		return 0
	}
	now := m.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	restored := 0
	var deleteIDs []string
	for _, b := range bundles {
		if b == nil || b.ID == "" || b.Target == "" {
			continue
		}
		if b.expired(now) {
			deleteIDs = append(deleteIDs, b.ID)
			continue
		}
		q := m.ensureQueue(b.Target)
		dropped := q.enqueue(b, m.cfg.PerNodeCapacity)
		if len(dropped) > 0 {
			m.mDropped += uint64(len(dropped))
			for _, db := range dropped {
				if db == nil {
					continue
				}
				m.dropByPriority[db.Priority]++
				deleteIDs = append(deleteIDs, db.ID)
			}
		}
		restored++
	}
	if m.persist != nil && len(deleteIDs) > 0 {
		_ = m.persist.DeleteDTNBundles(deleteIDs)
	}
	return restored
}

func (m *Manager) Enqueue(target string, payload []byte, opts ...EnqueueOptions) (*Bundle, error) {
	if target == "" {
		return nil, errors.New("empty target")
	}
	opt := enqueueOptions{Priority: PriorityNormal}
	for _, fn := range opts {
		if fn != nil {
			fn(&opt)
		}
	}
	ttl := opt.TTL
	if ttl <= 0 {
		ttl = m.cfg.DefaultTTL
	}
	now := m.Now()
	bundle := &Bundle{
		ID:         utils.GenerateUUID(),
		Target:     target,
		Payload:    append([]byte(nil), payload...),
		Priority:   opt.Priority,
		EnqueuedAt: now,
		HoldUntil:  opt.HoldUntil,
		DeliverBy:  now.Add(ttl),
		Meta:       opt.Meta,
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.persist != nil {
		if err := m.persist.UpsertDTNBundle(bundle); err != nil {
			return nil, err
		}
	}
	q := m.ensureQueue(target)
	dropped := q.enqueue(bundle, m.cfg.PerNodeCapacity)
	if len(dropped) > 0 {
		m.mDropped += uint64(len(dropped))
		var dropIDs []string
		for _, db := range dropped {
			if db == nil {
				continue
			}
			m.dropByPriority[db.Priority]++
			dropIDs = append(dropIDs, db.ID)
		}
		if m.persist != nil && len(dropIDs) > 0 {
			_ = m.persist.DeleteDTNBundles(dropIDs)
		}
	}
	m.mEnqueued++
	return bundle.cloneShallow(), nil
}

func (m *Manager) Ready(now time.Time, max int) []*Bundle {
	m.mu.Lock()
	defer m.mu.Unlock()
	if max <= 0 {
		max = m.cfg.DispatchBatch
	}
	ready := make([]*Bundle, 0, max)
	var expiredIDs []string
	targets := make([]string, 0, len(m.queues))
	for target, q := range m.queues {
		if q == nil || len(q.items) == 0 {
			continue
		}
		targets = append(targets, target)
	}
	sort.Strings(targets)
	for _, target := range targets {
		q := m.queues[target]
		if q == nil || len(q.items) == 0 {
			continue
		}
		cand, expired := q.popReady(now, max-len(ready))
		m.mExpired += uint64(len(expired))
		for _, b := range expired {
			if b == nil || b.ID == "" {
				continue
			}
			expiredIDs = append(expiredIDs, b.ID)
		}
		ready = append(ready, cand...)
		if len(q.items) == 0 {
			delete(m.queues, target)
		}
		if len(ready) >= max {
			break
		}
	}
	if m.persist != nil && len(expiredIDs) > 0 {
		_ = m.persist.DeleteDTNBundles(expiredIDs)
	}
	sortBundles(ready)
	clones := make([]*Bundle, len(ready))
	for i, b := range ready {
		clones[i] = b.cloneShallow()
	}
	return clones
}

// ReadyFor 仅针对指定目标返回就绪的 bundle，并从该队列移除它们。
func (m *Manager) ReadyFor(target string, now time.Time, max int) []*Bundle {
	if m == nil || target == "" {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	q := m.queues[target]
	if q == nil {
		return nil
	}
	ready, expired := q.popReady(now, max)
	m.mExpired += uint64(len(expired))
	if m.persist != nil && len(expired) > 0 {
		ids := make([]string, 0, len(expired))
		for _, b := range expired {
			if b == nil || b.ID == "" {
				continue
			}
			ids = append(ids, b.ID)
		}
		if len(ids) > 0 {
			_ = m.persist.DeleteDTNBundles(ids)
		}
	}
	if len(q.items) == 0 {
		delete(m.queues, target)
	}
	clones := make([]*Bundle, len(ready))
	for i, b := range ready {
		clones[i] = b.cloneShallow()
	}
	return clones
}

// Metrics returns basic queue counters.
func (m *Manager) Metrics() (enqueued, dropped, expired uint64) {
	if m == nil {
		return 0, 0, 0
	}
	m.mu.Lock()
	enq := m.mEnqueued
	drp := m.mDropped
	exp := m.mExpired
	m.mu.Unlock()
	return enq, drp, exp
}

func (m *Manager) Requeue(bundle *Bundle, delay time.Duration) {
	if bundle == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	bundle.Attempts++
	if delay > 0 {
		bundle.HoldUntil = m.Now().Add(delay)
	} else {
		bundle.HoldUntil = time.Time{}
	}
	if m.persist != nil {
		_ = m.persist.UpsertDTNBundle(bundle)
	}
	q := m.ensureQueue(bundle.Target)
	dropped := q.enqueue(bundle, m.cfg.PerNodeCapacity)
	if len(dropped) > 0 {
		m.mDropped += uint64(len(dropped))
		var dropIDs []string
		for _, db := range dropped {
			if db == nil {
				continue
			}
			m.dropByPriority[db.Priority]++
			dropIDs = append(dropIDs, db.ID)
		}
		if m.persist != nil && len(dropIDs) > 0 {
			_ = m.persist.DeleteDTNBundles(dropIDs)
		}
	}
}

// RecalculateHoldForTarget sets a new HoldUntil for all non-expired bundles of a target.
// If hold.IsZero(), clears HoldUntil (ready immediately). Returns number of bundles updated.
func (m *Manager) RecalculateHoldForTarget(target string, hold time.Time) int {
	if m == nil || target == "" {
		return 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	q := m.queues[target]
	if q == nil || len(q.items) == 0 {
		return 0
	}
	now := m.Now()
	changed := 0
	var touched []*Bundle
	for _, b := range q.items {
		if b == nil || b.expired(now) {
			continue
		}
		if hold.IsZero() {
			if !b.HoldUntil.IsZero() {
				b.HoldUntil = time.Time{}
				changed++
				touched = append(touched, b)
			}
		} else {
			if !b.HoldUntil.Equal(hold) {
				b.HoldUntil = hold
				changed++
				touched = append(touched, b)
			}
		}
	}
	if m.persist != nil && len(touched) > 0 {
		for _, b := range touched {
			_ = m.persist.UpsertDTNBundle(b)
		}
	}
	return changed
}

func (m *Manager) Stats(target string) QueueStats {
	m.mu.Lock()
	defer m.mu.Unlock()
	stats := QueueStats{
		ByPriority:     make(map[Priority]int),
		DropByPriority: make(map[Priority]uint64),
		Capacity:       m.cfg.PerNodeCapacity,
	}
	now := m.Now()
	sampleCount := 0
	var totalWait time.Duration
	inspect := func(q *nodeQueue, tgt string) {
		if q == nil {
			return
		}
		for _, b := range q.items {
			if b == nil || b.expired(now) {
				continue
			}
			stats.Total++
			stats.ByPriority[b.Priority]++
			age := now.Sub(b.EnqueuedAt)
			sampleCount++
			totalWait += age
			if b.ready(now) {
				stats.Ready++
			} else if !b.HoldUntil.IsZero() && b.HoldUntil.After(now) {
				stats.Held++
			}
			if age > stats.OldestAge {
				stats.OldestAge = age
				stats.OldestID = b.ID
				stats.OldestTarget = tgt
			}
		}
		if q.watermark > stats.HighWatermark {
			stats.HighWatermark = q.watermark
		}
	}
	if target != "" {
		inspect(m.queues[target], target)
	} else {
		for tgt, q := range m.queues {
			inspect(q, tgt)
		}
	}
	if sampleCount > 0 {
		stats.AverageWait = totalWait / time.Duration(sampleCount)
	}
	for prio, val := range m.dropByPriority {
		if val == 0 {
			continue
		}
		stats.DropByPriority[prio] = val
	}
	stats.DroppedTotal = m.mDropped
	stats.ExpiredTotal = m.mExpired
	return stats
}

func (m *Manager) List(target string, limit int) []BundleSummary {
	m.mu.Lock()
	defer m.mu.Unlock()
	if limit <= 0 {
		limit = 10
	}
	var items []*Bundle
	if target != "" {
		if q, ok := m.queues[target]; ok {
			items = append(items, q.items...)
		}
	} else {
		for _, q := range m.queues {
			items = append(items, q.items...)
		}
	}
	sortBundles(items)
	if len(items) > limit {
		items = items[:limit]
	}
	now := m.Now()
	summaries := make([]BundleSummary, 0, len(items))
	for _, b := range items {
		if b == nil {
			continue
		}
		summaries = append(summaries, BundleSummary{
			ID:        b.ID,
			Target:    b.Target,
			Priority:  b.Priority,
			Attempts:  b.Attempts,
			Age:       now.Sub(b.EnqueuedAt),
			HoldUntil: b.HoldUntil,
			DeliverBy: b.DeliverBy,
			Preview:   previewPayload(b.Payload),
		})
	}
	return summaries
}

// Remove 删除队列中具有给定 ID 的 bundle。
//
// 返回：
//   - removedBundle: 被删除的 bundle（浅拷贝；用于在 ACK/诊断路径中读取 payload），若未找到则为 nil
//   - removed: 是否发生删除
func (m *Manager) Remove(id string) (*Bundle, bool) {
	if m == nil || id == "" {
		return nil, false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	removed := false
	var removedBundle *Bundle
	for tgt, q := range m.queues {
		if q == nil || len(q.items) == 0 {
			continue
		}
		keep := make([]*Bundle, 0, len(q.items))
		for _, b := range q.items {
			if b == nil {
				continue
			}
			if b.ID == id {
				removed = true
				if removedBundle == nil {
					removedBundle = b.cloneShallow()
				}
				continue
			}
			keep = append(keep, b)
		}
		q.items = keep
		if len(q.items) == 0 {
			delete(m.queues, tgt)
		}
	}
	if m.persist != nil {
		_ = m.persist.DeleteDTNBundle(id)
	}
	return removedBundle, removed
}

func (m *Manager) ensureQueue(target string) *nodeQueue {
	if q, ok := m.queues[target]; ok && q != nil {
		return q
	}
	q := &nodeQueue{}
	m.queues[target] = q
	return q
}

func (q *nodeQueue) enqueue(bundle *Bundle, capacity int) []*Bundle {
	if bundle == nil {
		return nil
	}
	q.items = append(q.items, bundle)
	if len(q.items) > q.watermark {
		q.watermark = len(q.items)
	}
	if capacity > 0 && len(q.items) > capacity {
		return q.dropForCapacity(capacity)
	}
	return nil
}

func (q *nodeQueue) popReady(now time.Time, max int) ([]*Bundle, []*Bundle) {
	if q == nil || len(q.items) == 0 {
		return nil, nil
	}
	ready := make([]*Bundle, 0, max)
	keep := make([]*Bundle, 0, len(q.items))
	expired := make([]*Bundle, 0)
	for _, b := range q.items {
		if b == nil {
			continue
		}
		if b.expired(now) {
			expired = append(expired, b)
			continue
		}
		if !b.ready(now) || (max > 0 && len(ready) >= max) {
			keep = append(keep, b)
			continue
		}
		ready = append(ready, b)
	}
	q.items = keep
	return ready, expired
}

func (q *nodeQueue) dropForCapacity(capacity int) []*Bundle {
	excess := len(q.items) - capacity
	if excess <= 0 {
		return nil
	}
	dropped := make([]*Bundle, 0, excess)
	for i := 0; i < excess; i++ {
		idx := q.findDropIndex()
		if idx < 0 || idx >= len(q.items) {
			break
		}
		dropped = append(dropped, q.items[idx])
		q.items = append(q.items[:idx], q.items[idx+1:]...)
	}
	return dropped
}

func (q *nodeQueue) findDropIndex() int {
	if q == nil || len(q.items) == 0 {
		return -1
	}
	idx := 0
	var worst *Bundle
	for i, b := range q.items {
		if b == nil {
			return i
		}
		if worst == nil {
			worst = b
			idx = i
			continue
		}
		if b.Priority > worst.Priority {
			worst = b
			idx = i
			continue
		}
		if b.Priority == worst.Priority && b.EnqueuedAt.Before(worst.EnqueuedAt) {
			worst = b
			idx = i
		}
	}
	return idx
}

func sortBundles(items []*Bundle) {
	sort.SliceStable(items, func(i, j int) bool {
		bi := items[i]
		bj := items[j]
		if bi == nil || bj == nil {
			return false
		}
		if bi.Priority != bj.Priority {
			return bi.Priority < bj.Priority
		}
		if !bi.EnqueuedAt.Equal(bj.EnqueuedAt) {
			return bi.EnqueuedAt.Before(bj.EnqueuedAt)
		}
		if bi.ID != bj.ID {
			return bi.ID < bj.ID
		}
		return false
	})
}

func previewPayload(data []byte) string {
	if len(data) == 0 {
		return ""
	}
	const maxBytes = 24
	n := len(data)
	if n > maxBytes {
		n = maxBytes
	}
	preview := hex.EncodeToString(data[:n])
	if len(data) > maxBytes {
		preview += "..."
	}
	return preview
}
