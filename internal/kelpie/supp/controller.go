package supp

import (
	"strings"
	"sync"
	"time"
)

type SuppLinkStatus int

const (
	SuppLinkNegotiating SuppLinkStatus = iota
	SuppLinkHealthy
	SuppLinkDegraded
	SuppLinkFailoverCandidate
	SuppLinkPromoted
	SuppLinkRetired
)

type suppLinkRecord struct {
	LinkUUID       string
	Listener       string
	Dialer         string
	ListenIP       string
	ListenPort     uint16
	DialDispatched bool

	Status           SuppLinkStatus
	LastTransition   time.Time
	FailureReason    string
	Ack              map[string]bool
	LastHeartbeat    map[string]time.Time
	Health           map[string]SuppLinkHealth
	Ready            bool
	RequestedAt      time.Time
	LastStatusUpdate time.Time
}

func (r *suppLinkRecord) clone() *suppLinkRecord {
	if r == nil {
		return nil
	}
	cp := *r
	if r.Ack != nil {
		cp.Ack = make(map[string]bool, len(r.Ack))
		for k, v := range r.Ack {
			cp.Ack[k] = v
		}
	}
	if r.LastHeartbeat != nil {
		cp.LastHeartbeat = make(map[string]time.Time, len(r.LastHeartbeat))
		for k, v := range r.LastHeartbeat {
			cp.LastHeartbeat[k] = v
		}
	}
	if r.Health != nil {
		cp.Health = make(map[string]SuppLinkHealth, len(r.Health))
		for k, v := range r.Health {
			cp.Health[k] = v
		}
	}
	return &cp
}

type suppLinkController struct {
	mu    sync.RWMutex
	links map[string]*suppLinkRecord
}

var suppCtrl = newSuppLinkController()

func newSuppLinkController() *suppLinkController {
	return &suppLinkController{
		links: make(map[string]*suppLinkRecord),
	}
}

func (c *suppLinkController) register(linkUUID, listener, dialer, defaultIP string) *suppLinkRecord {
	c.mu.Lock()
	defer c.mu.Unlock()
	record := &suppLinkRecord{
		LinkUUID:       linkUUID,
		Listener:       listener,
		Dialer:         dialer,
		ListenIP:       defaultIP,
		Status:         SuppLinkNegotiating,
		RequestedAt:    time.Now(),
		LastTransition: time.Now(),
		Ack:            make(map[string]bool),
		LastHeartbeat:  make(map[string]time.Time),
		Health:         make(map[string]SuppLinkHealth),
	}
	c.links[linkUUID] = record
	return record.clone()
}

func (c *suppLinkController) get(linkUUID string) *suppLinkRecord {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cloneLocked(linkUUID)
}

func (c *suppLinkController) cloneLocked(linkUUID string) *suppLinkRecord {
	if entry, ok := c.links[linkUUID]; ok {
		return entry.clone()
	}
	return nil
}

func (c *suppLinkController) restore(record *suppLinkRecord) {
	if c == nil || record == nil || record.LinkUUID == "" {
		return
	}
	if record.Ack == nil {
		record.Ack = make(map[string]bool)
	}
	if record.LastHeartbeat == nil {
		record.LastHeartbeat = make(map[string]time.Time)
	}
	if record.Health == nil {
		record.Health = make(map[string]SuppLinkHealth)
	}
	c.mu.Lock()
	c.links[record.LinkUUID] = record.clone()
	c.mu.Unlock()
}

func (c *suppLinkController) markAck(linkUUID, agent string) (bool, *suppLinkRecord) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.links[linkUUID]
	if !ok {
		return false, nil
	}
	if entry.Ack == nil {
		entry.Ack = make(map[string]bool)
	}
	entry.Ack[agent] = true
	ready := entry.Ack[entry.Listener] && entry.Ack[entry.Dialer]
	return ready, entry.clone()
}

func (c *suppLinkController) markReady(linkUUID string) *suppLinkRecord {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.links[linkUUID]
	if !ok {
		return nil
	}
	entry.Status = SuppLinkHealthy
	entry.Ready = true
	now := time.Now()
	entry.LastTransition = now
	entry.LastStatusUpdate = now
	if entry.LastHeartbeat == nil {
		entry.LastHeartbeat = make(map[string]time.Time, 2)
	}
	if entry.Health == nil {
		entry.Health = make(map[string]SuppLinkHealth, 2)
	}
	entry.LastHeartbeat[entry.Listener] = now
	entry.LastHeartbeat[entry.Dialer] = now
	entry.Health[entry.Listener] = SuppLinkHealthAlive
	entry.Health[entry.Dialer] = SuppLinkHealthAlive
	return entry.clone()
}

func (c *suppLinkController) recordHeartbeat(linkUUID, endpoint string, ts time.Time, alive bool) (*suppLinkRecord, heartbeatEvent) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.links[linkUUID]
	if !ok {
		return nil, heartbeatIgnored
	}
	if endpoint != entry.Listener && endpoint != entry.Dialer {
		return entry.clone(), heartbeatIgnored
	}
	if entry.LastHeartbeat == nil {
		entry.LastHeartbeat = make(map[string]time.Time)
	}
	if entry.Health == nil {
		entry.Health = make(map[string]SuppLinkHealth)
	}
	if ts.IsZero() {
		ts = time.Now()
	}
	entry.LastHeartbeat[endpoint] = ts
	if !alive {
		entry.Health[endpoint] = SuppLinkHealthFailed
		if entry.Status != SuppLinkDegraded && entry.Status != SuppLinkRetired {
			entry.Status = SuppLinkDegraded
			entry.LastStatusUpdate = time.Now()
		}
		return entry.clone(), heartbeatClosed
	}
	entry.Health[endpoint] = SuppLinkHealthAlive
	if entry.Status == SuppLinkDegraded {
		entry.Status = SuppLinkHealthy
		entry.LastStatusUpdate = time.Now()
	}
	return entry.clone(), heartbeatUpdated
}

func (c *suppLinkController) markDialDispatched(linkUUID string, ip string, port uint16) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.links[linkUUID]
	if !ok {
		return false
	}
	already := entry.DialDispatched
	entry.DialDispatched = true
	if ip != "" {
		entry.ListenIP = ip
	}
	if port != 0 {
		entry.ListenPort = port
	}
	return !already
}

func (c *suppLinkController) markFailed(linkUUID, reason string) (*suppLinkRecord, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.links[linkUUID]
	if !ok {
		return nil, false
	}
	// 幂等处理：一旦链路被标记为 degraded/retired，就不要重复触发失败处理。
	// 调用方会利用 transition 标记避免反复 teardown 和日志刷屏。
	if entry.Status == SuppLinkDegraded || entry.Status == SuppLinkRetired {
		if entry.FailureReason == "" && reason != "" {
			entry.FailureReason = reason
			entry.LastStatusUpdate = time.Now()
		}
		return entry.clone(), false
	}
	entry.Status = SuppLinkDegraded
	entry.FailureReason = reason
	entry.LastStatusUpdate = time.Now()
	return entry.clone(), true
}

func (c *suppLinkController) setStatus(linkUUID string, status SuppLinkStatus, reason string) *suppLinkRecord {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.links[linkUUID]
	if !ok {
		return nil
	}
	entry.Status = status
	entry.FailureReason = reason
	entry.LastTransition = time.Now()
	entry.LastStatusUpdate = entry.LastTransition
	return entry.clone()
}

func (c *suppLinkController) snapshot() []*suppLinkRecord {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make([]*suppLinkRecord, 0, len(c.links))
	for _, entry := range c.links {
		result = append(result, entry.clone())
	}
	return result
}

func (c *suppLinkController) findByPrefix(prefix string) *suppLinkRecord {
	c.mu.RLock()
	defer c.mu.RUnlock()
	lower := strings.ToLower(prefix)
	for _, entry := range c.links {
		if strings.HasPrefix(strings.ToLower(entry.LinkUUID), lower) {
			return entry.clone()
		}
	}
	return nil
}

func (c *suppLinkController) activeEntries() []*suppLinkRecord {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make([]*suppLinkRecord, 0, len(c.links))
	for _, entry := range c.links {
		if entry.Status == SuppLinkHealthy || entry.Status == SuppLinkDegraded || entry.Status == SuppLinkFailoverCandidate {
			result = append(result, entry.clone())
		}
	}
	return result
}

func (c *suppLinkController) activeLinkCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	count := 0
	for _, entry := range c.links {
		if entry == nil {
			continue
		}
		switch entry.Status {
		case SuppLinkHealthy, SuppLinkDegraded, SuppLinkFailoverCandidate:
			count++
		}
	}
	return count
}

func (c *suppLinkController) promote(linkUUID string) *suppLinkRecord {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.links[linkUUID]
	if !ok {
		return nil
	}
	entry.Status = SuppLinkPromoted
	entry.LastStatusUpdate = time.Now()
	return entry.clone()
}
