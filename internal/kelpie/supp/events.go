package supp

import (
	"sync"
	"time"
)

// SuppLinkHealth 表示补链终端的健康状态。
type SuppLinkHealth int

const (
	SuppLinkHealthUnknown SuppLinkHealth = iota
	SuppLinkHealthAlive
	SuppLinkHealthFailed
)

// String 返回 SuppLinkHealth 的文本表示。
func (h SuppLinkHealth) String() string {
	switch h {
	case SuppLinkHealthAlive:
		return "alive"
	case SuppLinkHealthFailed:
		return "failed"
	default:
		return "unknown"
	}
}

type hookRegistry[T any] struct {
	mu    sync.RWMutex
	next  uint64
	hooks map[uint64]T
}

func newHookRegistry[T any]() *hookRegistry[T] {
	return &hookRegistry[T]{hooks: make(map[uint64]T)}
}

func (r *hookRegistry[T]) add(h T) func() {
	if r == nil {
		return func() {}
	}
	r.mu.Lock()
	id := r.next
	r.next++
	if r.hooks == nil {
		r.hooks = make(map[uint64]T)
	}
	r.hooks[id] = h
	r.mu.Unlock()
	return func() {
		r.mu.Lock()
		delete(r.hooks, id)
		r.mu.Unlock()
	}
}

func (r *hookRegistry[T]) snapshot() []T {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.hooks) == 0 {
		return nil
	}
	out := make([]T, 0, len(r.hooks))
	for _, hook := range r.hooks {
		out = append(out, hook)
	}
	return out
}

var (
	suppFailoverHooks     = newHookRegistry[func(string)]()
	nodeAddedHooks        = newHookRegistry[func(string)]()
	nodeRemovedHooks      = newHookRegistry[func(string)]()
	suppLinkFailedHooks   = newHookRegistry[func(string, []string)]()
	suppLinkRetiredHooks  = newHookRegistry[func(string, []string, string)]()
	suppLinkPromotedHooks = newHookRegistry[func(string, string, string)]()
	suppHeartbeatHooks    = newHookRegistry[func(string, string, SuppLinkHealth, time.Time)]()

	pendingNodeAdds   []string
	pendingNodeAddsMu sync.Mutex
)

// RegisterSuppFailoverHook 注册补链节点失效的回调。
func RegisterSuppFailoverHook(hook func(string)) func() {
	if hook == nil {
		return func() {}
	}
	return suppFailoverHooks.add(hook)
}

// PublishSuppFailover 触发补链节点失效事件。
func PublishSuppFailover(uuid string) {
	if uuid == "" {
		return
	}
	for _, hook := range suppFailoverHooks.snapshot() {
		if hook == nil {
			continue
		}
		go hook(uuid)
	}
}

// RegisterNodeAddedHook 注册 Kelpie 拓扑新增节点事件。
func RegisterNodeAddedHook(hook func(string)) func() {
	if hook == nil {
		return func() {}
	}
	cancel := nodeAddedHooks.add(hook)
	pendingNodeAddsMu.Lock()
	queued := append([]string(nil), pendingNodeAdds...)
	pendingNodeAdds = nil
	pendingNodeAddsMu.Unlock()
	for _, uuid := range queued {
		if uuid == "" {
			continue
		}
		go hook(uuid)
	}
	return cancel
}

// PublishNodeAdded 通知拓扑节点已上线。
func PublishNodeAdded(uuid string) {
	if uuid == "" {
		return
	}
	hooks := nodeAddedHooks.snapshot()
	if len(hooks) == 0 {
		pendingNodeAddsMu.Lock()
		pendingNodeAdds = append(pendingNodeAdds, uuid)
		pendingNodeAddsMu.Unlock()
		return
	}
	for _, hook := range hooks {
		if hook == nil {
			continue
		}
		go hook(uuid)
	}
}

// RegisterSuppLinkFailedHook 注册补链链路失败事件。
func RegisterSuppLinkFailedHook(hook func(string, []string)) func() {
	if hook == nil {
		return func() {}
	}
	return suppLinkFailedHooks.add(hook)
}

// PublishSuppLinkFailed 广播补链失败。
func PublishSuppLinkFailed(linkUUID string, endpoints []string) {
	if linkUUID == "" {
		return
	}
	for _, hook := range suppLinkFailedHooks.snapshot() {
		if hook == nil {
			continue
		}
		go hook(linkUUID, append([]string(nil), endpoints...))
	}
}

// RegisterSuppLinkRetiredHook 注册补链退役事件。
func RegisterSuppLinkRetiredHook(hook func(string, []string, string)) func() {
	if hook == nil {
		return func() {}
	}
	return suppLinkRetiredHooks.add(hook)
}

// PublishSuppLinkRetired 广播补链退役。
func PublishSuppLinkRetired(linkUUID string, endpoints []string, reason string) {
	if linkUUID == "" {
		return
	}
	for _, hook := range suppLinkRetiredHooks.snapshot() {
		if hook == nil {
			continue
		}
		go hook(linkUUID, append([]string(nil), endpoints...), reason)
	}
}

// RegisterSuppLinkPromotedHook 注册补链晋升事件。
func RegisterSuppLinkPromotedHook(hook func(string, string, string)) func() {
	if hook == nil {
		return func() {}
	}
	return suppLinkPromotedHooks.add(hook)
}

// PublishSuppLinkPromoted 广播补链晋升为树边。
func PublishSuppLinkPromoted(linkUUID, parentUUID, childUUID string) {
	if linkUUID == "" {
		return
	}
	for _, hook := range suppLinkPromotedHooks.snapshot() {
		if hook == nil {
			continue
		}
		go hook(linkUUID, parentUUID, childUUID)
	}
}

// RegisterSuppHeartbeatHook 注册补链心跳事件。
func RegisterSuppHeartbeatHook(hook func(string, string, SuppLinkHealth, time.Time)) func() {
	if hook == nil {
		return func() {}
	}
	return suppHeartbeatHooks.add(hook)
}

// PublishSuppHeartbeat 广播补链节点心跳。
func PublishSuppHeartbeat(linkUUID, endpoint string, status SuppLinkHealth, ts time.Time) {
	if linkUUID == "" || endpoint == "" {
		return
	}
	for _, hook := range suppHeartbeatHooks.snapshot() {
		if hook == nil {
			continue
		}
		go hook(linkUUID, endpoint, status, ts)
	}
}

// RegisterNodeRemovedHook 注册节点离线事件。
func RegisterNodeRemovedHook(hook func(string)) func() {
	if hook == nil {
		return func() {}
	}
	return nodeRemovedHooks.add(hook)
}

// PublishNodeRemoved 广播节点离线。
func PublishNodeRemoved(uuid string) {
	if uuid == "" {
		return
	}
	for _, hook := range nodeRemovedHooks.snapshot() {
		if hook == nil {
			continue
		}
		go hook(uuid)
	}
}
