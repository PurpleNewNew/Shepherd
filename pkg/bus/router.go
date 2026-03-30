package bus

import (
    "context"
    "errors"
    "sync"
    "sync/atomic"

    "codeberg.org/agnoie/shepherd/protocol"
)

var ErrNoHandler = errors.New("bus: no handler for message type")
var ErrBackpressure = errors.New("bus: backpressure drop")

type Handler func(ctx context.Context, header *protocol.Header, payload interface{}) error

type entry struct {
    id      uint64
    handler Handler
}

type Router struct {
    mu       sync.RWMutex
    handlers map[uint16][]entry
    nextID   uint64
    metrics  *routerMetrics
}

func NewRouter() *Router {
    return &Router{handlers: make(map[uint16][]entry), metrics: newRouterMetrics()}
}

func (r *Router) Register(msgType uint16, handler Handler) func() {
    if handler == nil {
        return func() {}
    }
    r.mu.Lock()
    r.nextID++
    id := r.nextID
    r.handlers[msgType] = append(r.handlers[msgType], entry{id: id, handler: handler})
    r.mu.Unlock()
    return func() {
        r.mu.Lock()
        defer r.mu.Unlock()
        entries := r.handlers[msgType]
        for i, e := range entries {
            if e.id == id {
                r.handlers[msgType] = append(entries[:i], entries[i+1:]...)
                break
            }
        }
        if len(r.handlers[msgType]) == 0 {
            delete(r.handlers, msgType)
        }
    }
}

func (r *Router) Dispatch(ctx context.Context, header *protocol.Header, payload interface{}) error {
    if header == nil {
        return ErrNoHandler
    }
    r.mu.RLock()
    handlers := append([]entry(nil), r.handlers[header.MessageType]...)
    r.mu.RUnlock()
    if len(handlers) == 0 {
        return ErrNoHandler
    }
    counter := r.metrics.counter(header.MessageType)
    var joined error
    for _, h := range handlers {
        if h.handler == nil {
            continue
        }
        counter.dispatched.Add(1)
        if err := h.handler(ctx, header, payload); err != nil && !errors.Is(err, context.Canceled) {
            counter.errors.Add(1)
            if joined == nil {
                joined = err
            } else {
                joined = errors.Join(joined, err)
            }
        }
    }
    return joined
}

func (r *Router) RecordDrop(msgType uint16) {
    if r == nil || r.metrics == nil {
        return
    }
    r.metrics.counter(msgType).drops.Add(1)
}

func (r *Router) Stats() map[uint16]RouterCounter {
    if r == nil || r.metrics == nil {
        return nil
    }
    return r.metrics.snapshot()
}

type routerMetrics struct {
    counters sync.Map
}

type RouterCounter struct {
    Dispatched uint64
    Errors     uint64
    Drops      uint64
}

type routerCounterAtomic struct {
    dispatched atomic.Uint64
    errors     atomic.Uint64
    drops      atomic.Uint64
}

func newRouterMetrics() *routerMetrics { return &routerMetrics{} }

func (m *routerMetrics) counter(msgType uint16) *routerCounterAtomic {
    if m == nil { return nil }
    if v, ok := m.counters.Load(msgType); ok {
        return v.(*routerCounterAtomic)
    }
    c := &routerCounterAtomic{}
    actual, _ := m.counters.LoadOrStore(msgType, c)
    return actual.(*routerCounterAtomic)
}

func (m *routerMetrics) snapshot() map[uint16]RouterCounter {
    res := make(map[uint16]RouterCounter)
    if m == nil { return res }
    m.counters.Range(func(k, v interface{}) bool {
        mt := k.(uint16)
        c := v.(*routerCounterAtomic)
        res[mt] = RouterCounter{
            Dispatched: c.dispatched.Load(),
            Errors:     c.errors.Load(),
            Drops:      c.drops.Load(),
        }
        return true
    })
    return res
}
