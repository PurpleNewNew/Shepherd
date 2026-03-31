package process

import (
	"sync"

	"codeberg.org/agnoie/shepherd/internal/kelpie/topology"
	"codeberg.org/agnoie/shepherd/pkg/global"
	"codeberg.org/agnoie/shepherd/pkg/session"
)

type sessionRegistry struct {
	store *global.Store
	topo  *topology.Topology

	mu       sync.RWMutex
	sessions map[string]session.Session

	storeObserverOff func()

	callbackMu      sync.Mutex
	primaryCallback func(session.Session)
	lastPrimary     session.Session
}

func newSessionRegistry(store *global.Store, topo *topology.Topology) *sessionRegistry {
	reg := &sessionRegistry{
		store:    store,
		topo:     topo,
		sessions: make(map[string]session.Session),
	}
	reg.attachStoreObserver()
	return reg
}

func (r *sessionRegistry) attachStoreObserver() {
	if r == nil || r.store == nil {
		return
	}
	observer := &sessionStoreObserver{registry: r}
	r.storeObserverOff = r.store.RegisterObserver(observer)
	for _, uuid := range r.store.ListComponents() {
		observer.OnSessionAdded(uuid)
	}
	for _, uuid := range r.store.ActiveUUIDs() {
		observer.OnSessionActivated(uuid)
	}
}

func (r *sessionRegistry) set(uuid string, sess session.Session) {
	if r == nil || uuid == "" || sess == nil {
		return
	}
	r.mu.Lock()
	r.sessions[uuid] = sess
	r.mu.Unlock()
	r.notifyPrimaryChange()
}

func (r *sessionRegistry) remove(uuid string) {
	if r == nil || uuid == "" {
		return
	}
	r.mu.Lock()
	delete(r.sessions, uuid)
	r.mu.Unlock()
	r.notifyPrimaryChange()
}

func (r *sessionRegistry) primary() session.Session {
	if r == nil {
		return nil
	}
	if r.store != nil {
		if sess := r.store.ActiveSession(); sess != nil {
			return sess
		}
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, sess := range r.sessions {
		if sess != nil {
			return sess
		}
	}
	return nil
}

func (r *sessionRegistry) sessionForComponent(uuid string) session.Session {
	if r == nil || uuid == "" {
		return nil
	}
	if r.store != nil {
		if sess := r.store.SessionFor(uuid); sess != nil {
			return sess
		}
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.sessions[uuid]
}

func (r *sessionRegistry) SetPrimaryCallback(cb func(session.Session)) {
	if r == nil {
		return
	}
	r.callbackMu.Lock()
	r.primaryCallback = cb
	r.callbackMu.Unlock()
	r.notifyPrimaryChange()
}

func (r *sessionRegistry) notifyPrimaryChange() {
	if r == nil {
		return
	}
	primary := r.primary()
	r.callbackMu.Lock()
	if r.lastPrimary == primary {
		r.callbackMu.Unlock()
		return
	}
	r.lastPrimary = primary
	cb := r.primaryCallback
	r.callbackMu.Unlock()
	if cb != nil {
		cb(primary)
	}
}

func (r *sessionRegistry) stop() {
	if r == nil {
		return
	}
	if r.storeObserverOff != nil {
		r.storeObserverOff()
		r.storeObserverOff = nil
	}
}

type sessionStoreObserver struct {
	registry *sessionRegistry
}

func (o *sessionStoreObserver) OnSessionAdded(uuid string) {
	if o == nil || o.registry == nil || uuid == "" {
		return
	}
	if sess := o.registry.sessionForComponent(uuid); sess != nil {
		o.registry.set(uuid, sess)
	}
}

func (o *sessionStoreObserver) OnSessionRemoved(uuid string) {
	if o == nil || o.registry == nil || uuid == "" {
		return
	}
	o.registry.remove(uuid)
}

func (o *sessionStoreObserver) OnSessionActivated(uuid string) {
	if o == nil || o.registry == nil || uuid == "" {
		return
	}
	if sess := o.registry.sessionForComponent(uuid); sess != nil {
		o.registry.set(uuid, sess)
	}
}

func (o *sessionStoreObserver) OnSessionDeactivated(uuid string) {
	if o == nil || o.registry == nil || uuid == "" {
		return
	}
	o.registry.remove(uuid)
}

// 编译期校验接口实现。
var _ global.StoreObserver = (*sessionStoreObserver)(nil)
