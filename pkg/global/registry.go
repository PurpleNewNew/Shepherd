package global

import (
	"net"
	"sync"

	"codeberg.org/agnoie/shepherd/pkg/session"
	"codeberg.org/agnoie/shepherd/pkg/utils"
	"codeberg.org/agnoie/shepherd/protocol"
)

// componentStore 负责维护 UUID 与组件之间的关联关系。
type componentStore struct {
	*protocol.MessageComponent
	mu       sync.RWMutex
	sessions map[string]*protocol.MessageComponent
	activeID string
	primary  string
	actives  map[string]struct{}
}

func newComponentStore() *componentStore {
	return &componentStore{
		sessions: make(map[string]*protocol.MessageComponent),
		actives:  make(map[string]struct{}),
	}
}

func (cs *componentStore) register(uuid string, conn net.Conn, secret, upstream, downstream string, setActive bool) {
	if cs == nil {
		return
	}
	cs.mu.Lock()
	defer cs.mu.Unlock()

	safeConn := utils.NewSafeConn(conn)
	component := &protocol.MessageComponent{
		Secret:     secret,
		Conn:       safeConn,
		UUID:       uuid,
		Flags:      0,
		Upstream:   upstream,
		Downstream: downstream,
	}
	if cs.sessions == nil {
		cs.sessions = make(map[string]*protocol.MessageComponent)
	}
	cs.sessions[uuid] = component
	if setActive || cs.MessageComponent == nil {
		cs.activateInternal(uuid, setActive)
	}
}

func (cs *componentStore) updateConn(conn net.Conn) {
	if cs == nil {
		return
	}
	cs.mu.Lock()
	defer cs.mu.Unlock()

	safeConn := utils.NewSafeConn(conn)

	if cs.MessageComponent != nil {
		cs.MessageComponent.Conn = safeConn
	}
	if cs.activeID != "" {
		if session, ok := cs.sessions[cs.activeID]; ok {
			session.Conn = safeConn
		}
	}
}

func (cs *componentStore) updateConnFor(uuid string, conn net.Conn) {
	if cs == nil {
		return
	}
	cs.mu.Lock()
	defer cs.mu.Unlock()
	safeConn := utils.NewSafeConn(conn)
	if session, ok := cs.sessions[uuid]; ok {
		session.Conn = safeConn
		if cs.activeID == uuid {
			cs.MessageComponent = session
		}
	}
}

func (cs *componentStore) get(uuid string) (*protocol.MessageComponent, bool) {
	if cs == nil {
		return nil, false
	}
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	session, ok := cs.sessions[uuid]
	return session, ok
}

func (cs *componentStore) list() []string {
	if cs == nil {
		return nil
	}
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	if len(cs.sessions) == 0 {
		return nil
	}
	ids := make([]string, 0, len(cs.sessions))
	for id := range cs.sessions {
		ids = append(ids, id)
	}
	return ids
}

func (cs *componentStore) activeUUID() string {
	if cs == nil {
		return ""
	}
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	if cs.primary != "" {
		return cs.primary
	}
	return cs.activeID
}

func (cs *componentStore) remove(uuid string) {
	if cs == nil {
		return
	}
	cs.mu.Lock()
	defer cs.mu.Unlock()
	delete(cs.sessions, uuid)
	delete(cs.actives, uuid)
	if cs.activeID == uuid {
		cs.MessageComponent = nil
		cs.activeID = ""
	}
	if cs.primary == uuid {
		cs.primary = ""
		for candidate := range cs.actives {
			if session, ok := cs.sessions[candidate]; ok {
				cs.primary = candidate
				cs.MessageComponent = session
				cs.activeID = candidate
				break
			}
		}
	}
}

func (cs *componentStore) setProtocolFlags(uuid string, flags uint16) {
	if cs == nil {
		return
	}
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if session, ok := cs.sessions[uuid]; ok {
		session.Flags = flags
		if cs.activeID == uuid {
			cs.MessageComponent = session
		}
	}
}

func (cs *componentStore) protocolFlags(uuid string) (uint16, bool) {
	if cs == nil {
		return 0, false
	}
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	if session, ok := cs.sessions[uuid]; ok {
		return session.Flags, true
	}
	return 0, false
}

func (cs *componentStore) session(uuid string) session.Session {
	if cs == nil || uuid == "" {
		return nil
	}
	return &componentSession{store: cs, uuid: uuid}
}

func (cs *componentStore) activateInternal(uuid string, primary bool) bool {
	if cs == nil {
		return false
	}
	if cs.sessions == nil {
		return false
	}
	session, ok := cs.sessions[uuid]
	if !ok {
		return false
	}
	if cs.actives == nil {
		cs.actives = make(map[string]struct{})
	}
	cs.actives[uuid] = struct{}{}
	if primary || cs.primary == "" {
		cs.primary = uuid
		cs.MessageComponent = session
		cs.activeID = uuid
	}
	return true
}

func (cs *componentStore) deactivate(uuid string) bool {
	if cs == nil || uuid == "" {
		return false
	}
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if _, ok := cs.actives[uuid]; !ok {
		return false
	}
	delete(cs.actives, uuid)
	if cs.primary == uuid {
		cs.primary = ""
		cs.MessageComponent = nil
		cs.activeID = ""
		for candidate := range cs.actives {
			cs.primary = candidate
			if session, ok := cs.sessions[candidate]; ok {
				cs.MessageComponent = session
				cs.activeID = candidate
				break
			}
		}
	}
	return true
}

func (cs *componentStore) activate(uuid string) bool {
	if cs == nil {
		return false
	}
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.activateInternal(uuid, true)
}

func (cs *componentStore) activeUUIDs() []string {
	if cs == nil {
		return nil
	}
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	if len(cs.actives) == 0 {
		return nil
	}
	result := make([]string, 0, len(cs.actives))
	for id := range cs.actives {
		result = append(result, id)
	}
	return result
}
