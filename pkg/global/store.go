package global

import (
	"fmt"
	"net"
	"sync"

	"codeberg.org/agnoie/shepherd/pkg/session"
	"codeberg.org/agnoie/shepherd/protocol"
)

// Store 保存运行期连接 / 会话状态
type Store struct {
	components *componentStore

	tlsMu      sync.RWMutex
	tlsEnabled bool

	tokenMu      sync.RWMutex
	preAuthToken string

	observersMu sync.RWMutex
	observers   map[uint64]StoreObserver
	nextObsID   uint64

	transports *protocol.Transports
}

type StoreObserver interface {
	OnSessionAdded(uuid string)
	OnSessionRemoved(uuid string)
	OnSessionActivated(uuid string)
	OnSessionDeactivated(uuid string)
}

func NewStoreWithTransports(transports *protocol.Transports) *Store {
	if transports == nil {
		transports = protocol.DefaultTransports()
	}
	return &Store{
		components: newComponentStore(),
		observers:  make(map[uint64]StoreObserver),
		transports: transports,
	}
}

func (s *Store) SetTLSEnabled(enabled bool) {
	if s == nil {
		return
	}
	s.tlsMu.Lock()
	s.tlsEnabled = enabled
	s.tlsMu.Unlock()
}

func (s *Store) TLSEnabled() bool {
	if s == nil {
		return false
	}
	s.tlsMu.RLock()
	enabled := s.tlsEnabled
	s.tlsMu.RUnlock()
	return enabled
}

func (s *Store) SetPreAuthToken(token string) error {
	if s == nil {
		return fmt.Errorf("global.Store: nil store")
	}
	if token == "" {
		return fmt.Errorf("global.Store: pre-auth token must not be empty")
	}
	s.tokenMu.Lock()
	s.preAuthToken = token
	s.tokenMu.Unlock()
	return nil
}

func (s *Store) PreAuthToken() string {
	if s == nil {
		return ""
	}
	s.tokenMu.RLock()
	token := s.preAuthToken
	s.tokenMu.RUnlock()
	return token
}

func (s *Store) componentsStore() *componentStore {
	if s.components == nil {
		s.components = newComponentStore()
	}
	return s.components
}

func (s *Store) notify(fn func(StoreObserver)) {
	if s == nil || fn == nil {
		return
	}
	s.observersMu.RLock()
	defer s.observersMu.RUnlock()
	for _, obs := range s.observers {
		if obs != nil {
			fn(obs)
		}
	}
}

func (s *Store) InitializeComponent(conn net.Conn, secret, uuid, upstream, downstream string) {
	if s == nil {
		return
	}
	if uuid == "" {
		uuid = protocol.ADMIN_UUID
	}
	s.componentsStore().register(uuid, conn, secret, upstream, downstream, true)
	if s.transports != nil {
		s.transports.SetComponentTransports(uuid, upstream, downstream)
	}
	s.notify(func(obs StoreObserver) { obs.OnSessionAdded(uuid); obs.OnSessionActivated(uuid) })
}

func (s *Store) RegisterComponent(conn net.Conn, secret, uuid, upstream, downstream string) {
	if s == nil || uuid == "" {
		return
	}
	s.componentsStore().register(uuid, conn, secret, upstream, downstream, false)
	if s.transports != nil {
		s.transports.SetComponentTransports(uuid, upstream, downstream)
	}
	s.notify(func(obs StoreObserver) { obs.OnSessionAdded(uuid) })
}

func (s *Store) UpdateActiveConn(conn net.Conn) {
	if s != nil {
		s.componentsStore().updateConn(conn)
	}
}

func (s *Store) UpdateProtocolFlags(uuid string, flags uint16) {
	if s == nil {
		return
	}
	if uuid == "" {
		uuid = s.ActiveUUID()
	}
	if uuid == "" {
		return
	}
	s.componentsStore().setProtocolFlags(uuid, flags)
}

func (s *Store) ProtocolFlagsFor(uuid string) (uint16, bool) {
	if s == nil {
		return 0, false
	}
	return s.componentsStore().protocolFlags(uuid)
}
func (s *Store) ActiveProtocolFlags() uint16 {
	if s == nil {
		return 0
	}
	return s.componentsStore().activeProtocolFlags()
}

func (s *Store) ConfigureMessage(msg protocol.Message, uuid string) {
	if s == nil || msg == nil {
		return
	}
	flags, ok := s.ProtocolFlagsFor(uuid)
	if !ok && uuid == "" {
		flags = s.ActiveProtocolFlags()
	}
	protocol.SetMessageMeta(msg, flags)
}

func (s *Store) NewDownMsg(conn net.Conn, secret, uuid string) protocol.Message {
	transport := s.transports
	if transport == nil {
		transport = protocol.DefaultTransports()
	}
	msg := transport.NewDownMsg(conn, secret, uuid)
	s.ConfigureMessage(msg, uuid)
	return msg
}
func (s *Store) NewUpMsg(conn net.Conn, secret, uuid string) protocol.Message {
	transport := s.transports
	if transport == nil {
		transport = protocol.DefaultTransports()
	}
	msg := transport.NewUpMsg(conn, secret, uuid)
	s.ConfigureMessage(msg, uuid)
	return msg
}

func (s *Store) ActivateComponent(uuid string) bool {
	if s == nil {
		return false
	}
	if s.componentsStore().activate(uuid) {
		s.notify(func(obs StoreObserver) { obs.OnSessionActivated(uuid) })
		return true
	}
	return false
}

func (s *Store) DeactivateComponent(uuid string) bool {
	if s == nil || uuid == "" {
		return false
	}
	if s.componentsStore().deactivate(uuid) {
		s.notify(func(obs StoreObserver) { obs.OnSessionDeactivated(uuid) })
		return true
	}
	return false
}

func (s *Store) Component(uuid string) (*protocol.MessageComponent, bool) {
	if s == nil {
		return nil, false
	}
	return s.componentsStore().get(uuid)
}
func (s *Store) ActiveUUID() string {
	if s == nil {
		return ""
	}
	return s.componentsStore().activeUUID()
}
func (s *Store) ActiveUUIDs() []string {
	if s == nil {
		return nil
	}
	return s.componentsStore().activeUUIDs()
}

func (s *Store) ActiveConn() net.Conn {
	if sess := s.ActiveSession(); sess != nil {
		return sess.Conn()
	}
	return nil
}
func (s *Store) ActiveSecret() string {
	if sess := s.ActiveSession(); sess != nil {
		return sess.Secret()
	}
	return ""
}
func (s *Store) ListComponents() []string {
	if s == nil {
		return nil
	}
	return s.componentsStore().list()
}

func (s *Store) ActiveSession() session.Session {
	if s == nil {
		return nil
	}
	id := s.componentsStore().activeUUID()
	if id == "" {
		return nil
	}
	return s.componentsStore().session(id)
}

func (s *Store) ActiveSessions() []session.Session {
	if s == nil {
		return nil
	}
	uuids := s.ActiveUUIDs()
	if len(uuids) == 0 {
		return nil
	}
	sessions := make([]session.Session, 0, len(uuids))
	for _, id := range uuids {
		if sess := s.componentsStore().session(id); sess != nil {
			sessions = append(sessions, sess)
		}
	}
	return sessions
}

func (s *Store) SessionFor(uuid string) session.Session {
	if s == nil || uuid == "" {
		return nil
	}
	if _, ok := s.Component(uuid); !ok {
		return nil
	}
	return s.componentsStore().session(uuid)
}

func (s *Store) Reset() {
	if s == nil {
		return
	}
	uuids := s.ListComponents()
	s.components = newComponentStore()
	for _, uuid := range uuids {
		if s.transports != nil {
			s.transports.ClearComponentTransports(uuid)
		}
		s.notify(func(obs StoreObserver) { obs.OnSessionDeactivated(uuid); obs.OnSessionRemoved(uuid) })
	}
}

func (s *Store) RemoveComponent(uuid string) {
	if s == nil {
		return
	}
	s.componentsStore().remove(uuid)
	if s.transports != nil {
		s.transports.ClearComponentTransports(uuid)
	}
	s.notify(func(obs StoreObserver) { obs.OnSessionDeactivated(uuid); obs.OnSessionRemoved(uuid) })
}

func (s *Store) RegisterObserver(obs StoreObserver) func() {
	if s == nil || obs == nil {
		return func() {}
	}
	s.observersMu.Lock()
	defer s.observersMu.Unlock()
	s.nextObsID++
	id := s.nextObsID
	if s.observers == nil {
		s.observers = make(map[uint64]StoreObserver)
	}
	s.observers[id] = obs
	return func() { s.observersMu.Lock(); delete(s.observers, id); s.observersMu.Unlock() }
}
