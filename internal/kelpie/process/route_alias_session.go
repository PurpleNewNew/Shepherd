package process

import (
	"net"
	"sync"

	"codeberg.org/agnoie/shepherd/pkg/session"
)

type routeAliasSession struct {
	mu     sync.RWMutex
	conn   net.Conn
	secret string
	uuid   string
	flags  uint16
}

func newRouteAliasSession(sess session.Session) session.Session {
	if sess == nil {
		return nil
	}
	return &routeAliasSession{
		conn:   sess.Conn(),
		secret: sess.Secret(),
		uuid:   sess.UUID(),
		flags:  sess.ProtocolFlags(),
	}
}

func (s *routeAliasSession) Conn() net.Conn {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.conn
}

func (s *routeAliasSession) Secret() string {
	if s == nil {
		return ""
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.secret
}

func (s *routeAliasSession) UUID() string {
	if s == nil {
		return ""
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.uuid
}

func (s *routeAliasSession) UpdateConn(conn net.Conn) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.conn = conn
	s.mu.Unlock()
}

func (s *routeAliasSession) ProtocolFlags() uint16 {
	if s == nil {
		return 0
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.flags
}

func (s *routeAliasSession) SetProtocolFlags(flags uint16) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.flags = flags
	s.mu.Unlock()
}
