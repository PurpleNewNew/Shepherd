package global

import (
	"net"

	"codeberg.org/agnoie/shepherd/protocol"
)

type componentSession struct {
	store *componentStore
	uuid  string
}

func (s *componentSession) component() *protocol.MessageComponent {
	if s == nil || s.store == nil {
		return nil
	}
	s.store.mu.RLock()
	defer s.store.mu.RUnlock()
	if s.store.sessions == nil {
		return nil
	}
	if comp, ok := s.store.sessions[s.uuid]; ok {
		return comp
	}
	return nil
}

func (s *componentSession) Conn() net.Conn {
	if comp := s.component(); comp != nil {
		return comp.Conn
	}
	return nil
}

func (s *componentSession) Secret() string {
	if comp := s.component(); comp != nil {
		return comp.Secret
	}
	return ""
}

func (s *componentSession) UUID() string {
	if s == nil {
		return ""
	}
	if s.uuid != "" {
		return s.uuid
	}
	if comp := s.component(); comp != nil {
		return comp.UUID
	}
	return ""
}

func (s *componentSession) UpdateConn(conn net.Conn) {
	if s == nil || s.store == nil || s.uuid == "" {
		return
	}
	s.store.updateConnFor(s.uuid, conn)
}

func (s *componentSession) ProtocolVersion() uint16 {
	if comp := s.component(); comp != nil {
		return comp.Version
	}
	return 0
}

func (s *componentSession) ProtocolFlags() uint16 {
	if comp := s.component(); comp != nil {
		return comp.Flags
	}
	return 0
}

func (s *componentSession) SetProtocol(version, flags uint16) {
	if s == nil || s.store == nil || s.uuid == "" {
		return
	}
	s.store.setProtocol(s.uuid, version, flags)
}
