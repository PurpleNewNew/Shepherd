package process

import (
	"net"

	"codeberg.org/agnoie/shepherd/protocol"
)

type testSession struct {
	conn net.Conn
	uuid string
}

func newTestSession(uuid string) (*testSession, net.Conn) {
	server, client := net.Pipe()
	return &testSession{conn: server, uuid: uuid}, client
}

func (s *testSession) Conn() net.Conn { return s.conn }

func (s *testSession) Secret() string { return "secret" }

func (s *testSession) UUID() string { return s.uuid }

func (s *testSession) UpdateConn(conn net.Conn) { s.conn = conn }

func (s *testSession) ProtocolVersion() uint16 { return protocol.CurrentProtocolVersion }

func (s *testSession) ProtocolFlags() uint16 { return protocol.DefaultProtocolFlags }

func (s *testSession) SetProtocol(uint16, uint16) {}
