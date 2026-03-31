package process

import "net"

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

func (s *testSession) ProtocolFlags() uint16 { return 0 }

func (s *testSession) SetProtocolFlags(uint16) {}
