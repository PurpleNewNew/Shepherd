package process

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"codeberg.org/agnoie/shepherd/pkg/global"
	"codeberg.org/agnoie/shepherd/protocol"
)

func TestSendConnectDone(t *testing.T) {
	agent := &Agent{}
	store := global.NewStoreWithTransports(nil)
	agent.store = store
	spy := newConnectSpyConn()
	agent.session = &connectMockSession{
		conn:   spy,
		secret: "secret",
		uuid:   "AGENT-TEST",
	}

	agent.sendConnectDone(true)

	frame := <-spy.writes
	header, payload, err := decodeConnectFrame(frame, "secret")
	if err != nil {
		t.Fatalf("decode frame failed: %v", err)
	}
	if got, want := header.MessageType, uint16(protocol.CONNECTDONE); got != want {
		t.Fatalf("unexpected message type: got %d want %d", got, want)
	}
	done, ok := payload.(*protocol.ConnectDone)
	if !ok {
		t.Fatalf("expected *protocol.ConnectDone, got %T", payload)
	}
	if done.OK != 1 {
		t.Fatalf("expected OK=1, got %d", done.OK)
	}

	agent.sendConnectDone(false)
	frame = <-spy.writes
	_, payload, err = decodeConnectFrame(frame, "secret")
	if err != nil {
		t.Fatalf("decode second frame failed: %v", err)
	}
	if done, ok = payload.(*protocol.ConnectDone); !ok {
		t.Fatalf("expected *protocol.ConnectDone, got %T", payload)
	} else if done.OK != 0 {
		t.Fatalf("expected OK=0, got %d", done.OK)
	}
}

// Helpers adapted from integration shell tests.
type connectSpyConn struct {
	buf    []byte
	writes chan []byte
}

func newConnectSpyConn() *connectSpyConn { return &connectSpyConn{writes: make(chan []byte, 16)} }

func (s *connectSpyConn) Read(p []byte) (int, error) {
	if len(s.buf) == 0 {
		chunk, ok := <-s.writes
		if !ok {
			return 0, io.EOF
		}
		s.buf = append(s.buf, chunk...)
	}
	n := copy(p, s.buf)
	s.buf = s.buf[n:]
	return n, nil
}

func (s *connectSpyConn) Write(p []byte) (int, error) {
	s.writes <- append([]byte(nil), p...)
	return len(p), nil
}

func (s *connectSpyConn) Close() error                     { close(s.writes); return nil }
func (s *connectSpyConn) LocalAddr() net.Addr              { return nil }
func (s *connectSpyConn) RemoteAddr() net.Addr             { return nil }
func (s *connectSpyConn) SetDeadline(time.Time) error      { return nil }
func (s *connectSpyConn) SetReadDeadline(time.Time) error  { return nil }
func (s *connectSpyConn) SetWriteDeadline(time.Time) error { return nil }

type connectMockSession struct {
	conn   net.Conn
	secret string
	uuid   string
}

func (m *connectMockSession) Conn() net.Conn             { return m.conn }
func (m *connectMockSession) Secret() string             { return m.secret }
func (m *connectMockSession) UUID() string               { return m.uuid }
func (m *connectMockSession) UpdateConn(conn net.Conn)   { m.conn = conn }
func (m *connectMockSession) ProtocolVersion() uint16    { return protocol.CurrentProtocolVersion }
func (m *connectMockSession) ProtocolFlags() uint16      { return protocol.DefaultProtocolFlags }
func (m *connectMockSession) SetProtocol(uint16, uint16) {}

func decodeConnectFrame(frame []byte, secret string) (*protocol.Header, interface{}, error) {
	conn := &connectBufferConn{Reader: bytes.NewReader(frame)}
	msg := protocol.NewUpMsg(conn, secret, protocol.ADMIN_UUID)
	protocol.SetMessageMeta(msg, protocol.CurrentProtocolVersion, protocol.DefaultProtocolFlags)
	header, payload, err := protocol.DestructMessage(msg)
	if err != nil {
		return nil, nil, err
	}
	return header, payload, nil
}

type connectBufferConn struct{ *bytes.Reader }

func (b *connectBufferConn) Write([]byte) (int, error)        { return 0, fmt.Errorf("write unsupported") }
func (b *connectBufferConn) Close() error                     { return nil }
func (b *connectBufferConn) LocalAddr() net.Addr              { return nil }
func (b *connectBufferConn) RemoteAddr() net.Addr             { return nil }
func (b *connectBufferConn) SetDeadline(time.Time) error      { return nil }
func (b *connectBufferConn) SetReadDeadline(time.Time) error  { return nil }
func (b *connectBufferConn) SetWriteDeadline(time.Time) error { return nil }
