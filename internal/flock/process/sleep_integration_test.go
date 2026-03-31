package process

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"codeberg.org/agnoie/shepherd/internal/flock/initial"
	"codeberg.org/agnoie/shepherd/pkg/global"
	"codeberg.org/agnoie/shepherd/pkg/session"
	"codeberg.org/agnoie/shepherd/protocol"
)

func TestSleepUpdateHandlerEmitsAck(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := global.NewStoreWithTransports(nil)
	options := &initial.Options{
		PreAuthToken: "integration-token",
		SleepSeconds: 4,
		WorkSeconds:  8,
		SleepJitter:  0.15,
	}
	agent := NewAgent(ctx, options, store, nil)
	agent.UUID = "AGENT-SLEEP-A"

	spy := newSpyConn()
	mockSess := &mockSession{
		conn:   spy,
		secret: "integration-secret",
		uuid:   agent.UUID,
		flags:  protocol.DefaultProtocolFlags,
	}
	agent.BindSession(mockSess)

	handler := agent.sleepUpdateHandler()
	update := &protocol.SleepUpdate{
		Flags:          protocol.SleepUpdateFlagSleepSeconds | protocol.SleepUpdateFlagWorkSeconds | protocol.SleepUpdateFlagJitter,
		SleepSeconds:   15,
		WorkSeconds:    5,
		JitterPermille: 300,
	}
	if err := handler(context.Background(), nil, update); err != nil {
		t.Fatalf("sleep update handler returned error: %v", err)
	}

	frame := spy.expectWrite(t, time.Second)
	header, payload := decodeUpMessage(t, frame, mockSess.secret)
	if header.MessageType != uint16(protocol.SLEEP_UPDATE_ACK) {
		t.Fatalf("expected SLEEP_UPDATE_ACK, got %d", header.MessageType)
	}
	ack, ok := payload.(*protocol.SleepUpdateAck)
	if !ok {
		t.Fatalf("expected *protocol.SleepUpdateAck, got %T", payload)
	}
	if ack.OK != 1 {
		t.Fatalf("expected OK=1, got %d (err=%s)", ack.OK, ack.Error)
	}
	if agent.options.SleepSeconds != 15 || agent.options.WorkSeconds != 5 {
		t.Fatalf("agent options not updated: sleep=%d work=%d", agent.options.SleepSeconds, agent.options.WorkSeconds)
	}
}

func TestSleepUpdateHandlerAckFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := global.NewStoreWithTransports(nil)
	options := &initial.Options{
		PreAuthToken: "integration-token",
		SleepSeconds: 6,
		WorkSeconds:  9,
	}
	agent := NewAgent(ctx, options, store, nil)
	agent.UUID = "AGENT-SLEEP-B"

	spy := newSpyConn()
	mockSess := &mockSession{
		conn:   spy,
		secret: "integration-secret",
		uuid:   agent.UUID,
		flags:  protocol.DefaultProtocolFlags,
	}
	agent.BindSession(mockSess)

	handler := agent.sleepUpdateHandler()
	update := &protocol.SleepUpdate{
		Flags:        protocol.SleepUpdateFlagSleepSeconds,
		SleepSeconds: -10,
	}
	if err := handler(context.Background(), nil, update); err == nil {
		t.Fatalf("expected handler error for invalid sleep value")
	}

	frame := spy.expectWrite(t, time.Second)
	header, payload := decodeUpMessage(t, frame, mockSess.secret)
	if header.MessageType != uint16(protocol.SLEEP_UPDATE_ACK) {
		t.Fatalf("expected SLEEP_UPDATE_ACK, got %d", header.MessageType)
	}
	ack, ok := payload.(*protocol.SleepUpdateAck)
	if !ok {
		t.Fatalf("expected *protocol.SleepUpdateAck, got %T", payload)
	}
	if ack.OK != 0 || ack.Error == "" {
		t.Fatalf("expected failure ACK with message, got OK=%d err=%q", ack.OK, ack.Error)
	}
}

// --- 从集成测试初始化逻辑复制过来的辅助函数 ---

type spyConn struct {
	buf    []byte
	writes chan []byte
}

func newSpyConn() *spyConn {
	return &spyConn{writes: make(chan []byte, 8)}
}

func (s *spyConn) Read(p []byte) (int, error) {
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

func (s *spyConn) Write(p []byte) (int, error) {
	s.writes <- append([]byte(nil), p...)
	return len(p), nil
}

func (s *spyConn) Close() error {
	close(s.writes)
	return nil
}

func (s *spyConn) LocalAddr() net.Addr              { return nil }
func (s *spyConn) RemoteAddr() net.Addr             { return nil }
func (s *spyConn) SetDeadline(time.Time) error      { return nil }
func (s *spyConn) SetReadDeadline(time.Time) error  { return nil }
func (s *spyConn) SetWriteDeadline(time.Time) error { return nil }

func (s *spyConn) expectWrite(t *testing.T, timeout time.Duration) []byte {
	t.Helper()
	select {
	case data := <-s.writes:
		return data
	case <-time.After(timeout):
		t.Fatalf("timeout waiting for write")
		return nil
	}
}

type mockSession struct {
	conn   net.Conn
	secret string
	uuid   string
	flags  uint16
}

func (m *mockSession) Conn() net.Conn            { return m.conn }
func (m *mockSession) Secret() string            { return m.secret }
func (m *mockSession) UUID() string              { return m.uuid }
func (m *mockSession) UpdateConn(conn net.Conn)  { m.conn = conn }
func (m *mockSession) ProtocolFlags() uint16     { return m.flags }
func (m *mockSession) SetProtocolFlags(f uint16) { m.flags = f }

var _ session.Session = (*mockSession)(nil)

func decodeUpMessage(t *testing.T, frame []byte, secret string) (*protocol.Header, interface{}) {
	t.Helper()
	reader := &bufferConn{Reader: bytes.NewReader(frame)}
	msg := protocol.NewUpMsg(reader, secret, protocol.ADMIN_UUID)
	protocol.SetMessageMeta(msg, protocol.DefaultProtocolFlags)
	header, payload, err := protocol.DestructMessage(msg)
	if err != nil {
		t.Fatalf("failed to decode message: %v", err)
	}
	return header, payload
}

type bufferConn struct {
	*bytes.Reader
}

func (b *bufferConn) Write([]byte) (int, error)        { return 0, fmt.Errorf("write unsupported") }
func (b *bufferConn) Close() error                     { return nil }
func (b *bufferConn) LocalAddr() net.Addr              { return nil }
func (b *bufferConn) RemoteAddr() net.Addr             { return nil }
func (b *bufferConn) SetDeadline(time.Time) error      { return nil }
func (b *bufferConn) SetReadDeadline(time.Time) error  { return nil }
func (b *bufferConn) SetWriteDeadline(time.Time) error { return nil }
