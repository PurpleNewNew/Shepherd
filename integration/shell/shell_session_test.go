package shell_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"runtime"
	"strings"
	"testing"
	"time"

	agentInitial "codeberg.org/agnoie/shepherd/internal/flock/initial"
	"codeberg.org/agnoie/shepherd/internal/flock/manager"
	agentProcess "codeberg.org/agnoie/shepherd/internal/flock/process"
	"codeberg.org/agnoie/shepherd/pkg/global"
	"codeberg.org/agnoie/shepherd/protocol"
)

// 基于 STREAM_* 的 shell 集成测试。
func TestShellStreamSessionMaintainsState(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell stream integration test not supported on Windows")
	}

	prevUp, prevDown := protocol.DefaultTransports().Upstream(), protocol.DefaultTransports().Downstream()
	protocol.SetDefaultTransports("raw", "raw")
	defer protocol.SetDefaultTransports(prevUp, prevDown)

	secret := "integration-shell-secret"
	sessionID := "integration-shell-session"
	agentUUID := "AGENTSH001"
	streamID := uint32(0x1001)

	store := global.NewStoreWithTransports(nil)
	mgr := manager.NewManager(store)

	// Use in-memory fake session so the test does not spawn a real shell.
	agentProcess.TestOnlySetShellSessionFactory(func(opts *agentInitial.Options, id string, mode uint16) (*manager.ShellSession, error) {
		pr, pw := io.Pipe()
		session := &manager.ShellSession{
			ID:        id,
			Stdout:    pr,
			Mode:      protocol.ShellModePipe,
			Charset:   "utf-8",
			CreatedAt: time.Now(),
			Done:      make(chan struct{}),
		}
		go func() {
			fmt.Fprint(pw, "REUSE_OK\n")
			_ = pw.Close()
		}()
		return session, nil
	})
	t.Cleanup(func() { agentProcess.TestOnlySetShellSessionFactory(nil) })

	spy := newSpyConn()
	mockSess := &mockSession{conn: spy, secret: secret, uuid: agentUUID, version: protocol.CurrentProtocolVersion, flags: protocol.DefaultProtocolFlags}
	mgr.SetSession(mockSess)
	if mgr.ActiveSession() == nil {
		t.Fatalf("active session not set")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agentProcess.DispatchShellMess(ctx, mgr, &agentInitial.Options{Charset: "utf-8"})

	// 绑定 stream 并以 stream 方式启动 shell
	mgr.ShellManager.SetStreamForSession(sessionID, streamID)
	mgr.ShellManager.ShellMessChan <- &agentProcess.ShellReqWithStream{SessionID: sessionID, Mode: protocol.ShellModePipe, Resume: false}

	// 等待 STREAM_DATA 返回包含 REUSE_OK
	data := waitForStreamDataContaining(t, spy, secret, streamID, "REUSE_OK", 10*time.Second)
	if !strings.Contains(data, "REUSE_OK") {
		t.Fatalf("expected stream data to contain REUSE_OK, got %q", data)
	}

	// 管道关闭后应当收到 STREAM_CLOSE
	waitForStreamClose(t, spy, secret, streamID, 10*time.Second)
}

type mockSession struct {
	conn    net.Conn
	secret  string
	uuid    string
	version uint16
	flags   uint16
}

func (m *mockSession) Conn() net.Conn           { return m.conn }
func (m *mockSession) Secret() string           { return m.secret }
func (m *mockSession) UUID() string             { return m.uuid }
func (m *mockSession) UpdateConn(conn net.Conn) { m.conn = conn }
func (m *mockSession) ProtocolVersion() uint16  { return m.version }
func (m *mockSession) ProtocolFlags() uint16    { return m.flags }
func (m *mockSession) SetProtocol(v, f uint16)  { m.version, m.flags = v, f }

func decodeUpFrame(frame []byte, secret string) (*protocol.Header, interface{}, error) {
	conn := &bufferConn{Reader: bytes.NewReader(frame)}
	msg := protocol.NewUpMsg(conn, secret, protocol.ADMIN_UUID)
	protocol.SetMessageMeta(msg, protocol.CurrentProtocolVersion, protocol.DefaultProtocolFlags)
	header, payload, err := protocol.DestructMessage(msg)
	if err != nil {
		return nil, nil, err
	}
	return header, payload, nil
}

type spyConn struct {
	buf    []byte
	writes chan []byte
}

func newSpyConn() *spyConn { return &spyConn{writes: make(chan []byte, 64)} }

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
func (s *spyConn) Close() error                     { close(s.writes); return nil }
func (s *spyConn) LocalAddr() net.Addr              { return nil }
func (s *spyConn) RemoteAddr() net.Addr             { return nil }
func (s *spyConn) SetDeadline(time.Time) error      { return nil }
func (s *spyConn) SetReadDeadline(time.Time) error  { return nil }
func (s *spyConn) SetWriteDeadline(time.Time) error { return nil }

type bufferConn struct{ *bytes.Reader }

func (b *bufferConn) Write([]byte) (int, error)        { return 0, fmt.Errorf("write unsupported") }
func (b *bufferConn) Close() error                     { return nil }
func (b *bufferConn) LocalAddr() net.Addr              { return nil }
func (b *bufferConn) RemoteAddr() net.Addr             { return nil }
func (b *bufferConn) SetDeadline(time.Time) error      { return nil }
func (b *bufferConn) SetReadDeadline(time.Time) error  { return nil }
func (b *bufferConn) SetWriteDeadline(time.Time) error { return nil }

func truncate(s string, limit int) string {
	if len(s) <= limit {
		return s
	}
	if limit <= 3 {
		return s[:limit]
	}
	return s[:limit-3] + "..."
}

func waitForStreamDataContaining(t *testing.T, spy *spyConn, secret string, streamID uint32, expected string, timeout time.Duration) string {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case frame := <-spy.writes:
			header, payload, err := decodeUpFrame(frame, secret)
			if err != nil {
				continue
			}
			t.Logf("[frame] type=%d", header.MessageType)
			if header.MessageType != uint16(protocol.STREAM_DATA) {
				continue
			}
			data, ok := payload.(*protocol.StreamData)
			if !ok || data.StreamID != streamID {
				continue
			}
			text := string(data.Payload)
			t.Logf("[stream-data] payload=%q", truncate(text, 64))
			if strings.Contains(text, expected) {
				return text
			}
		case <-deadline:
			t.Fatalf("timed out waiting for STREAM_DATA containing %q", expected)
		}
	}
}

func waitForStreamClose(t *testing.T, spy *spyConn, secret string, streamID uint32, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case frame := <-spy.writes:
			header, payload, err := decodeUpFrame(frame, secret)
			if err != nil || header.MessageType != uint16(protocol.STREAM_CLOSE) {
				continue
			}
			closeMsg, ok := payload.(*protocol.StreamClose)
			if ok && closeMsg.StreamID == streamID {
				return
			}
		case <-deadline:
			t.Fatalf("timed out waiting for STREAM_CLOSE")
		}
	}
}
