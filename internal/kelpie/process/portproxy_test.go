package process

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
)

func TestPortProxyStartForwardBridgesConnections(t *testing.T) {
	ctxFn := func() context.Context { return context.Background() }
	streamCh := make(chan net.Conn, 1)
	opener := func(ctx context.Context, target, sessionID string, meta map[string]string) (io.ReadWriteCloser, error) {
		server, client := net.Pipe()
		streamCh <- client
		return server, nil
	}
	mgr := newPortProxyManager(ctxFn, opener)
	desc, err := mgr.StartForward(context.Background(), "node-1", "127.0.0.1:0", "10.0.0.5:80")
	if err != nil {
		t.Fatalf("start forward failed: %v", err)
	}
	if desc.Kind() != kindForwardProxy {
		t.Fatalf("unexpected kind: %s", desc.Kind())
	}
	opts := desc.Options()
	remote := opts["remote"]
	if remote != "10.0.0.5:80" {
		t.Fatalf("unexpected remote: %s", remote)
	}
	bind := opts["bind"]
	conn, err := net.Dial("tcp", bind)
	if err != nil {
		t.Fatalf("dial forward listener failed: %v", err)
	}
	remoteConn := <-streamCh
	// 客户端侧清理
	t.Cleanup(func() {
		conn.Close()
		remoteConn.Close()
		mgr.StopForward("node-1", desc.ID())
	})
	if _, err := conn.Write([]byte("ping")); err != nil {
		t.Fatalf("write to local conn failed: %v", err)
	}
	buf := make([]byte, 4)
	if _, err := io.ReadFull(remoteConn, buf); err != nil {
		t.Fatalf("remote read failed: %v", err)
	}
	if string(buf) != "ping" {
		t.Fatalf("unexpected data to remote: %s", string(buf))
	}
	if _, err := remoteConn.Write([]byte("pong")); err != nil {
		t.Fatalf("write to remote failed: %v", err)
	}
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("local read failed: %v", err)
	}
	if string(buf) != "pong" {
		t.Fatalf("unexpected data to local: %s", string(buf))
	}
	remoteConn.Close()
	conn.Close()
	descs, err := mgr.StopForward("node-1", desc.ID())
	if err != nil {
		t.Fatalf("stop forward failed: %v", err)
	}
	if len(descs) != 1 {
		t.Fatalf("expected 1 tunnel stopped, got %d", len(descs))
	}
	if len(mgr.forwards) != 0 {
		t.Fatalf("forward map not cleared")
	}
}

func TestPortProxyStartBackwardAndStop(t *testing.T) {
	ctxFn := func() context.Context { return context.Background() }
	server, client := net.Pipe()
	opener := func(ctx context.Context, target, sessionID string, meta map[string]string) (io.ReadWriteCloser, error) {
		if meta["kind"] != kindBackwardProxy {
			t.Fatalf("unexpected kind: %v", meta["kind"])
		}
		if meta["rport"] != "9000" || meta["lport"] != "8080" {
			t.Fatalf("unexpected ports: %v", meta)
		}
		return server, nil
	}
	mgr := newPortProxyManager(ctxFn, opener)
	desc, err := mgr.StartBackward(context.Background(), "node-1", "9000", "8080")
	if err != nil {
		t.Fatalf("start backward failed: %v", err)
	}
	if desc.Kind() != kindBackwardProxy {
		t.Fatalf("unexpected kind: %s", desc.Kind())
	}
	opts := desc.Options()
	rport := opts["remote_port"]
	lport := opts["local_port"]
	if rport != "9000" || lport != "8080" {
		t.Fatalf("unexpected sanitized ports %s/%s", rport, lport)
	}
	client.Close()
	descs, err := mgr.StopBackward("node-1", desc.ID())
	if err != nil {
		t.Fatalf("stop backward failed: %v", err)
	}
	if len(descs) != 1 {
		t.Fatalf("expected 1 backward tunnel stopped, got %d", len(descs))
	}
	if len(mgr.backwards) != 0 {
		t.Fatalf("backward map not cleared")
	}
}

func TestPortProxyStopAllForwards(t *testing.T) {
	ctxFn := func() context.Context { return context.Background() }
	mgr := newPortProxyManager(ctxFn, func(ctx context.Context, target, sessionID string, meta map[string]string) (io.ReadWriteCloser, error) {
		server, _ := net.Pipe()
		return server, nil
	})
	if _, err := mgr.StartForward(context.Background(), "node-1", "127.0.0.1:0", "10.0.0.1:80"); err != nil {
		t.Fatalf("start forward #1 failed: %v", err)
	}
	if _, err := mgr.StartForward(context.Background(), "node-1", "127.0.0.1:0", "10.0.0.2:80"); err != nil {
		t.Fatalf("start forward #2 failed: %v", err)
	}
	descs, err := mgr.StopForward("node-1", "")
	if err != nil {
		t.Fatalf("stop all forwards failed: %v", err)
	}
	if len(descs) != 2 {
		t.Fatalf("expected to stop 2 tunnels, got %d", len(descs))
	}
}

func TestPortProxyValidationErrors(t *testing.T) {
	ctxFn := func() context.Context { return context.Background() }
	mgr := newPortProxyManager(ctxFn, func(ctx context.Context, target, sessionID string, meta map[string]string) (io.ReadWriteCloser, error) {
		server, _ := net.Pipe()
		return server, nil
	})
	if _, err := mgr.StartForward(context.Background(), "", "0.0.0.0:0", "10.0.0.1:80"); !errors.Is(err, ErrProxyMissingTarget) {
		t.Fatalf("expected missing target error, got %v", err)
	}
	if _, err := mgr.StartForward(context.Background(), "node-1", "", "10.0.0.1:80"); !errors.Is(err, ErrProxyMissingBind) {
		t.Fatalf("expected missing bind error, got %v", err)
	}
	if _, err := mgr.StartForward(context.Background(), "node-1", "127.0.0.1:0", "not-a-host"); !errors.Is(err, ErrProxyInvalidRemote) {
		t.Fatalf("expected invalid remote error, got %v", err)
	}
	if _, err := mgr.StartBackward(context.Background(), "node-1", "bad", "8080"); !errors.Is(err, ErrProxyInvalidPort) {
		t.Fatalf("expected invalid port error, got %v", err)
	}
	if _, err := mgr.StopForward("", "anything"); !errors.Is(err, ErrProxyMissingTarget) {
		t.Fatalf("expected missing target on stop, got %v", err)
	}
	if _, err := mgr.StopForward("node-1", "missing"); !errors.Is(err, ErrProxyNotFound) {
		t.Fatalf("expected not found on stop, got %v", err)
	}
}

func TestPortProxyList(t *testing.T) {
	ctxFn := func() context.Context { return context.Background() }
	stream := func(ctx context.Context, target, sessionID string, meta map[string]string) (io.ReadWriteCloser, error) {
		server, _ := net.Pipe()
		return server, nil
	}
	mgr := newPortProxyManager(ctxFn, stream)
	fwd, err := mgr.StartForward(context.Background(), "node-b", "127.0.0.1:0", "8.8.8.8:53")
	if err != nil {
		t.Fatalf("start forward failed: %v", err)
	}
	if _, err := mgr.StartBackward(context.Background(), "node-a", "9001", "8000"); err != nil {
		t.Fatalf("start backward failed: %v", err)
	}
	list := mgr.List()
	if len(list) != 2 {
		t.Fatalf("expected 2 proxies, got %d", len(list))
	}
	found := make(map[string]string)
	for _, desc := range list {
		found[desc.Target()] = desc.Kind()
	}
	if found["node-a"] != kindBackwardProxy || found["node-b"] != kindForwardProxy {
		t.Fatalf("unexpected proxy kinds: %+v", found)
	}
	// 确保关闭后列表能反映当前视图
	mgr.StopForward(fwd.Target(), fwd.ID())
	mgr.StopBackward("node-a", "")
	if len(mgr.List()) != 0 {
		t.Fatalf("expected list to be empty after stopping proxies")
	}
}
