package process

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"codeberg.org/agnoie/shepherd/internal/flock/initial"
	"codeberg.org/agnoie/shepherd/internal/flock/manager"
	"codeberg.org/agnoie/shepherd/pkg/global"
	"codeberg.org/agnoie/shepherd/protocol"
)

// fakeConn 实现 net.Conn 接口，仅记录写入的数据。
type fakeConn struct {
	net.Conn
	writes [][]byte
}

func newFakeConn() *fakeConn { return &fakeConn{} }

func (f *fakeConn) Write(p []byte) (int, error) {
	cp := make([]byte, len(p))
	copy(cp, p)
	f.writes = append(f.writes, cp)
	return len(p), nil
}

func (f *fakeConn) Read(p []byte) (int, error)       { return 0, io.EOF }
func (f *fakeConn) Close() error                     { return nil }
func (f *fakeConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (f *fakeConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (f *fakeConn) SetDeadline(time.Time) error      { return nil }
func (f *fakeConn) SetReadDeadline(time.Time) error  { return nil }
func (f *fakeConn) SetWriteDeadline(time.Time) error { return nil }

func setupAgentWithSuppLink(t *testing.T) (*Agent, *manager.Manager, *fakeConn, *fakeConn) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	prevUp, prevDown := protocol.DefaultTransports().Upstream(), protocol.DefaultTransports().Downstream()
	protocol.SetDefaultTransports("raw", "raw")
	store := global.NewStoreWithTransports(nil)
	if err := store.SetPreAuthToken("test-preauth-token"); err != nil {
		t.Fatalf("set preauth token: %v", err)
	}
	mgr := manager.NewManager(store)
	go mgr.Run(ctx)
	agent := NewAgent(ctx, &initial.Options{PreAuthToken: "test-preauth-token"}, store, nil)
	agent.UUID = "AGENT-A"
	agent.mgr = mgr

	upstream := newFakeConn()
	store.Reset()
	t.Cleanup(store.Reset)
	t.Cleanup(func() { protocol.SetDefaultTransports(prevUp, prevDown) })
	store.InitializeComponent(upstream, "secret", agent.UUID, "raw", "raw")
	agent.BindSession(store.ActiveSession())

	suppConn := newFakeConn()
	mgr.SupplementalManager.TaskChan <- &manager.SupplementalTask{Mode: manager.SuppAddOrUpdate, Link: &manager.SupplementalLink{
		LinkUUID: "link-1",
		PeerUUID: "CHILD-1",
		Role:     protocol.SuppLinkRoleInitiator,
		State:    manager.SuppStateReady,
	}}
	<-mgr.SupplementalManager.ResultChan
	mgr.SupplementalManager.TaskChan <- &manager.SupplementalTask{Mode: manager.SuppAttachConn, LinkUUID: "link-1", Conn: suppConn}
	res := <-mgr.SupplementalManager.ResultChan
	if !res.OK {
		t.Fatalf("failed to attach supplemental conn")
	}

	go agent.waitingSupplemental()

	return agent, mgr, upstream, suppConn
}

func TestSuppFailoverPromotionIntegration(t *testing.T) {
	agent, _, _, suppConn := setupAgentWithSuppLink(t)

	cmd := &protocol.SuppFailoverCommand{
		LinkUUID:   "link-1",
		ParentUUID: "PARENT-1",
		ChildUUID:  "CHILD-1",
		Role:       protocol.SuppFailoverRoleParent,
		Flags:      protocol.SuppFailoverFlagInitiator,
	}
	agent.handleSuppFailoverCommand(cmd)

	time.Sleep(50 * time.Millisecond)

	if _, ok := agent.pendingFailovers["link-1"]; !ok {
		t.Fatalf("pending failover not registered")
	}

	if len(suppConn.writes) == 0 {
		t.Fatalf("expected promotion message written to supplemental conn")
	}

	promotion := &protocol.SuppLinkPromote{
		LinkUUID:   "link-1",
		ParentUUID: "PARENT-1",
		ChildUUID:  "CHILD-1",
		Role:       protocol.SuppPromoteRoleChild,
	}
	agent.handleSuppPromotionMessage("link-1", suppConn, promotion)

	select {
	case candidate := <-agent.failoverConnChan:
		if candidate == nil || candidate.conn != suppConn {
			t.Fatalf("failover candidate mismatch")
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("expected failover candidate queued")
	}
}
