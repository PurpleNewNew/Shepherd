package process

import (
	"testing"

	"codeberg.org/agnoie/shepherd/internal/kelpie/topology"
	"codeberg.org/agnoie/shepherd/pkg/global"
	"codeberg.org/agnoie/shepherd/pkg/session"
)

func TestSessionRegistryPrimarySelection(t *testing.T) {
	reg := newSessionRegistry(nil, nil)
	defer reg.stop()

	var callbacks []session.Session
	reg.SetPrimaryCallback(func(sess session.Session) { callbacks = append(callbacks, sess) })

	s1, client1 := newTestSession("ENTRY-1")
	defer func() {
		_ = s1.Conn().Close()
		_ = client1.Close()
	}()

	reg.set("ENTRY-1", s1)
	if got := reg.primary(); got != s1 {
		t.Fatalf("expected primary to be s1, got %v", got)
	}

	s2, client2 := newTestSession("ENTRY-2")
	defer func() {
		_ = s2.Conn().Close()
		_ = client2.Close()
	}()

	reg.set("ENTRY-2", s2)
	if got := reg.sessionForComponent("ENTRY-2"); got != s2 {
		t.Fatalf("expected session to be s2, got %v", got)
	}
	reg.remove("ENTRY-1")
	if got := reg.primary(); got != s2 {
		t.Fatalf("expected primary to fall back to s2, got %v", got)
	}
	reg.remove("ENTRY-2")
	if got := reg.primary(); got != nil {
		t.Fatalf("expected primary to be nil after removal, got %v", got)
	}
	if len(callbacks) == 0 || callbacks[len(callbacks)-1] != nil {
		t.Fatalf("expected final callback with nil primary, callbacks=%v", callbacks)
	}
}

func TestSessionRegistryStoreObserver(t *testing.T) {
	store := global.NewStoreWithTransports(nil)
	topo := topology.NewTopology()
	reg := newSessionRegistry(store, topo)
	defer reg.stop()

	sess, client := newTestSession("ENTRY-OBS")
	defer func() {
		_ = sess.Conn().Close()
		_ = client.Close()
	}()

	store.InitializeComponent(sess.Conn(), "secret", "ENTRY-OBS", "raw", "raw")
	if got := reg.sessionForComponent("ENTRY-OBS"); got == nil {
		t.Fatalf("expected registry to pick up session from store observer")
	}
}
