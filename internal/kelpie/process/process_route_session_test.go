package process

import (
	"errors"
	"net"
	"testing"

	"codeberg.org/agnoie/shepherd/internal/kelpie/topology"
	"codeberg.org/agnoie/shepherd/pkg/global"
	"codeberg.org/agnoie/shepherd/protocol"
)

func registerStoreSession(t *testing.T, store *global.Store, uuid string, activate bool) {
	t.Helper()
	conn, peer := net.Pipe()
	t.Cleanup(func() {
		_ = conn.Close()
		_ = peer.Close()
	})
	store.RegisterComponent(conn, "secret", uuid, "raw", "raw")
	if activate && !store.ActivateComponent(uuid) {
		t.Fatalf("activate %s failed", uuid)
	}
}

func TestSessionForUUIDPrefersRouteFirstHop(t *testing.T) {
	store := global.NewStoreWithTransports(nil)
	registerStoreSession(t, store, "root", false)
	registerStoreSession(t, store, "other", true) // active session should not hijack routed delivery

	admin := &Admin{
		store: store,
		adminStreamState: adminStreamState{
			routeOverride: func(uuid string) (string, bool) {
				if uuid == "target" {
					return "root:target", true
				}
				return "", false
			},
		},
	}

	sess := admin.sessionForUUID("target")
	if sess == nil {
		t.Fatalf("expected session for routed target")
	}
	if sess.UUID() != "root" {
		t.Fatalf("expected first-hop session root, got %s", sess.UUID())
	}
}

func TestSessionForUUIDNoFallbackToUnrelatedActiveSession(t *testing.T) {
	store := global.NewStoreWithTransports(nil)
	registerStoreSession(t, store, "other", true)

	admin := &Admin{
		store: store,
		adminStreamState: adminStreamState{
			routeOverride: func(uuid string) (string, bool) {
				if uuid == "target" {
					return "root:target", true
				}
				return "", false
			},
		},
	}

	if sess := admin.sessionForUUID("target"); sess != nil {
		t.Fatalf("expected nil session when first hop unavailable, got %s", sess.UUID())
	}
}

func TestSessionForRouteUsesAdminEntryWhenRepairSessionIsPrimary(t *testing.T) {
	store := global.NewStoreWithTransports(nil)
	registerStoreSession(t, store, protocol.ADMIN_UUID, true)
	registerStoreSession(t, store, "repair-entry", true)

	topo := topology.NewTopology()
	go topo.Run()
	t.Cleanup(topo.Stop)
	if _, err := topo.Execute(&topology.TopoTask{
		Mode:       topology.ADDNODE,
		Target:     topology.NewNode("root", "127.0.0.1"),
		ParentUUID: protocol.ADMIN_UUID,
	}); err != nil {
		t.Fatalf("add root node: %v", err)
	}

	admin := &Admin{
		store:    store,
		topology: topo,
		adminSessionState: adminSessionState{
			sessions: newSessionRegistry(store, topo),
		},
	}
	t.Cleanup(admin.sessions.stop)

	sess := admin.sessionForRoute("root:target")
	if sess == nil {
		t.Fatalf("expected admin entry session")
	}
	if sess.UUID() != protocol.ADMIN_UUID {
		t.Fatalf("expected admin entry session, got %s", sess.UUID())
	}
}

func TestNewDownstreamMessageForRouteRequiresFirstHopSession(t *testing.T) {
	store := global.NewStoreWithTransports(nil)
	registerStoreSession(t, store, "other", true)

	admin := &Admin{store: store}
	if _, err := admin.newDownstreamMessageForRoute("target", "root:target"); err == nil {
		t.Fatalf("expected error when first-hop session is unavailable")
	}
}

func TestCheckTargetReadyRejectsQuarantinedHopEvenWithStaleRoute(t *testing.T) {
	store := global.NewStoreWithTransports(nil)
	registerStoreSession(t, store, "root", true)

	topo := topology.NewTopology()
	go topo.Run()
	t.Cleanup(topo.Stop)
	if _, err := topo.Execute(&topology.TopoTask{
		Mode:       topology.ADDNODE,
		Target:     topology.NewNode("root", "127.0.0.1"),
		ParentUUID: protocol.ADMIN_UUID,
	}); err != nil {
		t.Fatalf("add root node: %v", err)
	}
	if _, err := topo.Execute(&topology.TopoTask{
		Mode:       topology.ADDNODE,
		Target:     topology.NewNode("leaf", "127.0.0.2"),
		ParentUUID: "root",
	}); err != nil {
		t.Fatalf("add leaf node: %v", err)
	}
	for _, task := range []*topology.TopoTask{
		{Mode: topology.ADDEDGE, UUID: protocol.ADMIN_UUID, NeighborUUID: "root"},
		{Mode: topology.ADDEDGE, UUID: "root", NeighborUUID: "leaf"},
		{Mode: topology.CALCULATE},
	} {
		if _, err := topo.Execute(task); err != nil {
			t.Fatalf("topology task %d: %v", task.Mode, err)
		}
	}

	admin := &Admin{
		store:       store,
		topology:    topo,
		topoService: topology.NewService(topo),
		adminSessionState: adminSessionState{
			sessions: newSessionRegistry(store, topo),
		},
	}
	t.Cleanup(admin.sessions.stop)

	if err := admin.CheckTargetReady("leaf"); err != nil {
		t.Fatalf("expected leaf to be reachable before quarantine: %v", err)
	}
	if _, err := topo.Execute(&topology.TopoTask{Mode: topology.QUARANTINENODE, UUID: "root"}); err != nil {
		t.Fatalf("quarantine root: %v", err)
	}
	err := admin.CheckTargetReady("leaf")
	if !errors.Is(err, ErrTargetUnreachable) {
		t.Fatalf("expected target unreachable through quarantined hop, got %v", err)
	}
}

func TestRouteUsesSupplementalParsesRouteSegments(t *testing.T) {
	tests := []struct {
		name  string
		route string
		want  bool
	}{
		{name: "empty", route: "", want: false},
		{name: "tree", route: "root:child:leaf", want: false},
		{name: "supp", route: "root#supp:child:leaf", want: true},
		{name: "later supp", route: "root:child#supp:leaf", want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := routeUsesSupplemental(tt.route); got != tt.want {
				t.Fatalf("routeUsesSupplemental(%q)=%v, want %v", tt.route, got, tt.want)
			}
		})
	}
}
