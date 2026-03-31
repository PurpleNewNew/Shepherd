package process

import (
	"net"
	"testing"

	"codeberg.org/agnoie/shepherd/pkg/global"
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

func TestNewDownstreamMessageForRouteRequiresFirstHopSession(t *testing.T) {
	store := global.NewStoreWithTransports(nil)
	registerStoreSession(t, store, "other", true)

	admin := &Admin{store: store}
	if _, err := admin.newDownstreamMessageForRoute("target", "root:target"); err == nil {
		t.Fatalf("expected error when first-hop session is unavailable")
	}
}
