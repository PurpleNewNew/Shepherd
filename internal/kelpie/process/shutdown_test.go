package process

import (
	"errors"
	"testing"

	"codeberg.org/agnoie/shepherd/internal/kelpie/manager"
)

func TestShutdownNodeUsesOverrides(t *testing.T) {
	admin := &Admin{mgr: &manager.Manager{}}
	admin.routeOverride = func(uuid string) (string, bool) {
		if uuid != "node-1" {
			t.Fatalf("unexpected uuid %s", uuid)
		}
		return "route-1", true
	}
	called := false
	admin.shutdownOverride = func(route, uuid string) {
		called = true
		if route != "route-1" || uuid != "node-1" {
			t.Fatalf("unexpected route/uuid %s/%s", route, uuid)
		}
	}
	if err := admin.ShutdownNode("node-1"); err != nil {
		t.Fatalf("ShutdownNode returned error: %v", err)
	}
	if !called {
		t.Fatalf("shutdown override not invoked")
	}
}

func TestShutdownNodeValidation(t *testing.T) {
	admin := &Admin{mgr: &manager.Manager{}}
	if err := admin.ShutdownNode(""); !errors.Is(err, ErrShutdownMissingTarget) {
		t.Fatalf("expected missing target error, got %v", err)
	}
	admin.routeOverride = func(uuid string) (string, bool) {
		return "", false
	}
	if err := admin.ShutdownNode("node-2"); err == nil {
		t.Fatalf("expected error when route not found")
	}
}
