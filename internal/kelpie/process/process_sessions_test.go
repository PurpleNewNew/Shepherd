package process

import (
	"context"
	"net"
	"testing"

	"codeberg.org/agnoie/shepherd/internal/kelpie/initial"
	"codeberg.org/agnoie/shepherd/internal/kelpie/topology"
	"codeberg.org/agnoie/shepherd/pkg/global"
)

func TestSessionsSnapshot(t *testing.T) {
	store := global.NewStoreWithTransports(nil)
	topo := topology.NewTopology()
	admin := &Admin{store: store, topology: topo}

	conn1, conn1Peer := net.Pipe()
	defer conn1Peer.Close()
	store.RegisterComponent(conn1, "secret", "entry-1", "raw", "raw")
	store.ActivateComponent("entry-1")
	_ = topo.SetNetwork("entry-1", "net-a")

	sessions := admin.Sessions(SessionFilter{})
	if len(sessions) != 1 {
		t.Fatalf("expected single session, got %d", len(sessions))
	}
	if !sessions[0].Active || !sessions[0].Connected {
		t.Fatalf("expected session active and connected: %+v", sessions[0])
	}
}

func TestDropSession(t *testing.T) {
	store := global.NewStoreWithTransports(nil)
	topo := topology.NewTopology()
	admin := &Admin{
		store:    store,
		topology: topo,
		options:  &initial.Options{},
	}

	conn, peer := net.Pipe()
	defer peer.Close()
	store.RegisterComponent(conn, "secret", "entry-drop", "raw", "raw")
	store.ActivateComponent("entry-drop")

	if err := admin.DropSession(""); err == nil {
		t.Fatalf("expected error for empty uuid")
	}
	if err := admin.DropSession("entry-drop"); err != nil {
		t.Fatalf("unexpected drop error: %v", err)
	}
	if comps := store.ListComponents(); len(comps) != 0 {
		t.Fatalf("expected component removed, still have %v", comps)
	}
}

func TestSessionDiagnosticsIncludeProcesses(t *testing.T) {
	admin := &Admin{}
	admin.initStreamEngine()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handle, err := admin.OpenStream(ctx, "node-1", "sess-1", map[string]string{
		"kind":     "shell",
		"operator": "alice",
		"path":     "/bin/sh",
	})
	if err != nil {
		t.Fatalf("open stream failed: %v", err)
	}
	defer handle.Close()

	diag, err := admin.SessionDiagnostics("node-1", true, false)
	if err != nil {
		t.Fatalf("session diagnostics failed: %v", err)
	}
	if len(diag.Process) != 1 {
		t.Fatalf("expected one process entry, got %d", len(diag.Process))
	}
	proc := diag.Process[0]
	if proc.Name != "shell" {
		t.Fatalf("unexpected process name: %s", proc.Name)
	}
	if proc.User != "alice" {
		t.Fatalf("unexpected process user: %s", proc.User)
	}
	if proc.Path != "/bin/sh" {
		t.Fatalf("unexpected process path: %s", proc.Path)
	}
	if proc.PID == "" {
		t.Fatalf("expected non-empty process id")
	}
}
