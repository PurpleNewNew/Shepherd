package manager

import (
	"net"
	"testing"
	"time"
)

func TestChildrenManagerLifecycle(t *testing.T) {
	manager := newChildrenManager()

	connParent, connChild := net.Pipe()
	defer connChild.Close()

	manager.AddChild("child-1", connParent)

	if conn, ok := manager.GetConn("child-1"); !ok || conn == nil {
		t.Fatalf("expected to retrieve child connection")
	}

	manager.NotifyChild(&ChildInfo{UUID: "child-1", Conn: connParent})
	select {
	case info := <-manager.ChildComeChan:
		if info == nil || info.UUID != "child-1" {
			t.Fatalf("unexpected child notification: %#v", info)
		}
	case <-time.After(time.Second):
		t.Fatalf("timeout waiting for child notification")
	}

	children := manager.AllChildren()
	if len(children) != 1 || children[0] != "child-1" {
		t.Fatalf("unexpected children list: %#v", children)
	}

	manager.RemoveChild("child-1")

	if _, ok := manager.GetConn("child-1"); ok {
		t.Fatalf("expected lookup to fail after deletion")
	}

	manager.Close()
}
