package process

import (
	"context"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"codeberg.org/agnoie/shepherd/internal/flock/initial"
	"codeberg.org/agnoie/shepherd/internal/flock/manager"
	"codeberg.org/agnoie/shepherd/pkg/global"
	"codeberg.org/agnoie/shepherd/pkg/share"
	"codeberg.org/agnoie/shepherd/protocol"
)

func TestRepairListenerHandlesConcurrentConnections(t *testing.T) {
	prevUp, prevDown := protocol.DefaultTransports().Upstream(), protocol.DefaultTransports().Downstream()
	protocol.SetDefaultTransports("raw", "raw")
	t.Cleanup(func() {
		protocol.SetDefaultTransports(prevUp, prevDown)
	})

	baseSecret := "repair-base-secret"
	sessionSecret := share.DeriveSessionSecret(baseSecret, false)
	preAuth := share.GeneratePreAuthToken(baseSecret)

	store := global.NewStoreWithTransports(nil)
	if err := store.SetPreAuthToken(preAuth); err != nil {
		t.Fatalf("set preauth token: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr := manager.NewManager(store)

	var accepted int32

	agent := &Agent{
		ctx:    ctx,
		cancel: cancel,
		UUID:   "AGENT-REPAIR-TEST",
		options: &initial.Options{
			Mode:       initial.NORMAL_PASSIVE,
			Secret:     sessionSecret,
			RepairBind: "127.0.0.1",
			RepairPort: 0,
			Upstream:   "raw",
			Downstream: "raw",
		},
		store: store,
		mgr:   mgr,
	}
	agent.repairConnHook = func(conn net.Conn) {
		if conn != nil {
			_ = conn.Close()
		}
		atomic.AddInt32(&accepted, 1)
	}

	agent.startRepairListener()
	if agent.repairListener == nil {
		t.Fatalf("repair listener not started")
	}
	addr := net.JoinHostPort(agent.repairBind, strconv.Itoa(agent.repairPort))

	const clients = 8
	var wg sync.WaitGroup
	wg.Add(clients)
	for i := 0; i < clients; i++ {
		go func() {
			defer wg.Done()
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				t.Errorf("dial repair target: %v", err)
				return
			}
			_ = conn.Close()
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("repair listener did not accept concurrent connections in time")
	}

	deadline := time.After(2 * time.Second)
	for {
		if atomic.LoadInt32(&accepted) == clients {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("expected %d connections handled, got %d", clients, atomic.LoadInt32(&accepted))
		case <-time.After(10 * time.Millisecond):
		}
	}

	cancel()
	if agent.repairListener != nil {
		_ = agent.repairListener.Close()
	}
}
