package integration_test

import (
	"context"
	"net"
	"testing"
	"time"

	adminInitial "codeberg.org/agnoie/shepherd/internal/kelpie/initial"
	"codeberg.org/agnoie/shepherd/internal/kelpie/printer"
	"codeberg.org/agnoie/shepherd/internal/kelpie/process"
	"codeberg.org/agnoie/shepherd/internal/kelpie/topology"
	"codeberg.org/agnoie/shepherd/pkg/global"
	"codeberg.org/agnoie/shepherd/pkg/share"
	"codeberg.org/agnoie/shepherd/protocol"
)

func TestManualRepairRestoresConnection(t *testing.T) {
	secret := "integration-repair-secret"
	token := share.GeneratePreAuthToken(secret)

	printer.InitPrinter()

	prevUp, prevDown := protocol.DefaultTransports().Upstream(), protocol.DefaultTransports().Downstream()
	protocol.SetDefaultTransports("raw", "raw")
	defer protocol.SetDefaultTransports(prevUp, prevDown)

	topo := topology.NewTopology()
	go topo.Run()
	t.Cleanup(topo.Stop)

	svc := topology.NewService(topo)
	store := global.NewStoreWithTransports(nil)
	if err := store.SetPreAuthToken(token); err != nil {
		t.Fatalf("set preauth token: %v", err)
	}

	planner := process.NewSupplementalPlanner(topo, svc, nil, store)
	baseOptions := &adminInitial.Options{
		Mode:         adminInitial.NORMAL_ACTIVE,
		Secret:       secret,
		Downstream:   "raw",
		PreAuthToken: token,
		TlsEnable:    false,
	}
	planner.SetBaseOptions(baseOptions)

	ctx, cancel := context.WithCancel(context.Background())
	planner.Start(ctx)
	t.Cleanup(func() {
		cancel()
		planner.Stop()
	})

	nodeUUID := "repair-node-01"
	node := topology.NewNode(nodeUUID, "127.0.0.1")
	if _, err := topo.Execute(&topology.TopoTask{
		Mode:       topology.REONLINENODE,
		Target:     node,
		ParentUUID: protocol.ADMIN_UUID,
	}); err != nil {
		t.Fatalf("register node: %v", err)
	}

	if _, err := topo.Execute(&topology.TopoTask{
		Mode:           topology.UPDATECONNINFO,
		UUID:           nodeUUID,
		DialAddress:    "127.0.0.1",
		FallbackPort:   44321,
		Transport:      "raw",
		RepairAttempts: 2,
	}); err != nil {
		t.Fatalf("update connection info: %v", err)
	}

	done := make(chan struct{})
	var serverConn net.Conn
	planner.SetRepairDialer(func(opt *adminInitial.Options) (net.Conn, *protocol.Negotiation, string, error) {
		client, server := net.Pipe()
		serverConn = server
		go func() {
			<-done
			server.Close()
		}()
		nego := &protocol.Negotiation{
			Version: protocol.CurrentProtocolVersion,
			Flags:   protocol.DefaultProtocolFlags,
		}
		return client, nego, nodeUUID, nil
	})
	defer func() {
		close(done)
		planner.SetRepairDialer(nil)
		if serverConn != nil {
			_ = serverConn.Close()
		}
	}()

	if err := planner.RequestManualRepair(nodeUUID); err != nil {
		t.Fatalf("request manual repair: %v", err)
	}

	if !waitUntil(3*time.Second, func() bool {
		_, ok := store.Component(nodeUUID)
		return ok
	}) {
		snapshot := planner.MetricsSnapshot()
		events := planner.EventLog(10)
		t.Fatalf("repair connection not established in time | metrics: %+v pending=%d components=%v events=%v",
			snapshot, planner.PendingActions(), store.ListComponents(), events)
	}

	snapshot := planner.MetricsSnapshot()
	if snapshot.RepairAttempts != 1 || snapshot.RepairSuccess != 1 || snapshot.RepairFailures != 0 {
		t.Fatalf("unexpected repair metrics: %+v", snapshot)
	}

	ctxCheck, cancelCheck := context.WithTimeout(context.Background(), time.Second)
	defer cancelCheck()
	result, err := svc.Request(ctxCheck, &topology.TopoTask{Mode: topology.GETCONNINFO, UUID: nodeUUID})
	if err != nil {
		t.Fatalf("fetch connection info: %v", err)
	}
	if result == nil {
		t.Fatalf("connection info is nil")
	}
	if result.RepairFailures != 0 {
		t.Fatalf("expected repair failures reset to 0, got %d", result.RepairFailures)
	}
	if result.LastSuccess.IsZero() {
		t.Fatalf("expected last success timestamp to be updated")
	}
}

func waitUntil(timeout time.Duration, fn func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return fn()
}
