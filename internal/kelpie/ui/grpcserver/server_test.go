package grpcserver

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"codeberg.org/agnoie/shepherd/internal/dataplanepb"
	"codeberg.org/agnoie/shepherd/internal/kelpie/dataplane"
	"codeberg.org/agnoie/shepherd/internal/kelpie/dtn"
	"codeberg.org/agnoie/shepherd/internal/kelpie/process"
	"codeberg.org/agnoie/shepherd/internal/kelpie/supp"
	"codeberg.org/agnoie/shepherd/internal/kelpie/topology"
	"codeberg.org/agnoie/shepherd/internal/kelpie/uipb"
	"codeberg.org/agnoie/shepherd/pkg/bus"
	"codeberg.org/agnoie/shepherd/protocol"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMapPivotListenerMode(t *testing.T) {
	tests := []struct {
		mode     uipb.PivotListenerMode
		expected int
		wantErr  bool
	}{
		{uipb.PivotListenerMode_PIVOT_LISTENER_MODE_UNSPECIFIED, process.ListenerModeNormal, false},
		{uipb.PivotListenerMode_PIVOT_LISTENER_MODE_NORMAL, process.ListenerModeNormal, false},
		{uipb.PivotListenerMode_PIVOT_LISTENER_MODE_IPTABLES, process.ListenerModeIptables, false},
		{uipb.PivotListenerMode_PIVOT_LISTENER_MODE_SOREUSE, process.ListenerModeSoReuse, false},
		{uipb.PivotListenerMode(-1), 0, true},
	}
	for _, tt := range tests {
		got, err := mapPivotListenerMode(tt.mode)
		if tt.wantErr {
			if err == nil {
				t.Fatalf("expected error for mode %v", tt.mode)
			}
			continue
		}
		if err != nil {
			t.Fatalf("unexpected error for mode %v: %v", tt.mode, err)
		}
		if got != tt.expected {
			t.Fatalf("mode %v: expected %d, got %d", tt.mode, tt.expected, got)
		}
	}
}

func TestConvertSupplementalMetrics(t *testing.T) {
	snapshot := process.SupplementalMetricsSnapshot{
		Dispatched:      5,
		Success:         4,
		Failures:        1,
		Dropped:         2,
		Recycled:        3,
		RepairAttempts:  7,
		RepairSuccess:   5,
		RepairFailures:  2,
		QueueHigh:       9,
		QueueDepth:      1,
		LastFailure:     "oops",
		EventSeq:        42,
		LastGraphReport: []string{"a", "b"},
	}
	converted := convertSupplementalMetrics(snapshot)
	if converted.GetDispatched() != 5 || converted.GetEventSeq() != 42 {
		t.Fatalf("unexpected metrics conversion: %+v", converted)
	}
	if len(converted.GetLastGraphReport()) != 2 {
		t.Fatalf("expected graph report copy")
	}
}

func TestConvertSupplementalEvents(t *testing.T) {
	now := time.Now()
	events := []process.SupplementalPlannerEvent{
		{Seq: 1, Kind: "k", Action: "a", SourceUUID: "s", TargetUUID: "t", Detail: "d", Timestamp: now},
	}
	converted := convertSupplementalEvents(events)
	if len(converted) != 1 {
		t.Fatalf("expected single event")
	}
	if converted[0].GetSeq() != 1 || converted[0].GetTimestamp() == "" {
		t.Fatalf("expected timestamp and seq, got %+v", converted[0])
	}
}

func TestConvertSupplementalQuality(t *testing.T) {
	qualities := []process.SupplementalQualitySnapshot{
		{NodeUUID: "node-a", HealthScore: 0.5, LatencyScore: 0.2, FailureScore: 0.1, QueueScore: 0.3, StalenessScore: 0.4, TotalSuccess: 10, TotalFailures: 2, LastHeartbeat: time.Now()},
	}
	converted := convertSupplementalQuality(qualities)
	if len(converted) != 1 {
		t.Fatalf("expected one entry")
	}
	if converted[0].GetNodeUuid() != "node-a" || converted[0].GetHealthScore() != 0.5 {
		t.Fatalf("unexpected quality conversion: %+v", converted[0])
	}
	if converted[0].GetLastHeartbeat() == "" {
		t.Fatalf("expected heartbeat timestamp")
	}
}

func TestNetworkHandlersValidation(t *testing.T) {
	svcNil := &service{}
	if _, err := svcNil.ListNetworks(context.Background(), nil); status.Code(err) != codes.Unavailable {
		t.Fatalf("expected unavailable when admin nil, got %v", err)
	}
	svc := &service{admin: &process.Admin{}}
	if _, err := svc.UseNetwork(context.Background(), nil); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected invalid argument for nil request, got %v", err)
	}
	if _, err := svcNil.ResetNetwork(context.Background(), nil); status.Code(err) != codes.Unavailable {
		t.Fatalf("expected unavailable when admin nil, got %v", err)
	}
	if _, err := svc.SetNodeNetwork(context.Background(), nil); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected invalid argument for nil request, got %v", err)
	}
}

func TestCloseStreamValidation(t *testing.T) {
	svcNil := &service{}
	if _, err := svcNil.CloseStream(context.Background(), &uipb.CloseStreamRequest{StreamId: 1}); status.Code(err) != codes.Unavailable {
		t.Fatalf("expected unavailable when admin missing, got %v", err)
	}
	svc := &service{admin: &process.Admin{}}
	if _, err := svc.CloseStream(context.Background(), nil); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected invalid argument for nil request, got %v", err)
	}
	if _, err := svc.CloseStream(context.Background(), &uipb.CloseStreamRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected invalid argument for missing id, got %v", err)
	}
	called := false
	svc.closeStreamOverride = func(id uint32, reason string) error {
		called = true
		if id != 42 || reason != "test" {
			return fmt.Errorf("bad args")
		}
		return nil
	}
	svc.admin = nil
	if _, err := svc.CloseStream(context.Background(), &uipb.CloseStreamRequest{StreamId: 42, Reason: "test"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Fatalf("override not invoked")
	}
	svc.closeStreamOverride = func(uint32, string) error {
		return fmt.Errorf("boom")
	}
	if _, err := svc.CloseStream(context.Background(), &uipb.CloseStreamRequest{StreamId: 99}); status.Code(err) != codes.Internal {
		t.Fatalf("expected internal error, got %v", err)
	}
}

func TestStartDialLifecycle(t *testing.T) {
	events := make(chan *uipb.UiEvent, 8)
	svc := &service{
		subscribers: map[uint64]chan *uipb.UiEvent{1: events},
		connectNodeOverride: func(ctx context.Context, target, addr string) error {
			if target != "node-a" || addr != "1.2.3.4:5" {
				return fmt.Errorf("unexpected connect args")
			}
			return nil
		},
	}
	resp, err := svc.StartDial(context.Background(), &uipb.StartDialRequest{TargetUuid: "node-a", Address: "1.2.3.4:5"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.GetDialId() == "" {
		t.Fatalf("expected dial id")
	}
	got := collectDialKinds(t, events, 3)
	expected := []uipb.DialEvent_Kind{
		uipb.DialEvent_DIAL_EVENT_ENQUEUED,
		uipb.DialEvent_DIAL_EVENT_RUNNING,
		uipb.DialEvent_DIAL_EVENT_COMPLETED,
	}
	if len(got) != len(expected) {
		t.Fatalf("unexpected dial events: %v", got)
	}
	for i := range expected {
		if got[i] != expected[i] {
			t.Fatalf("event %d mismatch: got %v, want %v", i, got[i], expected[i])
		}
	}
}

func TestCancelDialEmitsEvent(t *testing.T) {
	events := make(chan *uipb.UiEvent, 8)
	canceled := make(chan struct{})
	svc := &service{
		subscribers: map[uint64]chan *uipb.UiEvent{1: events},
		connectNodeOverride: func(ctx context.Context, target, addr string) error {
			<-ctx.Done()
			close(canceled)
			return ctx.Err()
		},
	}
	resp, err := svc.StartDial(context.Background(), &uipb.StartDialRequest{TargetUuid: "node-b", Address: "5.6.7.8:9"})
	if err != nil {
		t.Fatalf("unexpected start error: %v", err)
	}
	if _, err := svc.CancelDial(context.Background(), &uipb.CancelDialRequest{DialId: resp.GetDialId()}); err != nil {
		t.Fatalf("unexpected cancel error: %v", err)
	}
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatalf("connect override not canceled")
	}
	kinds := collectDialKinds(t, events, 3)
	if kinds[len(kinds)-1] != uipb.DialEvent_DIAL_EVENT_CANCELED {
		t.Fatalf("expected cancel event, got %v", kinds)
	}
}

func TestListDialsExposesActive(t *testing.T) {
	allow := make(chan struct{})
	events := make(chan *uipb.UiEvent, 8)
	svc := &service{
		subscribers: map[uint64]chan *uipb.UiEvent{1: events},
		connectNodeOverride: func(ctx context.Context, target, addr string) error {
			<-allow
			return nil
		},
	}
	resp, err := svc.StartDial(context.Background(), &uipb.StartDialRequest{TargetUuid: "node-c", Address: "10.0.0.1:2"})
	if err != nil {
		t.Fatalf("start dial failed: %v", err)
	}
	waitForDialEvent(t, events, uipb.DialEvent_DIAL_EVENT_RUNNING)
	list, err := svc.ListDials(context.Background(), &uipb.ListDialRequest{})
	if err != nil {
		t.Fatalf("list dials failed: %v", err)
	}
	if len(list.GetDials()) != 1 || list.GetDials()[0].GetDialId() != resp.GetDialId() {
		t.Fatalf("unexpected dial list: %+v", list.GetDials())
	}
	close(allow)
	waitForDialEvent(t, events, uipb.DialEvent_DIAL_EVENT_COMPLETED)
}

func TestProxyEventsStartStop(t *testing.T) {
	events := make(chan *uipb.UiEvent, 4)
	svc := &service{
		subscribers: map[uint64]chan *uipb.UiEvent{1: events},
		startForwardProxyOverride: func(ctx context.Context, target, bind, remote string) (*process.ProxyDescriptor, error) {
			if target != "node-a" || remote != "1.2.3.4:5" {
				return nil, fmt.Errorf("unexpected args")
			}
			return process.NewProxyDescriptor("fwd-1", target, "forward-proxy", map[string]string{"bind": "127.0.0.1:0", "remote": remote}), nil
		},
		stopForwardProxyOverride: func(target, proxyID string) ([]*process.ProxyDescriptor, error) {
			return []*process.ProxyDescriptor{process.NewProxyDescriptor(proxyID, target, "forward-proxy", map[string]string{"bind": "127.0.0.1:0"})}, nil
		},
	}
	if _, err := svc.StartForwardProxy(context.Background(), &uipb.StartForwardProxyRequest{TargetUuid: "node-a", LocalBind: "127.0.0.1:0", RemoteAddr: "1.2.3.4:5"}); err != nil {
		t.Fatalf("start forward proxy failed: %v", err)
	}
	startEvt := waitForProxyEvent(t, events)
	if startEvt.GetKind() != uipb.ProxyEvent_PROXY_EVENT_STARTED || startEvt.GetProxy().GetProxyId() != "fwd-1" {
		t.Fatalf("unexpected start event: %+v", startEvt)
	}
	if _, err := svc.StopForwardProxy(context.Background(), &uipb.StopForwardProxyRequest{TargetUuid: "node-a", ProxyId: "fwd-1"}); err != nil {
		t.Fatalf("stop forward proxy failed: %v", err)
	}
	stopEvt := waitForProxyEvent(t, events)
	if stopEvt.GetKind() != uipb.ProxyEvent_PROXY_EVENT_STOPPED || stopEvt.GetProxy().GetTargetUuid() != "node-a" {
		t.Fatalf("unexpected stop event: %+v", stopEvt)
	}
}

func TestSleepEventBroadcast(t *testing.T) {
	events := make(chan *uipb.UiEvent, 1)
	svc := &service{
		subscribers: map[uint64]chan *uipb.UiEvent{1: events},
		updateSleepOverride: func(target string, params process.SleepUpdateParams) error {
			if target != "node-a" {
				return fmt.Errorf("unexpected target")
			}
			if params.SleepSeconds == nil || *params.SleepSeconds != 15 {
				return fmt.Errorf("unexpected params")
			}
			return nil
		},
	}
	sleep := int32(15)
	if _, err := svc.UpdateSleep(context.Background(), &uipb.UpdateSleepRequest{TargetUuid: "node-a", SleepSeconds: &sleep}); err != nil {
		t.Fatalf("update sleep failed: %v", err)
	}
	evt := waitForSleepEvent(t, events)
	if evt.GetTargetUuid() != "node-a" || evt.GetSleepSeconds() != 15 {
		t.Fatalf("unexpected sleep event: %+v", evt)
	}
}

func TestSupplementalEventsBroadcast(t *testing.T) {
	events := make(chan *uipb.UiEvent, 1)
	svc := &service{subscribers: map[uint64]chan *uipb.UiEvent{1: events}}
	svc.registerSupplementalHooks()
	defer svc.close()
	supp.PublishSuppLinkFailed("link-1", []string{"parent", "child"})
	evt := waitForSupplementalEvent(t, events)
	if evt.GetKind() != "link" || evt.GetAction() != "failed" {
		t.Fatalf("unexpected supplemental event: %+v", evt)
	}
}

func collectDialKinds(t *testing.T, ch <-chan *uipb.UiEvent, count int) []uipb.DialEvent_Kind {
	helper := make([]uipb.DialEvent_Kind, 0, count)
	deadline := time.After(2 * time.Second)
	for len(helper) < count {
		select {
		case evt := <-ch:
			if evt == nil {
				continue
			}
			if dial := evt.GetDialEvent(); dial != nil {
				helper = append(helper, dial.GetKind())
			}
		case <-deadline:
			t.Fatalf("timeout waiting for dial events")
		}
	}
	return helper
}

func TestListProxiesAggregates(t *testing.T) {
	svc := &service{
		listProxiesOverride: func() []*process.ProxyDescriptor {
			return []*process.ProxyDescriptor{
				process.NewProxyDescriptor("fwd-2", "node-b", "forward-proxy", map[string]string{"bind": "b", "remote": "1.1.1.1:1"}),
				process.NewProxyDescriptor("bwd-1", "node-a", "backward-proxy", map[string]string{"remote_port": "9000"}),
			}
		},
	}
	resp, err := svc.ListProxies(context.Background(), &uipb.ListProxiesRequest{})
	if err != nil {
		t.Fatalf("list proxies failed: %v", err)
	}
	if len(resp.GetProxies()) != 2 || resp.GetProxies()[0].GetTargetUuid() != "node-a" {
		t.Fatalf("unexpected proxy list: %+v", resp.GetProxies())
	}
}

func TestListSleepProfilesFilters(t *testing.T) {
	sleep := 30
	svc := &service{
		listSleepProfilesOverride: func() []process.SessionInfo {
			return []process.SessionInfo{
				{TargetUUID: "node-a", SleepSeconds: &sleep, Status: process.SessionStatusActive, Metadata: map[string]string{"next_wake_at": "2024-01-01T00:00:00Z"}},
				{TargetUUID: "node-b"},
			}
		},
	}
	resp, err := svc.ListSleepProfiles(context.Background(), &uipb.ListSleepProfilesRequest{})
	if err != nil {
		t.Fatalf("list sleep profiles failed: %v", err)
	}
	if len(resp.GetProfiles()) != 1 || resp.GetProfiles()[0].GetTargetUuid() != "node-a" {
		t.Fatalf("unexpected profiles: %+v", resp.GetProfiles())
	}
}

func TestListRepairsAggregates(t *testing.T) {
	svc := &service{
		sessionsOverride: func(filter process.SessionFilter) []process.SessionInfo {
			return []process.SessionInfo{{
				TargetUUID:   "node-a",
				Status:       process.SessionStatusRepairing,
				LastError:    "dial",
				StatusReason: "retry",
			}}
		},
		listRepairsOverride: func() []process.RepairStatusSnapshot {
			return []process.RepairStatusSnapshot{
				{TargetUUID: "node-a", Attempts: 2},
				{TargetUUID: "node-b", Attempts: 1, NextAttempt: time.Unix(0, 0)},
			}
		},
	}
	resp, err := svc.ListRepairs(context.Background(), &uipb.ListRepairsRequest{})
	if err != nil {
		t.Fatalf("list repairs failed: %v", err)
	}
	if len(resp.GetRepairs()) != 2 || resp.GetRepairs()[0].GetTargetUuid() != "node-a" {
		t.Fatalf("unexpected repair list: %+v", resp.GetRepairs())
	}
	if resp.GetRepairs()[0].GetAttempts() != 2 || resp.GetRepairs()[0].GetReason() != "retry" {
		t.Fatalf("expected attempts merged: %+v", resp.GetRepairs()[0])
	}
}

func waitForDialEvent(t *testing.T, ch <-chan *uipb.UiEvent, target uipb.DialEvent_Kind) {
	deadline := time.After(2 * time.Second)
	for {
		select {
		case evt := <-ch:
			if evt == nil {
				continue
			}
			if dial := evt.GetDialEvent(); dial != nil && dial.GetKind() == target {
				return
			}
		case <-deadline:
			t.Fatalf("timeout waiting for dial event %v", target)
		}
	}
}

func waitForProxyEvent(t *testing.T, ch <-chan *uipb.UiEvent) *uipb.ProxyEvent {
	deadline := time.After(2 * time.Second)
	for {
		select {
		case evt := <-ch:
			if evt == nil {
				continue
			}
			if proxy := evt.GetProxyEvent(); proxy != nil {
				return proxy
			}
		case <-deadline:
			t.Fatalf("timeout waiting for proxy event")
		}
	}
}

func waitForSleepEvent(t *testing.T, ch <-chan *uipb.UiEvent) *uipb.SleepEvent {
	deadline := time.After(2 * time.Second)
	for {
		select {
		case evt := <-ch:
			if evt == nil {
				continue
			}
			if sleep := evt.GetSleepEvent(); sleep != nil {
				return sleep
			}
		case <-deadline:
			t.Fatalf("timeout waiting for sleep event")
		}
	}
}

func waitForSupplementalEvent(t *testing.T, ch <-chan *uipb.UiEvent) *uipb.SupplementalEvent {
	deadline := time.After(2 * time.Second)
	for {
		select {
		case evt := <-ch:
			if evt == nil {
				continue
			}
			if suppEvt := evt.GetSupplementalEvent(); suppEvt != nil {
				return suppEvt
			}
		case <-deadline:
			t.Fatalf("timeout waiting for supplemental event")
		}
	}
}

func TestStartShellValidation(t *testing.T) {
	svcNil := &service{}
	if _, err := svcNil.StartShell(context.Background(), &uipb.StartShellRequest{TargetUuid: "x"}); status.Code(err) != codes.Unavailable {
		t.Fatalf("expected unavailable when admin missing, got %v", err)
	}
	svc := &service{admin: &process.Admin{}}
	if _, err := svc.StartShell(context.Background(), nil); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected invalid argument for nil req, got %v", err)
	}
	resp, err := svc.StartShell(context.Background(), &uipb.StartShellRequest{TargetUuid: "node-1", Mode: uipb.ShellMode_SHELL_MODE_PTY})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.GetHandle().GetKind() != "shell" || resp.GetHandle().GetOptions()["mode"] != strconv.Itoa(int(protocol.ShellModePTY)) {
		t.Fatalf("unexpected handle: %+v", resp.GetHandle())
	}
}

func TestRoutingStrategyHandlers(t *testing.T) {
	svcNil := &service{}
	if _, err := svcNil.GetRoutingStrategy(context.Background(), &uipb.GetRoutingStrategyRequest{}); status.Code(err) != codes.Unavailable {
		t.Fatalf("expected unavailable when admin missing, got %v", err)
	}

	topo := topology.NewTopology()
	go topo.Run()
	t.Cleanup(topo.Stop)

	admin := process.NewAdmin(context.Background(), nil, topo, nil, nil, topology.PlannerMetricsSnapshot{}, nil, nil, nil, nil, nil)
	svc := &service{admin: admin}
	resp, err := svc.GetRoutingStrategy(context.Background(), &uipb.GetRoutingStrategyRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.GetStrategy() != uipb.RoutingStrategy_ROUTING_STRATEGY_HOPS {
		t.Fatalf("expected default hops strategy, got %v", resp.GetStrategy())
	}

	if _, err := svc.SetRoutingStrategy(context.Background(), &uipb.SetRoutingStrategyRequest{
		Strategy: uipb.RoutingStrategy_ROUTING_STRATEGY_LATENCY,
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	resp, err = svc.GetRoutingStrategy(context.Background(), &uipb.GetRoutingStrategyRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.GetStrategy() != uipb.RoutingStrategy_ROUTING_STRATEGY_LATENCY {
		t.Fatalf("expected latency strategy, got %v", resp.GetStrategy())
	}
}

func TestStartSocksProxyValidation(t *testing.T) {
	svc := &service{admin: &process.Admin{}}
	if _, err := svc.StartSocksProxy(context.Background(), nil); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected invalid argument for nil request")
	}
	if _, err := svc.StartSocksProxy(context.Background(), &uipb.StartSocksProxyRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected invalid argument for missing target")
	}
	if _, err := svc.StartSocksProxy(context.Background(), &uipb.StartSocksProxyRequest{
		TargetUuid: "node-1",
		Auth:       uipb.SocksProxyAuth_SOCKS_PROXY_AUTH_USERPASS,
		Username:   "",
		Password:   "secret",
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected invalid argument for missing username")
	}
	resp, err := svc.StartSocksProxy(context.Background(), &uipb.StartSocksProxyRequest{
		TargetUuid: "node-1",
		Auth:       uipb.SocksProxyAuth_SOCKS_PROXY_AUTH_USERPASS,
		Username:   "alice",
		Password:   "secret",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	handle := resp.GetHandle()
	if handle.GetKind() != "socks" || handle.GetSessionId() == "" || handle.GetOptions()["auth"] != "userpass" || handle.GetOptions()["username"] != "alice" {
		t.Fatalf("unexpected handle: %+v", handle)
	}
}

func TestStartSshSessionValidation(t *testing.T) {
	svc := &service{admin: &process.Admin{}}
	if _, err := svc.StartSshSession(context.Background(), nil); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected invalid argument for nil request")
	}
	if _, err := svc.StartSshSession(context.Background(), &uipb.StartSshSessionRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected invalid argument for missing fields")
	}
	resp, err := svc.StartSshSession(context.Background(), &uipb.StartSshSessionRequest{
		TargetUuid: "node-1",
		ServerAddr: "10.0.0.2:22",
		AuthMethod: uipb.SshSessionAuthMethod_SSH_SESSION_AUTH_METHOD_PASSWORD,
		Username:   "alice",
		Password:   "secret",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	handle := resp.GetHandle()
	if handle.GetKind() != "ssh" || handle.GetSessionId() == "" || handle.GetOptions()["addr"] != "10.0.0.2:22" || handle.GetOptions()["username"] != "alice" || handle.GetOptions()["method"] != "1" {
		t.Fatalf("unexpected handle: %+v", handle)
	}
}

func TestCompleteTransferValidation(t *testing.T) {
	svcNil := &service{}
	if _, err := svcNil.CompleteTransfer(context.Background(), &dataplanepb.CompleteTransferRequest{Token: "tok", TargetUuid: "node-1"}); status.Code(err) != codes.Unavailable {
		t.Fatalf("expected unavailable when dataplane missing, got %v", err)
	}
	svc := &service{dataplane: &dataplane.Manager{}}
	if _, err := svc.CompleteTransfer(context.Background(), nil); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected invalid argument for nil request")
	}
	if _, err := svc.CompleteTransfer(context.Background(), &dataplanepb.CompleteTransferRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected invalid argument for missing token")
	}
	if _, err := svc.CompleteTransfer(context.Background(), &dataplanepb.CompleteTransferRequest{Token: "tok"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected invalid argument for missing target uuid")
	}
	if _, err := svc.CompleteTransfer(context.Background(), &dataplanepb.CompleteTransferRequest{Token: "tok", TargetUuid: "node-1", Bytes: -1}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected invalid argument for negative bytes")
	}
	if _, err := svc.CompleteTransfer(context.Background(), &dataplanepb.CompleteTransferRequest{Token: "tok", TargetUuid: "node-1", Bytes: 10}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCompleteProxyValidation(t *testing.T) {
	svcNil := &service{}
	if _, err := svcNil.CompleteProxy(context.Background(), &dataplanepb.CompleteProxyRequest{Token: "tok", TargetUuid: "node-1"}); status.Code(err) != codes.Unavailable {
		t.Fatalf("expected unavailable when dataplane missing, got %v", err)
	}
	svc := &service{dataplane: &dataplane.Manager{}}
	if _, err := svc.CompleteProxy(context.Background(), nil); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected invalid argument for nil request")
	}
	if _, err := svc.CompleteProxy(context.Background(), &dataplanepb.CompleteProxyRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected invalid argument for missing token")
	}
	if _, err := svc.CompleteProxy(context.Background(), &dataplanepb.CompleteProxyRequest{Token: "tok"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected invalid argument for missing target uuid")
	}
	if _, err := svc.CompleteProxy(context.Background(), &dataplanepb.CompleteProxyRequest{Token: "tok", TargetUuid: "node-1"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestConvertNetworkInfos(t *testing.T) {
	infos := []process.NetworkInfo{{ID: "net-a", Entries: []string{"node-a"}}, {ID: "net-b"}}
	converted := convertNetworkInfos(infos, "net-a")
	if len(converted) != 2 {
		t.Fatalf("expected two entries")
	}
	if !converted[0].GetActive() || converted[1].GetActive() {
		t.Fatalf("expected only first network active")
	}
	if converted[0].GetNetworkId() != "net-a" || len(converted[0].GetTargetUuids()) != 1 {
		t.Fatalf("unexpected conversion result: %+v", converted[0])
	}
}

func TestPruneOfflineHandler(t *testing.T) {
	svcNil := &service{}
	if _, err := svcNil.PruneOffline(context.Background(), &uipb.PruneOfflineRequest{}); status.Code(err) != codes.Unavailable {
		t.Fatalf("expected unavailable when admin missing, got %v", err)
	}
	svcOK := &service{pruneOfflineOverride: func() (int, error) { return 3, nil }}
	resp, err := svcOK.PruneOffline(context.Background(), &uipb.PruneOfflineRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.GetRemoved() != 3 {
		t.Fatalf("expected removed count 3, got %d", resp.GetRemoved())
	}
	svcErr := &service{pruneOfflineOverride: func() (int, error) { return 0, errors.New("boom") }}
	if _, err := svcErr.PruneOffline(context.Background(), &uipb.PruneOfflineRequest{}); status.Code(err) != codes.Internal {
		t.Fatalf("expected internal error, got %v", err)
	}
}

func TestListNodesHandler(t *testing.T) {
	svc := &service{admin: &process.Admin{}}
	if _, err := svc.ListNodes(context.Background(), nil); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected invalid argument for nil request, got %v", err)
	}
	svcNil := &service{}
	if _, err := svcNil.ListNodes(context.Background(), &uipb.ListNodesRequest{}); status.Code(err) != codes.Unavailable {
		t.Fatalf("expected unavailable when admin missing, got %v", err)
	}
	now := time.Now().Add(-time.Minute)
	svcOK := &service{
		admin: &process.Admin{},
		topologySnapshotOverride: func(target, network string) topology.UISnapshot {
			return topology.UISnapshot{
				Nodes:       []topology.UINodeSnapshot{{UUID: "node-a"}},
				LastUpdated: now,
			}
		},
	}
	resp, err := svcOK.ListNodes(context.Background(), &uipb.ListNodesRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.GetNodes()) != 1 || resp.GetNodes()[0].GetUuid() != "node-a" {
		t.Fatalf("unexpected nodes response: %+v", resp.GetNodes())
	}
}

func TestGetTopologyHandler(t *testing.T) {
	svc := &service{admin: &process.Admin{}}
	if _, err := svc.GetTopology(context.Background(), nil); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected invalid argument for nil request, got %v", err)
	}
	svcNil := &service{}
	if _, err := svcNil.GetTopology(context.Background(), &uipb.GetTopologyRequest{}); status.Code(err) != codes.Unavailable {
		t.Fatalf("expected unavailable when admin missing, got %v", err)
	}
	now := time.Now()
	svcOK := &service{
		admin: &process.Admin{},
		topologySnapshotOverride: func(entry, network string) topology.UISnapshot {
			return topology.UISnapshot{
				Nodes:       []topology.UINodeSnapshot{{UUID: "node-a"}},
				Edges:       []topology.UIEdgeSnapshot{{ParentUUID: "node-a", ChildUUID: "node-b", Supplemental: true}},
				LastUpdated: now,
			}
		},
	}
	resp, err := svcOK.GetTopology(context.Background(), &uipb.GetTopologyRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.GetEdges()) != 1 || resp.GetEdges()[0].GetParentUuid() != "node-a" || resp.GetEdges()[0].GetChildUuid() != "node-b" {
		t.Fatalf("unexpected edges response: %+v", resp.GetEdges())
	}
	if resp.GetLastUpdated() == "" {
		t.Fatalf("expected last updated timestamp")
	}
}

func TestGetMetricsHandler(t *testing.T) {
	svcNil := &service{}
	if _, err := svcNil.GetMetrics(context.Background(), nil); status.Code(err) != codes.Unavailable {
		t.Fatalf("expected unavailable when admin missing, got %v", err)
	}
	svc := &service{
		admin: &process.Admin{},
		routerStatsOverride: func() map[uint16]bus.RouterCounter {
			return map[uint16]bus.RouterCounter{
				5: {Dispatched: 3, Errors: 1, Drops: 2},
			}
		},
		reconnectStatsOverride: func() process.ReconnectStatsView {
			return process.ReconnectStatsView{Attempts: 4, Success: 2, Failures: 1, LastError: "boom"}
		},
		dtnMetricsOverride: func() (dtn.QueueStats, time.Time) {
			return dtn.QueueStats{Total: 2, OldestID: "bundle-x"}, time.Unix(0, 0)
		},
		dtnStatsOverride: func() (uint64, uint64, uint64, uint64) {
			return 10, 8, 1, 2
		},
	}
	resp, err := svc.GetMetrics(context.Background(), &uipb.GetMetricsRequest{IncludeRouter: true})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.GetRouterMetrics()) != 1 || resp.GetRouterMetrics()[0].GetMessageType() != 5 {
		t.Fatalf("unexpected router metrics: %+v", resp.GetRouterMetrics())
	}
	if resp.GetReconnectMetrics() != nil {
		t.Fatalf("expected reconnect metrics to be omitted when not requested")
	}
	respAll, err := svc.GetMetrics(context.Background(), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if respAll.GetReconnectMetrics() == nil || respAll.GetReconnectMetrics().GetLastError() != "boom" {
		t.Fatalf("expected reconnect metrics in default response")
	}
	if respAll.GetDtnMetrics() == nil || respAll.GetDtnMetrics().GetGlobalQueue().GetTotal() != 2 {
		t.Fatalf("expected dtn metrics present: %+v", respAll.GetDtnMetrics())
	}
}

func TestGetDtnQueueStatsHandler(t *testing.T) {
	svcNil := &service{}
	if _, err := svcNil.GetDtnQueueStats(context.Background(), &uipb.GetDtnQueueStatsRequest{}); status.Code(err) != codes.Unavailable {
		t.Fatalf("expected unavailable when admin missing, got %v", err)
	}
	svc := &service{
		admin: &process.Admin{},
		queueStatsOverride: func(target string) (dtn.QueueStats, error) {
			return dtn.QueueStats{Total: 5, Ready: 2, OldestID: "bundle-1", OldestTarget: "node-a"}, nil
		},
	}
	resp, err := svc.GetDtnQueueStats(context.Background(), &uipb.GetDtnQueueStatsRequest{TargetUuid: "node-a"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.GetStats() == nil || resp.GetStats().GetTotal() != 5 {
		t.Fatalf("unexpected stats: %+v", resp.GetStats())
	}
}

func TestListDtnBundlesHandler(t *testing.T) {
	svcNil := &service{}
	if _, err := svcNil.ListDtnBundles(context.Background(), &uipb.ListDtnBundlesRequest{}); status.Code(err) != codes.Unavailable {
		t.Fatalf("expected unavailable when admin missing, got %v", err)
	}
	svc := &service{
		admin: &process.Admin{},
		listBundlesOverride: func(target string, limit int) ([]dtn.BundleSummary, error) {
			return []dtn.BundleSummary{{ID: "bundle-1", Target: "node-a", Priority: dtn.PriorityHigh, Attempts: 2}}, nil
		},
	}
	resp, err := svc.ListDtnBundles(context.Background(), &uipb.ListDtnBundlesRequest{Limit: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.GetBundles()) != 1 || resp.GetBundles()[0].GetPriority() != uipb.DtnPriority_DTN_PRIORITY_HIGH {
		t.Fatalf("unexpected bundles: %+v", resp.GetBundles())
	}
}

func TestEnqueueDtnPayloadHandler(t *testing.T) {
	svc := &service{admin: &process.Admin{}}
	if _, err := svc.EnqueueDtnPayload(context.Background(), nil); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected invalid argument for nil request, got %v", err)
	}
	if _, err := svc.EnqueueDtnPayload(context.Background(), &uipb.EnqueueDtnPayloadRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected invalid argument for missing target, got %v", err)
	}
	if _, err := svc.EnqueueDtnPayload(context.Background(), &uipb.EnqueueDtnPayloadRequest{TargetUuid: "node", Payload: ""}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected invalid argument for empty payload, got %v", err)
	}
	called := false
	svc.enqueueDtnOverride = func(target, data string, priority dtn.Priority, ttl time.Duration) (string, error) {
		called = true
		if target != "node" || priority != dtn.PriorityHigh {
			t.Fatalf("unexpected enqueue params: %s %v", target, priority)
		}
		return "bundle-123", nil
	}
	resp, err := svc.EnqueueDtnPayload(context.Background(), &uipb.EnqueueDtnPayloadRequest{TargetUuid: "node", Payload: "hello", Priority: uipb.DtnPriority_DTN_PRIORITY_HIGH, TtlSeconds: 30})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called || resp.GetBundleId() != "bundle-123" {
		t.Fatalf("enqueue override not invoked correctly: %+v", resp)
	}
}

func TestDtnPolicyHandlers(t *testing.T) {
	svc := &service{admin: &process.Admin{}}
	if _, err := svc.UpdateDtnPolicy(context.Background(), nil); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected invalid argument for nil request, got %v", err)
	}
	if _, err := svc.UpdateDtnPolicy(context.Background(), &uipb.UpdateDtnPolicyRequest{Key: "", Value: "1"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected invalid argument for missing key, got %v", err)
	}
	svc.dtnPolicyOverride = func() map[string]string {
		return map[string]string{"max_inflight_per_target": "4"}
	}
	called := false
	svc.setDtnPolicyOverride = func(key, value string) error {
		called = true
		if key != "max_inflight_per_target" || value != "8" {
			return fmt.Errorf("bad args")
		}
		return nil
	}
	resp, err := svc.UpdateDtnPolicy(context.Background(), &uipb.UpdateDtnPolicyRequest{Key: "max_inflight_per_target", Value: "8"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called || resp.GetEntries()["max_inflight_per_target"] != "4" {
		t.Fatalf("unexpected policy response: %+v", resp.GetEntries())
	}
	respGet, err := svc.GetDtnPolicy(context.Background(), &uipb.GetDtnPolicyRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(respGet.GetEntries()) == 0 {
		t.Fatalf("expected policy entries")
	}
}
