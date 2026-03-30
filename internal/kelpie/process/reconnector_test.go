package process

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"codeberg.org/agnoie/shepherd/internal/kelpie/initial"
	"codeberg.org/agnoie/shepherd/internal/kelpie/printer"
	"codeberg.org/agnoie/shepherd/protocol"
)

func TestReconnectorAttemptSuccess(t *testing.T) {
	opt := &initial.Options{Mode: initial.NORMAL_ACTIVE}
	rec := NewReconnector(context.Background(), opt, nil)

	server, client := net.Pipe()
	defer client.Close()
	defer server.Close()

	connUpdated := false
	topoCalled := false
	var protoSeen *protocol.Negotiation

	rec.WithDialer(func(*initial.Options) (net.Conn, *protocol.Negotiation, error) {
		return server, &protocol.Negotiation{Version: 2, Flags: 7}, nil
	}).WithConnUpdater(func(conn net.Conn) {
		if conn == server {
			connUpdated = true
		}
	}).WithTopoUpdater(func() error {
		topoCalled = true
		return nil
	}).WithProtocolUpdater(func(nego *protocol.Negotiation) {
		protoSeen = nego
	})

	conn, nego, err := rec.Attempt(context.Background(), opt)
	if err != nil {
		t.Fatalf("unexpected reconnect error: %v", err)
	}
	if conn != server {
		t.Fatalf("expected returned connection to be server side")
	}
	if nego == nil || nego.Version != 2 || nego.Flags != 7 {
		t.Fatalf("unexpected negotiation: %+v", nego)
	}
	if !connUpdated {
		t.Fatalf("connection updater not invoked")
	}
	if !topoCalled {
		t.Fatalf("topology updater not invoked")
	}
	if protoSeen == nil || protoSeen != nego {
		t.Fatalf("protocol updater did not receive negotiation")
	}

	stats := rec.Stats()
	if stats.Attempts != 1 || stats.Success != 1 || stats.Failures != 0 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}

func TestReconnectorAttemptFailure(t *testing.T) {
	opt := &initial.Options{Mode: initial.NORMAL_ACTIVE}
	rec := NewReconnector(context.Background(), opt, nil)
	rec.WithDialer(func(*initial.Options) (net.Conn, *protocol.Negotiation, error) {
		return nil, nil, errors.New("dial failed")
	}).WithConnUpdater(func(net.Conn) {
		// no-op for failure path
	})

	restoreFail := printer.TestOnlyReplaceFailPrinter(func(string, ...interface{}) {})
	defer restoreFail()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	if _, _, err := rec.Attempt(ctx, opt); err == nil {
		t.Fatalf("expected reconnect failure")
	}
	stats := rec.Stats()
	if stats.Attempts == 0 || stats.Failures == 0 {
		t.Fatalf("expected attempts and failures recorded: %+v", stats)
	}
}
