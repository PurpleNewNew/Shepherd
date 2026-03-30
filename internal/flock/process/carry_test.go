package process

import (
	"errors"
	"testing"
	"time"

	"codeberg.org/agnoie/shepherd/protocol"
)

func TestCarryBackoff(t *testing.T) {
	if got := carryBackoff(0); got != carryRetryBase {
		t.Fatalf("expected base backoff %s, got %s", carryRetryBase, got)
	}
	if got := carryBackoff(1); got != 4*time.Second {
		t.Fatalf("expected backoff 4s, got %s", got)
	}
	if got := carryBackoff(6); got != carryRetryMax {
		t.Fatalf("expected backoff cap %s, got %s", carryRetryMax, got)
	}
}

func TestShouldCarryRetry(t *testing.T) {
	agent := &Agent{}
	msg := &ChildrenMess{cHeader: &protocol.Header{MessageType: uint16(protocol.DTN_DATA)}}
	if !agent.shouldCarryRetry(ErrNoRouteToChild, msg) {
		t.Fatalf("expected retry for no route")
	}
	if !agent.shouldCarryRetry(ErrNoUpstreamSession, msg) {
		t.Fatalf("expected retry for no upstream session")
	}
	if agent.shouldCarryRetry(errors.New("other"), msg) {
		t.Fatalf("unexpected retry for unrelated error")
	}
	msg.cHeader.MessageType = uint16(protocol.SUPPLINKREQ)
	if !agent.shouldCarryRetry(ErrNoRouteToChild, msg) {
		t.Fatalf("expected retry for supplink request (no route)")
	}
	msg.cHeader.MessageType = uint16(protocol.NODECONNINFO)
	if agent.shouldCarryRetry(ErrNoRouteToChild, msg) {
		t.Fatalf("unexpected retry for non-eligible message")
	}
}
