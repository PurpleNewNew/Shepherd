package protocol

import (
	"net"
	"testing"
	"time"

	"codeberg.org/agnoie/shepherd/pkg/crypto"
)

func newSuppTestPair() (*RawMessage, *RawMessage) {
	left, right := net.Pipe()
	secret := crypto.KeyPadding([]byte("supp-secret"))
	sender := &RawMessage{
		Conn:         left,
		UUID:         ADMIN_UUID,
		CryptoSecret: secret,
	}
	receiver := &RawMessage{
		Conn:         right,
		UUID:         ADMIN_UUID,
		CryptoSecret: secret,
	}
	return sender, receiver
}

func TestSuppLinkRequestRoundTrip(t *testing.T) {
	sender, receiver := newSuppTestPair()
	defer sender.Conn.Close()
	defer receiver.Conn.Close()

	req := &SuppLinkRequest{
		RequestUUIDLen:   uint16(len("req-123")),
		RequestUUID:      "req-123",
		LinkUUIDLen:      uint16(len("link-abc")),
		LinkUUID:         "link-abc",
		InitiatorUUIDLen: uint16(len("node-init")),
		InitiatorUUID:    "node-init",
		TargetUUIDLen:    uint16(len("node-target")),
		TargetUUID:       "node-target",
		PeerIPLen:        uint16(len("10.0.0.5")),
		PeerIP:           "10.0.0.5",
		PeerPort:         8080,
		Action:           SuppLinkActionDial,
		Role:             SuppLinkRoleInitiator,
		Flags:            0,
		Timeout:          30,
	}

	header := &Header{
		Sender:      ADMIN_UUID,
		Accepter:    ADMIN_UUID,
		MessageType: uint16(SUPPLINKREQ),
		RouteLen:    0,
		Route:       "",
	}

	go func() {
		ConstructMessage(sender, header, req, false)
		sender.SendMessage()
	}()

	_, payload, err := DestructMessage(receiver)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	decoded, ok := payload.(*SuppLinkRequest)
	if !ok {
		t.Fatalf("unexpected payload %T", payload)
	}

	if decoded.LinkUUID != req.LinkUUID || decoded.TargetUUID != req.TargetUUID {
		t.Fatalf("decoded mismatch: %#v", decoded)
	}
	if decoded.PeerIP != req.PeerIP || decoded.PeerPort != req.PeerPort {
		t.Fatalf("peer info mismatch")
	}
}

func TestSuppLinkResponseRoundTrip(t *testing.T) {
	sender, receiver := newSuppTestPair()
	defer sender.Conn.Close()
	defer receiver.Conn.Close()

	resp := &SuppLinkResponse{
		RequestUUIDLen: uint16(len("req-xyz")),
		RequestUUID:    "req-xyz",
		LinkUUIDLen:    uint16(len("link-xyz")),
		LinkUUID:       "link-xyz",
		AgentUUIDLen:   uint16(len("agent-1")),
		AgentUUID:      "agent-1",
		PeerUUIDLen:    uint16(len("peer-1")),
		PeerUUID:       "peer-1",
		Role:           SuppLinkRoleResponder,
		Status:         1,
		MessageLen:     uint16(len("ok")),
		Message:        "ok",
		ListenIPLen:    uint16(len("10.0.0.8")),
		ListenIP:       "10.0.0.8",
		ListenPort:     9090,
	}

	header := &Header{
		Sender:      ADMIN_UUID,
		Accepter:    ADMIN_UUID,
		MessageType: uint16(SUPPLINKRESP),
		RouteLen:    0,
		Route:       "",
	}

	go func() {
		ConstructMessage(sender, header, resp, false)
		sender.SendMessage()
	}()

	_, payload, err := DestructMessage(receiver)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	decoded, ok := payload.(*SuppLinkResponse)
	if !ok {
		t.Fatalf("unexpected payload %T", payload)
	}

	if decoded.Status != resp.Status || decoded.Message != resp.Message {
		t.Fatalf("response mismatch: %#v", decoded)
	}
	if decoded.AgentUUID != resp.AgentUUID {
		t.Fatalf("agent uuid mismatch")
	}
}

func TestSuppLinkHeartbeatRoundTrip(t *testing.T) {
	sender, receiver := newSuppTestPair()
	defer sender.Conn.Close()
	defer receiver.Conn.Close()

	hb := &SuppLinkHeartbeat{
		LinkUUIDLen: uint16(len("link-hb")),
		LinkUUID:    "link-hb",
		PeerUUIDLen: uint16(len("peer-hb")),
		PeerUUID:    "peer-hb",
		Status:      1,
		Timestamp:   time.Now().Unix(),
	}

	header := &Header{
		Sender:      ADMIN_UUID,
		Accepter:    ADMIN_UUID,
		MessageType: uint16(SUPPLINKHEARTBEAT),
		RouteLen:    0,
		Route:       "",
	}

	go func() {
		ConstructMessage(sender, header, hb, false)
		sender.SendMessage()
	}()

	_, payload, err := DestructMessage(receiver)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	decoded, ok := payload.(*SuppLinkHeartbeat)
	if !ok {
		t.Fatalf("unexpected payload %T", payload)
	}

	if decoded.LinkUUID != hb.LinkUUID || decoded.Status != hb.Status {
		t.Fatalf("heartbeat mismatch: %#v", decoded)
	}
}

func TestSuppFailoverCommandRoundTrip(t *testing.T) {
	sender, receiver := newSuppTestPair()
	defer sender.Conn.Close()
	defer receiver.Conn.Close()

	cmd := &SuppFailoverCommand{
		LinkUUIDLen:   uint16(len("link-ff")),
		LinkUUID:      "link-ff",
		ParentUUIDLen: uint16(len("parent-1")),
		ParentUUID:    "parent-1",
		ChildUUIDLen:  uint16(len("child-1")),
		ChildUUID:     "child-1",
		Role:          1,
		Flags:         2,
	}

	header := &Header{
		Sender:      ADMIN_UUID,
		Accepter:    ADMIN_UUID,
		MessageType: uint16(SUPPFAILOVER),
		RouteLen:    0,
		Route:       "",
	}

	go func() {
		ConstructMessage(sender, header, cmd, false)
		sender.SendMessage()
	}()

	_, payload, err := DestructMessage(receiver)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	decoded, ok := payload.(*SuppFailoverCommand)
	if !ok {
		t.Fatalf("unexpected payload %T", payload)
	}
	if decoded.LinkUUID != cmd.LinkUUID || decoded.ParentUUID != cmd.ParentUUID || decoded.Flags != cmd.Flags {
		t.Fatalf("failover command mismatch: %#v", decoded)
	}
}

func TestSuppLinkPromoteRoundTrip(t *testing.T) {
	sender, receiver := newSuppTestPair()
	defer sender.Conn.Close()
	defer receiver.Conn.Close()

	promote := &SuppLinkPromote{
		LinkUUIDLen:   uint16(len("link-promote")),
		LinkUUID:      "link-promote",
		ParentUUIDLen: uint16(len("parent-x")),
		ParentUUID:    "parent-x",
		ChildUUIDLen:  uint16(len("child-x")),
		ChildUUID:     "child-x",
		Role:          0,
	}

	header := &Header{
		Sender:      ADMIN_UUID,
		Accepter:    ADMIN_UUID,
		MessageType: uint16(SUPPLINKPROMOTE),
		RouteLen:    0,
		Route:       "",
	}

	go func() {
		ConstructMessage(sender, header, promote, false)
		sender.SendMessage()
	}()

	_, payload, err := DestructMessage(receiver)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	decoded, ok := payload.(*SuppLinkPromote)
	if !ok {
		t.Fatalf("unexpected payload %T", payload)
	}
	if decoded.ChildUUID != promote.ChildUUID || decoded.Role != promote.Role {
		t.Fatalf("promote mismatch: %#v", decoded)
	}
}
