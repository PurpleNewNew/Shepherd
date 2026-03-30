package protocol

import (
	"net"
	"testing"
	"time"

	"codeberg.org/agnoie/shepherd/pkg/crypto"
)

func newRawMessagePair() (*RawMessage, *RawMessage) {
	left, right := net.Pipe()
	secret := crypto.KeyPadding([]byte("shepherd-secret"))
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

func TestGossipUpdateSerializationRoundTrip(t *testing.T) {
	sender, receiver := newRawMessagePair()
	defer sender.Conn.Close()
	defer receiver.Conn.Close()

	nodeData := []byte(`{"uuid":"node-a","ip":"10.0.0.1"}`)
	payload := &GossipUpdate{
		TTL:           5,
		NodeData:      nodeData,
		NodeDataLen:   uint64(len(nodeData)),
		SenderUUID:    "node-a-uuid",
		SenderUUIDLen: uint16(len("node-a-uuid")),
		Timestamp:     time.Now().Unix(),
	}

	header := &Header{
		Sender:      ADMIN_UUID,
		Accepter:    ADMIN_UUID,
		MessageType: uint16(GOSSIP_UPDATE),
		RouteLen:    0,
		Route:       "",
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		ConstructMessage(sender, header, payload, false)
		sender.SendMessage()
	}()

	outHeader, message, err := DestructMessage(receiver)
	if err != nil {
		t.Fatalf("failed to decode gossip update: %v", err)
	}
	if outHeader.MessageType != uint16(GOSSIP_UPDATE) {
		t.Fatalf("unexpected message type: %d", outHeader.MessageType)
	}

	update, ok := message.(*GossipUpdate)
	if !ok {
		t.Fatalf("decoded payload type mismatch: %T", message)
	}

	if update.TTL != payload.TTL {
		t.Fatalf("TTL mismatch: want %d got %d", payload.TTL, update.TTL)
	}
	if string(update.NodeData) != string(payload.NodeData) {
		t.Fatalf("node data mismatch: %s vs %s", string(payload.NodeData), string(update.NodeData))
	}
	if update.SenderUUID != payload.SenderUUID {
		t.Fatalf("sender uuid mismatch: %s vs %s", payload.SenderUUID, update.SenderUUID)
	}

	<-done
}

func TestGossipRouteUpdateSerializationWithBool(t *testing.T) {
	sender, receiver := newRawMessagePair()
	defer sender.Conn.Close()
	defer receiver.Conn.Close()

	payload := &GossipRouteUpdate{
		SenderUUID:     "admin-node",
		SenderUUIDLen:  uint16(len("admin-node")),
		TargetUUID:     "target-node",
		TargetUUIDLen:  uint16(len("target-node")),
		NextHopUUID:    "next-hop-node",
		NextHopUUIDLen: uint16(len("next-hop-node")),
		HopCount:       3,
		Weight:         7,
		IsReachable:    true,
		Timestamp:      time.Now().Unix(),
	}

	header := &Header{
		Sender:      ADMIN_UUID,
		Accepter:    ADMIN_UUID,
		MessageType: uint16(GOSSIP_ROUTE_UPDATE),
		RouteLen:    0,
		Route:       "",
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		ConstructMessage(sender, header, payload, false)
		sender.SendMessage()
	}()

	_, message, err := DestructMessage(receiver)
	if err != nil {
		t.Fatalf("failed to decode route update: %v", err)
	}

	route, ok := message.(*GossipRouteUpdate)
	if !ok {
		t.Fatalf("decoded payload type mismatch: %T", message)
	}

	if !route.IsReachable {
		t.Fatalf("expected IsReachable to remain true")
	}
	if route.HopCount != payload.HopCount {
		t.Fatalf("hop count mismatch: want %d got %d", payload.HopCount, route.HopCount)
	}
	if route.NextHopUUID != payload.NextHopUUID {
		t.Fatalf("next hop mismatch: %s vs %s", payload.NextHopUUID, route.NextHopUUID)
	}

	<-done
}
