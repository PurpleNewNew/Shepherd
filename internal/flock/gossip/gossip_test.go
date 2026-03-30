package gossip

import (
	"encoding/json"
	"testing"
	"time"

	"codeberg.org/agnoie/shepherd/protocol"
)

func TestProcessIncomingMessageDeduplicates(t *testing.T) {
	cfg := &protocol.GossipConfig{MaxTTL: 3}
	gm := NewGossipManager("local-node", "", 0, cfg)

	payload := []byte(`{"sender_uuid":"peer-node","sender_ip":"10.0.0.2","sender_port":1234,"timestamp":1700000000}`)

	gm.ProcessIncomingMessage(protocol.GOSSIP_DISCOVER, payload, "peer-node")
	gm.ProcessIncomingMessage(protocol.GOSSIP_DISCOVER, payload, "peer-node")

	select {
	case <-gm.messageChan:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected gossip message to be enqueued")
	}

	select {
	case <-gm.messageChan:
		t.Fatalf("duplicate gossip message passed through")
	default:
	}
}

func TestForwardGossipMessageDeduplicates(t *testing.T) {
	cfg := &protocol.GossipConfig{MaxTTL: 5}
	gm := NewGossipManager("local-node", "", 0, cfg)

	update := &protocol.GossipUpdate{
		TTL:        3,
		NodeData:   []byte(`{"uuid":"peer-node"}`),
		SenderUUID: "peer-node",
		Timestamp:  1700000000,
	}
	data, err := json.Marshal(update)
	if err != nil {
		t.Fatalf("failed to marshal gossip update: %v", err)
	}

	gm.ForwardGossipMessage(protocol.GOSSIP_UPDATE, data, "peer-node")
	gm.ForwardGossipMessage(protocol.GOSSIP_UPDATE, data, "peer-node")

	gm.mutex.RLock()
	count := len(gm.messageBuffer)
	gm.mutex.RUnlock()
	if count != 1 {
		t.Fatalf("expected gossip buffer to hold 1 entry, got %d", count)
	}
}
