package process

import (
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"codeberg.org/agnoie/shepherd/internal/flock/initial"
	"codeberg.org/agnoie/shepherd/internal/flock/manager"
	"codeberg.org/agnoie/shepherd/pkg/bus"
	"codeberg.org/agnoie/shepherd/pkg/crypto"
	"codeberg.org/agnoie/shepherd/pkg/global"
	"codeberg.org/agnoie/shepherd/protocol"
)

func TestChangeRouteWithSupplementalSegment(t *testing.T) {
	header := &protocol.Header{Route: "NODE1#supp:NODE2", RouteLen: 0}
	next, preferSupp := changeRoute(header)
	if next != "NODE1" {
		t.Fatalf("expected first hop NODE1, got %s", next)
	}
	if !preferSupp {
		t.Fatalf("expected supplemental preference")
	}
	if header.Route != "NODE2" {
		t.Fatalf("expected remaining route NODE2, got %s", header.Route)
	}
}

func TestNextHopFromRouteSkipsSelf(t *testing.T) {
	header := &protocol.Header{Route: "SELF:CHILD", RouteLen: 0}
	next, preferSupp := nextHopFromRoute("SELF", header)
	if next != "CHILD" {
		t.Fatalf("expected next hop CHILD, got %s", next)
	}
	if preferSupp {
		t.Fatalf("expected preferSupp=false for CHILD hop")
	}
	if header.Route != "" || header.RouteLen != 0 {
		t.Fatalf("expected route to be fully consumed, got %q len=%d", header.Route, header.RouteLen)
	}
}

func TestParseRouteSegment(t *testing.T) {
	if hop, supp := parseRouteSegment("ABC#supp"); hop != "ABC" || !supp {
		t.Fatalf("expected supplemental hop ABC, got %s %v", hop, supp)
	}
	if hop, supp := parseRouteSegment("ABC"); hop != "ABC" || supp {
		t.Fatalf("expected normal hop, got %s %v", hop, supp)
	}
}

func TestShouldDispatchTerminalRoute(t *testing.T) {
	if shouldDispatchTerminalRoute(nil, "") {
		t.Fatalf("nil header should not dispatch")
	}
	if !shouldDispatchTerminalRoute(&protocol.Header{
		Accepter: protocol.TEMP_UUID,
		RouteLen: 0,
		Route:    "",
	}, "") {
		t.Fatalf("TEMP accepter with empty route should dispatch")
	}
	if shouldDispatchTerminalRoute(&protocol.Header{
		Accepter: protocol.TEMP_UUID,
		RouteLen: 0,
		Route:    "",
	}, "child") {
		t.Fatalf("non-empty next hop should not dispatch as terminal")
	}
	if shouldDispatchTerminalRoute(&protocol.Header{
		Accepter: protocol.ADMIN_UUID,
		RouteLen: 0,
		Route:    "",
	}, "") {
		t.Fatalf("non-TEMP accepter should not dispatch as terminal")
	}
	if shouldDispatchTerminalRoute(&protocol.Header{
		Accepter: protocol.TEMP_UUID,
		RouteLen: 5,
		Route:    "child",
	}, "") {
		t.Fatalf("non-empty route should not dispatch as terminal")
	}
}

func TestHandleSupplementalLocalDispatch(t *testing.T) {
	prevUp, prevDown := protocol.DefaultTransports().Upstream(), protocol.DefaultTransports().Downstream()
	protocol.SetDefaultTransports("raw", "raw")
	defer protocol.SetDefaultTransports(prevUp, prevDown)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	store := global.NewStoreWithTransports(nil)
	if err := store.SetPreAuthToken("test-preauth-token"); err != nil {
		t.Fatalf("set preauth token: %v", err)
	}
	agent := NewAgent(ctx, &initial.Options{PreAuthToken: "test-preauth-token"}, store, nil)
	agent.UUID = "AGENTNODE1"
	agent.mgr = manager.NewManager(store)
	go agent.mgr.Run(ctx)
	delivered := make(chan *protocol.DTNData, 1)
	agent.router = bus.NewRouter()
	agent.routerReady = true
	agent.router.Register(uint16(protocol.DTN_DATA), func(ctx context.Context, header *protocol.Header, payload interface{}) error {
		data, ok := payload.(*protocol.DTNData)
		if !ok {
			return nil
		}
		select {
		case delivered <- data:
		default:
		}
		return nil
	})

	upConn, upPeer := net.Pipe()
	defer upPeer.Close()
	defer upConn.Close()

	store.Reset()
	t.Cleanup(store.Reset)
	store.InitializeComponent(upConn, "secret", agent.UUID, "raw", "raw")
	agent.BindSession(store.ActiveSession())

	suppConnAgent, suppConnPeer := net.Pipe()
	defer suppConnPeer.Close()
	defer suppConnAgent.Close()

	dispatcher := agent.dispatcherFor("PEERAGENT1")
	go agent.handleDataFromSupplemental(suppConnAgent, "supp-link", "PEERAGENT1", dispatcher)

	sender := &protocol.RawMessage{
		Conn:         suppConnPeer,
		UUID:         "PEERAGENT1",
		CryptoSecret: crypto.KeyPadding([]byte("secret")),
	}
	payload := []byte("memo:test")
	header := &protocol.Header{
		Sender:      "PEERAGENT1",
		Accepter:    agent.UUID,
		MessageType: uint16(protocol.DTN_DATA),
		RouteLen:    0,
		Route:       "",
	}
	req := &protocol.DTNData{
		BundleID:    "bundle-test",
		BundleIDLen: uint16(len("bundle-test")),
		Payload:     payload,
		PayloadLen:  uint64(len(payload)),
	}

	protocol.ConstructMessage(sender, header, req, false)
	sender.SendMessage()

	select {
	case msg := <-delivered:
		if msg.BundleID != "bundle-test" {
			t.Fatalf("unexpected bundle id %q", msg.BundleID)
		}
	case <-time.After(time.Second):
		t.Fatalf("timeout waiting for supplemental dispatch")
	}
}

func TestHandleIncomingGossipWithoutParentSendsToAdmin(t *testing.T) {
	prevUp, prevDown := protocol.DefaultTransports().Upstream(), protocol.DefaultTransports().Downstream()
	protocol.SetDefaultTransports("raw", "raw")
	defer protocol.SetDefaultTransports(prevUp, prevDown)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	store := global.NewStoreWithTransports(nil)
	if err := store.SetPreAuthToken("test-preauth-token"); err != nil {
		t.Fatalf("set preauth token: %v", err)
	}

	agent := NewAgent(ctx, &initial.Options{PreAuthToken: "test-preauth-token"}, store, nil)
	agent.UUID = "ROOTNODE"

	upConn, upPeer := net.Pipe()
	defer upPeer.Close()
	defer upConn.Close()

	store.Reset()
	t.Cleanup(store.Reset)
	store.InitializeComponent(upConn, "secret", agent.UUID, "raw", "raw")
	agent.BindSession(store.ActiveSession())

	nodeInfo := &protocol.NodeInfo{
		UUID: "LEAFNODE",
		Memo: "memo-v1",
	}
	nodeData, err := json.Marshal(nodeInfo)
	if err != nil {
		t.Fatalf("marshal node info: %v", err)
	}
	update := &protocol.GossipUpdate{
		TTL:           6,
		NodeDataLen:   uint64(len(nodeData)),
		NodeData:      nodeData,
		SenderUUIDLen: uint16(len("LEAFNODE")),
		SenderUUID:    "LEAFNODE",
		Timestamp:     time.Now().Unix(),
	}

	done := make(chan struct{})
	go func() {
		agent.handleIncomingGossip(update, "LEAFNODE")
		close(done)
	}()

	_ = upPeer.SetReadDeadline(time.Now().Add(2 * time.Second))
	rMessage := protocol.NewDownMsg(upPeer, "secret", protocol.ADMIN_UUID)
	header, payload, err := protocol.DestructMessage(rMessage)
	if err != nil {
		t.Fatalf("read forwarded gossip update: %v", err)
	}
	if header.MessageType != uint16(protocol.GOSSIP_UPDATE) {
		t.Fatalf("expected gossip update message, got type=%d", header.MessageType)
	}
	if header.Accepter != protocol.ADMIN_UUID {
		t.Fatalf("expected accepter=%s, got %s", protocol.ADMIN_UUID, header.Accepter)
	}

	got, ok := payload.(*protocol.GossipUpdate)
	if !ok {
		t.Fatalf("expected *protocol.GossipUpdate payload, got %T", payload)
	}
	if got.SenderUUID != "LEAFNODE" {
		t.Fatalf("expected sender uuid LEAFNODE, got %s", got.SenderUUID)
	}
	if string(got.NodeData) != string(nodeData) {
		t.Fatalf("forwarded gossip payload mismatch")
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("handleIncomingGossip did not return")
	}
}

func TestMemoUpdateHandlerTriggersDeferredPropagationWithoutSession(t *testing.T) {
	agent := &Agent{
		gossipTrigger: make(chan struct{}, 1),
	}
	handler := agent.memoUpdateHandler()
	if err := handler(context.Background(), nil, &protocol.MyMemo{Memo: "memo-via-handler"}); err != nil {
		t.Fatalf("memo update handler returned error: %v", err)
	}
	if got := agent.Memo; got != "memo-via-handler" {
		t.Fatalf("memo not updated, got %q", got)
	}
	select {
	case <-agent.gossipTrigger:
	default:
		t.Fatalf("expected deferred gossip trigger signal")
	}
}

func TestApplyDTNPayloadMemoTriggersDeferredPropagationWithoutSession(t *testing.T) {
	agent := &Agent{
		gossipTrigger: make(chan struct{}, 1),
	}
	data := &protocol.DTNData{
		BundleID:    "bundle-1",
		BundleIDLen: uint16(len("bundle-1")),
		Payload:     []byte("memo:from-dtn"),
		PayloadLen:  uint64(len("memo:from-dtn")),
	}
	if err := agent.applyDTNPayload(data, nil); err != nil {
		t.Fatalf("applyDTNPayload returned error: %v", err)
	}
	if got := agent.Memo; got != "from-dtn" {
		t.Fatalf("memo not updated from DTN payload, got %q", got)
	}
	select {
	case <-agent.gossipTrigger:
	default:
		t.Fatalf("expected deferred gossip trigger signal")
	}
}
