package integration_test

import (
	"fmt"
	"net"
	"testing"
	"time"

	agentInitial "codeberg.org/agnoie/shepherd/internal/flock/initial"
	adminInitial "codeberg.org/agnoie/shepherd/internal/kelpie/initial"
	"codeberg.org/agnoie/shepherd/internal/kelpie/printer"
	"codeberg.org/agnoie/shepherd/internal/kelpie/topology"
	"codeberg.org/agnoie/shepherd/pkg/share"
	"codeberg.org/agnoie/shepherd/pkg/share/handshake"
	"codeberg.org/agnoie/shepherd/protocol"
)

func acquireFreePort(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to acquire free port: %v", err)
	}
	defer listener.Close()
	addr := listener.Addr().(*net.TCPAddr)
	return fmt.Sprintf("127.0.0.1:%d", addr.Port)
}

func TestAdminAgentHandshakeRegistersNode(t *testing.T) {
	secret := "integrate-handshake"
	token := share.GeneratePreAuthToken(secret)
	printer.InitPrinter()

	prevUp, prevDown := protocol.DefaultTransports().Upstream(), protocol.DefaultTransports().Downstream()
	protocol.SetDefaultTransports("raw", "raw")
	defer protocol.SetDefaultTransports(prevUp, prevDown)

	topo := topology.NewTopology()
	go topo.Run()
	t.Cleanup(topo.Stop)

	listenAddr := acquireFreePort(t)

	adminOpts := &adminInitial.Options{
		Mode:         adminInitial.NORMAL_PASSIVE,
		Secret:       secret,
		Listen:       listenAddr,
		PreAuthToken: token,
	}

	type result struct {
		conn net.Conn
		err  error
	}

	adminReady := make(chan result, 1)
	go func() {
		conn, _, err := adminInitial.NormalPassive(adminOpts, topo)
		adminReady <- result{conn: conn, err: err}
	}()

	time.Sleep(50 * time.Millisecond)

	agentConn, agentUUID, flags, err := performAgentHandshake(listenAddr, secret, token)
	if err != nil {
		t.Fatalf("agent handshake failed: %v", err)
	}
	defer agentConn.Close()
	if flags&protocol.FlagSupportChunked != 0 {
		t.Fatalf("unexpected chunked flag for raw handshake, got %#x", flags)
	}

	adminResult := <-adminReady
	if adminResult.err != nil {
		t.Fatalf("admin handshake failed: %v", adminResult.err)
	}
	defer adminResult.conn.Close()

	if err := topo.Enqueue(&topology.TopoTask{
		Mode: topology.GETUUIDNUM,
		UUID: agentUUID,
	}); err != nil {
		t.Fatalf("enqueue uuid lookup: %v", err)
	}
	getResult := <-topo.ResultChan
	if getResult.IDNum < 0 {
		t.Fatalf("handshake did not register agent node, uuid=%s", agentUUID)
	}
}

func TestAdminActiveHandshakeToAgentPassive(t *testing.T) {
	secret := "integrate-admin-active"
	token := share.GeneratePreAuthToken(secret)
	printer.InitPrinter()

	prevUp, prevDown := protocol.DefaultTransports().Upstream(), protocol.DefaultTransports().Downstream()
	protocol.SetDefaultTransports("raw", "raw")
	defer protocol.SetDefaultTransports(prevUp, prevDown)

	listenAddr := acquireFreePort(t)

	type agentResult struct {
		conn net.Conn
		uuid string
		err  error
	}
	agentReady := make(chan agentResult, 1)
	go func() {
		agentOpts := &agentInitial.Options{
			Mode:         agentInitial.NORMAL_PASSIVE,
			Secret:       secret,
			Listen:       listenAddr,
			Upstream:     "raw",
			Downstream:   "raw",
			PreAuthToken: token,
			TlsEnable:    false,
		}
		conn, uuid, _, err := agentInitial.NormalPassive(agentOpts)
		agentReady <- agentResult{conn: conn, uuid: uuid, err: err}
	}()

	time.Sleep(50 * time.Millisecond)

	topo := topology.NewTopology()
	go topo.Run()
	t.Cleanup(topo.Stop)

	adminOpts := &adminInitial.Options{
		Mode:         adminInitial.NORMAL_ACTIVE,
		Secret:       secret,
		Downstream:   "raw",
		Connect:      listenAddr,
		PreAuthToken: token,
		TlsEnable:    false,
	}

	adminConn, _, err := adminInitial.NormalActive(adminOpts, topo, nil)
	if err != nil {
		t.Fatalf("admin active handshake failed: %v", err)
	}
	defer adminConn.Close()

	var ar agentResult
	select {
	case ar = <-agentReady:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for agent handshake result")
	}
	if ar.err != nil {
		t.Fatalf("agent passive handshake failed: %v", ar.err)
	}
	defer ar.conn.Close()
	if ar.uuid == "" {
		t.Fatalf("agent uuid missing")
	}

	if err := topo.Enqueue(&topology.TopoTask{
		Mode: topology.GETUUIDNUM,
		UUID: ar.uuid,
	}); err != nil {
		t.Fatalf("enqueue uuid lookup: %v", err)
	}
	getResult := <-topo.ResultChan
	if getResult.IDNum < 0 {
		t.Fatalf("admin active handshake did not register agent node, uuid=%s", ar.uuid)
	}
}

func performAgentHandshake(addr, secret, token string) (net.Conn, string, uint16, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, "", 0, fmt.Errorf("dial: %w", err)
	}
	param := &protocol.NegParam{Conn: conn}
	proto := protocol.NewUpProto(param)
	if proto == nil {
		conn.Close()
		return nil, "", 0, fmt.Errorf("unsupported upstream protocol")
	}
	if err := proto.CNegotiate(); err != nil {
		conn.Close()
		return nil, "", 0, fmt.Errorf("negotiate: %w", err)
	}
	conn = param.Conn
	if err := share.ActivePreAuth(conn, token); err != nil {
		conn.Close()
		return nil, "", 0, fmt.Errorf("pre-auth: %w", err)
	}
	localFlags := protocol.DefaultProtocolFlags
	if protocol.DefaultTransports().Upstream() != "http" {
		localFlags &^= protocol.FlagSupportChunked
	}
	hi := &protocol.HIMess{
		GreetingLen: uint16(len("Shhh...")),
		Greeting:    "Shhh...",
		UUIDLen:     uint16(len(protocol.TEMP_UUID)),
		UUID:        protocol.TEMP_UUID,
		IsAdmin:     0,
		IsReconnect: 0,
		ProtoFlags:  localFlags,
	}
	header := &protocol.Header{
		Sender:      protocol.TEMP_UUID,
		Accepter:    protocol.ADMIN_UUID,
		MessageType: protocol.HI,
		RouteLen:    uint32(len([]byte(protocol.TEMP_ROUTE))),
		Route:       protocol.TEMP_ROUTE,
	}
	sMessage := protocol.NewUpMsg(conn, secret, protocol.TEMP_UUID)
	protocol.ConstructMessage(sMessage, header, hi, false)
	sMessage.SendMessage()

	rMessage := protocol.NewUpMsg(conn, secret, protocol.TEMP_UUID)
	fHeader, fMessage, err := protocol.DestructMessage(rMessage)
	if err != nil {
		conn.Close()
		return nil, "", 0, fmt.Errorf("handshake response: %w", err)
	}
	if fHeader.MessageType != protocol.HI {
		conn.Close()
		return nil, "", 0, fmt.Errorf("unexpected message type %d", fHeader.MessageType)
	}
	mmess := fMessage.(*protocol.HIMess)
	if !handshake.ValidGreeting(handshake.RoleAdmin, mmess.Greeting) || mmess.IsAdmin != 1 {
		conn.Close()
		return nil, "", 0, fmt.Errorf("invalid HI response")
	}

	sessionSecret := share.DeriveSessionSecret(secret, false)
	message, err := receiveUUID(conn, sessionSecret)
	if err != nil {
		conn.Close()
		return nil, "", 0, err
	}
	if message.ProtoFlags != localFlags {
		conn.Close()
		return nil, "", 0, fmt.Errorf("unexpected uuid flags %#x", message.ProtoFlags)
	}
	return conn, message.UUID, message.ProtoFlags, nil
}

func receiveUUID(conn net.Conn, secret string) (*protocol.UUIDMess, error) {
	rMessage := protocol.NewUpMsg(conn, secret, protocol.TEMP_UUID)
	fHeader, fMessage, err := protocol.DestructMessage(rMessage)
	if err != nil {
		return nil, fmt.Errorf("uuid response: %w", err)
	}
	if fHeader.MessageType != protocol.UUID {
		return nil, fmt.Errorf("unexpected uuid message type %d", fHeader.MessageType)
	}
	uuid, ok := fMessage.(*protocol.UUIDMess)
	if !ok {
		return nil, fmt.Errorf("unexpected uuid payload type %T", fMessage)
	}
	return uuid, nil
}
