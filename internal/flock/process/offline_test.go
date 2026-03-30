package process

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"codeberg.org/agnoie/shepherd/internal/flock/initial"
	"codeberg.org/agnoie/shepherd/pkg/global"
	"codeberg.org/agnoie/shepherd/pkg/share"
	"codeberg.org/agnoie/shepherd/protocol"
)

func TestReconHandshakeSecret(t *testing.T) {
	agent := &Agent{}
	options := &initial.Options{Secret: "s3cr3t", PreAuthToken: "test-preauth-token"}
	if got := agent.reconHandshakeSecret(options); got != "s3cr3t" {
		t.Fatalf("expected secret, got %q", got)
	}

	options.TlsEnable = true
	if got := agent.reconHandshakeSecret(options); got != "s3cr3t" {
		t.Fatalf("expected secret preserved under TLS, got %q", got)
	}

	store := global.NewStoreWithTransports(nil)
	if err := store.SetPreAuthToken("test-preauth-token"); err != nil {
		t.Fatalf("set preauth token: %v", err)
	}
	store.SetTLSEnabled(true)
	agent.store = store
	options.TlsEnable = false
	if got := agent.reconHandshakeSecret(options); got != "s3cr3t" {
		t.Fatalf("expected secret preserved when store reports TLS, got %q", got)
	}
}

func TestApplyNegotiationResult(t *testing.T) {
	prevUp, prevDown := protocol.DefaultTransports().Upstream(), protocol.DefaultTransports().Downstream()
	protocol.SetDefaultTransports("http", prevDown)
	defer protocol.SetDefaultTransports(prevUp, prevDown)

	options := &initial.Options{
		Secret:        "s3cr3t",
		TlsEnable:     true,
		HTTPUserAgent: "test",
	}

	negotiation := protocol.Negotiation{Flags: protocol.FlagSupportChunked}
	if err := applyNegotiationResult(options, negotiation); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if options.Secret != "s3cr3t" {
		t.Fatalf("expected secret preserved for TLS, got %q", options.Secret)
	}

	negotiation = protocol.Negotiation{Flags: 0}
	if err := applyNegotiationResult(options, negotiation); err == nil {
		t.Fatalf("expected error when chunked flag missing")
	}
}

func TestManualActiveReconnectIntegration(t *testing.T) {
	secret := "manual-active-reconnect"
	token := share.GeneratePreAuthToken(secret)

	prevUp, prevDown := protocol.DefaultTransports().Upstream(), protocol.DefaultTransports().Downstream()
	protocol.SetDefaultTransports("raw", "raw")
	defer protocol.SetDefaultTransports(prevUp, prevDown)

	store := global.NewStoreWithTransports(nil)
	if err := store.SetPreAuthToken(token); err != nil {
		t.Fatalf("set preauth token: %v", err)
	}

	agentUUID := "AGENT-RECONNECT"
	store.InitializeComponent(nil, secret, agentUUID, "raw", "raw")
	store.UpdateProtocol(agentUUID, protocol.CurrentProtocolVersion, protocol.DefaultProtocolFlags)

	listenAddr := acquireFreePort(t)

	opts := &initial.Options{
		Mode:         initial.NORMAL_ACTIVE,
		Secret:       secret,
		Connect:      listenAddr,
		PreAuthToken: token,
	}

	ctx := context.Background()
	agent := NewAgent(ctx, opts, store, nil)
	agent.UUID = agentUUID

	adminReady := runTestAdminPassive(t, listenAddr, token, secret, agentUUID)

	reconnectCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := agent.manualActiveReconnect(reconnectCtx, opts, nil, 2, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("manualActiveReconnect failed: %v", err)
	}
	if conn == nil {
		t.Fatalf("manualActiveReconnect returned nil connection")
	}
	defer conn.Close()

	adminRes := <-adminReady
	if adminRes.err != nil {
		t.Fatalf("admin passive handshake failed: %v", adminRes.err)
	}
	defer adminRes.conn.Close()
	if adminRes.nego == nil {
		t.Fatalf("admin negotiation nil")
	}

	version, flags, ok := store.ProtocolFor(agentUUID)
	if !ok {
		t.Fatalf("expected store protocol metadata for %s", agentUUID)
	}
	if version != protocol.CurrentProtocolVersion {
		t.Fatalf("unexpected protocol version: want %d got %d", protocol.CurrentProtocolVersion, version)
	}
	if flags&protocol.FlagSupportChunked == 0 {
		t.Fatalf("expected chunked flag in store metadata, got %#x", flags)
	}
	if opts.Secret != secret {
		t.Fatalf("agent options secret mutated, want %q got %q", secret, opts.Secret)
	}
}

func acquireFreePort(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to acquire free port: %v", err)
	}
	defer listener.Close()
	return listener.Addr().String()
}

type adminHandshakeResult struct {
	conn net.Conn
	nego *protocol.Negotiation
	err  error
}

func runTestAdminPassive(t *testing.T, listenAddr, token, secret, agentUUID string) <-chan adminHandshakeResult {
	t.Helper()
	result := make(chan adminHandshakeResult, 1)
	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		result <- adminHandshakeResult{err: fmt.Errorf("listen: %w", err)}
		return result
	}

	go func() {
		defer listener.Close()

		conn, err := listener.Accept()
		if err != nil {
			result <- adminHandshakeResult{err: fmt.Errorf("accept: %w", err)}
			return
		}
		param := &protocol.NegParam{Conn: conn}
		proto := protocol.NewDownProto(param)
		if err := proto.SNegotiate(); err != nil {
			conn.Close()
			result <- adminHandshakeResult{err: fmt.Errorf("negotiate: %w", err)}
			return
		}

		if err := share.PassivePreAuth(conn, token); err != nil {
			conn.Close()
			result <- adminHandshakeResult{err: fmt.Errorf("preauth: %w", err)}
			return
		}

		handshakeSecret := secret
		negotiation := protocol.Negotiation{
			Version: protocol.CurrentProtocolVersion,
			Flags:   protocol.DefaultProtocolFlags,
		}

		respHI := &protocol.HIMess{
			GreetingLen:  uint16(len("Keep slient")),
			Greeting:     "Keep slient",
			UUIDLen:      uint16(len(protocol.ADMIN_UUID)),
			UUID:         protocol.ADMIN_UUID,
			IsAdmin:      1,
			IsReconnect:  0,
			ProtoVersion: negotiation.Version,
			ProtoFlags:   negotiation.Flags,
		}
		hiHeader := &protocol.Header{
			Version:     negotiation.Version,
			Flags:       negotiation.Flags,
			Sender:      protocol.ADMIN_UUID,
			Accepter:    protocol.TEMP_UUID,
			MessageType: protocol.HI,
			RouteLen:    uint32(len([]byte(protocol.TEMP_ROUTE))),
			Route:       protocol.TEMP_ROUTE,
		}
		sMessage := protocol.NewDownMsg(conn, handshakeSecret, protocol.ADMIN_UUID)
		protocol.SetMessageMeta(sMessage, negotiation.Version, negotiation.Flags)
		protocol.ConstructMessage(sMessage, hiHeader, respHI, false)
		sMessage.SendMessage()

		uuidMess := &protocol.UUIDMess{
			UUIDLen:      uint16(len(agentUUID)),
			UUID:         agentUUID,
			ProtoVersion: negotiation.Version,
			ProtoFlags:   negotiation.Flags,
		}
		uuidHeader := &protocol.Header{
			Version:     negotiation.Version,
			Flags:       negotiation.Flags,
			Sender:      protocol.ADMIN_UUID,
			Accepter:    protocol.TEMP_UUID,
			MessageType: protocol.UUID,
			RouteLen:    uint32(len([]byte(protocol.TEMP_ROUTE))),
			Route:       protocol.TEMP_ROUTE,
		}
		uuidMsg := protocol.NewDownMsg(conn, handshakeSecret, protocol.ADMIN_UUID)
		protocol.SetMessageMeta(uuidMsg, negotiation.Version, negotiation.Flags)
		protocol.ConstructMessage(uuidMsg, uuidHeader, uuidMess, false)
		uuidMsg.SendMessage()

		result <- adminHandshakeResult{conn: conn, nego: &negotiation, err: nil}
	}()
	return result
}
