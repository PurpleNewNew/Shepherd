package protocol

import (
	"net"
	"testing"

	"codeberg.org/agnoie/shepherd/pkg/crypto"
)

func TestRawMessageV1Encoding(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	SetDefaultTransports("raw", "raw")

	secret := crypto.KeyPadding([]byte("phase1-secret"))

	go func() {
		defer client.Close()
		header := &Header{
			Version:     CurrentProtocolVersion,
			Flags:       DefaultProtocolFlags,
			Sender:      ADMIN_UUID,
			Accepter:    TEMP_UUID,
			MessageType: HI,
			Route:       TEMP_ROUTE,
		}
		hi := &HIMess{
			GreetingLen:  uint16(len("hi-v1")),
			Greeting:     "hi-v1",
			UUIDLen:      uint16(len(ADMIN_UUID)),
			UUID:         ADMIN_UUID,
			IsAdmin:      1,
			IsReconnect:  0,
			ProtoVersion: CurrentProtocolVersion,
			ProtoFlags:   DefaultProtocolFlags,
		}
		msg := NewDownMsg(client, "phase1-secret", ADMIN_UUID)
		raw := msg.(*RawMessage)
		raw.CryptoSecret = secret
		ConstructMessage(msg, header, hi, false)
		msg.SendMessage()
	}()

	rMsg := NewDownMsg(server, "phase1-secret", TEMP_UUID)
	raw := rMsg.(*RawMessage)
	raw.CryptoSecret = secret
	header, payload, err := DestructMessage(rMsg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if header.Version != CurrentProtocolVersion {
		t.Fatalf("expected version %d, got %d", CurrentProtocolVersion, header.Version)
	}
	if header.Flags != DefaultProtocolFlags {
		t.Fatalf("expected flags %#x, got %#x", DefaultProtocolFlags, header.Flags)
	}
	hi, ok := payload.(*HIMess)
	if !ok {
		t.Fatalf("unexpected payload type %T", payload)
	}
	if hi.Greeting != "hi-v1" {
		t.Fatalf("unexpected greeting %s", hi.Greeting)
	}
	if hi.ProtoVersion != CurrentProtocolVersion {
		t.Fatalf("expected proto version %d, got %d", CurrentProtocolVersion, hi.ProtoVersion)
	}
	if hi.ProtoFlags != DefaultProtocolFlags {
		t.Fatalf("unexpected proto flags %#x", hi.ProtoFlags)
	}
}

func TestRawUUIDMessageV1Encoding(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	SetDefaultTransports("raw", "raw")

	secret := crypto.KeyPadding([]byte("phase1-secret"))

	go func() {
		defer client.Close()
		header := &Header{
			Version:     CurrentProtocolVersion,
			Flags:       DefaultProtocolFlags,
			Sender:      ADMIN_UUID,
			Accepter:    TEMP_UUID,
			MessageType: UUID,
			Route:       TEMP_ROUTE,
		}
		uuid := &UUIDMess{
			UUIDLen:      uint16(len("child")),
			UUID:         "child",
			ProtoVersion: CurrentProtocolVersion,
			ProtoFlags:   DefaultProtocolFlags,
		}
		msg := NewDownMsg(client, "phase1-secret", ADMIN_UUID)
		raw := msg.(*RawMessage)
		raw.CryptoSecret = secret
		ConstructMessage(msg, header, uuid, false)
		msg.SendMessage()
	}()

	rMsg := NewDownMsg(server, "phase1-secret", TEMP_UUID)
	raw := rMsg.(*RawMessage)
	raw.CryptoSecret = secret
	header, payload, err := DestructMessage(rMsg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if header.Version != CurrentProtocolVersion {
		t.Fatalf("expected version %d, got %d", CurrentProtocolVersion, header.Version)
	}
	if header.Flags != DefaultProtocolFlags {
		t.Fatalf("expected flags %#x, got %#x", DefaultProtocolFlags, header.Flags)
	}
	uuid, ok := payload.(*UUIDMess)
	if !ok {
		t.Fatalf("unexpected payload type %T", payload)
	}
	if uuid.UUID != "child" {
		t.Fatalf("unexpected uuid %s", uuid.UUID)
	}
	if uuid.ProtoVersion != CurrentProtocolVersion {
		t.Fatalf("expected proto version %d, got %d", CurrentProtocolVersion, uuid.ProtoVersion)
	}
	if uuid.ProtoFlags != DefaultProtocolFlags {
		t.Fatalf("expected proto flags %#x, got %#x", DefaultProtocolFlags, uuid.ProtoFlags)
	}
}

func TestRawTempUUIDMultiHopRouteIsPassThroughOnIntermediateHop(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	SetDefaultTransports("raw", "raw")

	secret := crypto.KeyPadding([]byte("phase1-secret"))

	go func() {
		defer client.Close()
		header := &Header{
			Version:     CurrentProtocolVersion,
			Flags:       DefaultProtocolFlags,
			Sender:      ADMIN_UUID,
			Accepter:    TEMP_UUID,
			MessageType: HI,
			Route:       "PIVOT:CHILD",
		}
		hi := &HIMess{
			GreetingLen:  uint16(len("hi-multi-hop")),
			Greeting:     "hi-multi-hop",
			UUIDLen:      uint16(len(ADMIN_UUID)),
			UUID:         ADMIN_UUID,
			IsAdmin:      1,
			IsReconnect:  0,
			ProtoVersion: CurrentProtocolVersion,
			ProtoFlags:   DefaultProtocolFlags,
		}
		msg := NewDownMsg(client, "phase1-secret", ADMIN_UUID)
		raw := msg.(*RawMessage)
		raw.CryptoSecret = secret
		ConstructMessage(msg, header, hi, false)
		msg.SendMessage()
	}()

	// 中间跳点：不应解密，因此 payload 必须保持为原始字节。
	rMsg := NewDownMsg(server, "phase1-secret", "PIVOT")
	raw := rMsg.(*RawMessage)
	raw.CryptoSecret = secret
	header, payload, err := DestructMessage(rMsg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if header.Accepter != TEMP_UUID {
		t.Fatalf("expected accepter %q, got %q", TEMP_UUID, header.Accepter)
	}
	if _, ok := payload.([]byte); !ok {
		t.Fatalf("expected pass-through []byte payload, got %T", payload)
	}
}

func TestRawTempUUIDSingleHopRouteDecryptsForMatchingUUID(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	SetDefaultTransports("raw", "raw")

	secret := crypto.KeyPadding([]byte("phase1-secret"))

	go func() {
		defer client.Close()
		header := &Header{
			Version:     CurrentProtocolVersion,
			Flags:       DefaultProtocolFlags,
			Sender:      ADMIN_UUID,
			Accepter:    TEMP_UUID,
			MessageType: HI,
			Route:       "FINALNODE",
		}
		hi := &HIMess{
			GreetingLen:  uint16(len("hi-final")),
			Greeting:     "hi-final",
			UUIDLen:      uint16(len(ADMIN_UUID)),
			UUID:         ADMIN_UUID,
			IsAdmin:      1,
			IsReconnect:  0,
			ProtoVersion: CurrentProtocolVersion,
			ProtoFlags:   DefaultProtocolFlags,
		}
		msg := NewDownMsg(client, "phase1-secret", ADMIN_UUID)
		raw := msg.(*RawMessage)
		raw.CryptoSecret = secret
		ConstructMessage(msg, header, hi, false)
		msg.SendMessage()
	}()

	// 最后一跳：route 只剩一个且匹配当前 UUID，因此应解密并反序列化。
	rMsg := NewDownMsg(server, "phase1-secret", "FINALNODE")
	raw := rMsg.(*RawMessage)
	raw.CryptoSecret = secret
	_, payload, err := DestructMessage(rMsg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := payload.(*HIMess); !ok {
		t.Fatalf("expected *HIMess payload, got %T", payload)
	}
}

func TestRawTempUUIDEmptyRouteDecryptsOnFinalHop(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	SetDefaultTransports("raw", "raw")

	secret := crypto.KeyPadding([]byte("phase1-secret"))

	go func() {
		defer client.Close()
		header := &Header{
			Version:     CurrentProtocolVersion,
			Flags:       DefaultProtocolFlags,
			Sender:      ADMIN_UUID,
			Accepter:    TEMP_UUID,
			MessageType: HI,
			Route:       "",
			RouteLen:    0,
		}
		hi := &HIMess{
			GreetingLen:  uint16(len("hi-empty-route")),
			Greeting:     "hi-empty-route",
			UUIDLen:      uint16(len(ADMIN_UUID)),
			UUID:         ADMIN_UUID,
			IsAdmin:      1,
			IsReconnect:  0,
			ProtoVersion: CurrentProtocolVersion,
			ProtoFlags:   DefaultProtocolFlags,
		}
		msg := NewDownMsg(client, "phase1-secret", ADMIN_UUID)
		raw := msg.(*RawMessage)
		raw.CryptoSecret = secret
		ConstructMessage(msg, header, hi, false)
		msg.SendMessage()
	}()

	rMsg := NewDownMsg(server, "phase1-secret", "FINALNODE")
	raw := rMsg.(*RawMessage)
	raw.CryptoSecret = secret
	_, payload, err := DestructMessage(rMsg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := payload.(*HIMess); !ok {
		t.Fatalf("expected *HIMess payload, got %T", payload)
	}
}
