package protocol

import (
	"bytes"
	"io"
	"net"
	"testing"
	"time"

	"codeberg.org/agnoie/shepherd/pkg/crypto"
)

type mockAddr string

func (a mockAddr) Network() string { return "tcp" }
func (a mockAddr) String() string  { return string(a) }

type writeCaptureConn struct {
	buf bytes.Buffer
}

func (c *writeCaptureConn) Read(_ []byte) (int, error)       { return 0, io.EOF }
func (c *writeCaptureConn) Write(p []byte) (int, error)      { return c.buf.Write(p) }
func (c *writeCaptureConn) Close() error                     { return nil }
func (c *writeCaptureConn) LocalAddr() net.Addr              { return mockAddr("local:0") }
func (c *writeCaptureConn) RemoteAddr() net.Addr             { return mockAddr("remote:80") }
func (c *writeCaptureConn) SetDeadline(time.Time) error      { return nil }
func (c *writeCaptureConn) SetReadDeadline(time.Time) error  { return nil }
func (c *writeCaptureConn) SetWriteDeadline(time.Time) error { return nil }
func (c *writeCaptureConn) Bytes() []byte                    { return c.buf.Bytes() }

type readReplayConn struct {
	reader *bytes.Reader
}

func newReadReplayConn(data []byte) *readReplayConn {
	return &readReplayConn{reader: bytes.NewReader(data)}
}

func (c *readReplayConn) Read(p []byte) (int, error)       { return c.reader.Read(p) }
func (c *readReplayConn) Write(p []byte) (int, error)      { return len(p), nil }
func (c *readReplayConn) Close() error                     { return nil }
func (c *readReplayConn) LocalAddr() net.Addr              { return mockAddr("local:0") }
func (c *readReplayConn) RemoteAddr() net.Addr             { return mockAddr("remote:80") }
func (c *readReplayConn) SetDeadline(time.Time) error      { return nil }
func (c *readReplayConn) SetReadDeadline(time.Time) error  { return nil }
func (c *readReplayConn) SetWriteDeadline(time.Time) error { return nil }

func TestHTTPMessageChunkedRoundTrip(t *testing.T) {
	senderConn := &writeCaptureConn{}

	secret := "chunk-secret"
	key := crypto.KeyPadding([]byte(secret))

	sendRaw := &RawMessage{
		Conn:         senderConn,
		UUID:         TEMP_UUID,
		CryptoSecret: key,
		Version:      1,
		Flags:        DefaultProtocolFlags,
	}
	sendMsg := NewHTTPMessage(sendRaw, HTTPConfig{
		Host:      "example.com",
		Path:      "/chunk",
		UserAgent: "UnitTest/1.0",
		ChunkSize: 8,
	})
	SetMessageMeta(sendMsg, 1, DefaultProtocolFlags)

	header := &Header{
		Sender:      TEMP_UUID,
		Accepter:    ADMIN_UUID,
		MessageType: HI,
		Route:       TEMP_ROUTE,
		RouteLen:    uint32(len(TEMP_ROUTE)),
	}
	payload := &HIMess{
		GreetingLen:  uint16(len("Hello")),
		Greeting:     "Hello",
		UUIDLen:      uint16(len(TEMP_UUID)),
		UUID:         TEMP_UUID,
		IsAdmin:      0,
		IsReconnect:  0,
		ProtoVersion: 1,
		ProtoFlags:   DefaultProtocolFlags,
	}

	ConstructMessage(sendMsg, header, payload, false)
	expectedMessage := append([]byte(nil), sendMsg.RawMessage.HeaderBuffer...)
	expectedMessage = append(expectedMessage, sendMsg.RawMessage.DataBuffer...)
	sendMsg.SendMessage()
	wire := append([]byte(nil), senderConn.Bytes()...)

	recvConn := newReadReplayConn(wire)
	recvRaw := &RawMessage{
		Conn:         recvConn,
		UUID:         ADMIN_UUID,
		CryptoSecret: key,
	}
	recvMsg := NewHTTPMessage(recvRaw, HTTPConfig{})
	SetMessageMeta(recvMsg, 1, DefaultProtocolFlags)

	recvMsg.DeconstructHeader()
	if !headerWantsChunked(recvMsg.headers) {
		t.Fatalf("expected chunked header, got %#v", recvMsg.headers)
	}
	body, err := recvMsg.decodeChunkedBody()
	if err != nil {
		t.Fatalf("decodeChunkedBody failed: %v", err)
	}
	if !bytes.Equal(body, expectedMessage) {
		t.Fatalf("decoded body length %d does not match raw payload length %d", len(body), len(expectedMessage))
	}
	savedConn := recvMsg.RawMessage.Conn
	chunk := &chunkConn{Conn: savedConn, reader: bytes.NewReader(body)}
	recvMsg.RawMessage.Conn = chunk
	recvHeader, recvPayload, err := recvMsg.RawMessage.DeconstructData()
	recvMsg.RawMessage.Conn = savedConn
	if err != nil {
		t.Fatalf("failed to decode raw message: %v", err)
	}
	if recvHeader == nil || recvHeader.MessageType != HI {
		t.Fatalf("unexpected header: %#v", recvHeader)
	}
	recvHI, ok := recvPayload.(*HIMess)
	if !ok {
		t.Fatalf("unexpected payload type %T", recvPayload)
	}
	if recvHI.Greeting != "Hello" {
		t.Fatalf("unexpected greeting %q", recvHI.Greeting)
	}
}
