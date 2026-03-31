package protocol

import (
	"bufio"
	"fmt"
	"net"
	"regexp"
	"strings"
	"testing"
)

func TestWSProtoCNegotiateHandlesLargeResponseHeader(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	serverErr := make(chan error, 1)
	go func() {
		reader := bufio.NewReader(serverConn)
		reqHeader, err := readWSHeader(reader)
		if err != nil {
			serverErr <- err
			return
		}
		re := regexp.MustCompile(`Sec-WebSocket-Key: (.*)`)
		match := re.FindStringSubmatch(strings.ReplaceAll(string(reqHeader), "\r\n", "\n"))
		if len(match) < 2 {
			serverErr <- fmt.Errorf("missing websocket key")
			return
		}
		accept, err := getNonceAccept([]byte(match[1]))
		if err != nil {
			serverErr <- err
			return
		}
		resp := "HTTP/1.1 101 Switching Protocols\r\n" +
			"Connection: Upgrade\r\n" +
			"Upgrade: websocket\r\n" +
			"X-Fill: " + strings.Repeat("a", 1400) + "\r\n" +
			"Sec-WebSocket-Accept: " + string(accept) + "\r\n\r\n"
		_, err = serverConn.Write([]byte(resp))
		serverErr <- err
	}()

	proto := &WSProto{
		param:    &NegParam{Conn: clientConn, Domain: "example.com"},
		RawProto: new(RawProto),
	}
	if err := proto.CNegotiate(); err != nil {
		t.Fatalf("CNegotiate failed: %v", err)
	}
	if _, ok := proto.param.Conn.(*wsConn); !ok {
		t.Fatalf("expected websocket connection wrapper, got %T", proto.param.Conn)
	}
	if err := <-serverErr; err != nil {
		t.Fatalf("server handshake failed: %v", err)
	}
}

func TestWSProtoSNegotiateHandlesLargeRequestHeader(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	const nonce = "dGhlIHNhbXBsZSBub25jZQ=="
	clientErr := make(chan error, 1)
	go func() {
		req := "GET /deadbeef HTTP/1.1\r\n" +
			"Host: example.com\r\n" +
			"Upgrade: websocket\r\n" +
			"Connection: Upgrade\r\n" +
			"X-Fill: " + strings.Repeat("b", 1400) + "\r\n" +
			"Sec-WebSocket-Key: " + nonce + "\r\n" +
			"Origin: https://google.com\r\n" +
			"Sec-WebSocket-Version: 13\r\n\r\n"
		if _, err := clientConn.Write([]byte(req)); err != nil {
			clientErr <- err
			return
		}
		reader := bufio.NewReader(clientConn)
		respHeader, err := readWSHeader(reader)
		if err != nil {
			clientErr <- err
			return
		}
		accept, err := getNonceAccept([]byte(nonce))
		if err != nil {
			clientErr <- err
			return
		}
		resp := string(respHeader)
		if !strings.Contains(resp, "HTTP/1.1 101 Switching Protocols") {
			clientErr <- fmt.Errorf("unexpected response status: %q", resp)
			return
		}
		if !strings.Contains(resp, "Sec-WebSocket-Accept: "+string(accept)) {
			clientErr <- fmt.Errorf("missing accept header: %q", resp)
			return
		}
		clientErr <- nil
	}()

	proto := &WSProto{
		param:    &NegParam{Conn: serverConn},
		RawProto: new(RawProto),
	}
	if err := proto.SNegotiate(); err != nil {
		t.Fatalf("SNegotiate failed: %v", err)
	}
	if _, ok := proto.param.Conn.(*wsConn); !ok {
		t.Fatalf("expected websocket connection wrapper, got %T", proto.param.Conn)
	}
	if err := <-clientErr; err != nil {
		t.Fatalf("client handshake failed: %v", err)
	}
}
