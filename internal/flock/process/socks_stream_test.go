package process

import (
	"errors"
	"net"
	"testing"
)

func TestParseSocksAuthOptions(t *testing.T) {
	method, user, pass, err := parseSocksAuthOptions(map[string]string{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if method != socksAuthNone || user != "" || pass != "" {
		t.Fatalf("unexpected none auth result: method=%d user=%q pass=%q", method, user, pass)
	}

	method, user, pass, err = parseSocksAuthOptions(map[string]string{
		"auth":     "userpass",
		"username": "alice",
		"password": "secret",
	})
	if err != nil {
		t.Fatalf("unexpected userpass error: %v", err)
	}
	if method != socksAuthUserPass || user != "alice" || pass != "secret" {
		t.Fatalf("unexpected userpass result: method=%d user=%q pass=%q", method, user, pass)
	}

	if _, _, _, err := parseSocksAuthOptions(map[string]string{"auth": "userpass", "password": "x"}); err == nil {
		t.Fatalf("expected missing username error")
	}
	if _, _, _, err := parseSocksAuthOptions(map[string]string{"auth": "unknown"}); err == nil {
		t.Fatalf("expected unsupported auth error")
	}
}

func TestParseSocksGreeting(t *testing.T) {
	buf := []byte{socksVersion5, 2, socksAuthNone, socksAuthUserPass}

	method, consumed, err := parseSocksGreeting(buf, socksAuthUserPass)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if method != socksAuthUserPass || consumed != len(buf) {
		t.Fatalf("unexpected greeting parse result: method=%d consumed=%d", method, consumed)
	}

	method, consumed, err = parseSocksGreeting(buf, 0x7f)
	if err != nil {
		t.Fatalf("unexpected mismatch parse error: %v", err)
	}
	if method != socksStatusNoAccept || consumed != len(buf) {
		t.Fatalf("expected no-accept, got method=%d consumed=%d", method, consumed)
	}

	_, _, err = parseSocksGreeting([]byte{socksVersion5, 1}, socksAuthNone)
	if !errors.Is(err, errNeedMoreSocksData) {
		t.Fatalf("expected need-more-data, got: %v", err)
	}
}

func TestParseSocksUserPass(t *testing.T) {
	req := []byte{0x01, 5, 'a', 'l', 'i', 'c', 'e', 6, 's', 'e', 'c', 'r', 'e', 't'}
	ok, consumed, err := parseSocksUserPass(req, "alice", "secret")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok || consumed != len(req) {
		t.Fatalf("unexpected auth parse result: ok=%v consumed=%d", ok, consumed)
	}

	ok, consumed, err = parseSocksUserPass(req, "alice", "bad")
	if err != nil {
		t.Fatalf("unexpected mismatch error: %v", err)
	}
	if ok || consumed != len(req) {
		t.Fatalf("expected auth mismatch, got ok=%v consumed=%d", ok, consumed)
	}

	_, _, err = parseSocksUserPass([]byte{0x01, 5, 'a'}, "alice", "secret")
	if !errors.Is(err, errNeedMoreSocksData) {
		t.Fatalf("expected need-more-data, got: %v", err)
	}
}

func TestParseSocksConnectRequest(t *testing.T) {
	domain := []byte{
		socksVersion5, socksCmdConnect, 0x00, socksAtypDomainName,
		11, 'e', 'x', 'a', 'm', 'p', 'l', 'e', '.', 'c', 'o', 'm',
		0x01, 0xbb,
	}
	host, port, consumed, rep, err := parseSocksConnectRequest(domain)
	if err != nil {
		t.Fatalf("unexpected domain parse error: %v", err)
	}
	if host != "example.com" || port != 443 || consumed != len(domain) || rep != socksStatusOK {
		t.Fatalf("unexpected domain parse result: host=%q port=%d consumed=%d rep=%d", host, port, consumed, rep)
	}

	ipv4 := []byte{socksVersion5, socksCmdConnect, 0x00, socksAtypIPv4, 127, 0, 0, 1, 0x00, 0x50}
	host, port, consumed, rep, err = parseSocksConnectRequest(ipv4)
	if err != nil {
		t.Fatalf("unexpected ipv4 parse error: %v", err)
	}
	if host != "127.0.0.1" || port != 80 || consumed != len(ipv4) || rep != socksStatusOK {
		t.Fatalf("unexpected ipv4 parse result: host=%q port=%d consumed=%d rep=%d", host, port, consumed, rep)
	}

	_, _, _, rep, err = parseSocksConnectRequest([]byte{socksVersion5, 0x02, 0x00, socksAtypIPv4, 1, 1, 1, 1, 0, 80})
	if err == nil || rep != 0x07 {
		t.Fatalf("expected unsupported-cmd error with rep=0x07, got err=%v rep=%d", err, rep)
	}

	_, _, _, rep, err = parseSocksConnectRequest([]byte{socksVersion5, socksCmdConnect, 0x00, 0x09, 0, 0})
	if err == nil || rep != 0x08 {
		t.Fatalf("expected unsupported-atyp error with rep=0x08, got err=%v rep=%d", err, rep)
	}
}

func TestSocksReply(t *testing.T) {
	reply := socksReply(socksStatusFail, nil)
	if len(reply) != 10 {
		t.Fatalf("unexpected reply length: %d", len(reply))
	}
	if reply[0] != socksVersion5 || reply[1] != socksStatusFail || reply[3] != socksAtypIPv4 {
		t.Fatalf("unexpected nil-bound reply header: %v", reply[:4])
	}

	reply = socksReply(socksStatusOK, &net.TCPAddr{IP: net.IPv4(1, 2, 3, 4), Port: 1080})
	if reply[1] != socksStatusOK {
		t.Fatalf("unexpected status: %d", reply[1])
	}
	if reply[4] != 1 || reply[5] != 2 || reply[6] != 3 || reply[7] != 4 {
		t.Fatalf("unexpected bound ip in reply: %v", reply[4:8])
	}
	if reply[8] != 0x04 || reply[9] != 0x38 {
		t.Fatalf("unexpected bound port in reply: %v", reply[8:10])
	}
}
