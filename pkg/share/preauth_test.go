package share

import (
	"net"
	"testing"
)

func TestActivePreAuthSuccess(t *testing.T) {
	secret := "active-success"
	token := GeneratePreAuthToken(secret)
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	done := make(chan error, 1)
	go func() {
		done <- PassivePreAuth(server, token)
	}()

	if err := ActivePreAuth(client, token); err != nil {
		t.Fatalf("ActivePreAuth failed: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("PassivePreAuth failed: %v", err)
	}
}

func TestActivePreAuthReject(t *testing.T) {
	secret := "active-reject"
	token := GeneratePreAuthToken(secret)
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	done := make(chan error, 1)
	go func() {
		done <- PassivePreAuth(server, token)
	}()

	if err := ActivePreAuth(client, "wrong-token"); err == nil {
		t.Fatalf("expected failure for mismatched token")
	}
	if err := <-done; err == nil {
		t.Fatalf("passive side should detect invalid token")
	}
}

func TestPassivePreAuthSuccess(t *testing.T) {
	secret := "passive-success"
	token := GeneratePreAuthToken(secret)
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	done := make(chan error, 1)
	go func() {
		done <- ActivePreAuth(client, token)
	}()

	if err := PassivePreAuth(server, token); err != nil {
		t.Fatalf("PassivePreAuth failed: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("ActivePreAuth failed: %v", err)
	}
}

func TestPassivePreAuthReject(t *testing.T) {
	secret := "passive-reject"
	token := GeneratePreAuthToken(secret)
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	done := make(chan error, 1)
	go func() {
		done <- ActivePreAuth(client, "bad-token")
	}()

	if err := PassivePreAuth(server, token); err == nil {
		t.Fatalf("expected failure for invalid passive token")
	}
	if err := <-done; err == nil {
		t.Fatalf("active side should fail handshake")
	}
}
