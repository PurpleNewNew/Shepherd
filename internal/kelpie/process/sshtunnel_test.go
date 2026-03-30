package process

import (
	"context"
	"io"
	"testing"

	"codeberg.org/agnoie/shepherd/internal/kelpie/uipb"
)

type fakeStream struct {
	closed bool
	writes [][]byte
}

func (f *fakeStream) Read(p []byte) (int, error) { return 0, io.EOF }

func (f *fakeStream) Write(p []byte) (int, error) {
	copyBuf := append([]byte(nil), p...)
	f.writes = append(f.writes, copyBuf)
	return len(p), nil
}

func (f *fakeStream) Close() error {
	f.closed = true
	return nil
}

func TestStartSSHTunnelPassword(t *testing.T) {
	stream := &fakeStream{}
	admin := &Admin{}
	var capturedMeta map[string]string
	admin.streamOpenOverride = func(ctx context.Context, target, sessionID string, meta map[string]string) (io.ReadWriteCloser, error) {
		capturedMeta = meta
		return stream, nil
	}
	if err := admin.StartSSHTunnel(context.Background(), "node-1", "10.0.0.5:22", "7000", uipb.SshTunnelAuthMethod_SSH_TUNNEL_AUTH_METHOD_PASSWORD, "alice", "secret", nil); err != nil {
		t.Fatalf("StartSSHTunnel password failed: %v", err)
	}
	if stream.closed != true {
		t.Fatalf("stream not closed")
	}
	if len(stream.writes) != 0 {
		t.Fatalf("unexpected data writes for password auth")
	}
	if capturedMeta["kind"] != "ssh-tunnel" || capturedMeta["addr"] != "10.0.0.5:22" || capturedMeta["port"] != "7000" {
		t.Fatalf("unexpected metadata: %#v", capturedMeta)
	}
	if capturedMeta["method"] != "1" || capturedMeta["username"] != "alice" || capturedMeta["password"] != "secret" {
		t.Fatalf("missing auth metadata: %#v", capturedMeta)
	}
}

func TestStartSSHTunnelCert(t *testing.T) {
	stream := &fakeStream{}
	admin := &Admin{}
	admin.streamOpenOverride = func(ctx context.Context, target, sessionID string, meta map[string]string) (io.ReadWriteCloser, error) {
		return stream, nil
	}
	cert := []byte("dummy-cert")
	if err := admin.StartSSHTunnel(context.Background(), "node-9", "1.2.3.4:22", "8000", uipb.SshTunnelAuthMethod_SSH_TUNNEL_AUTH_METHOD_CERT, "bob", "", cert); err != nil {
		t.Fatalf("StartSSHTunnel cert failed: %v", err)
	}
	if len(stream.writes) != 1 || string(stream.writes[0]) != string(cert) {
		t.Fatalf("certificate payload not forwarded")
	}
}

func TestStartSSHTunnelValidation(t *testing.T) {
	admin := &Admin{}
	if err := admin.StartSSHTunnel(context.Background(), "", "1.2.3.4:22", "8000", uipb.SshTunnelAuthMethod_SSH_TUNNEL_AUTH_METHOD_PASSWORD, "u", "p", nil); err == nil {
		t.Fatalf("expected error for missing target")
	}
	if err := admin.StartSSHTunnel(context.Background(), "node", "", "8000", uipb.SshTunnelAuthMethod_SSH_TUNNEL_AUTH_METHOD_PASSWORD, "u", "p", nil); err == nil {
		t.Fatalf("expected error for missing addr")
	}
	if err := admin.StartSSHTunnel(context.Background(), "node", "1.2.3.4:22", "", uipb.SshTunnelAuthMethod_SSH_TUNNEL_AUTH_METHOD_PASSWORD, "u", "p", nil); err == nil {
		t.Fatalf("expected error for missing port")
	}
	if err := admin.StartSSHTunnel(context.Background(), "node", "1.2.3.4:22", "8000", uipb.SshTunnelAuthMethod_SSH_TUNNEL_AUTH_METHOD_PASSWORD, "", "p", nil); err == nil {
		t.Fatalf("expected error for missing username")
	}
	if err := admin.StartSSHTunnel(context.Background(), "node", "1.2.3.4:22", "8000", uipb.SshTunnelAuthMethod_SSH_TUNNEL_AUTH_METHOD_CERT, "u", "", nil); err == nil {
		t.Fatalf("expected error for missing cert payload")
	}
}
