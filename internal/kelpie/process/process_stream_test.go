package process

import (
	"context"
	"testing"
	"time"

	"codeberg.org/agnoie/shepherd/internal/kelpie/dtn"
	"codeberg.org/agnoie/shepherd/internal/kelpie/stream"
)

func TestAdminCloseStream(t *testing.T) {
	admin := &Admin{
		adminStreamState: adminStreamState{
			streamEngine: stream.New(stream.DefaultConfig(), func(string, []byte) error { return nil }, func(string, ...interface{}) {}),
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := admin.streamEngine.Open(ctx, "node-1", stream.Options{Target: "node-1", Meta: map[string]string{"session": "sess-1"}})
	if err != nil {
		t.Fatalf("failed to open stream: %v", err)
	}
	streamID := s.ID()
	if err := admin.CloseStream(0, ""); err == nil {
		t.Fatalf("expected error for zero stream id")
	}
	if err := admin.CloseStream(streamID, "testing"); err != nil {
		t.Fatalf("unexpected close error: %v", err)
	}
	if err := admin.CloseStream(streamID, "again"); err == nil {
		t.Fatalf("expected error for missing stream")
	}
	_ = s.Close()
}

func TestStreamCloseReason(t *testing.T) {
	admin := &Admin{}
	admin.rememberStreamReason(42, "test")
	if got := admin.StreamCloseReason(42); got != "test" {
		t.Fatalf("expected reason test, got %q", got)
	}
	if got := admin.StreamCloseReason(42); got != "" {
		t.Fatalf("expected empty reason after consume, got %q", got)
	}
}

func TestStreamDiagnosticsIncludeMetadataAndReason(t *testing.T) {
	admin := &Admin{}
	admin.enqueueDiagOverride = func(string, string, dtn.Priority, time.Duration) (string, error) {
		return "bundle", nil
	}
	admin.initStreamEngine()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	streamHandle, err := admin.OpenStream(ctx, "node-1", "sess-1", map[string]string{"kind": "shell", "custom": "value"})
	if err != nil {
		t.Fatalf("open stream failed: %v", err)
	}
	defer streamHandle.Close()
	streamObj, ok := streamHandle.(*stream.Stream)
	if !ok {
		t.Fatalf("unexpected stream type %T", streamHandle)
	}
	diags := admin.StreamDiagnostics()
	if len(diags) != 1 {
		t.Fatalf("expected one diag entry, got %d", len(diags))
	}
	if diags[0].Metadata["custom"] != "value" {
		t.Fatalf("metadata missing custom value: %+v", diags[0].Metadata)
	}
	streamID := streamObj.ID()
	if err := admin.CloseStream(streamID, "intentional"); err != nil {
		t.Fatalf("close stream failed: %v", err)
	}
	if reason := admin.StreamCloseReason(streamID); reason != "intentional" {
		t.Fatalf("expected reason intentional, got %q", reason)
	}
}
