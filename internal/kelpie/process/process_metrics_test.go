package process

import (
	"errors"
	"testing"
)

func TestReconnectStatsSnapshot(t *testing.T) {
	rec := &Reconnector{}
	rec.metrics.recordAttempt()
	rec.metrics.recordFailure(errors.New("handshake failed"))
	rec.metrics.recordAttempt()
	rec.metrics.recordSuccess()

	stats := rec.Stats()
	if stats.Attempts != 2 {
		t.Fatalf("expected 2 attempts, got %d", stats.Attempts)
	}
	if stats.Success != 1 {
		t.Fatalf("expected 1 success, got %d", stats.Success)
	}
	if stats.Failures != 1 {
		t.Fatalf("expected 1 failure, got %d", stats.Failures)
	}
	if stats.LastError != "" {
		t.Fatalf("expected last error cleared after success, got %q", stats.LastError)
	}
}
