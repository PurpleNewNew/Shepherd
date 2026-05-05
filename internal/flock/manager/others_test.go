package manager

import "testing"

func TestSetStreamForSessionResetsSeqOnRemap(t *testing.T) {
	mgr := newShellManager()
	const sessionID = "session-1"

	mgr.SetStreamForSession(sessionID, 10)
	if got := mgr.NextSeqForSession(sessionID); got != 1 {
		t.Fatalf("first seq = %d, want 1", got)
	}
	if got := mgr.NextSeqForSession(sessionID); got != 2 {
		t.Fatalf("second seq = %d, want 2", got)
	}

	mgr.SetStreamForSession(sessionID, 10)
	if got := mgr.NextSeqForSession(sessionID); got != 3 {
		t.Fatalf("same stream should preserve seq, got %d want 3", got)
	}

	mgr.SetStreamForSession(sessionID, 20)
	if got := mgr.StreamForSession(sessionID); got != 20 {
		t.Fatalf("stream id = %d, want 20", got)
	}
	if got := mgr.NextSeqForSession(sessionID); got != 1 {
		t.Fatalf("remapped stream should reset seq, got %d want 1", got)
	}
}
