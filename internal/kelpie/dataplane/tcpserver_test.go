package dataplane

import (
	"testing"
	"time"
)

func TestEstimateUploadCloseWait_Base(t *testing.T) {
	got := estimateUploadCloseWait(TokenMeta{}, 0)
	want := 120 * time.Second
	if got != want {
		t.Fatalf("estimateUploadCloseWait()=%s, want=%s", got, want)
	}
}

func TestEstimateUploadCloseWait_ScalesWithPayload(t *testing.T) {
	// 8 MiB 按 32 KiB/s 传输，再加 45 秒余量，得到 301 秒
	got := estimateUploadCloseWait(TokenMeta{}, 8*1024*1024)
	want := 301 * time.Second
	if got != want {
		t.Fatalf("estimateUploadCloseWait(8MiB)=%s, want=%s", got, want)
	}
}

func TestEstimateUploadCloseWait_UsesSizeHint(t *testing.T) {
	meta := TokenMeta{SizeHint: 8 * 1024 * 1024}
	got := estimateUploadCloseWait(meta, 0)
	want := 301 * time.Second
	if got != want {
		t.Fatalf("estimateUploadCloseWait(size_hint=8MiB)=%s, want=%s", got, want)
	}
}

func TestEstimateUploadCloseWait_CappedByMaxWait(t *testing.T) {
	meta := TokenMeta{MaxRate: 8 * 1024} // 8 KiB/s => estimate > 10m, should cap
	got := estimateUploadCloseWait(meta, 8*1024*1024)
	want := 10 * time.Minute
	if got != want {
		t.Fatalf("estimateUploadCloseWait(cap)=%s, want=%s", got, want)
	}
}
