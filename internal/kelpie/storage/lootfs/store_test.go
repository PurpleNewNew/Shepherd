package lootfs

import (
	"io"
	"strings"
	"testing"
)

func TestStoreStreamAllowsEmptyContent(t *testing.T) {
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	ref, size, hash, err := store.StoreStream("loot-empty", strings.NewReader(""), "")
	if err != nil {
		t.Fatalf("store empty stream: %v", err)
	}
	if ref == "" {
		t.Fatalf("expected storage ref")
	}
	if size != 0 {
		t.Fatalf("size = %d, want 0", size)
	}
	const emptySHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	if hash != emptySHA256 {
		t.Fatalf("hash = %s, want %s", hash, emptySHA256)
	}

	reader, openedSize, err := store.Open(ref)
	if err != nil {
		t.Fatalf("open empty loot: %v", err)
	}
	defer reader.Close()
	content, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read empty loot: %v", err)
	}
	if openedSize != 0 || len(content) != 0 {
		t.Fatalf("opened size/content = %d/%d, want 0/0", openedSize, len(content))
	}
}
