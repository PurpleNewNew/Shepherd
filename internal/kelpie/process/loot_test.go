package process

import (
	"fmt"
	"path/filepath"
	"testing"

	"codeberg.org/agnoie/shepherd/internal/kelpie/storage/lootfs"
)

type memoryLootStore struct {
	records map[string]LootRecord
}

func (s *memoryLootStore) SaveLoot(rec LootRecord) error {
	if s.records == nil {
		s.records = make(map[string]LootRecord)
	}
	s.records[rec.ID] = rec
	return nil
}

func (s *memoryLootStore) ListLoot(filter LootFilter) ([]LootRecord, error) {
	var records []LootRecord
	for _, rec := range s.records {
		records = append(records, rec)
	}
	return records, nil
}

func (s *memoryLootStore) GetLoot(lootID string) (LootRecord, error) {
	rec, ok := s.records[lootID]
	if !ok {
		return LootRecord{}, fmt.Errorf("loot not found")
	}
	return rec, nil
}

func TestSubmitLootStoresContentAndLoadsItBack(t *testing.T) {
	contentStore, err := lootfs.New(filepath.Join(t.TempDir(), "loot"))
	if err != nil {
		t.Fatalf("new loot store: %v", err)
	}
	admin := &Admin{
		adminListenerState: adminListenerState{
			lootStore:        &memoryLootStore{},
			lootContentStore: contentStore,
		},
	}

	saved, err := admin.SubmitLoot(LootRecord{
		TargetUUID: "node-a",
		Name:       "sample.txt",
		Mime:       "text/plain",
	}, []byte("hello loot"))
	if err != nil {
		t.Fatalf("submit loot: %v", err)
	}
	if saved.ID == "" {
		t.Fatalf("expected generated loot id")
	}
	if saved.StorageRef == "" {
		t.Fatalf("expected stored content path")
	}
	if !filepath.IsAbs(saved.StorageRef) {
		t.Fatalf("expected absolute storage ref, got %q", saved.StorageRef)
	}
	if saved.Size != uint64(len("hello loot")) {
		t.Fatalf("unexpected stored size: %d", saved.Size)
	}
	if saved.Hash == "" {
		t.Fatalf("expected content hash to be recorded")
	}

	record, content, err := admin.GetLootContent(saved.ID)
	if err != nil {
		t.Fatalf("get loot content: %v", err)
	}
	if record.ID != saved.ID {
		t.Fatalf("expected record %s, got %s", saved.ID, record.ID)
	}
	if string(content) != "hello loot" {
		t.Fatalf("unexpected content: %q", string(content))
	}
}
