package dtn

import (
	"slices"
	"testing"
	"time"
)

type fakePersistor struct {
	upsertIDs []string
	deleteIDs []string
}

func (f *fakePersistor) UpsertDTNBundle(bundle *Bundle) error {
	if bundle == nil {
		return nil
	}
	f.upsertIDs = append(f.upsertIDs, bundle.ID)
	return nil
}

func (f *fakePersistor) DeleteDTNBundle(id string) error {
	if id == "" {
		return nil
	}
	f.deleteIDs = append(f.deleteIDs, id)
	return nil
}

func (f *fakePersistor) DeleteDTNBundles(ids []string) error {
	for _, id := range ids {
		_ = f.DeleteDTNBundle(id)
	}
	return nil
}

func (f *fakePersistor) LoadDTNBundles() ([]*Bundle, error) { return nil, nil }

func TestManager_RecalculateHoldForTarget_Persists(t *testing.T) {
	now := time.Unix(100, 0)
	m := NewManager(Config{PerNodeCapacity: 8, DefaultTTL: time.Hour, DispatchBatch: 8, DispatchInterval: time.Second})
	m.clock = func() time.Time { return now }
	fp := &fakePersistor{}
	m.SetPersistor(fp)

	b1, err := m.Enqueue("n1", []byte("a"))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	b2, err := m.Enqueue("n1", []byte("b"))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	fp.upsertIDs = nil

	hold := now.Add(10 * time.Second)
	changed := m.RecalculateHoldForTarget("n1", hold)
	if changed != 2 {
		t.Fatalf("expected changed=2, got %d", changed)
	}
	if len(fp.upsertIDs) < 2 {
		t.Fatalf("expected >=2 upserts, got %d", len(fp.upsertIDs))
	}
	if !slices.Contains(fp.upsertIDs, b1.ID) || !slices.Contains(fp.upsertIDs, b2.ID) {
		t.Fatalf("expected upserts for both bundles, got=%v", fp.upsertIDs)
	}
}

func TestManager_Ready_ExpiresAndDeletesPersisted(t *testing.T) {
	base := time.Unix(200, 0)
	m := NewManager(Config{PerNodeCapacity: 8, DefaultTTL: time.Minute, DispatchBatch: 8, DispatchInterval: time.Second})
	m.clock = func() time.Time { return base }
	fp := &fakePersistor{}
	m.SetPersistor(fp)

	b, err := m.Enqueue("n1", []byte("x"), WithTTL(1*time.Second))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	fp.deleteIDs = nil

	got := m.Ready(base.Add(2*time.Second), 8)
	if len(got) != 0 {
		t.Fatalf("expected no ready bundles, got %d", len(got))
	}
	_, _, expired := m.Metrics()
	if expired != 1 {
		t.Fatalf("expected expired=1, got %d", expired)
	}
	if !slices.Contains(fp.deleteIDs, b.ID) {
		t.Fatalf("expected persisted delete for expired bundle id=%s, got=%v", b.ID, fp.deleteIDs)
	}
}

func TestManager_Remove_DeletesPersisted(t *testing.T) {
	base := time.Unix(300, 0)
	m := NewManager(Config{PerNodeCapacity: 8, DefaultTTL: time.Minute, DispatchBatch: 8, DispatchInterval: time.Second})
	m.clock = func() time.Time { return base }
	fp := &fakePersistor{}
	m.SetPersistor(fp)

	b, err := m.Enqueue("n1", []byte("x"))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	fp.deleteIDs = nil

	_, removed := m.Remove(b.ID)
	if !removed {
		t.Fatalf("expected removed=true")
	}
	if !slices.Contains(fp.deleteIDs, b.ID) {
		t.Fatalf("expected persisted delete for removed bundle id=%s, got=%v", b.ID, fp.deleteIDs)
	}
}

func TestManager_Restore_SkipsExpiredAndDeletesPersisted(t *testing.T) {
	base := time.Unix(400, 0)
	m := NewManager(Config{PerNodeCapacity: 8, DefaultTTL: time.Minute, DispatchBatch: 8, DispatchInterval: time.Second})
	m.clock = func() time.Time { return base }
	fp := &fakePersistor{}
	m.SetPersistor(fp)

	expired := &Bundle{ID: "expired", Target: "n1", Payload: []byte("x"), Priority: PriorityNormal, EnqueuedAt: base.Add(-10 * time.Second), DeliverBy: base.Add(-1 * time.Second)}
	ok := &Bundle{ID: "ok", Target: "n1", Payload: []byte("y"), Priority: PriorityNormal, EnqueuedAt: base.Add(-5 * time.Second), DeliverBy: base.Add(1 * time.Minute)}

	fp.deleteIDs = nil
	restored := m.Restore([]*Bundle{expired, ok})
	if restored != 1 {
		t.Fatalf("expected restored=1, got %d", restored)
	}
	if !slices.Contains(fp.deleteIDs, expired.ID) {
		t.Fatalf("expected persisted delete for expired bundle id=%s, got=%v", expired.ID, fp.deleteIDs)
	}
	ready := m.Ready(base, 8)
	if len(ready) != 1 || ready[0].ID != ok.ID {
		ids := make([]string, 0, len(ready))
		for _, b := range ready {
			if b != nil {
				ids = append(ids, b.ID)
			}
		}
		t.Fatalf("expected ready=[%s], got=%v", ok.ID, ids)
	}
}

func TestManager_Enqueue_CapacityDropDeletesPersisted(t *testing.T) {
	base := time.Unix(500, 0)
	m := NewManager(Config{PerNodeCapacity: 1, DefaultTTL: time.Minute, DispatchBatch: 8, DispatchInterval: time.Second})
	i := 0
	m.clock = func() time.Time {
		tm := base.Add(time.Duration(i) * time.Second)
		i++
		return tm
	}
	fp := &fakePersistor{}
	m.SetPersistor(fp)

	b1, err := m.Enqueue("n1", []byte("x"), WithPriority(PriorityLow))
	if err != nil {
		t.Fatalf("enqueue b1: %v", err)
	}
	fp.deleteIDs = nil
	_, err = m.Enqueue("n1", []byte("y"), WithPriority(PriorityLow))
	if err != nil {
		t.Fatalf("enqueue b2: %v", err)
	}
	if !slices.Contains(fp.deleteIDs, b1.ID) {
		t.Fatalf("expected persisted delete for dropped bundle id=%s, got=%v", b1.ID, fp.deleteIDs)
	}
	stats := m.Stats("n1")
	if stats.DroppedTotal != 1 {
		t.Fatalf("expected DroppedTotal=1, got %d", stats.DroppedTotal)
	}
}
