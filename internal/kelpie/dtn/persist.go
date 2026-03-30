package dtn

// Persistor provides optional DTN bundle persistence for Kelpie.
//
// Semantics:
// - Bundles should be persisted on enqueue and kept until a final success ACK.
// - This enables "at least once" delivery across Kelpie restarts (inflight bundles are restored).
//
// NOTE: The persistor is intentionally defined in the dtn package to avoid coupling the
// queue implementation to a specific database/backend.
type Persistor interface {
	UpsertDTNBundle(bundle *Bundle) error
	DeleteDTNBundle(id string) error
	DeleteDTNBundles(ids []string) error
	LoadDTNBundles() ([]*Bundle, error)
}

