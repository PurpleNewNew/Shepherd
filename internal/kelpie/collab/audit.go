package collab

import (
	"sync"
	"time"

	"github.com/google/uuid"

	"codeberg.org/agnoie/shepherd/internal/kelpie/storage/sqlite"
)

type AuditStore interface {
	InsertAudit(sqlite.AuditRecord) error
	ListAudit(sqlite.AuditFilter) ([]sqlite.AuditRecord, error)
}

type Recorder struct {
	store AuditStore
	mu    sync.RWMutex
	sinks []func(sqlite.AuditRecord)
}

type Entry struct {
	Tenant   string
	Username string
	Role     string
	Method   string
	Target   string
	Params   string
	Status   string
	Error    string
	Peer     string
}

type Filter struct {
	Tenant   string
	Username string
	Method   string
	From     time.Time
	To       time.Time
	Limit    int
}

func NewRecorder(store AuditStore) *Recorder {
	return &Recorder{store: store}
}

func (r *Recorder) Record(entry Entry) error {
	if r == nil || r.store == nil {
		return nil
	}
	rec := sqlite.AuditRecord{
		ID:        uuid.NewString(),
		Tenant:    entry.Tenant,
		Username:  entry.Username,
		Role:      entry.Role,
		Method:    entry.Method,
		Target:    entry.Target,
		Params:    entry.Params,
		Status:    entry.Status,
		Error:     entry.Error,
		Peer:      entry.Peer,
		CreatedAt: time.Now().UTC(),
	}
	if err := r.store.InsertAudit(rec); err != nil {
		return err
	}
	r.broadcast(rec)
	return nil
}

func (r *Recorder) RegisterSink(fn func(sqlite.AuditRecord)) {
	if r == nil || fn == nil {
		return
	}
	r.mu.Lock()
	r.sinks = append(r.sinks, fn)
	r.mu.Unlock()
}

func (r *Recorder) broadcast(rec sqlite.AuditRecord) {
	r.mu.RLock()
	sinks := append([]func(sqlite.AuditRecord){}, r.sinks...)
	r.mu.RUnlock()
	for _, fn := range sinks {
		fn(rec)
	}
}

func (r *Recorder) List(filter Filter) ([]sqlite.AuditRecord, error) {
	if r == nil || r.store == nil {
		return nil, nil
	}
	return r.store.ListAudit(sqlite.AuditFilter{
		Tenant:   filter.Tenant,
		Username: filter.Username,
		Method:   filter.Method,
		From:     filter.From,
		To:       filter.To,
		Limit:    filter.Limit,
	})
}
