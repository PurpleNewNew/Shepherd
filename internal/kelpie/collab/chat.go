package collab

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"codeberg.org/agnoie/shepherd/internal/kelpie/storage/sqlite"
)

type ChatStore interface {
	InsertChat(sqlite.ChatRecord) error
	ListChat(limit int, before time.Time) ([]sqlite.ChatRecord, error)
}

type Service struct {
	store ChatStore
	mu    sync.RWMutex
	sinks []func(sqlite.ChatRecord)
}

func NewService(store ChatStore) *Service {
	return &Service{store: store}
}

func (s *Service) Append(username, role, message string) (sqlite.ChatRecord, error) {
	if s == nil || s.store == nil {
		return sqlite.ChatRecord{}, fmt.Errorf("chat service unavailable")
	}
	msg := strings.TrimSpace(message)
	if msg == "" {
		return sqlite.ChatRecord{}, fmt.Errorf("message required")
	}
	if len(msg) > 4096 {
		msg = msg[:4096]
	}
	rec := sqlite.ChatRecord{
		ID:        uuid.NewString(),
		Username:  strings.ToLower(username),
		Role:      role,
		Message:   msg,
		CreatedAt: time.Now().UTC(),
	}
	if err := s.store.InsertChat(rec); err != nil {
		return sqlite.ChatRecord{}, err
	}
	s.broadcast(rec)
	return rec, nil
}

func (s *Service) List(limit int, before time.Time) ([]sqlite.ChatRecord, error) {
	if s == nil || s.store == nil {
		return nil, fmt.Errorf("chat service unavailable")
	}
	return s.store.ListChat(limit, before)
}

func (s *Service) RegisterSink(fn func(sqlite.ChatRecord)) {
	if s == nil || fn == nil {
		return
	}
	s.mu.Lock()
	s.sinks = append(s.sinks, fn)
	s.mu.Unlock()
}

func (s *Service) broadcast(rec sqlite.ChatRecord) {
	s.mu.RLock()
	sinks := append([]func(sqlite.ChatRecord){}, s.sinks...)
	s.mu.RUnlock()
	for _, fn := range sinks {
		fn(rec)
	}
}
