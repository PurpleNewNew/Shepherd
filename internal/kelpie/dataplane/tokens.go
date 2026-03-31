package dataplane

import (
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"
)

// TokenMeta 描述一次性数据通道令牌的约束信息。
type TokenMeta struct {
	Tenant    string
	Operator  string
	Target    string
	Direction string // upload / download / proxy
	MaxSize   int64
	MaxRate   float64 // bytes per second; 0 = unlimited
	ExpiresAt time.Time
	Hash      string // optional SHA256 hex (lowercase)
	Offset    int64  // required starting offset for this token
	SizeHint  int64  // optional total/remaining size (bytes)
	Retries   int    // allowed retries after failed attempt (consume + requeue)
	// 可选的不透明元数据，用于审计或日志记录。
	Extra map[string]string
}

// TokenStore 维护短生命周期的令牌。
type TokenStore struct {
	mu        sync.RWMutex
	tokens    map[string]TokenMeta
	ttl       time.Duration
	lastPrune time.Time
}

func NewTokenStore(defaultTTL time.Duration) *TokenStore {
	if defaultTTL <= 0 {
		defaultTTL = 10 * time.Minute
	}
	return &TokenStore{
		tokens: make(map[string]TokenMeta),
		ttl:    defaultTTL,
	}
}

func (s *TokenStore) DefaultTTL() time.Duration {
	if s == nil || s.ttl <= 0 {
		return 10 * time.Minute
	}
	return s.ttl
}

// Issue 生成令牌并记录约束。
func (s *TokenStore) Issue(meta TokenMeta) string {
	if meta.ExpiresAt.IsZero() {
		meta.ExpiresAt = time.Now().Add(s.ttl)
	}
	if meta.Retries < 0 {
		meta.Retries = 0
	}
	token := randomToken(32)
	s.mu.Lock()
	s.pruneExpiredLocked(time.Now())
	s.tokens[token] = meta
	s.mu.Unlock()
	return token
}

// Consume 在校验通过后删除令牌，确保一次性。
func (s *TokenStore) Consume(token string) (TokenMeta, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	meta, ok := s.tokens[token]
	if !ok {
		return TokenMeta{}, false
	}
	delete(s.tokens, token)
	if time.Now().After(meta.ExpiresAt) {
		return TokenMeta{}, false
	}
	return meta, true
}

// Requeue 将令牌放回 store，通常用于失败重试；由调用方保证 Retries 已递减。
func (s *TokenStore) Requeue(token string, meta TokenMeta) {
	if token == "" {
		return
	}
	if meta.Retries < 0 {
		meta.Retries = 0
	}
	s.mu.Lock()
	s.tokens[token] = meta
	s.mu.Unlock()
}

// Peek 返回令牌但不删除（仅用于调试）。
func (s *TokenStore) Peek(token string) (TokenMeta, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneExpiredLocked(time.Now())
	meta, ok := s.tokens[token]
	if !ok || time.Now().After(meta.ExpiresAt) {
		return TokenMeta{}, false
	}
	return meta, true
}

func (s *TokenStore) pruneExpiredLocked(now time.Time) {
	if s == nil {
		return
	}
	// 减少开销：限频 1 分钟或当 token 数量较大时再清理。
	if !s.lastPrune.IsZero() && now.Sub(s.lastPrune) < time.Minute && len(s.tokens) < 1024 {
		return
	}
	for t, meta := range s.tokens {
		if now.After(meta.ExpiresAt) {
			delete(s.tokens, t)
		}
	}
	s.lastPrune = now
}

func randomToken(n int) string {
	buf := make([]byte, n)
	_, _ = rand.Read(buf)
	return hex.EncodeToString(buf)
}
