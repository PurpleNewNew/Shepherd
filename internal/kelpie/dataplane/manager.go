package dataplane

import (
	"strings"
	"time"
)

// Manager 负责为控制面准备数据通路令牌与元信息。
type Manager struct {
	cfg    Config
	tokens *TokenStore
	ttl    time.Duration
}

func NewManager(cfg Config, store *TokenStore, defaultTTL time.Duration) *Manager {
	if defaultTTL <= 0 && store != nil {
		defaultTTL = store.DefaultTTL()
	}
	if defaultTTL <= 0 {
		defaultTTL = 10 * time.Minute
	}
	return &Manager{cfg: cfg, tokens: store, ttl: defaultTTL}
}

// PrepareTransfer 为单次文件/流量传输生成令牌和入口。
// 方向：upload / download
func (m *Manager) PrepareTransfer(tenant, operator, target, direction string, maxSize int64, maxRate float64, sizeHint int64, hash string, offset int64, ttl time.Duration, extra map[string]string) (token string, endpoint string, meta TokenMeta) {
	if m == nil || m.tokens == nil {
		return "", "", TokenMeta{}
	}
	if offset < 0 {
		offset = 0
	}
	if sizeHint < 0 {
		sizeHint = 0
	}
	if ttl <= 0 {
		ttl = m.ttl
	}
	extras := make(map[string]string, len(extra))
	for k, v := range extra {
		extras[k] = v
	}
	meta = TokenMeta{
		Tenant:    strings.TrimSpace(tenant),
		Operator:  strings.TrimSpace(operator),
		Target:    strings.TrimSpace(target),
		Direction: strings.TrimSpace(direction),
		MaxSize:   firstPositive(maxSize, firstPositive(sizeHint, m.cfg.MaxSize)),
		MaxRate:   firstPositiveFloat(maxRate, m.cfg.MaxRateBps),
		SizeHint:  sizeHint,
		Hash:      strings.ToLower(strings.TrimSpace(hash)),
		Offset:    offset,
		ExpiresAt: time.Now().Add(ttl),
		Retries:   int(firstPositive(int64(m.cfg.TokenRetries), 1)),
		Extra:     extras,
	}
	token = m.tokens.Issue(meta)
	return token, m.endpoint(), meta
}

// PrepareProxy 为 Kelpie 继电的代理/端口监听生成令牌。
func (m *Manager) PrepareProxy(tenant, operator, target string, ttl time.Duration, extra map[string]string) (token string, endpoint string, meta TokenMeta) {
	if m == nil || m.tokens == nil {
		return "", "", TokenMeta{}
	}
	if ttl <= 0 {
		ttl = m.ttl
	}
	meta = TokenMeta{
		Tenant:    strings.TrimSpace(tenant),
		Operator:  strings.TrimSpace(operator),
		Target:    strings.TrimSpace(target),
		Direction: "proxy",
		MaxSize:   m.cfg.MaxSize,
		MaxRate:   m.cfg.MaxRateBps,
		ExpiresAt: time.Now().Add(ttl),
		Extra:     extra,
	}
	token = m.tokens.Issue(meta)
	return token, m.endpoint(), meta
}

func firstPositive(val int64, fallback int64) int64 {
	if val > 0 {
		return val
	}
	return fallback
}

func firstPositiveFloat(val, fallback float64) float64 {
	if val > 0 {
		return val
	}
	return fallback
}

// endpoint 返回带 scheme 的监听地址，供客户端选择 TCP/TLS。
func (m *Manager) endpoint() string {
	if m == nil {
		return ""
	}
	host := strings.TrimSpace(m.cfg.Listen)
	if host == "" {
		return ""
	}
	scheme := "tcp://"
	if m.cfg.EnableTLS {
		scheme = "tcps://"
	}
	return scheme + host
}
