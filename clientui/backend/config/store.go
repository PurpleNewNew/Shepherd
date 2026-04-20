package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"time"
)

// Store 持久化 Stockman 客户端的轻量级状态：最近连接与 TOFU 指纹白名单。
// 采用 JSON 文件存储，避免引入额外依赖。
type Store struct {
	path string
	mu   sync.RWMutex
	data persistedData
}

type persistedData struct {
	Version      int                       `json:"version"`
	Theme        string                    `json:"theme,omitempty"`
	Recent       []RecentConnection        `json:"recent"`
	Fingerprints map[string]TrustedCertRef `json:"fingerprints"`
}

// RecentConnection 描述一条最近使用的连接记录。
// Token 仅在用户勾选“记住 token”时保存，默认留空，每次手输。
type RecentConnection struct {
	ID          string    `json:"id"`
	Label       string    `json:"label,omitempty"`
	Endpoint    string    `json:"endpoint"`
	Token       string    `json:"token,omitempty"`
	TLSEnabled  bool      `json:"tls_enabled"`
	ServerName  string    `json:"server_name,omitempty"`
	Fingerprint string    `json:"fingerprint,omitempty"`
	LastUsedAt  time.Time `json:"last_used_at"`
	CreatedAt   time.Time `json:"created_at"`
}

// TrustedCertRef 描述一条 TOFU 通过后的证书指纹。
type TrustedCertRef struct {
	Endpoint    string    `json:"endpoint"`
	Fingerprint string    `json:"fingerprint"`
	TrustedAt   time.Time `json:"trusted_at"`
}

const (
	currentVersion = 1
	maxRecent      = 12
)

// New 打开或创建配置文件。若文件不存在或损坏，则初始化空数据（旧文件会被覆盖）。
func New() (*Store, error) {
	path, err := DefaultPath()
	if err != nil {
		return nil, err
	}
	return NewAt(path)
}

// NewAt 在指定路径打开或创建配置文件，便于测试。
func NewAt(path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create config dir: %w", err)
	}
	store := &Store{path: path}
	store.data.Version = currentVersion
	store.data.Fingerprints = make(map[string]TrustedCertRef)
	if err := store.load(); err != nil && !errors.Is(err, os.ErrNotExist) {
		// 损坏文件：重命名备份，创建新文件。
		backup := path + ".corrupt-" + time.Now().Format("20060102-150405")
		_ = os.Rename(path, backup)
	}
	return store, nil
}

// DefaultPath 按平台惯例给出配置文件路径。
func DefaultPath() (string, error) {
	base, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}
	var subdir string
	switch runtime.GOOS {
	case "darwin":
		// UserConfigDir 在 macOS 下返回 ~/Library/Application Support，
		// 我们使用 "Shepherd/Stockman" 作为子目录，保持和 Finder 友好。
		subdir = filepath.Join("Shepherd", "Stockman")
	case "windows":
		subdir = filepath.Join("Shepherd", "Stockman")
	default:
		subdir = filepath.Join("shepherd", "stockman")
	}
	return filepath.Join(base, subdir, "config.json"), nil
}

func (s *Store) load() error {
	raw, err := os.ReadFile(s.path)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := json.Unmarshal(raw, &s.data); err != nil {
		return fmt.Errorf("parse config: %w", err)
	}
	if s.data.Fingerprints == nil {
		s.data.Fingerprints = make(map[string]TrustedCertRef)
	}
	if s.data.Version == 0 {
		s.data.Version = currentVersion
	}
	return nil
}

func (s *Store) save() error {
	s.data.Version = currentVersion
	raw, err := json.MarshalIndent(s.data, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

// Path 返回配置文件路径（用于诊断/UI 展示）。
func (s *Store) Path() string { return s.path }

// ListRecent 返回最近连接的快照，按 LastUsedAt 倒序。
func (s *Store) ListRecent() []RecentConnection {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]RecentConnection, len(s.data.Recent))
	copy(out, s.data.Recent)
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].LastUsedAt.After(out[j].LastUsedAt)
	})
	return out
}

// UpsertRecent 写入或更新一条连接记录（按 endpoint 去重）。
func (s *Store) UpsertRecent(rec RecentConnection) error {
	if rec.Endpoint == "" {
		return errors.New("endpoint required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	if rec.CreatedAt.IsZero() {
		rec.CreatedAt = now
	}
	rec.LastUsedAt = now
	found := false
	for i := range s.data.Recent {
		if s.data.Recent[i].Endpoint == rec.Endpoint {
			rec.CreatedAt = s.data.Recent[i].CreatedAt
			if rec.ID == "" {
				rec.ID = s.data.Recent[i].ID
			}
			s.data.Recent[i] = rec
			found = true
			break
		}
	}
	if !found {
		if rec.ID == "" {
			rec.ID = fmt.Sprintf("conn-%d", now.UnixNano())
		}
		s.data.Recent = append(s.data.Recent, rec)
	}
	if len(s.data.Recent) > maxRecent {
		// 按 LastUsedAt 倒序保留 maxRecent 条。
		sort.SliceStable(s.data.Recent, func(i, j int) bool {
			return s.data.Recent[i].LastUsedAt.After(s.data.Recent[j].LastUsedAt)
		})
		s.data.Recent = s.data.Recent[:maxRecent]
	}
	return s.save()
}

// RemoveRecent 删除一条连接记录。
func (s *Store) RemoveRecent(id string) error {
	if id == "" {
		return errors.New("id required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, rec := range s.data.Recent {
		if rec.ID != id {
			s.data.Recent[n] = rec
			n++
		}
	}
	s.data.Recent = s.data.Recent[:n]
	return s.save()
}

// LookupFingerprint 返回 endpoint 对应的已信任指纹（大写，去冒号后的原始 hex 也接受）。
func (s *Store) LookupFingerprint(endpoint string) (TrustedCertRef, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ref, ok := s.data.Fingerprints[endpoint]
	return ref, ok
}

// TrustFingerprint 记录一个 endpoint 的证书指纹，后续连接将按此校验。
func (s *Store) TrustFingerprint(endpoint, fingerprint string) error {
	if endpoint == "" || fingerprint == "" {
		return errors.New("endpoint and fingerprint required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.data.Fingerprints == nil {
		s.data.Fingerprints = make(map[string]TrustedCertRef)
	}
	s.data.Fingerprints[endpoint] = TrustedCertRef{
		Endpoint:    endpoint,
		Fingerprint: fingerprint,
		TrustedAt:   time.Now(),
	}
	return s.save()
}

// ForgetFingerprint 删除指纹信任。
func (s *Store) ForgetFingerprint(endpoint string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data.Fingerprints, endpoint)
	return s.save()
}

// SetTheme 简单保存界面主题偏好。
func (s *Store) SetTheme(theme string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data.Theme = theme
	return s.save()
}

// GetTheme 读取主题偏好；为空则返回默认值 "dark"。
func (s *Store) GetTheme() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.data.Theme == "" {
		return "dark"
	}
	return s.data.Theme
}
