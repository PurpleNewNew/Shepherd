package lootfs

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"mime"
	"os"
	"path/filepath"
	"strings"
)

// Store 提供基于文件系统的 loot 内容落盘。
type Store struct {
	baseDir string
}

func New(baseDir string) (*Store, error) {
	baseDir = strings.TrimSpace(baseDir)
	if baseDir == "" {
		return nil, fmt.Errorf("lootfs: baseDir required")
	}
	absBase, err := filepath.Abs(baseDir)
	if err != nil {
		return nil, fmt.Errorf("lootfs: resolve base dir: %w", err)
	}
	if err := os.MkdirAll(absBase, 0o755); err != nil {
		return nil, fmt.Errorf("lootfs: prepare dir: %w", err)
	}
	return &Store{baseDir: absBase}, nil
}

// Store 将内容写入磁盘，并返回存储引用（绝对路径）、大小和 sha256。
func (s *Store) Store(lootID string, data []byte, mimeType string) (string, uint64, string, error) {
	if s == nil {
		return "", 0, "", fmt.Errorf("lootfs: nil store")
	}
	if strings.TrimSpace(lootID) == "" {
		return "", 0, "", fmt.Errorf("lootfs: loot id required")
	}
	if len(data) == 0 {
		return "", 0, "", fmt.Errorf("lootfs: empty content")
	}
	filename := lootID
	if ext := guessExt(mimeType); ext != "" {
		filename = fmt.Sprintf("%s%s", lootID, ext)
	}
	fullPath := filepath.Join(s.baseDir, filename)
	if err := os.WriteFile(fullPath, data, 0o644); err != nil {
		return "", 0, "", fmt.Errorf("lootfs: write file: %w", err)
	}
	hash := sha256.Sum256(data)
	return fullPath, uint64(len(data)), hex.EncodeToString(hash[:]), nil
}

// Load 读取 Store 管理目录内的内容。
func (s *Store) Load(storageRef string) ([]byte, error) {
	if s == nil {
		return nil, fmt.Errorf("lootfs: nil store")
	}
	fullPath, err := s.resolveStoragePath(storageRef)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(fullPath)
	if err != nil {
		return nil, fmt.Errorf("lootfs: read file: %w", err)
	}
	return data, nil
}

func (s *Store) resolveStoragePath(storageRef string) (string, error) {
	storageRef = strings.TrimSpace(storageRef)
	if storageRef == "" {
		return "", fmt.Errorf("lootfs: storage ref required")
	}
	fullPath := filepath.Clean(storageRef)
	if !filepath.IsAbs(fullPath) {
		fullPath = filepath.Join(s.baseDir, fullPath)
	}
	baseDir := filepath.Clean(s.baseDir)
	rel, err := filepath.Rel(baseDir, fullPath)
	if err != nil {
		return "", fmt.Errorf("lootfs: resolve storage ref: %w", err)
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return "", fmt.Errorf("lootfs: storage ref outside base dir")
	}
	return fullPath, nil
}

func guessExt(mimeType string) string {
	mimeType = strings.TrimSpace(strings.ToLower(mimeType))
	if mimeType == "" {
		return ""
	}
	if exts, err := mime.ExtensionsByType(mimeType); err == nil && len(exts) > 0 {
		return exts[0]
	}
	switch mimeType {
	case "image/png":
		return ".png"
	case "image/jpeg":
		return ".jpg"
	case "text/plain":
		return ".txt"
	}
	return ""
}
