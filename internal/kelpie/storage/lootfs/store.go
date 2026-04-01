package lootfs

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
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
	if len(data) == 0 {
		return "", 0, "", fmt.Errorf("lootfs: empty content")
	}
	return s.StoreStream(lootID, bytes.NewReader(data), mimeType)
}

// StoreStream 将流式内容写入磁盘，并返回存储引用、大小和 sha256。
func (s *Store) StoreStream(lootID string, r io.Reader, mimeType string) (string, uint64, string, error) {
	if s == nil {
		return "", 0, "", fmt.Errorf("lootfs: nil store")
	}
	if strings.TrimSpace(lootID) == "" {
		return "", 0, "", fmt.Errorf("lootfs: loot id required")
	}
	if r == nil {
		return "", 0, "", fmt.Errorf("lootfs: reader required")
	}
	filename := lootID
	if ext := guessExt(mimeType); ext != "" {
		filename = fmt.Sprintf("%s%s", lootID, ext)
	}
	fullPath := filepath.Join(s.baseDir, filename)
	tmpFile, err := os.CreateTemp(s.baseDir, lootID+".tmp-*")
	if err != nil {
		return "", 0, "", fmt.Errorf("lootfs: create temp file: %w", err)
	}
	tmpPath := tmpFile.Name()
	defer func() {
		_ = tmpFile.Close()
		_ = os.Remove(tmpPath)
	}()
	hasher := sha256.New()
	written, err := io.Copy(io.MultiWriter(tmpFile, hasher), r)
	if err != nil {
		return "", 0, "", fmt.Errorf("lootfs: write stream: %w", err)
	}
	if written <= 0 {
		return "", 0, "", fmt.Errorf("lootfs: empty content")
	}
	if err := tmpFile.Chmod(0o644); err != nil {
		return "", 0, "", fmt.Errorf("lootfs: chmod temp file: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		return "", 0, "", fmt.Errorf("lootfs: close temp file: %w", err)
	}
	if err := os.Rename(tmpPath, fullPath); err != nil {
		return "", 0, "", fmt.Errorf("lootfs: commit file: %w", err)
	}
	return fullPath, uint64(written), hex.EncodeToString(hasher.Sum(nil)), nil
}

// Open 以只读流的形式打开已落盘的 loot 内容，并返回大小。
func (s *Store) Open(storageRef string) (io.ReadCloser, uint64, error) {
	if s == nil {
		return nil, 0, fmt.Errorf("lootfs: nil store")
	}
	fullPath, err := s.resolveStoragePath(storageRef)
	if err != nil {
		return nil, 0, err
	}
	f, err := os.Open(fullPath)
	if err != nil {
		return nil, 0, fmt.Errorf("lootfs: open file: %w", err)
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, 0, fmt.Errorf("lootfs: stat file: %w", err)
	}
	size := uint64(0)
	if info.Size() > 0 {
		size = uint64(info.Size())
	}
	return f, size, nil
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
