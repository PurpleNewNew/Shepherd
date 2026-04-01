package process

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"mime"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"codeberg.org/agnoie/shepherd/pkg/filebrowser"
)

const defaultMaxFileSize int64 = 0

// 简单文件流：file-put/file-get
type fileStream struct {
	mode         string // put/get
	path         string
	f            *os.File
	written      int64
	hasher       hash.Hash
	size         int64
	mime         string
	limit        int64  // max bytes allowed for this transfer (0=unlimited)
	expectedSize int64  // expected bytes for this transfer (after offset)
	expectedHash string // optional sha256 hex (lowercase)
	offset       int64
	failed       bool
	failReason   string
	closed       bool
}

func (agent *Agent) fileOnOpen(streamID uint32, opts map[string]string) {
	if agent == nil {
		return
	}
	kind := strings.ToLower(strings.TrimSpace(opts["kind"]))
	path := strings.TrimSpace(opts["path"])
	maxSize := parsePositiveInt64(opts["max_size"])
	expectedSize := parsePositiveInt64(opts["size"])
	expectedHash := strings.ToLower(strings.TrimSpace(opts["hash"]))
	offset := parseOffset(opts["offset"])
	expectedTotal := expectedSize
	if expectedTotal > 0 {
		expectedTotal += offset
	}
	limit := effectiveLimit(maxSize, expectedTotal)
	if path == "" {
		agent.sendStreamClose(streamID, 1, "empty path")
		return
	}
	if limit > 0 && offset > limit {
		agent.sendStreamClose(streamID, 1, "offset exceeds limit")
		return
	}
	target, err := agent.sanitizeFilePath(path)
	if err != nil {
		agent.sendStreamClose(streamID, 1, err.Error())
		return
	}
	switch kind {
	case "file-put":
		remaining := remainingAfterOffset(expectedSize, offset)
		if expectedSize > 0 && remaining == 0 {
			agent.sendStreamClose(streamID, 1, "offset exceeds expected size")
			return
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			agent.sendStreamClose(streamID, 1, err.Error())
			return
		}
		f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			agent.sendStreamClose(streamID, 1, err.Error())
			return
		}
		info, statErr := f.Stat()
		if statErr != nil {
			agent.sendStreamClose(streamID, 1, statErr.Error())
			_ = f.Close()
			return
		}
		if offset > 0 {
			if info.Size() < offset {
				agent.sendStreamClose(streamID, 1, "offset exceeds existing file size")
				_ = f.Close()
				return
			}
			if _, err := f.Seek(offset, io.SeekStart); err != nil {
				agent.sendStreamClose(streamID, 1, err.Error())
				_ = f.Close()
				return
			}
		} else {
			if err := f.Truncate(0); err != nil {
				agent.sendStreamClose(streamID, 1, err.Error())
				_ = f.Close()
				return
			}
		}
		st := &fileStream{
			mode:         "put",
			path:         target,
			f:            f,
			hasher:       sha256.New(),
			mime:         mime.TypeByExtension(filepath.Ext(target)),
			written:      offset,
			limit:        limit,
			expectedSize: remaining,
			expectedHash: expectedHash,
			offset:       offset,
		}
		agent.fileMu.Lock()
		if agent.fileByID == nil {
			agent.fileByID = make(map[uint32]*fileStream)
		}
		agent.fileByID[streamID] = st
		agent.fileMu.Unlock()
	case "file-get":
		info, err := os.Stat(target)
		if err != nil {
			agent.sendStreamClose(streamID, 1, err.Error())
			return
		}
		if limit > 0 && info.Size() > limit {
			agent.sendStreamClose(streamID, 1, "file too large")
			return
		}
		f, err := os.Open(target)
		if err != nil {
			agent.sendStreamClose(streamID, 1, err.Error())
			return
		}
		if offset > 0 {
			if offset >= info.Size() {
				agent.sendStreamClose(streamID, 1, "offset exceeds file size")
				_ = f.Close()
				return
			}
			if _, err := f.Seek(offset, io.SeekStart); err != nil {
				agent.sendStreamClose(streamID, 1, err.Error())
				_ = f.Close()
				return
			}
		}
		actualRemaining := info.Size() - offset
		if expectedSize > 0 && actualRemaining != expectedSize {
			agent.sendStreamClose(streamID, 1, "file size mismatch")
			_ = f.Close()
			return
		}
		st := &fileStream{
			mode:         "get",
			path:         target,
			f:            f,
			hasher:       sha256.New(),
			size:         actualRemaining,
			expectedSize: actualRemaining,
			expectedHash: expectedHash,
			mime:         mime.TypeByExtension(filepath.Ext(target)),
			offset:       offset,
			limit:        limit,
		}
		agent.fileMu.Lock()
		if agent.fileByID == nil {
			agent.fileByID = make(map[uint32]*fileStream)
		}
		agent.fileByID[streamID] = st
		agent.fileMu.Unlock()
		go func(id uint32, st *fileStream) {
			buf := make([]byte, 32768)
			for {
				n, err := st.f.Read(buf)
				if n > 0 {
					chunk := append([]byte(nil), buf[:n]...)
					agent.sendStreamData(id, chunk)
					st.written += int64(n)
					if st.hasher != nil {
						_, _ = st.hasher.Write(chunk)
					}
				}
				if err != nil {
					if err != io.EOF {
						st.failed = true
						st.failReason = err.Error()
					}
					agent.fileOnClose(id)
					return
				}
			}
		}(streamID, st)
	case "file-list":
		go agent.fileListOnOpen(streamID, path)
	default:
		agent.sendStreamClose(streamID, 1, "unsupported file mode")
	}
}

func (agent *Agent) fileListOnOpen(streamID uint32, requestedPath string) {
	if agent == nil {
		return
	}
	requestedPath = strings.TrimSpace(requestedPath)
	if requestedPath == "" {
		listing, err := agent.rootListing()
		if err != nil {
			agent.sendStreamClose(streamID, 1, err.Error())
			return
		}
		listing.RequestedPath = ""
		payload, err := filebrowser.Marshal(listing)
		if err != nil {
			agent.sendStreamClose(streamID, 1, err.Error())
			return
		}
		for len(payload) > 0 {
			chunk := payload
			if len(chunk) > 32768 {
				chunk = payload[:32768]
			}
			agent.sendStreamData(streamID, chunk)
			payload = payload[len(chunk):]
		}
		agent.sendStreamClose(streamID, 0, fmt.Sprintf("ok entries=%d", len(listing.Entries)))
		return
	}
	target, err := agent.sanitizeFilePath(requestedPath)
	if err != nil {
		agent.sendStreamClose(streamID, 1, err.Error())
		return
	}
	root := filesystemRoot(target)
	listing, err := directoryListing(target, requestedPath, root)
	if err != nil {
		agent.sendStreamClose(streamID, 1, err.Error())
		return
	}
	payload, err := filebrowser.Marshal(listing)
	if err != nil {
		agent.sendStreamClose(streamID, 1, err.Error())
		return
	}
	for len(payload) > 0 {
		chunk := payload
		if len(chunk) > 32768 {
			chunk = payload[:32768]
		}
		agent.sendStreamData(streamID, chunk)
		payload = payload[len(chunk):]
	}
	agent.sendStreamClose(streamID, 0, fmt.Sprintf("ok entries=%d", len(listing.Entries)))
}

func (agent *Agent) rootListing() (filebrowser.Listing, error) {
	if runtime.GOOS == "windows" {
		entries := make([]filebrowser.Entry, 0, 26)
		for drive := 'A'; drive <= 'Z'; drive++ {
			path := fmt.Sprintf("%c:\\", drive)
			info, err := os.Stat(path)
			if err != nil || !info.IsDir() {
				continue
			}
			entries = append(entries, filebrowser.Entry{
				Name:    path[:2],
				Path:    path,
				IsDir:   true,
				IsDrive: true,
				Mode:    info.Mode().String(),
			})
		}
		return filebrowser.Listing{
			DisplayPath: "Computer",
			VirtualRoot: true,
			Entries:     entries,
		}, nil
	}
	return directoryListing("/", "/", "/")
}

func directoryListing(target, requestedPath, root string) (filebrowser.Listing, error) {
	info, err := os.Stat(target)
	if err != nil {
		return filebrowser.Listing{}, err
	}
	if !info.IsDir() {
		return filebrowser.Listing{}, fmt.Errorf("path is not a directory")
	}
	entries, err := os.ReadDir(target)
	if err != nil {
		return filebrowser.Listing{}, err
	}
	sort.Slice(entries, func(i, j int) bool {
		leftName := strings.ToLower(entries[i].Name())
		rightName := strings.ToLower(entries[j].Name())
		leftDir := entries[i].IsDir()
		rightDir := entries[j].IsDir()
		if leftDir != rightDir {
			return leftDir
		}
		return leftName < rightName
	})

	listing := filebrowser.Listing{
		RequestedPath: requestedPath,
		ResolvedPath:  target,
		DisplayPath:   target,
		RootPath:      root,
	}
	if canGoUp(root, target) {
		listing.CanGoUp = true
		listing.ParentPath = filepath.Dir(target)
	}
	listing.Entries = make([]filebrowser.Entry, 0, len(entries))
	for _, entry := range entries {
		fullPath := filepath.Join(target, entry.Name())
		lstat, err := os.Lstat(fullPath)
		if err != nil {
			continue
		}
		isSymlink := lstat.Mode()&os.ModeSymlink != 0
		isDir := lstat.IsDir()
		size := lstat.Size()
		modifiedAt := lstat.ModTime().UTC().Format(time.RFC3339Nano)
		mode := lstat.Mode().String()
		if isSymlink {
			if targetInfo, statErr := os.Stat(fullPath); statErr == nil {
				isDir = targetInfo.IsDir()
				if !isDir {
					size = targetInfo.Size()
				}
				modifiedAt = targetInfo.ModTime().UTC().Format(time.RFC3339Nano)
			}
		}
		listing.Entries = append(listing.Entries, filebrowser.Entry{
			Name:       entry.Name(),
			Path:       fullPath,
			IsDir:      isDir,
			IsSymlink:  isSymlink,
			Size:       size,
			Mode:       mode,
			ModifiedAt: modifiedAt,
			Hidden:     strings.HasPrefix(entry.Name(), "."),
		})
	}
	return listing, nil
}

func (agent *Agent) fileOnData(streamID uint32, data []byte) {
	if len(data) == 0 || agent == nil {
		return
	}
	agent.fileMu.Lock()
	st := agent.fileByID[streamID]
	agent.fileMu.Unlock()
	if st == nil || st.f == nil || st.mode != "put" {
		return
	}
	if st.failed {
		return
	}
	nextTotal := st.written + int64(len(data))
	transferred := nextTotal - st.offset
	if st.expectedSize > 0 && transferred > st.expectedSize {
		st.failed = true
		st.failReason = fmt.Sprintf("file exceeds expected size (%d > %d)", transferred, st.expectedSize)
		agent.fileOnClose(streamID)
		return
	}
	if st.limit > 0 && nextTotal > st.limit {
		st.failed = true
		st.failReason = fmt.Sprintf("file exceeds limit (%d > %d)", nextTotal, st.limit)
		agent.fileOnClose(streamID)
		return
	}
	if _, err := st.f.Write(data); err != nil {
		st.failed = true
		st.failReason = err.Error()
		agent.fileOnClose(streamID)
		return
	}
	st.written = nextTotal
	if st.hasher != nil {
		_, _ = st.hasher.Write(data)
	}
}

func (agent *Agent) fileOnClose(streamID uint32) {
	if agent == nil {
		return
	}
	agent.fileMu.Lock()
	st := agent.fileByID[streamID]
	delete(agent.fileByID, streamID)
	agent.fileMu.Unlock()
	if st == nil {
		return
	}
	if st.closed {
		return
	}
	st.closed = true
	if st.f != nil {
		_ = st.f.Close()
	}

	var (
		code   uint16
		reason string
	)
	switch st.mode {
	case "put":
		actualSize := st.written
		if info, err := os.Stat(st.path); err == nil {
			actualSize = info.Size()
			expectedTotal := st.expectedSize
			if expectedTotal > 0 {
				expectedTotal += st.offset
			}
			if expectedTotal > 0 && actualSize != expectedTotal {
				st.failed = true
				st.failReason = fmt.Sprintf("size mismatch expected=%d got=%d", expectedTotal, actualSize)
			}
			if st.limit > 0 && actualSize > st.limit {
				st.failed = true
				st.failReason = fmt.Sprintf("file exceeds limit (%d > %d)", actualSize, st.limit)
			}
		} else {
			st.failed = true
			st.failReason = err.Error()
		}

		hashHex := ""
		if st.expectedHash != "" || st.hasher != nil {
			if size, sumHex, err := computeFileHash(st.path); err == nil {
				actualSize = size
				hashHex = sumHex
				if st.expectedHash != "" && !strings.EqualFold(hashHex, st.expectedHash) {
					st.failed = true
					st.failReason = fmt.Sprintf("hash mismatch expected=%s got=%s", st.expectedHash, hashHex)
				}
			} else {
				st.failed = true
				st.failReason = err.Error()
			}
		}

		if st.failed {
			_ = os.Remove(st.path)
			code = 1
			if st.failReason == "" {
				st.failReason = "file transfer failed"
			}
			reason = st.failReason
		} else {
			if hashHex == "" && st.hasher != nil {
				hashHex = hex.EncodeToString(st.hasher.Sum(nil))
			}
			reason = fmt.Sprintf("ok size=%d hash=%s mime=%s", actualSize, hashHex, st.mime)
		}
	case "get":
		hashHex := ""
		if st.hasher != nil {
			hashHex = hex.EncodeToString(st.hasher.Sum(nil))
		}
		if st.failed {
			code = 1
			if st.failReason == "" {
				st.failReason = "file read failed"
			}
			reason = st.failReason
		} else if st.expectedSize > 0 && st.written != st.expectedSize {
			code = 1
			reason = fmt.Sprintf("size mismatch expected=%d got=%d", st.expectedSize, st.written)
		} else if st.expectedHash != "" && !strings.EqualFold(hashHex, st.expectedHash) {
			code = 1
			reason = fmt.Sprintf("hash mismatch expected=%s got=%s", st.expectedHash, hashHex)
		} else {
			reason = fmt.Sprintf("ok size=%d hash=%s mime=%s", st.written, hashHex, st.mime)
		}
	default:
		return
	}
	agent.sendStreamClose(streamID, code, reason)
}

func computeFileHash(path string) (int64, string, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, "", err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return n, "", err
	}
	return n, hex.EncodeToString(h.Sum(nil)), nil
}

func parsePositiveInt64(val string) int64 {
	if strings.TrimSpace(val) == "" {
		return 0
	}
	n, err := strconv.ParseInt(strings.TrimSpace(val), 10, 64)
	if err != nil || n <= 0 {
		return 0
	}
	return n
}

func remainingAfterOffset(size, offset int64) int64 {
	if size <= 0 {
		return 0
	}
	if offset < 0 {
		offset = 0
	}
	if offset >= size {
		return 0
	}
	return size - offset
}

func effectiveLimit(maxSize, expectedTotal int64) int64 {
	limit := maxSize
	if limit <= 0 {
		limit = defaultMaxFileSize
	}
	if expectedTotal > 0 && (limit == 0 || expectedTotal < limit) {
		limit = expectedTotal
	}
	return limit
}

func parseOffset(val string) int64 {
	if strings.TrimSpace(val) == "" {
		return 0
	}
	n, err := strconv.ParseInt(strings.TrimSpace(val), 10, 64)
	if err != nil || n < 0 {
		return 0
	}
	return n
}

func (agent *Agent) sanitizeFilePath(path string) (string, error) {
	if agent == nil {
		return "", fmt.Errorf("agent unavailable")
	}
	base := strings.TrimSpace(agent.workDir)
	if base == "" {
		base = "."
	}
	base = filepath.Clean(base)
	clean := filepath.Clean(strings.TrimSpace(path))
	if clean == "." && strings.TrimSpace(path) == "" {
		return base, nil
	}
	if volume := filepath.VolumeName(clean); volume != "" && volume == clean {
		clean += string(filepath.Separator)
	}
	if filepath.IsAbs(clean) {
		return clean, nil
	}
	return filepath.Clean(filepath.Join(base, clean)), nil
}

func canGoUp(root, target string) bool {
	root = filepath.Clean(root)
	target = filepath.Clean(target)
	if root == "" || target == "" || root == target {
		return false
	}
	rel, err := filepath.Rel(root, target)
	if err != nil {
		return false
	}
	if rel == "." || rel == "" || strings.HasPrefix(rel, "..") {
		return false
	}
	return true
}

func filesystemRoot(path string) string {
	clean := filepath.Clean(path)
	if !filepath.IsAbs(clean) {
		if abs, err := filepath.Abs(clean); err == nil {
			clean = abs
		}
	}
	volume := filepath.VolumeName(clean)
	if volume != "" {
		return volume + string(filepath.Separator)
	}
	if filepath.IsAbs(clean) {
		return string(filepath.Separator)
	}
	return filepath.Clean(clean)
}
