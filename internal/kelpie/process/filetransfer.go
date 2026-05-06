package process

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// RemoteFileTransferResult summarizes a completed remote file upload.
type RemoteFileTransferResult struct {
	RemotePath string
	Size       uint64
	Hash       string
	Mime       string
	Message    string
}

type closeWriter interface {
	CloseWrite() error
}

// UploadRemoteFile streams data from Kelpie to a target node using the existing
// file-put stream handler implemented by flock.
func (admin *Admin) UploadRemoteFile(ctx context.Context, targetUUID, remotePath string, r io.Reader, size uint64, sha256Hex string) (RemoteFileTransferResult, error) {
	if admin == nil {
		return RemoteFileTransferResult{}, fmt.Errorf("admin unavailable")
	}
	targetUUID = strings.TrimSpace(targetUUID)
	remotePath = strings.TrimSpace(remotePath)
	sha256Hex = strings.ToLower(strings.TrimSpace(sha256Hex))
	if targetUUID == "" {
		return RemoteFileTransferResult{}, fmt.Errorf("target uuid required")
	}
	if remotePath == "" {
		return RemoteFileTransferResult{}, fmt.Errorf("remote path required")
	}
	if r == nil {
		return RemoteFileTransferResult{}, fmt.Errorf("upload reader required")
	}

	meta := map[string]string{
		"kind": "file-put",
		"path": remotePath,
	}
	if size > 0 {
		meta["size"] = strconv.FormatUint(size, 10)
	}
	if sha256Hex != "" {
		meta["hash"] = sha256Hex
	}
	streamHandle, err := admin.OpenStream(ctx, targetUUID, "", meta)
	if err != nil {
		return RemoteFileTransferResult{}, fmt.Errorf("open remote upload stream: %w", err)
	}
	defer streamHandle.Close()

	if _, err := io.Copy(streamHandle, r); err != nil {
		return RemoteFileTransferResult{}, fmt.Errorf("write remote upload stream: %w", err)
	}
	if closer, ok := streamHandle.(closeWriter); ok {
		if err := closer.CloseWrite(); err != nil {
			return RemoteFileTransferResult{}, fmt.Errorf("close remote upload writer: %w", err)
		}
	}
	_, _ = io.Copy(io.Discard, streamHandle)

	reason := ""
	if streamID := streamHandleID(streamHandle); streamID != 0 {
		reason = admin.StreamCloseReason(streamID)
	}
	if reason != "" && !streamCloseSucceeded(reason) {
		return RemoteFileTransferResult{}, fmt.Errorf("remote file upload failed: %s", reason)
	}
	result := RemoteFileTransferResult{
		RemotePath: remotePath,
		Size:       size,
		Hash:       sha256Hex,
		Message:    reason,
	}
	if parsed := parseStreamReason(reason); parsed != nil {
		if parsed.size > 0 {
			result.Size = uint64(parsed.size)
		}
		if parsed.hash != "" {
			result.Hash = parsed.hash
		}
		if parsed.mime != "" {
			result.Mime = parsed.mime
		}
	}
	return result, nil
}
