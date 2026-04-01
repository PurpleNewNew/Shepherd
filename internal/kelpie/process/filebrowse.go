package process

import (
	"context"
	"fmt"
	"io"
	"strings"

	"codeberg.org/agnoie/shepherd/pkg/filebrowser"
)

// ListRemoteFiles 通过结构化 file-list stream 浏览目标节点的远端目录。
func (admin *Admin) ListRemoteFiles(ctx context.Context, targetUUID, remotePath string) (filebrowser.Listing, error) {
	if admin == nil {
		return filebrowser.Listing{}, fmt.Errorf("admin unavailable")
	}
	targetUUID = strings.TrimSpace(targetUUID)
	remotePath = strings.TrimSpace(remotePath)
	if targetUUID == "" {
		return filebrowser.Listing{}, fmt.Errorf("target uuid required")
	}
	streamHandle, err := admin.OpenStream(ctx, targetUUID, "", map[string]string{
		"kind": "file-list",
		"path": remotePath,
	})
	if err != nil {
		return filebrowser.Listing{}, fmt.Errorf("open remote file list stream: %w", err)
	}
	defer streamHandle.Close()

	data, err := io.ReadAll(streamHandle)
	if err != nil {
		return filebrowser.Listing{}, fmt.Errorf("read remote file list: %w", err)
	}
	if streamID := streamHandleID(streamHandle); streamID != 0 {
		reason := admin.StreamCloseReason(streamID)
		if reason != "" && !streamCloseSucceeded(reason) {
			return filebrowser.Listing{}, fmt.Errorf("remote file list failed: %s", reason)
		}
	}
	listing, err := filebrowser.Unmarshal(data)
	if err != nil {
		return filebrowser.Listing{}, fmt.Errorf("decode remote file list: %w", err)
	}
	return listing, nil
}
