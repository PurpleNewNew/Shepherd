package grpcserver

import (
	"context"
	"io"
	"strings"

	"codeberg.org/agnoie/shepherd/internal/kelpie/uipb"
	"codeberg.org/agnoie/shepherd/pkg/filebrowser"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *service) ListRemoteFiles(ctx context.Context, req *uipb.ListRemoteFilesRequest) (*uipb.ListRemoteFilesResponse, error) {
	if s == nil || s.admin == nil {
		return nil, status.Error(codes.Unavailable, "admin unavailable")
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "missing request")
	}
	targetUUID := strings.TrimSpace(req.GetTargetUuid())
	if targetUUID == "" {
		return nil, status.Error(codes.InvalidArgument, "target uuid required")
	}
	listing, err := s.admin.ListRemoteFiles(ctx, targetUUID, req.GetPath())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return convertRemoteFileListing(listing), nil
}

func (s *service) UploadRemoteFile(stream uipb.KelpieUIService_UploadRemoteFileServer) error {
	if s == nil || s.admin == nil {
		return status.Error(codes.Unavailable, "admin unavailable")
	}
	first, err := stream.Recv()
	if err != nil {
		if err == io.EOF {
			return status.Error(codes.InvalidArgument, "empty upload stream")
		}
		return status.Errorf(codes.Unavailable, "receive upload metadata: %v", err)
	}
	targetUUID := strings.TrimSpace(first.GetTargetUuid())
	remotePath := strings.TrimSpace(first.GetRemotePath())
	if targetUUID == "" || remotePath == "" {
		return status.Error(codes.InvalidArgument, "target uuid and remote path required")
	}

	reader, writer := io.Pipe()
	recvErrCh := make(chan error, 1)
	go func() {
		defer close(recvErrCh)
		if data := first.GetData(); len(data) > 0 {
			if _, err := writer.Write(data); err != nil {
				_ = writer.CloseWithError(err)
				recvErrCh <- err
				return
			}
		}
		for {
			chunk, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					_ = writer.Close()
					return
				}
				_ = writer.CloseWithError(err)
				recvErrCh <- err
				return
			}
			if data := chunk.GetData(); len(data) > 0 {
				if _, err := writer.Write(data); err != nil {
					_ = writer.CloseWithError(err)
					recvErrCh <- err
					return
				}
			}
		}
	}()

	result, err := s.admin.UploadRemoteFile(stream.Context(), targetUUID, remotePath, reader, first.GetSize(), first.GetSha256())
	if err != nil {
		_ = reader.CloseWithError(err)
		return status.Errorf(codes.Internal, "upload remote file failed: %v", err)
	}
	if recvErr := <-recvErrCh; recvErr != nil {
		return status.Errorf(codes.Unavailable, "receive upload data: %v", recvErr)
	}
	return stream.SendAndClose(&uipb.UploadRemoteFileResponse{
		RemotePath: result.RemotePath,
		Size:       result.Size,
		Sha256:     result.Hash,
		Mime:       result.Mime,
		Message:    result.Message,
	})
}

func convertRemoteFileListing(listing filebrowser.Listing) *uipb.ListRemoteFilesResponse {
	resp := &uipb.ListRemoteFilesResponse{
		RequestedPath: listing.RequestedPath,
		ResolvedPath:  listing.ResolvedPath,
		DisplayPath:   listing.DisplayPath,
		RootPath:      listing.RootPath,
		ParentPath:    listing.ParentPath,
		CanGoUp:       listing.CanGoUp,
		VirtualRoot:   listing.VirtualRoot,
	}
	if len(listing.Entries) > 0 {
		resp.Entries = make([]*uipb.RemoteFileEntry, 0, len(listing.Entries))
		for _, entry := range listing.Entries {
			size := uint64(0)
			if entry.Size > 0 {
				size = uint64(entry.Size)
			}
			resp.Entries = append(resp.Entries, &uipb.RemoteFileEntry{
				Name:       entry.Name,
				Path:       entry.Path,
				IsDir:      entry.IsDir,
				IsDrive:    entry.IsDrive,
				IsSymlink:  entry.IsSymlink,
				Size:       size,
				Mode:       entry.Mode,
				ModifiedAt: entry.ModifiedAt,
				Hidden:     entry.Hidden,
			})
		}
	}
	return resp
}
