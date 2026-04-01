package grpcserver

import (
	"context"
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
