package grpcserver

import (
	"context"
	"io"
	"strings"
	"time"

	"codeberg.org/agnoie/shepherd/internal/kelpie/process"
	"codeberg.org/agnoie/shepherd/internal/kelpie/uipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func convertLootCategory(cat process.LootCategory) uipb.LootCategory {
	switch cat {
	case process.LootCategoryFile:
		return uipb.LootCategory_LOOT_CATEGORY_FILE
	case process.LootCategoryScreenshot:
		return uipb.LootCategory_LOOT_CATEGORY_SCREENSHOT
	case process.LootCategoryTicket:
		return uipb.LootCategory_LOOT_CATEGORY_TICKET
	default:
		return uipb.LootCategory_LOOT_CATEGORY_UNSPECIFIED
	}
}

func convertProtoLootCategory(cat uipb.LootCategory) process.LootCategory {
	switch cat {
	case uipb.LootCategory_LOOT_CATEGORY_FILE:
		return process.LootCategoryFile
	case uipb.LootCategory_LOOT_CATEGORY_SCREENSHOT:
		return process.LootCategoryScreenshot
	case uipb.LootCategory_LOOT_CATEGORY_TICKET:
		return process.LootCategoryTicket
	default:
		return ""
	}
}

func normalizeTags(tags []string) []string {
	var result []string
	seen := make(map[string]struct{})
	for _, t := range tags {
		t = strings.TrimSpace(strings.ToLower(t))
		if t == "" {
			continue
		}
		if _, ok := seen[t]; ok {
			continue
		}
		seen[t] = struct{}{}
		result = append(result, t)
	}
	return result
}

func buildLootItem(rec process.LootRecord) *uipb.LootItem {
	item := &uipb.LootItem{
		LootId:     rec.ID,
		TargetUuid: rec.TargetUUID,
		Operator:   rec.Operator,
		Category:   convertLootCategory(rec.Category),
		Name:       rec.Name,
		StorageRef: rec.StorageRef,
		OriginPath: rec.OriginPath,
		Hash:       rec.Hash,
		Size:       rec.Size,
		Mime:       rec.Mime,
		Metadata:   rec.Metadata,
		Tags:       rec.Tags,
	}
	if !rec.CreatedAt.IsZero() {
		item.CreatedAt = rec.CreatedAt.UTC().Format(time.RFC3339Nano)
	}
	return item
}

func (s *service) broadcastLootAdded(rec process.LootRecord) {
	if s == nil {
		return
	}
	s.broadcast(&uipb.UiEvent{
		Payload: &uipb.UiEvent_LootEvent{
			LootEvent: &uipb.LootEvent{
				Kind: uipb.LootEvent_LOOT_EVENT_ADDED,
				Item: buildLootItem(rec),
			},
		},
	})
}

func (s *service) ListLoot(ctx context.Context, req *uipb.ListLootRequest) (*uipb.ListLootResponse, error) {
	if s == nil || s.admin == nil {
		return nil, status.Error(codes.Unavailable, "admin unavailable")
	}
	limit := int(req.GetLimit())
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	filter := process.LootFilter{
		TargetUUID: strings.TrimSpace(req.GetTargetUuid()),
		Limit:      limit,
		BeforeID:   strings.TrimSpace(req.GetBeforeId()),
		Tags:       normalizeTags(req.GetTags()),
	}
	if req.GetCategory() != uipb.LootCategory_LOOT_CATEGORY_UNSPECIFIED {
		filter.Category = convertProtoLootCategory(req.GetCategory())
	}
	records := s.admin.ListLoot(filter)
	resp := &uipb.ListLootResponse{}
	for _, rec := range records {
		resp.Items = append(resp.Items, buildLootItem(rec))
	}
	return resp, nil
}

func (s *service) SubmitLoot(ctx context.Context, req *uipb.SubmitLootRequest) (*uipb.SubmitLootResponse, error) {
	if s == nil || s.admin == nil {
		return nil, status.Error(codes.Unavailable, "admin unavailable")
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "missing request")
	}
	target := strings.TrimSpace(req.GetTargetUuid())
	if target == "" {
		return nil, status.Error(codes.InvalidArgument, "target uuid required")
	}
	name := strings.TrimSpace(req.GetName())
	if name == "" {
		return nil, status.Error(codes.InvalidArgument, "name required")
	}
	content := req.GetContent()
	storageRef := strings.TrimSpace(req.GetStorageRef())
	if len(content) == 0 && storageRef == "" {
		return nil, status.Error(codes.InvalidArgument, "content or storage_ref required")
	}
	category := convertProtoLootCategory(req.GetCategory())
	operator := s.currentOperator(ctx)
	rec := process.LootRecord{
		TargetUUID: target,
		Operator:   operator,
		Category:   category,
		Name:       name,
		StorageRef: storageRef,
		OriginPath: strings.TrimSpace(req.GetOriginPath()),
		Hash:       strings.TrimSpace(req.GetHash()),
		Size:       req.GetSize(),
		Mime:       strings.TrimSpace(req.GetMime()),
		Metadata:   req.GetMetadata(),
		Tags:       normalizeTags(req.GetTags()),
	}
	saved, err := s.admin.SubmitLoot(rec, content)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "submit loot failed: %v", err)
	}
	s.broadcastLootAdded(saved)
	return &uipb.SubmitLootResponse{Item: buildLootItem(saved)}, nil
}

func (s *service) CollectLootFile(ctx context.Context, req *uipb.CollectLootFileRequest) (*uipb.CollectLootFileResponse, error) {
	if s == nil || s.admin == nil {
		return nil, status.Error(codes.Unavailable, "admin unavailable")
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "missing request")
	}
	target := strings.TrimSpace(req.GetTargetUuid())
	remotePath := strings.TrimSpace(req.GetRemotePath())
	if target == "" || remotePath == "" {
		return nil, status.Error(codes.InvalidArgument, "target uuid and remote path required")
	}
	rec, err := s.admin.CollectLootFile(ctx, target, remotePath, s.currentOperator(ctx), normalizeTags(req.GetTags()))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "collect loot file failed: %v", err)
	}
	s.broadcastLootAdded(rec)
	return &uipb.CollectLootFileResponse{Item: buildLootItem(rec)}, nil
}

func (s *service) SyncLoot(req *uipb.SyncLootRequest, stream uipb.KelpieUIService_SyncLootServer) error {
	if s == nil || s.admin == nil {
		return status.Error(codes.Unavailable, "admin unavailable")
	}
	if req == nil || strings.TrimSpace(req.GetLootId()) == "" {
		return status.Error(codes.InvalidArgument, "loot_id required")
	}
	rec, reader, _, err := s.admin.OpenLootContent(req.GetLootId())
	if err != nil {
		return status.Errorf(codes.Internal, "open loot content: %v", err)
	}
	if reader == nil {
		return status.Error(codes.FailedPrecondition, "loot content is not stored on kelpie")
	}
	defer reader.Close()

	item := buildLootItem(rec)
	buf := make([]byte, 64*1024)
	first := true
	for {
		n, readErr := reader.Read(buf)
		if n > 0 {
			resp := &uipb.SyncLootChunk{
				Data: append([]byte(nil), buf[:n]...),
			}
			if first {
				resp.Item = item
				first = false
			}
			if err := stream.Send(resp); err != nil {
				return status.Errorf(codes.Unavailable, "send loot chunk: %v", err)
			}
		}
		if readErr != nil {
			if readErr == io.EOF {
				if first {
					return stream.Send(&uipb.SyncLootChunk{Item: item})
				}
				return nil
			}
			return status.Errorf(codes.Internal, "read loot content: %v", readErr)
		}
	}
}
