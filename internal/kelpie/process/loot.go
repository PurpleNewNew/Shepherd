package process

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
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
	Load(storageRef string) ([]byte, error)
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

// GetLootContent 返回指定 loot 记录及其可读内容。
func (admin *Admin) GetLootContent(lootID string) (LootRecord, []byte, error) {
	rec, err := admin.GetLoot(lootID)
	if err != nil {
		return LootRecord{}, nil, err
	}
	if strings.TrimSpace(rec.StorageRef) == "" || admin == nil || admin.lootContentStore == nil {
		return rec, nil, nil
	}
	content, err := admin.lootContentStore.Load(rec.StorageRef)
	if err != nil {
		return rec, nil, fmt.Errorf("load loot content: %w", err)
	}
	return rec, content, nil
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
