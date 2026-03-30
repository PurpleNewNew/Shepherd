package sqlite

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"codeberg.org/agnoie/shepherd/internal/kelpie/process"
)

// SaveLoot 持久化一条 loot 记录。
func (s *Store) SaveLoot(rec process.LootRecord) error {
	if s == nil || s.db == nil || rec.ID == "" {
		return nil
	}
	created := rec.CreatedAt.UTC()
	if created.IsZero() {
		created = time.Now().UTC()
	}
	tagsJSON, _ := json.Marshal(rec.Tags)
	metaJSON, _ := json.Marshal(rec.Metadata)
	_, err := s.db.Exec(`INSERT INTO loot(loot_id, target_uuid, operator, category, name, storage_ref, origin_path, hash, size, mime, metadata, tags, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(loot_id) DO UPDATE SET
			target_uuid=excluded.target_uuid,
			operator=excluded.operator,
			category=excluded.category,
			name=excluded.name,
			storage_ref=excluded.storage_ref,
			origin_path=excluded.origin_path,
			hash=excluded.hash,
			size=excluded.size,
			mime=excluded.mime,
			metadata=excluded.metadata,
			tags=excluded.tags,
			created_at=excluded.created_at;`,
		rec.ID,
		rec.TargetUUID,
		rec.Operator,
		string(rec.Category),
		rec.Name,
		rec.StorageRef,
		rec.OriginPath,
		rec.Hash,
		rec.Size,
		rec.Mime,
		string(metaJSON),
		string(tagsJSON),
		created.Format(time.RFC3339Nano),
	)
	if err != nil {
		return fmt.Errorf("save loot: %w", err)
	}
	return nil
}

// ListLoot 返回最近的若干条 loot 记录。
func (s *Store) ListLoot(filter process.LootFilter) ([]process.LootRecord, error) {
	if s == nil || s.db == nil {
		return nil, nil
	}
	limit := filter.Limit
	if limit <= 0 {
		limit = 50
	}
	if limit > 1000 {
		limit = 1000
	}
	query := `SELECT loot_id, target_uuid, operator, category, name, storage_ref, origin_path, hash, size, mime, metadata, tags, created_at
		FROM loot`
	var (
		where []string
		args  []any
	)
	if filter.TargetUUID != "" {
		where = append(where, "target_uuid = ?")
		args = append(args, filter.TargetUUID)
	}
	if filter.Category != "" {
		where = append(where, "category = ?")
		args = append(args, string(filter.Category))
	}
	if filter.BeforeID != "" {
		where = append(where, "created_at < (SELECT created_at FROM loot WHERE loot_id = ?)")
		args = append(args, filter.BeforeID)
	}
	if len(where) > 0 {
		query += " WHERE " + strings.Join(where, " AND ")
	}
	query += " ORDER BY created_at DESC LIMIT ?"
	args = append(args, limit)
	rows, err := s.db.Query(query, args...)
	if err != nil {
		if isNoSuchTable(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("list loot: %w", err)
	}
	defer rows.Close()

	var result []process.LootRecord
	for rows.Next() {
		var (
			rec       process.LootRecord
			targetRaw sql.NullString
			opRaw     sql.NullString
			catRaw    sql.NullString
			nameRaw   sql.NullString
			storage   sql.NullString
			origin    sql.NullString
			hashRaw   sql.NullString
			sizeRaw   sql.NullInt64
			mimeRaw   sql.NullString
			metaRaw   sql.NullString
			tagsRaw   sql.NullString
			tsRaw     sql.NullString
		)
		if err := rows.Scan(
			&rec.ID,
			&targetRaw,
			&opRaw,
			&catRaw,
			&nameRaw,
			&storage,
			&origin,
			&hashRaw,
			&sizeRaw,
			&mimeRaw,
			&metaRaw,
			&tagsRaw,
			&tsRaw,
		); err != nil {
			return nil, fmt.Errorf("scan loot: %w", err)
		}
		if targetRaw.Valid {
			rec.TargetUUID = targetRaw.String
		}
		if opRaw.Valid {
			rec.Operator = opRaw.String
		}
		if catRaw.Valid {
			rec.Category = process.LootCategory(catRaw.String)
		}
		if nameRaw.Valid {
			rec.Name = nameRaw.String
		}
		if storage.Valid {
			rec.StorageRef = storage.String
		}
		if origin.Valid {
			rec.OriginPath = origin.String
		}
		if hashRaw.Valid {
			rec.Hash = hashRaw.String
		}
		if sizeRaw.Valid && sizeRaw.Int64 > 0 {
			rec.Size = uint64(sizeRaw.Int64)
		}
		if mimeRaw.Valid {
			rec.Mime = mimeRaw.String
		}
		if metaRaw.Valid && metaRaw.String != "" {
			_ = json.Unmarshal([]byte(metaRaw.String), &rec.Metadata)
		}
		if tagsRaw.Valid && tagsRaw.String != "" {
			_ = json.Unmarshal([]byte(tagsRaw.String), &rec.Tags)
		}
		if tsRaw.Valid && tsRaw.String != "" {
			if ts, err := time.Parse(time.RFC3339Nano, tsRaw.String); err == nil {
				rec.CreatedAt = ts
			}
		}
		if len(filter.Tags) > 0 && !matchAllTags(rec.Tags, filter.Tags) {
			continue
		}
		result = append(result, rec)
	}
	return result, rows.Err()
}

// GetLoot 返回指定 loot 记录。
func (s *Store) GetLoot(lootID string) (process.LootRecord, error) {
	if s == nil || s.db == nil {
		return process.LootRecord{}, fmt.Errorf("loot store unavailable")
	}
	lootID = strings.TrimSpace(lootID)
	if lootID == "" {
		return process.LootRecord{}, fmt.Errorf("loot id required")
	}
	row := s.db.QueryRow(`SELECT loot_id, target_uuid, operator, category, name, storage_ref, origin_path, hash, size, mime, metadata, tags, created_at FROM loot WHERE loot_id = ?`, lootID)
	var (
		rec       process.LootRecord
		targetRaw sql.NullString
		opRaw     sql.NullString
		catRaw    sql.NullString
		nameRaw   sql.NullString
		storage   sql.NullString
		origin    sql.NullString
		hashRaw   sql.NullString
		sizeRaw   sql.NullInt64
		mimeRaw   sql.NullString
		metaRaw   sql.NullString
		tagsRaw   sql.NullString
		tsRaw     sql.NullString
	)
	if err := row.Scan(&rec.ID, &targetRaw, &opRaw, &catRaw, &nameRaw, &storage, &origin, &hashRaw, &sizeRaw, &mimeRaw, &metaRaw, &tagsRaw, &tsRaw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return process.LootRecord{}, fmt.Errorf("loot not found")
		}
		return process.LootRecord{}, fmt.Errorf("query loot: %w", err)
	}
	if targetRaw.Valid {
		rec.TargetUUID = targetRaw.String
	}
	if opRaw.Valid {
		rec.Operator = opRaw.String
	}
	if catRaw.Valid {
		rec.Category = process.LootCategory(catRaw.String)
	}
	if nameRaw.Valid {
		rec.Name = nameRaw.String
	}
	if storage.Valid {
		rec.StorageRef = storage.String
	}
	if origin.Valid {
		rec.OriginPath = origin.String
	}
	if hashRaw.Valid {
		rec.Hash = hashRaw.String
	}
	if sizeRaw.Valid && sizeRaw.Int64 > 0 {
		rec.Size = uint64(sizeRaw.Int64)
	}
	if mimeRaw.Valid {
		rec.Mime = mimeRaw.String
	}
	if metaRaw.Valid && metaRaw.String != "" {
		_ = json.Unmarshal([]byte(metaRaw.String), &rec.Metadata)
	}
	if tagsRaw.Valid && tagsRaw.String != "" {
		_ = json.Unmarshal([]byte(tagsRaw.String), &rec.Tags)
	}
	if tsRaw.Valid && tsRaw.String != "" {
		if ts, err := time.Parse(time.RFC3339Nano, tsRaw.String); err == nil {
			rec.CreatedAt = ts
		}
	}
	return rec, nil
}

func matchAllTags(have []string, want []string) bool {
	if len(want) == 0 {
		return true
	}
	set := make(map[string]struct{}, len(have))
	for _, t := range have {
		set[strings.ToLower(strings.TrimSpace(t))] = struct{}{}
	}
	for _, w := range want {
		w = strings.ToLower(strings.TrimSpace(w))
		if w == "" {
			continue
		}
		if _, ok := set[w]; !ok {
			return false
		}
	}
	return true
}
