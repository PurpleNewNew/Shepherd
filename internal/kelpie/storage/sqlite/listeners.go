package sqlite

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"codeberg.org/agnoie/shepherd/internal/kelpie/process"
)

// SaveListener 持久化监听记录。
func (s *ListenerRepository) SaveListener(record process.ListenerRecord) error {
	if s == nil || s.db == nil || record.ID == "" {
		return nil
	}
	metaBytes, _ := json.Marshal(record.Metadata)
	created := record.CreatedAt.UTC()
	if created.IsZero() {
		created = time.Now().UTC()
	}
	updated := record.UpdatedAt.UTC()
	if updated.IsZero() {
		updated = created
	}
	_, err := s.db.Exec(`INSERT INTO listeners(listener_id, target_uuid, protocol, bind, mode, status, last_error, metadata, route, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(listener_id) DO UPDATE SET
			target_uuid=excluded.target_uuid,
			protocol=excluded.protocol,
			bind=excluded.bind,
			mode=excluded.mode,
			status=excluded.status,
			last_error=excluded.last_error,
			metadata=excluded.metadata,
			route=excluded.route,
			created_at=excluded.created_at,
			updated_at=excluded.updated_at;`,
		record.ID,
		record.TargetUUID,
		record.Protocol,
		record.Bind,
		record.Mode,
		string(record.Status),
		record.LastError,
		string(metaBytes),
		record.Route,
		created.Format(time.RFC3339Nano),
		updated.Format(time.RFC3339Nano),
	)
	if err != nil {
		return fmt.Errorf("save listener: %w", err)
	}
	return nil
}

// DeleteListener 删除监听记录。
func (s *ListenerRepository) DeleteListener(id string) error {
	if s == nil || s.db == nil || id == "" {
		return nil
	}
	if _, err := s.db.Exec(`DELETE FROM listeners WHERE listener_id = ?`, id); err != nil {
		return fmt.Errorf("delete listener: %w", err)
	}
	return nil
}

// LoadListeners 读取所有监听记录。
func (s *ListenerRepository) LoadListeners() ([]process.ListenerRecord, error) {
	if s == nil || s.db == nil {
		return nil, nil
	}
	rows, err := s.db.Query(`SELECT listener_id, target_uuid, protocol, bind, mode, status, last_error, metadata, route, created_at, updated_at FROM listeners`)
	if err != nil {
		if isNoSuchTable(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("load listeners: %w", err)
	}
	defer rows.Close()
	var result []process.ListenerRecord
	for rows.Next() {
		var (
			rec        process.ListenerRecord
			status     sql.NullString
			lastError  sql.NullString
			metaRaw    sql.NullString
			route      sql.NullString
			createdRaw sql.NullString
			updatedRaw sql.NullString
		)
		if err := rows.Scan(
			&rec.ID,
			&rec.TargetUUID,
			&rec.Protocol,
			&rec.Bind,
			&rec.Mode,
			&status,
			&lastError,
			&metaRaw,
			&route,
			&createdRaw,
			&updatedRaw,
		); err != nil {
			return nil, fmt.Errorf("scan listener: %w", err)
		}
		if status.Valid {
			rec.Status = process.ListenerStatus(status.String)
		} else {
			rec.Status = process.ListenerStatusPending
		}
		if lastError.Valid {
			rec.LastError = lastError.String
		}
		if route.Valid {
			rec.Route = route.String
		}
		if metaRaw.Valid && metaRaw.String != "" {
			_ = json.Unmarshal([]byte(metaRaw.String), &rec.Metadata)
		}
		if rec.Metadata == nil {
			rec.Metadata = map[string]string{}
		}
		if createdRaw.Valid && createdRaw.String != "" {
			if ts, err := time.Parse(time.RFC3339Nano, createdRaw.String); err == nil {
				rec.CreatedAt = ts
			}
		}
		if updatedRaw.Valid && updatedRaw.String != "" {
			if ts, err := time.Parse(time.RFC3339Nano, updatedRaw.String); err == nil {
				rec.UpdatedAt = ts
			}
		}
		result = append(result, rec)
	}
	return result, rows.Err()
}
