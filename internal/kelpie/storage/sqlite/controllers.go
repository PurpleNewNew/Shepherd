package sqlite

import (
	"database/sql"
	"fmt"
	"time"

	"codeberg.org/agnoie/shepherd/internal/kelpie/process"
)

// SaveControllerListener 持久化 Kelpie Controller Listener 记录。
func (s *ControllerListenerRepository) SaveControllerListener(rec process.ControllerListenerRecord) error {
	if s == nil || s.db == nil || rec.ID == "" {
		return nil
	}
	created := rec.CreatedAt.UTC()
	if created.IsZero() {
		created = time.Now().UTC()
	}
	updated := rec.UpdatedAt.UTC()
	if updated.IsZero() {
		updated = created
	}
	_, err := s.db.Exec(`INSERT INTO controller_listeners(listener_id, bind, protocol, status, last_error, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(listener_id) DO UPDATE SET
			bind=excluded.bind,
			protocol=excluded.protocol,
			status=excluded.status,
			last_error=excluded.last_error,
			created_at=excluded.created_at,
			updated_at=excluded.updated_at;`,
		rec.ID,
		rec.Bind,
		string(rec.Protocol),
		string(rec.Status),
		rec.LastError,
		created.Format(time.RFC3339Nano),
		updated.Format(time.RFC3339Nano),
	)
	if err != nil {
		return fmt.Errorf("save controller listener: %w", err)
	}
	return nil
}

// DeleteControllerListener 删除 Controller Listener 记录。
func (s *ControllerListenerRepository) DeleteControllerListener(id string) error {
	if s == nil || s.db == nil || id == "" {
		return nil
	}
	if _, err := s.db.Exec(`DELETE FROM controller_listeners WHERE listener_id = ?`, id); err != nil {
		return fmt.Errorf("delete controller listener: %w", err)
	}
	return nil
}

// LoadControllerListeners 读取所有 Controller Listener 记录。
func (s *ControllerListenerRepository) LoadControllerListeners() ([]process.ControllerListenerRecord, error) {
	if s == nil || s.db == nil {
		return nil, nil
	}
	rows, err := s.db.Query(`SELECT listener_id, bind, protocol, status, last_error, created_at, updated_at FROM controller_listeners`)
	if err != nil {
		if isNoSuchTable(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("load controller listeners: %w", err)
	}
	defer rows.Close()
	var result []process.ControllerListenerRecord
	for rows.Next() {
		var (
			rec        process.ControllerListenerRecord
			statusRaw  sql.NullString
			lastErrRaw sql.NullString
			createdRaw sql.NullString
			updatedRaw sql.NullString
		)
		if err := rows.Scan(
			&rec.ID,
			&rec.Bind,
			&rec.Protocol,
			&statusRaw,
			&lastErrRaw,
			&createdRaw,
			&updatedRaw,
		); err != nil {
			return nil, fmt.Errorf("scan controller listener: %w", err)
		}
		if statusRaw.Valid {
			rec.Status = process.ControllerListenerStatus(statusRaw.String)
		}
		if lastErrRaw.Valid {
			rec.LastError = lastErrRaw.String
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
