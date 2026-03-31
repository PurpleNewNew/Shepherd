package sqlite

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"codeberg.org/agnoie/shepherd/internal/kelpie/dtn"
)

func fmtRFC3339Nano(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

func parseRFC3339Nano(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, nil
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, err
	}
	return t, nil
}

func (s *Store) UpsertDTNBundle(bundle *dtn.Bundle) error {
	if s == nil || s.db == nil {
		return nil
	}
	if bundle == nil || strings.TrimSpace(bundle.ID) == "" || strings.TrimSpace(bundle.Target) == "" {
		return fmt.Errorf("invalid bundle")
	}
	metaJSON := ""
	if len(bundle.Meta) > 0 {
		if raw, err := json.Marshal(bundle.Meta); err == nil {
			metaJSON = string(raw)
		}
	}
	_, err := s.db.Exec(
		`INSERT OR REPLACE INTO dtn_bundles
			(id, target_uuid, priority, payload, enqueued_at, hold_until, deliver_by, attempts, meta)
		 VALUES
			(?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		strings.TrimSpace(bundle.ID),
		strings.TrimSpace(bundle.Target),
		int(bundle.Priority),
		[]byte(bundle.Payload),
		fmtRFC3339Nano(bundle.EnqueuedAt),
		fmtRFC3339Nano(bundle.HoldUntil),
		fmtRFC3339Nano(bundle.DeliverBy),
		int(bundle.Attempts),
		metaJSON,
	)
	if err != nil {
		return fmt.Errorf("upsert dtn bundle: %w", err)
	}
	return nil
}

func (s *Store) DeleteDTNBundle(id string) error {
	if s == nil || s.db == nil {
		return nil
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return nil
	}
	if _, err := s.db.Exec(`DELETE FROM dtn_bundles WHERE id = ?`, id); err != nil {
		return fmt.Errorf("delete dtn bundle: %w", err)
	}
	return nil
}

func (s *Store) DeleteDTNBundles(ids []string) error {
	if s == nil || s.db == nil || len(ids) == 0 {
		return nil
	}
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("delete dtn bundles begin: %w", err)
	}
	stmt, err := tx.Prepare(`DELETE FROM dtn_bundles WHERE id = ?`)
	if err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("delete dtn bundles prepare: %w", err)
	}
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		if _, err := stmt.Exec(id); err != nil {
			_ = stmt.Close()
			_ = tx.Rollback()
			return fmt.Errorf("delete dtn bundles exec: %w", err)
		}
	}
	_ = stmt.Close()
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("delete dtn bundles commit: %w", err)
	}
	return nil
}

func (s *Store) LoadDTNBundles() ([]*dtn.Bundle, error) {
	if s == nil || s.db == nil {
		return nil, nil
	}
	rows, err := s.db.Query(`SELECT id, target_uuid, priority, payload, enqueued_at, hold_until, deliver_by, attempts, meta FROM dtn_bundles`)
	if err != nil {
		return nil, fmt.Errorf("load dtn bundles: %w", err)
	}
	defer rows.Close()

	var out []*dtn.Bundle
	for rows.Next() {
		var (
			id        string
			target    string
			priority  int
			payload   []byte
			enqAt     string
			holdUntil string
			deliverBy string
			attempts  int
			metaText  string
		)
		if err := rows.Scan(&id, &target, &priority, &payload, &enqAt, &holdUntil, &deliverBy, &attempts, &metaText); err != nil {
			return nil, fmt.Errorf("scan dtn bundles: %w", err)
		}
		enqueuedAt, err := parseRFC3339Nano(enqAt)
		if err != nil {
			return nil, fmt.Errorf("parse dtn bundle enqueued_at: %w", err)
		}
		hold, err := parseRFC3339Nano(holdUntil)
		if err != nil {
			return nil, fmt.Errorf("parse dtn bundle hold_until: %w", err)
		}
		deliver, err := parseRFC3339Nano(deliverBy)
		if err != nil {
			return nil, fmt.Errorf("parse dtn bundle deliver_by: %w", err)
		}
		var meta map[string]string
		metaText = strings.TrimSpace(metaText)
		if metaText != "" {
			_ = json.Unmarshal([]byte(metaText), &meta)
		}
		p := dtn.Priority(priority)
		if p < dtn.PriorityHigh || p > dtn.PriorityLow {
			p = dtn.PriorityNormal
		}
		if attempts < 0 {
			attempts = 0
		}
		out = append(out, &dtn.Bundle{
			ID:         strings.TrimSpace(id),
			Target:     strings.TrimSpace(target),
			Payload:    append([]byte(nil), payload...),
			Priority:   p,
			EnqueuedAt: enqueuedAt,
			HoldUntil:  hold,
			DeliverBy:  deliver,
			Attempts:   attempts,
			Meta:       meta,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("load dtn bundles: %w", err)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i] == nil || out[j] == nil {
			return false
		}
		if !out[i].EnqueuedAt.Equal(out[j].EnqueuedAt) {
			return out[i].EnqueuedAt.Before(out[j].EnqueuedAt)
		}
		return out[i].ID < out[j].ID
	})
	return out, nil
}
