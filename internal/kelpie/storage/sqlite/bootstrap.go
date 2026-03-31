package sqlite

import (
	"database/sql"
	"fmt"
	"time"

	"codeberg.org/agnoie/shepherd/internal/kelpie/topology"
)

// Load 从数据库重建拓扑状态。
func (s *TopologyRepository) Load() (*topology.Snapshot, error) {
	if s == nil || s.db == nil {
		return &topology.Snapshot{}, nil
	}
	nodes, err := s.loadNodes()
	if err != nil {
		return nil, err
	}
	edges, err := s.loadEdges()
	if err != nil {
		return nil, err
	}
	networks, err := s.loadNetworks()
	if err != nil {
		return nil, err
	}
	suppLinks, err := s.loadSupplementalLinks()
	if err != nil {
		return nil, err
	}
	metrics, err := s.loadPlannerMetrics()
	if err != nil {
		return nil, err
	}
	topoSupp := make([]topology.SupplementalLinkSnapshot, 0, len(suppLinks))
	for _, link := range suppLinks {
		topoSupp = append(topoSupp, topology.SupplementalLinkSnapshot{
			LinkUUID:       link.LinkUUID,
			Listener:       link.Listener,
			Dialer:         link.Dialer,
			Status:         link.Status,
			FailureReason:  link.FailureReason,
			LastTransition: link.LastTransition,
		})
	}
	topoMetrics := topology.PlannerMetricsSnapshot{
		Dispatched:     metrics.Dispatched,
		Success:        metrics.Success,
		Failures:       metrics.Failures,
		Dropped:        metrics.Dropped,
		Recycled:       metrics.Recycled,
		QueueHigh:      metrics.QueueHigh,
		LastFailure:    metrics.LastFailure,
		RepairAttempts: metrics.RepairAttempts,
		RepairSuccess:  metrics.RepairSuccess,
		RepairFailures: metrics.RepairFailures,
	}

	return &topology.Snapshot{
		Nodes:             nodes,
		Edges:             edges,
		Networks:          networks,
		SupplementalLinks: topoSupp,
		PlannerMetrics:    topoMetrics,
	}, nil
}

func (s *TopologyRepository) loadNodes() ([]topology.NodeSnapshot, error) {
	rows, err := s.db.Query(`SELECT uuid, parent_uuid, network_id, ip, port, listen_port, dial_address, fallback_port, transport, tls_enabled, last_success, repair_failures, repair_updated, hostname, username, memo, is_alive, last_seen FROM nodes`)
	if err != nil {
		return nil, fmt.Errorf("load nodes: %w", err)
	}
	defer rows.Close()
	var result []topology.NodeSnapshot
	for rows.Next() {
		var rec topology.NodeSnapshot
		var lastSeen string
		var isAlive int
		var dial sql.NullString
		var fallback sql.NullInt64
		var transport sql.NullString
		var tlsEnabled sql.NullInt64
		var lastSuccess sql.NullString
		var repairFailures sql.NullInt64
		var repairUpdated sql.NullString
		if err := rows.Scan(
			&rec.UUID,
			&rec.Parent,
			&rec.Network,
			&rec.IP,
			&rec.Port,
			&rec.ListenPort,
			&dial,
			&fallback,
			&transport,
			&tlsEnabled,
			&lastSuccess,
			&repairFailures,
			&repairUpdated,
			&rec.Hostname,
			&rec.Username,
			&rec.Memo,
			&isAlive,
			&lastSeen,
		); err != nil {
			return nil, fmt.Errorf("scan node: %w", err)
		}
		rec.IsAlive = isAlive == 1
		if dial.Valid {
			rec.DialAddress = dial.String
		}
		if fallback.Valid {
			rec.FallbackPort = int(fallback.Int64)
		}
		if transport.Valid {
			rec.Transport = transport.String
		}
		if tlsEnabled.Valid && tlsEnabled.Int64 != 0 {
			rec.TLSEnabled = true
		}
		if repairFailures.Valid {
			rec.RepairFailures = int(repairFailures.Int64)
		}
		if lastSeen != "" {
			if ts, err := time.Parse(time.RFC3339Nano, lastSeen); err == nil {
				rec.LastSeen = ts
			}
		}
		if lastSuccess.Valid && lastSuccess.String != "" {
			if ts, err := time.Parse(time.RFC3339Nano, lastSuccess.String); err == nil {
				rec.LastSuccess = ts
			}
		}
		if repairUpdated.Valid && repairUpdated.String != "" {
			if ts, err := time.Parse(time.RFC3339Nano, repairUpdated.String); err == nil {
				rec.RepairUpdated = ts
			}
		}
		result = append(result, rec)
	}
	return result, rows.Err()
}

func (s *TopologyRepository) loadEdges() ([]topology.EdgeSnapshot, error) {
	rows, err := s.db.Query(`SELECT from_uuid, to_uuid, type, weight FROM edges`)
	if err != nil {
		return nil, fmt.Errorf("load edges: %w", err)
	}
	defer rows.Close()
	var result []topology.EdgeSnapshot
	for rows.Next() {
		var rec topology.EdgeSnapshot
		var edgeType int
		if err := rows.Scan(&rec.From, &rec.To, &edgeType, &rec.Weight); err != nil {
			return nil, fmt.Errorf("scan edge: %w", err)
		}
		rec.Type = topology.EdgeType(edgeType)
		result = append(result, rec)
	}
	return result, rows.Err()
}

func (s *TopologyRepository) loadNetworks() (map[string]string, error) {
	rows, err := s.db.Query(`SELECT target_uuid, network_id FROM networks`)
	if err != nil {
		return nil, fmt.Errorf("load networks: %w", err)
	}
	defer rows.Close()
	result := make(map[string]string)
	for rows.Next() {
		var entry, network string
		if err := rows.Scan(&entry, &network); err != nil {
			return nil, fmt.Errorf("scan network: %w", err)
		}
		result[entry] = network
	}
	return result, rows.Err()
}

func (s *TopologyRepository) loadSupplementalLinks() ([]SupplementalLinkRecord, error) {
	rows, err := s.db.Query(`SELECT link_uuid, listener, dialer, status, failure_reason, last_transition FROM supplemental_links`)
	if err != nil {
		if isNoSuchTable(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("load supplemental_links: %w", err)
	}
	defer rows.Close()
	var result []SupplementalLinkRecord
	for rows.Next() {
		var rec SupplementalLinkRecord
		var last string
		if err := rows.Scan(&rec.LinkUUID, &rec.Listener, &rec.Dialer, &rec.Status, &rec.FailureReason, &last); err != nil {
			return nil, fmt.Errorf("scan supplemental link: %w", err)
		}
		if last != "" {
			if ts, err := time.Parse(time.RFC3339Nano, last); err == nil {
				rec.LastTransition = ts
			}
		}
		result = append(result, rec)
	}
	return result, rows.Err()
}

func (s *TopologyRepository) loadPlannerMetrics() (PlannerMetrics, error) {
	row := s.db.QueryRow(`SELECT dispatched, success, failures, dropped, recycled, queue_high, last_failure, repair_attempts, repair_success, repair_failures FROM planner_metrics LIMIT 1`)
	var rec PlannerMetrics
	var last sqlNullString
	var repairAttempts, repairSuccess, repairFailures sql.NullInt64
	if err := row.Scan(&rec.Dispatched, &rec.Success, &rec.Failures, &rec.Dropped, &rec.Recycled, &rec.QueueHigh, &last, &repairAttempts, &repairSuccess, &repairFailures); err != nil {
		if err == sql.ErrNoRows {
			return PlannerMetrics{}, nil
		}
		if isNoSuchTable(err) {
			return PlannerMetrics{}, nil
		}
		return PlannerMetrics{}, fmt.Errorf("load planner metrics: %w", err)
	}
	rec.LastFailure = last.String
	if repairAttempts.Valid {
		rec.RepairAttempts = uint64(repairAttempts.Int64)
	}
	if repairSuccess.Valid {
		rec.RepairSuccess = uint64(repairSuccess.Int64)
	}
	if repairFailures.Valid {
		rec.RepairFailures = uint64(repairFailures.Int64)
	}
	return rec, nil
}
