package sqlite

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"codeberg.org/agnoie/shepherd/internal/kelpie/topology"

	_ "modernc.org/sqlite"
)

// Store 将拓扑快照持久化到 SQLite 数据库。
type Store struct {
	db *sql.DB
}

// New 在指定路径创建或打开 SQLite 数据库并初始化结构。
func New(path string) (*Store, error) {
	if path == "" {
		path = "admin.db"
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	// Serialize connections to avoid concurrent write contention
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	// Improve write concurrency and resilience under bursts
	if _, err := db.Exec(`PRAGMA journal_mode=WAL;`); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("configure sqlite: %w", err)
	}
	// Back off rather than failing immediately on transient writer locks
	_, _ = db.Exec(`PRAGMA busy_timeout=5000;`)
	// A reasonable durability/perf tradeoff for admin UI telemetry
	_, _ = db.Exec(`PRAGMA synchronous=NORMAL;`)
	s := &Store{db: db}
	if err := s.initSchema(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) initSchema() error {
	const nodes = `CREATE TABLE IF NOT EXISTS nodes (
		uuid TEXT PRIMARY KEY,
		parent_uuid TEXT,
		network_id TEXT,
		ip TEXT,
		port INTEGER,
		listen_port INTEGER,
		dial_address TEXT,
		fallback_port INTEGER,
		transport TEXT,
		tls_enabled INTEGER,
		last_success TEXT,
		repair_failures INTEGER,
		repair_updated TEXT,
		hostname TEXT,
		username TEXT,
		memo TEXT,
		is_alive INTEGER,
		last_seen TEXT
	);`
	const edges = `CREATE TABLE IF NOT EXISTS edges (
		from_uuid TEXT,
		to_uuid TEXT,
		type INTEGER,
		weight INTEGER,
		PRIMARY KEY(from_uuid, to_uuid)
	);`
	const networks = `CREATE TABLE IF NOT EXISTS networks (
		target_uuid TEXT PRIMARY KEY,
		network_id TEXT
	);`
	const supplemental = `CREATE TABLE IF NOT EXISTS supplemental_links (
		link_uuid TEXT PRIMARY KEY,
		listener TEXT,
		dialer TEXT,
		status INTEGER,
		failure_reason TEXT,
		last_transition TEXT
	);`
	const planner = `CREATE TABLE IF NOT EXISTS planner_metrics (
	id INTEGER PRIMARY KEY CHECK (id = 1),
	dispatched INTEGER,
	success INTEGER,
	failures INTEGER,
	dropped INTEGER,
	recycled INTEGER,
	queue_high INTEGER,
	last_failure TEXT,
	repair_attempts INTEGER,
	repair_success INTEGER,
	repair_failures INTEGER
);`
	const listeners = `CREATE TABLE IF NOT EXISTS listeners (
	listener_id TEXT PRIMARY KEY,
	target_uuid TEXT,
	protocol TEXT,
	bind TEXT,
	mode INTEGER,
	status TEXT,
	last_error TEXT,
	metadata TEXT,
	route TEXT,
	created_at TEXT,
	updated_at TEXT
);`
	const controllerListeners = `CREATE TABLE IF NOT EXISTS controller_listeners (
	listener_id TEXT PRIMARY KEY,
	bind TEXT,
	protocol TEXT,
	status TEXT,
	last_error TEXT,
	created_at TEXT,
	updated_at TEXT
);`
	const loot = `CREATE TABLE IF NOT EXISTS loot (
	loot_id TEXT PRIMARY KEY,
	target_uuid TEXT,
	operator TEXT,
	category TEXT,
	name TEXT,
	storage_ref TEXT,
	origin_path TEXT,
	hash TEXT,
	size INTEGER,
	mime TEXT,
	metadata TEXT,
	tags TEXT,
	created_at TEXT
);`
	const dtnBundles = `CREATE TABLE IF NOT EXISTS dtn_bundles (
	id TEXT PRIMARY KEY,
	target_uuid TEXT,
	priority INTEGER,
	payload BLOB,
	enqueued_at TEXT,
	hold_until TEXT,
	deliver_by TEXT,
	attempts INTEGER,
	meta TEXT
);`
	if _, err := s.db.Exec(nodes); err != nil {
		return fmt.Errorf("init nodes: %w", err)
	}
	if err := s.ensureNodeColumns(); err != nil {
		return err
	}
	if _, err := s.db.Exec(edges); err != nil {
		return fmt.Errorf("init edges: %w", err)
	}
	if _, err := s.db.Exec(networks); err != nil {
		return fmt.Errorf("init networks: %w", err)
	}
	if _, err := s.db.Exec(supplemental); err != nil {
		return fmt.Errorf("init supplemental_links: %w", err)
	}
	if _, err := s.db.Exec(planner); err != nil {
		return fmt.Errorf("init planner_metrics: %w", err)
	}
	if err := s.ensurePlannerMetricsColumns(); err != nil {
		return err
	}
	if _, err := s.db.Exec(listeners); err != nil {
		return fmt.Errorf("init listeners: %w", err)
	}
	if _, err := s.db.Exec(controllerListeners); err != nil {
		return fmt.Errorf("init controller_listeners: %w", err)
	}
	if _, err := s.db.Exec(loot); err != nil {
		return fmt.Errorf("init loot: %w", err)
	}
	if err := s.ensureLootColumns(); err != nil {
		return err
	}
	if _, err := s.db.Exec(dtnBundles); err != nil {
		return fmt.Errorf("init dtn_bundles: %w", err)
	}
	if err := s.ensureCollabSchema(); err != nil {
		return err
	}
	return nil
}

func (s *Store) ensureNodeColumns() error {
	if s == nil || s.db == nil {
		return nil
	}
	columns := []struct {
		name string
		def  string
	}{
		{"dial_address", "TEXT"},
		{"fallback_port", "INTEGER"},
		{"transport", "TEXT"},
		{"tls_enabled", "INTEGER"},
		{"last_success", "TEXT"},
		{"repair_failures", "INTEGER"},
		{"repair_updated", "TEXT"},
	}
	for _, col := range columns {
		if err := s.ensureColumn("nodes", col.name, col.def); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) ensureLootColumns() error {
	if s == nil || s.db == nil {
		return nil
	}
	columns := []struct {
		name string
		def  string
	}{
		{"storage_ref", "TEXT"},
		{"origin_path", "TEXT"},
		{"hash", "TEXT"},
		{"size", "INTEGER"},
		{"mime", "TEXT"},
		{"metadata", "TEXT"},
		{"tags", "TEXT"},
	}
	for _, col := range columns {
		if err := s.ensureColumn("loot", col.name, col.def); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) ensurePlannerMetricsColumns() error {
	if s == nil || s.db == nil {
		return nil
	}
	columns := []struct {
		name string
		def  string
	}{
		{"repair_attempts", "INTEGER"},
		{"repair_success", "INTEGER"},
		{"repair_failures", "INTEGER"},
	}
	for _, col := range columns {
		if err := s.ensureColumn("planner_metrics", col.name, col.def); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) ensureColumn(table, column, definition string) error {
	if s == nil || s.db == nil {
		return nil
	}
	query := fmt.Sprintf("SELECT 1 FROM pragma_table_info('%s') WHERE name = ?", table)
	row := s.db.QueryRow(query, column)
	var dummy int
	err := row.Scan(&dummy)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, sql.ErrNoRows):
		alter := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", table, column, definition)
		if _, execErr := s.db.Exec(alter); execErr != nil {
			return fmt.Errorf("alter table %s add column %s: %w", table, column, execErr)
		}
		return nil
	default:
		return fmt.Errorf("probe table %s column %s: %w", table, column, err)
	}
}

// Close 释放底层数据库句柄。
func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

// UpsertNode 实现 topology.Persistence 接口的节点写入逻辑。
func (s *Store) UpsertNode(node topology.NodeSnapshot) error {
	if s == nil || s.db == nil {
		return nil
	}
	isAlive := 0
	if node.IsAlive {
		isAlive = 1
	}
	lastSeen := node.LastSeen.UTC().Format(time.RFC3339Nano)
	if node.LastSeen.IsZero() {
		lastSeen = ""
	}
	lastSuccess := node.LastSuccess.UTC().Format(time.RFC3339Nano)
	if node.LastSuccess.IsZero() {
		lastSuccess = ""
	}
	repairUpdated := node.RepairUpdated.UTC().Format(time.RFC3339Nano)
	if node.RepairUpdated.IsZero() {
		repairUpdated = ""
	}
	tlsEnabled := 0
	if node.TLSEnabled {
		tlsEnabled = 1
	}
	_, err := s.db.Exec(`INSERT INTO nodes
		(uuid, parent_uuid, network_id, ip, port, listen_port, dial_address, fallback_port, transport, tls_enabled, last_success, repair_failures, repair_updated, hostname, username, memo, is_alive, last_seen)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(uuid) DO UPDATE SET parent_uuid=excluded.parent_uuid,
			network_id=excluded.network_id,
			ip=excluded.ip,
			port=excluded.port,
			listen_port=excluded.listen_port,
			dial_address=excluded.dial_address,
			fallback_port=excluded.fallback_port,
			transport=excluded.transport,
			tls_enabled=excluded.tls_enabled,
			last_success=excluded.last_success,
			repair_failures=excluded.repair_failures,
			repair_updated=excluded.repair_updated,
			hostname=excluded.hostname,
			username=excluded.username,
			memo=excluded.memo,
			is_alive=excluded.is_alive,
			last_seen=excluded.last_seen;`,
		node.UUID,
		node.Parent,
		node.Network,
		node.IP,
		node.Port,
		node.ListenPort,
		node.DialAddress,
		node.FallbackPort,
		node.Transport,
		tlsEnabled,
		lastSuccess,
		node.RepairFailures,
		repairUpdated,
		node.Hostname,
		node.Username,
		node.Memo,
		isAlive,
		lastSeen,
	)
	if err != nil {
		return fmt.Errorf("upsert node: %w", err)
	}
	return nil
}

// DeleteNode 删除节点记录。
func (s *Store) DeleteNode(uuid string) error {
	if s == nil || s.db == nil {
		return nil
	}
	_, err := s.db.Exec(`DELETE FROM nodes WHERE uuid = ?`, uuid)
	if err != nil {
		return fmt.Errorf("delete node: %w", err)
	}
	_, err = s.db.Exec(`DELETE FROM edges WHERE from_uuid = ? OR to_uuid = ?`, uuid, uuid)
	if err != nil {
		return fmt.Errorf("delete node edges: %w", err)
	}
	return nil
}

// UpsertEdge 写入或更新边记录。
func (s *Store) UpsertEdge(edge topology.EdgeSnapshot) error {
	if s == nil || s.db == nil {
		return nil
	}
	_, err := s.db.Exec(`INSERT INTO edges(from_uuid, to_uuid, type, weight) VALUES (?, ?, ?, ?)
		ON CONFLICT(from_uuid, to_uuid) DO UPDATE SET type=excluded.type, weight=excluded.weight;`,
		edge.From, edge.To, int(edge.Type), edge.Weight)
	if err != nil {
		return fmt.Errorf("upsert edge: %w", err)
	}
	return nil
}

// DeleteEdge 删除边记录。
func (s *Store) DeleteEdge(from, to string) error {
	if s == nil || s.db == nil {
		return nil
	}
	_, err := s.db.Exec(`DELETE FROM edges WHERE from_uuid = ? AND to_uuid = ?`, from, to)
	if err != nil {
		return fmt.Errorf("delete edge: %w", err)
	}
	return nil
}

// UpsertNetwork 记录 target 与网络的关联关系。
func (s *Store) UpsertNetwork(targetUUID, networkID string) error {
	if s == nil || s.db == nil {
		return nil
	}
	_, err := s.db.Exec(`INSERT INTO networks(target_uuid, network_id) VALUES (?, ?)
		ON CONFLICT(target_uuid) DO UPDATE SET network_id=excluded.network_id;`, targetUUID, networkID)
	if err != nil {
		return fmt.Errorf("upsert network: %w", err)
	}
	return nil
}

// DeleteNetwork 删除 target 对应的网络映射。
func (s *Store) DeleteNetwork(targetUUID string) error {
	if s == nil || s.db == nil {
		return nil
	}
	_, err := s.db.Exec(`DELETE FROM networks WHERE target_uuid = ?`, targetUUID)
	if err != nil {
		return fmt.Errorf("delete network: %w", err)
	}
	return nil
}

// UpsertSupplementalLink 持久化补链状态。
func (s *Store) UpsertSupplementalLink(link topology.SupplementalLinkSnapshot) error {
	if s == nil || s.db == nil {
		return nil
	}
	_, err := s.db.Exec(`INSERT INTO supplemental_links(link_uuid, listener, dialer, status, failure_reason, last_transition)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(link_uuid) DO UPDATE SET listener=excluded.listener,
			dialer=excluded.dialer,
			status=excluded.status,
			failure_reason=excluded.failure_reason,
			last_transition=excluded.last_transition;`,
		link.LinkUUID,
		link.Listener,
		link.Dialer,
		link.Status,
		link.FailureReason,
		link.LastTransition.UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		return fmt.Errorf("upsert supplemental link: %w", err)
	}
	return nil
}

// DeleteSupplementalLink 删除补链记录。
func (s *Store) DeleteSupplementalLink(linkUUID string) error {
	if s == nil || s.db == nil {
		return nil
	}
	_, err := s.db.Exec(`DELETE FROM supplemental_links WHERE link_uuid = ?`, linkUUID)
	if err != nil {
		return fmt.Errorf("delete supplemental link: %w", err)
	}
	return nil
}

// UpsertPlannerMetrics 持久化调度器聚合指标。
func (s *Store) UpsertPlannerMetrics(metrics topology.PlannerMetricsSnapshot) error {
	if s == nil || s.db == nil {
		return nil
	}
	_, err := s.db.Exec(`INSERT INTO planner_metrics(id, dispatched, success, failures, dropped, recycled, queue_high, last_failure, repair_attempts, repair_success, repair_failures)
		VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET dispatched=excluded.dispatched,
			success=excluded.success,
			failures=excluded.failures,
			dropped=excluded.dropped,
			recycled=excluded.recycled,
			queue_high=excluded.queue_high,
			last_failure=excluded.last_failure,
			repair_attempts=excluded.repair_attempts,
			repair_success=excluded.repair_success,
			repair_failures=excluded.repair_failures;`,
		metrics.Dispatched,
		metrics.Success,
		metrics.Failures,
		metrics.Dropped,
		metrics.Recycled,
		metrics.QueueHigh,
		metrics.LastFailure,
		metrics.RepairAttempts,
		metrics.RepairSuccess,
		metrics.RepairFailures,
	)
	if err != nil {
		return fmt.Errorf("upsert planner metrics: %w", err)
	}
	return nil
}

func isNoSuchTable(err error) bool {
	return err != nil && (errors.Is(err, sql.ErrNoRows) || strings.Contains(err.Error(), "no such table"))
}
