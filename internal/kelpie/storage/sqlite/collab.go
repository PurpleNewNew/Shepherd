package sqlite

import (
	"fmt"
	"strings"
	"time"
)

type UserRecord struct {
	Username     string
	PasswordHash string
	Role         string
	Disabled     bool
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

type AuthSessionRecord struct {
	SessionID        string
	Username         string
	AccessTokenHash  string
	RefreshTokenHash string
	AccessExpiresAt  time.Time
	RefreshExpiresAt time.Time
	CreatedAt        time.Time
	UpdatedAt        time.Time
	Revoked          bool
	LastPeer         string
	UserAgent        string
}

type AuditRecord struct {
	ID        string
	Tenant    string
	Username  string
	Role      string
	Method    string
	Target    string
	Params    string
	Status    string
	Error     string
	Peer      string
	CreatedAt time.Time
}

type AuditFilter struct {
	Tenant   string
	Username string
	Method   string
	From     time.Time
	To       time.Time
	Limit    int
}

type ChatRecord struct {
	ID        string
	Username  string
	Role      string
	Message   string
	CreatedAt time.Time
}

func (s *CollabRepository) ensureCollabSchema() error {
	if s == nil || s.db == nil {
		return nil
	}
	const users = `CREATE TABLE IF NOT EXISTS users (
        username TEXT PRIMARY KEY,
        password_hash TEXT NOT NULL,
        role TEXT NOT NULL,
        disabled INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    );`
	const sessions = `CREATE TABLE IF NOT EXISTS auth_sessions (
        session_id TEXT PRIMARY KEY,
        username TEXT NOT NULL,
        access_token_hash TEXT NOT NULL,
        refresh_token_hash TEXT NOT NULL,
        access_expires_at TEXT NOT NULL,
        refresh_expires_at TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        revoked INTEGER NOT NULL DEFAULT 0,
        last_peer TEXT,
        user_agent TEXT
    );`
	const audit = `CREATE TABLE IF NOT EXISTS audit_logs (
        id TEXT PRIMARY KEY,
        tenant TEXT,
        username TEXT,
        role TEXT,
        method TEXT,
        target TEXT,
        parameters TEXT,
        status TEXT,
        error TEXT,
        peer TEXT,
        created_at TEXT NOT NULL
    );`
	const chat = `CREATE TABLE IF NOT EXISTS chat_messages (
        id TEXT PRIMARY KEY,
        username TEXT,
        role TEXT,
        message TEXT NOT NULL,
        created_at TEXT NOT NULL
    );`
	stmts := []string{users, sessions, audit, chat,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_auth_sessions_access ON auth_sessions(access_token_hash);`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_auth_sessions_refresh ON auth_sessions(refresh_token_hash);`,
		`CREATE INDEX IF NOT EXISTS idx_auth_sessions_username ON auth_sessions(username);`,
		`CREATE INDEX IF NOT EXISTS idx_audit_created ON audit_logs(created_at);`,
		`CREATE INDEX IF NOT EXISTS idx_audit_username ON audit_logs(username);`,
		`CREATE INDEX IF NOT EXISTS idx_chat_created ON chat_messages(created_at);`,
	}
	for _, stmt := range stmts {
		if _, err := s.db.Exec(stmt); err != nil {
			return fmt.Errorf("init collaboration schema: %w", err)
		}
	}
	// 为已有数据库补充多租字段。
	if err := s.ensureColumn("audit_logs", "tenant", "TEXT"); err != nil {
		return err
	}
	return nil
}

func (s *CollabRepository) CountUsers() (int, error) {
	if s == nil || s.db == nil {
		return 0, fmt.Errorf("store unavailable")
	}
	row := s.db.QueryRow(`SELECT COUNT(1) FROM users`)
	var count int
	if err := row.Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func (s *CollabRepository) UpsertUser(rec UserRecord) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("store unavailable")
	}
	name := strings.ToLower(strings.TrimSpace(rec.Username))
	if name == "" {
		return fmt.Errorf("username required")
	}
	if strings.TrimSpace(rec.PasswordHash) == "" {
		return fmt.Errorf("password hash required")
	}
	rec.Username = name
	if rec.CreatedAt.IsZero() {
		rec.CreatedAt = time.Now().UTC()
	}
	if rec.UpdatedAt.IsZero() {
		rec.UpdatedAt = rec.CreatedAt
	}
	_, err := s.db.Exec(`INSERT INTO users(username, password_hash, role, disabled, created_at, updated_at)
        VALUES(?,?,?,?,?,?)
        ON CONFLICT(username) DO UPDATE SET password_hash=excluded.password_hash, role=excluded.role,
            disabled=excluded.disabled, updated_at=excluded.updated_at`,
		rec.Username, rec.PasswordHash, rec.Role, boolToInt(rec.Disabled), rec.CreatedAt.Format(time.RFC3339Nano), rec.UpdatedAt.Format(time.RFC3339Nano))
	return err
}

func (s *CollabRepository) GetUser(username string) (UserRecord, error) {
	var rec UserRecord
	if s == nil || s.db == nil {
		return rec, fmt.Errorf("store unavailable")
	}
	row := s.db.QueryRow(`SELECT username, password_hash, role, disabled, created_at, updated_at FROM users WHERE username = ?`, strings.ToLower(strings.TrimSpace(username)))
	var disabled int
	var created, updated string
	if err := row.Scan(&rec.Username, &rec.PasswordHash, &rec.Role, &disabled, &created, &updated); err != nil {
		return rec, err
	}
	rec.Disabled = disabled != 0
	rec.CreatedAt, _ = time.Parse(time.RFC3339Nano, created)
	rec.UpdatedAt, _ = time.Parse(time.RFC3339Nano, updated)
	return rec, nil
}

func (s *CollabRepository) CreateSession(rec AuthSessionRecord) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("store unavailable")
	}
	if rec.CreatedAt.IsZero() {
		rec.CreatedAt = time.Now().UTC()
	}
	if rec.UpdatedAt.IsZero() {
		rec.UpdatedAt = rec.CreatedAt
	}
	_, err := s.db.Exec(`INSERT INTO auth_sessions(session_id, username, access_token_hash, refresh_token_hash, access_expires_at, refresh_expires_at, created_at, updated_at, revoked, last_peer, user_agent)
        VALUES(?,?,?,?,?,?,?,?,?,?,?)`, rec.SessionID, rec.Username, rec.AccessTokenHash, rec.RefreshTokenHash,
		rec.AccessExpiresAt.Format(time.RFC3339Nano), rec.RefreshExpiresAt.Format(time.RFC3339Nano), rec.CreatedAt.Format(time.RFC3339Nano), rec.UpdatedAt.Format(time.RFC3339Nano), boolToInt(rec.Revoked), rec.LastPeer, rec.UserAgent)
	return err
}

func (s *CollabRepository) updateSessionTokens(rec AuthSessionRecord) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("store unavailable")
	}
	rec.UpdatedAt = time.Now().UTC()
	_, err := s.db.Exec(`UPDATE auth_sessions SET access_token_hash=?, refresh_token_hash=?, access_expires_at=?, refresh_expires_at=?, updated_at=?, revoked=?, last_peer=? WHERE session_id=?`,
		rec.AccessTokenHash, rec.RefreshTokenHash, rec.AccessExpiresAt.Format(time.RFC3339Nano), rec.RefreshExpiresAt.Format(time.RFC3339Nano), rec.UpdatedAt.Format(time.RFC3339Nano), boolToInt(rec.Revoked), rec.LastPeer, rec.SessionID)
	return err
}

func (s *CollabRepository) SessionByAccessHash(hash string) (AuthSessionRecord, error) {
	return s.fetchSession(`access_token_hash = ?`, hash)
}

func (s *CollabRepository) SessionByRefreshHash(hash string) (AuthSessionRecord, error) {
	return s.fetchSession(`refresh_token_hash = ?`, hash)
}

func (s *CollabRepository) fetchSession(where string, arg any) (AuthSessionRecord, error) {
	var rec AuthSessionRecord
	if s == nil || s.db == nil {
		return rec, fmt.Errorf("store unavailable")
	}
	query := `SELECT session_id, username, access_token_hash, refresh_token_hash, access_expires_at, refresh_expires_at, created_at, updated_at, revoked, last_peer, user_agent FROM auth_sessions WHERE ` + where + ` LIMIT 1`
	row := s.db.QueryRow(query, arg)
	var accessExp, refreshExp, created, updated string
	var revoked int
	if err := row.Scan(&rec.SessionID, &rec.Username, &rec.AccessTokenHash, &rec.RefreshTokenHash, &accessExp, &refreshExp, &created, &updated, &revoked, &rec.LastPeer, &rec.UserAgent); err != nil {
		return rec, err
	}
	rec.AccessExpiresAt, _ = time.Parse(time.RFC3339Nano, accessExp)
	rec.RefreshExpiresAt, _ = time.Parse(time.RFC3339Nano, refreshExp)
	rec.CreatedAt, _ = time.Parse(time.RFC3339Nano, created)
	rec.UpdatedAt, _ = time.Parse(time.RFC3339Nano, updated)
	rec.Revoked = revoked != 0
	return rec, nil
}

func (s *CollabRepository) RevokeSession(sessionID string) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("store unavailable")
	}
	_, err := s.db.Exec(`UPDATE auth_sessions SET revoked=1, updated_at=? WHERE session_id=?`, time.Now().UTC().Format(time.RFC3339Nano), sessionID)
	return err
}

func (s *CollabRepository) InsertAudit(rec AuditRecord) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("store unavailable")
	}
	if rec.CreatedAt.IsZero() {
		rec.CreatedAt = time.Now().UTC()
	}
	_, err := s.db.Exec(`INSERT INTO audit_logs(id, tenant, username, role, method, target, parameters, status, error, peer, created_at)
        VALUES(?,?,?,?,?,?,?,?,?,?,?)`, rec.ID, strings.ToLower(strings.TrimSpace(rec.Tenant)), rec.Username, rec.Role, rec.Method, rec.Target, rec.Params, rec.Status, rec.Error, rec.Peer, rec.CreatedAt.Format(time.RFC3339Nano))
	return err
}

func (s *CollabRepository) ListAudit(filter AuditFilter) ([]AuditRecord, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("store unavailable")
	}
	clauses := []string{"1=1"}
	args := []any{}
	if filter.Tenant != "" {
		clauses = append(clauses, "tenant = ?")
		args = append(args, strings.ToLower(filter.Tenant))
	}
	if filter.Username != "" {
		clauses = append(clauses, "username = ?")
		args = append(args, strings.ToLower(filter.Username))
	}
	if filter.Method != "" {
		clauses = append(clauses, "method = ?")
		args = append(args, filter.Method)
	}
	if !filter.From.IsZero() {
		clauses = append(clauses, "created_at >= ?")
		args = append(args, filter.From.Format(time.RFC3339Nano))
	}
	if !filter.To.IsZero() {
		clauses = append(clauses, "created_at <= ?")
		args = append(args, filter.To.Format(time.RFC3339Nano))
	}
	limit := filter.Limit
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	query := fmt.Sprintf(`SELECT id, tenant, username, role, method, target, parameters, status, error, peer, created_at FROM audit_logs WHERE %s ORDER BY created_at DESC LIMIT %d`, strings.Join(clauses, " AND "), limit)
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var records []AuditRecord
	for rows.Next() {
		var rec AuditRecord
		var created string
		if err := rows.Scan(&rec.ID, &rec.Tenant, &rec.Username, &rec.Role, &rec.Method, &rec.Target, &rec.Params, &rec.Status, &rec.Error, &rec.Peer, &created); err != nil {
			return nil, err
		}
		rec.CreatedAt, _ = time.Parse(time.RFC3339Nano, created)
		records = append(records, rec)
	}
	return records, rows.Err()
}

func (s *CollabRepository) InsertChat(rec ChatRecord) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("store unavailable")
	}
	if rec.CreatedAt.IsZero() {
		rec.CreatedAt = time.Now().UTC()
	}
	_, err := s.db.Exec(`INSERT INTO chat_messages(id, username, role, message, created_at) VALUES(?,?,?,?,?)`, rec.ID, rec.Username, rec.Role, rec.Message, rec.CreatedAt.Format(time.RFC3339Nano))
	return err
}

func (s *CollabRepository) ListChat(limit int, before time.Time) ([]ChatRecord, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("store unavailable")
	}
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	clauses := []string{"1=1"}
	args := []any{}
	if !before.IsZero() {
		clauses = append(clauses, "created_at < ?")
		args = append(args, before.Format(time.RFC3339Nano))
	}
	query := fmt.Sprintf(`SELECT id, username, role, message, created_at FROM chat_messages WHERE %s ORDER BY created_at DESC LIMIT %d`, strings.Join(clauses, " AND "), limit)
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var records []ChatRecord
	for rows.Next() {
		var rec ChatRecord
		var created string
		if err := rows.Scan(&rec.ID, &rec.Username, &rec.Role, &rec.Message, &created); err != nil {
			return nil, err
		}
		rec.CreatedAt, _ = time.Parse(time.RFC3339Nano, created)
		records = append(records, rec)
	}
	return records, rows.Err()
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func (s *CollabRepository) UpdateSession(rec AuthSessionRecord) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("store unavailable")
	}
	return s.updateSessionTokens(rec)
}

func (s *CollabRepository) DeleteSession(sessionID string) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("store unavailable")
	}
	_, err := s.db.Exec(`DELETE FROM auth_sessions WHERE session_id=?`, sessionID)
	return err
}
