package process

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"codeberg.org/agnoie/shepherd/pkg/utils"
	"codeberg.org/agnoie/shepherd/protocol"
)

// SessionStatus 表示会话的高层健康状态。
type SessionStatus string

const (
	SessionStatusUnknown    SessionStatus = "unknown"
	SessionStatusActive     SessionStatus = "active"
	SessionStatusDegraded   SessionStatus = "degraded"
	SessionStatusFailed     SessionStatus = "failed"
	SessionStatusMarkedDead SessionStatus = "marked_dead"
	SessionStatusRepairing  SessionStatus = "repairing"
)

// SessionMarkAction 枚举支持的标记操作。
type SessionMarkAction string

const (
	SessionMarkAlive       SessionMarkAction = "alive"
	SessionMarkDead        SessionMarkAction = "dead"
	SessionMarkMaintenance SessionMarkAction = "maintenance"
)

// SessionInfo 描述当前被跟踪的 session/connection。
type SessionInfo struct {
	TargetUUID    string
	Active        bool
	Connected     bool
	RemoteAddr    string
	Upstream      string
	Downstream    string
	NetworkID     string
	LastSeen      time.Time
	Status        SessionStatus
	LastError     string
	LastOperator  string
	StatusReason  string
	SleepProfile  string
	LastCommand   string
	SleepSeconds  *int
	WorkSeconds   *int
	JitterPercent *float64
	Metadata      map[string]string
}

// SessionDiagnostics 汇总扩展的会话信息。
type SessionDiagnostics struct {
	Session SessionInfo
	Metrics []SessionMetric
	Issues  []SessionIssue
	Process []SessionProcess
}

// SessionMetric 表示诊断信息中的一个 name/value 对。
type SessionMetric struct {
	Name  string
	Value string
}

// SessionIssue 表示一次检测到的会话异常。
type SessionIssue struct {
	Code    string
	Message string
	Detail  string
}

// SessionProcess 表示轻量级的进程或句柄描述。
type SessionProcess struct {
	PID   string
	Name  string
	User  string
	State string
	Path  string
	Since time.Time
}

// SessionFilter 用于收窄会话列表查询范围。
type SessionFilter struct {
	Targets         []string
	Statuses        []SessionStatus
	IncludeInactive bool
}

type sessionState struct {
	Status        SessionStatus
	LastError     string
	Reason        string
	Operator      string
	UpdatedAt     time.Time
	JitterPercent *float64
	LastCommand   string
	LastCommandAt time.Time
}

func (admin *Admin) sessionState(target string) sessionState {
	admin.sessionMetaMu.RLock()
	state := admin.sessionMeta[strings.ToLower(target)]
	admin.sessionMetaMu.RUnlock()
	return state
}

func (admin *Admin) setSessionState(target string, state sessionState) {
	admin.sessionMetaMu.Lock()
	if admin.sessionMeta == nil {
		admin.sessionMeta = make(map[string]sessionState)
	}
	admin.sessionMeta[strings.ToLower(target)] = state
	admin.sessionMetaMu.Unlock()
}

func normalizeSessionFilter(filter SessionFilter) (map[string]struct{}, map[SessionStatus]struct{}) {
	var (
		targets  map[string]struct{}
		statuses map[SessionStatus]struct{}
	)
	if len(filter.Targets) > 0 {
		targets = make(map[string]struct{}, len(filter.Targets))
		for _, t := range filter.Targets {
			trim := strings.ToLower(strings.TrimSpace(t))
			if trim == "" {
				continue
			}
			targets[trim] = struct{}{}
		}
	}
	if len(filter.Statuses) > 0 {
		statuses = make(map[SessionStatus]struct{}, len(filter.Statuses))
		for _, st := range filter.Statuses {
			if st == "" {
				continue
			}
			statuses[st] = struct{}{}
		}
	}
	return targets, statuses
}

// Sessions 返回当前已知的 session 信息。
func (admin *Admin) Sessions(filter SessionFilter) []SessionInfo {
	if admin == nil || admin.store == nil {
		return nil
	}
	uuids := admin.store.ListComponents()
	if len(uuids) == 0 {
		return nil
	}
	targets, statuses := normalizeSessionFilter(filter)
	includeInactive := filter.IncludeInactive
	result := make([]SessionInfo, 0, len(uuids))
	for _, uuid := range uuids {
		info := admin.buildSessionInfo(uuid)
		if len(targets) > 0 {
			if _, ok := targets[strings.ToLower(info.TargetUUID)]; !ok {
				continue
			}
		}
		if len(statuses) > 0 {
			if _, ok := statuses[info.Status]; !ok {
				continue
			}
		}
		if !includeInactive && !info.Active && !info.Connected {
			continue
		}
		result = append(result, info)
	}
	sort.Slice(result, func(i, j int) bool {
		return strings.Compare(result[i].TargetUUID, result[j].TargetUUID) < 0
	})
	return result
}

func (admin *Admin) buildSessionInfo(uuid string) SessionInfo {
	info := SessionInfo{TargetUUID: uuid}
	info.Metadata = make(map[string]string)
	if admin.store != nil {
		actives := admin.store.ActiveUUIDs()
		for _, active := range actives {
			if strings.EqualFold(active, uuid) {
				info.Active = true
				break
			}
		}
		if comp, ok := admin.store.Component(uuid); ok {
			info.Upstream = comp.Upstream
			info.Downstream = comp.Downstream
		}
		if sess := admin.store.SessionFor(uuid); sess != nil {
			info.Connected = sess.Conn() != nil
			if conn := sess.Conn(); conn != nil {
				if addr := conn.RemoteAddr(); addr != nil {
					info.RemoteAddr = addr.String()
				}
			}
		}
	}
	if admin.topology != nil {
		info.NetworkID = admin.topology.NetworkFor(uuid)
		if runtime, ok := admin.topology.NodeRuntime(uuid); ok {
			info.LastSeen = runtime.LastSeen
			if !runtime.LastSeen.IsZero() {
				info.Metadata["last_seen_at"] = runtime.LastSeen.UTC().Format(time.RFC3339Nano)
			}
			if runtime.SleepSeconds > 0 {
				sleep := runtime.SleepSeconds
				info.SleepSeconds = &sleep
			}
			if runtime.WorkSeconds > 0 {
				work := runtime.WorkSeconds
				info.WorkSeconds = &work
			}
			info.SleepProfile = formatSleepProfile(runtime.SleepSeconds, runtime.WorkSeconds)
			if info.SleepProfile != "" {
				info.Metadata["sleep_profile"] = info.SleepProfile
			}
			if runtime.Memo != "" {
				info.Metadata["memo"] = runtime.Memo
			}
			if !runtime.NextWake.IsZero() {
				info.Metadata["next_wake_at"] = runtime.NextWake.UTC().Format(time.RFC3339Nano)
			}
		}
	}
	state := admin.sessionState(uuid)
	if state.Status == "" {
		if info.Connected {
			state.Status = SessionStatusActive
		} else {
			state.Status = SessionStatusUnknown
		}
	}
	info.Status = state.Status
	info.LastError = state.LastError
	info.LastOperator = state.Operator
	info.StatusReason = state.Reason
	info.LastCommand = state.LastCommand
	if state.JitterPercent != nil {
		val := *state.JitterPercent
		info.JitterPercent = &val
	}
	info.Metadata["session_status"] = string(info.Status)
	if !state.UpdatedAt.IsZero() {
		info.Metadata["status_updated_at"] = state.UpdatedAt.UTC().Format(time.RFC3339Nano)
	}
	if info.StatusReason != "" {
		info.Metadata["status_reason"] = info.StatusReason
	}
	if info.LastCommand != "" {
		info.Metadata["last_command"] = info.LastCommand
	}
	if !state.LastCommandAt.IsZero() {
		info.Metadata["last_command_at"] = state.LastCommandAt.UTC().Format(time.RFC3339Nano)
	}
	return info
}

// MarkSession 更新一个 session 的手动状态。
func (admin *Admin) MarkSession(target string, action SessionMarkAction, operator, reason string) (SessionInfo, error) {
	target = strings.TrimSpace(target)
	if admin == nil || admin.store == nil {
		return SessionInfo{}, fmt.Errorf("admin unavailable")
	}
	if target == "" {
		return SessionInfo{}, fmt.Errorf("target uuid required")
	}
	status := SessionStatusUnknown
	switch action {
	case SessionMarkAlive:
		status = SessionStatusActive
	case SessionMarkDead:
		status = SessionStatusMarkedDead
	case SessionMarkMaintenance:
		status = SessionStatusDegraded
	default:
		return SessionInfo{}, fmt.Errorf("unsupported mark action %s", action)
	}
	state := admin.sessionState(target)
	state.Status = status
	state.LastError = reason
	state.Reason = reason
	state.Operator = operator
	state.UpdatedAt = time.Now().UTC()
	state.LastCommand = fmt.Sprintf("mark:%s", action)
	state.LastCommandAt = state.UpdatedAt
	admin.setSessionState(target, state)
	info := admin.buildSessionInfo(target)
	return info, nil
}

// RepairSession 入队一个手动 repair/补链请求。
func (admin *Admin) RepairSession(target string, force bool, reason string) (SessionInfo, error) {
	target = strings.TrimSpace(target)
	if admin == nil {
		return SessionInfo{}, fmt.Errorf("admin unavailable")
	}
	if target == "" {
		return SessionInfo{}, fmt.Errorf("target uuid required")
	}
	if err := admin.RequestRepair(target); err != nil {
		return SessionInfo{}, err
	}
	state := admin.sessionState(target)
	state.Status = SessionStatusRepairing
	state.LastError = reason
	state.UpdatedAt = time.Now().UTC()
	state.LastCommand = "repair"
	state.LastCommandAt = state.UpdatedAt
	admin.setSessionState(target, state)
	return admin.buildSessionInfo(target), nil
}

// ReconnectSession 会强制丢弃该 session，从而触发重连。
func (admin *Admin) ReconnectSession(target, reason string) (SessionInfo, error) {
	target = strings.TrimSpace(target)
	if admin == nil {
		return SessionInfo{}, fmt.Errorf("admin unavailable")
	}
	if target == "" {
		return SessionInfo{}, fmt.Errorf("target uuid required")
	}
	_ = admin.DropSession(target)
	state := admin.sessionState(target)
	state.Status = SessionStatusUnknown
	state.LastError = reason
	state.UpdatedAt = time.Now().UTC()
	state.LastCommand = "reconnect"
	state.LastCommandAt = state.UpdatedAt
	admin.setSessionState(target, state)
	return admin.buildSessionInfo(target), nil
}

// TerminateSession 会彻底强制移除该 session 记录。
func (admin *Admin) TerminateSession(target, reason string) (bool, error) {
	target = strings.TrimSpace(target)
	if admin == nil {
		return false, fmt.Errorf("admin unavailable")
	}
	if target == "" {
		return false, fmt.Errorf("target uuid required")
	}
	if err := admin.DropSession(target); err != nil {
		return false, err
	}
	admin.sessionMetaMu.Lock()
	delete(admin.sessionMeta, strings.ToLower(target))
	admin.sessionMetaMu.Unlock()
	return true, nil
}

// SessionDiagnostics 返回扩展信息，并遵循 UI API 的 include 开关。
func (admin *Admin) SessionDiagnostics(target string, includeProcesses, includeMetrics bool) (SessionDiagnostics, error) {
	target = strings.TrimSpace(target)
	if target == "" {
		return SessionDiagnostics{}, fmt.Errorf("target uuid required")
	}
	if !admin.sessionKnown(target) {
		return SessionDiagnostics{}, fmt.Errorf("session %s not found", target)
	}
	info := admin.buildSessionInfo(target)

	metrics := make([]SessionMetric, 0, 12)
	appendMetric := func(name, value string) {
		if !includeMetrics {
			return
		}
		if strings.TrimSpace(value) == "" {
			return
		}
		metrics = append(metrics, SessionMetric{Name: name, Value: value})
	}
	appendMetric("session.status", string(info.Status))
	appendMetric("session.active", strconv.FormatBool(info.Active))
	appendMetric("session.connected", strconv.FormatBool(info.Connected))
	appendMetric("session.network_id", info.NetworkID)
	appendMetric("session.remote_addr", info.RemoteAddr)
	appendMetric("session.sleep_profile", info.SleepProfile)
	if info.SleepSeconds != nil {
		appendMetric("session.sleep_seconds", strconv.Itoa(*info.SleepSeconds))
	}
	if info.WorkSeconds != nil {
		appendMetric("session.work_seconds", strconv.Itoa(*info.WorkSeconds))
	}
	if info.JitterPercent != nil {
		appendMetric("session.jitter_percent", fmt.Sprintf("%.2f", *info.JitterPercent))
	}
	if !info.LastSeen.IsZero() {
		appendMetric("session.last_seen_at", info.LastSeen.UTC().Format(time.RFC3339Nano))
	}
	appendMetric("session.last_command", info.LastCommand)
	appendMetric("session.last_operator", info.LastOperator)
	if len(info.Metadata) > 0 {
		keys := make([]string, 0, len(info.Metadata))
		for k := range info.Metadata {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			appendMetric("meta."+k, info.Metadata[k])
		}
	}
	issues := make([]SessionIssue, 0, 3)
	if info.Status == SessionStatusMarkedDead {
		issues = append(issues, SessionIssue{Code: "marked_dead", Message: "session marked dead", Detail: info.StatusReason})
	}
	if info.Status == SessionStatusFailed {
		issues = append(issues, SessionIssue{Code: "failed", Message: "session reported failure", Detail: info.LastError})
	}
	if info.Status == SessionStatusRepairing {
		issues = append(issues, SessionIssue{Code: "repairing", Message: "session repairing", Detail: info.StatusReason})
	}
	if !info.Connected {
		issues = append(issues, SessionIssue{Code: "disconnected", Message: "session not connected", Detail: info.StatusReason})
	}
	if info.LastError != "" {
		issues = append(issues, SessionIssue{Code: "last_error", Message: "last error", Detail: info.LastError})
	}

	result := SessionDiagnostics{
		Session: info,
		Issues:  issues,
	}
	if includeMetrics {
		result.Metrics = metrics
	}
	if includeProcesses {
		result.Process = admin.collectSessionProcesses(target)
	}
	return result, nil
}

func (admin *Admin) sessionKnown(target string) bool {
	target = strings.TrimSpace(target)
	if target == "" || admin == nil {
		return false
	}
	if strings.EqualFold(target, protocol.ADMIN_UUID) {
		return true
	}
	if admin.store != nil {
		if _, ok := admin.store.Component(target); ok {
			return true
		}
		if sess := admin.store.SessionFor(target); sess != nil {
			return true
		}
	}
	if admin.topology != nil {
		if _, ok := admin.topology.NodeRuntime(target); ok {
			return true
		}
	}
	for _, diag := range admin.StreamDiagnostics() {
		if strings.EqualFold(strings.TrimSpace(diag.Target), target) {
			return true
		}
	}
	return false
}

func (admin *Admin) collectSessionProcesses(target string) []SessionProcess {
	target = strings.TrimSpace(target)
	if target == "" || admin == nil {
		return []SessionProcess{}
	}
	if remote := admin.remoteProcessSnapshot(target); len(remote) > 0 {
		return remote
	}

	processes := make([]SessionProcess, 0, 16)
	processes = append(processes, admin.streamProcessesForSession(target)...)
	processes = append(processes, admin.listenerProcessesForSession(target)...)
	processes = append(processes, admin.proxyProcessesForSession(target)...)
	return processes
}

func (admin *Admin) streamProcessesForSession(target string) []SessionProcess {
	target = strings.TrimSpace(target)
	if admin == nil || target == "" {
		return []SessionProcess{}
	}
	diags := admin.StreamDiagnostics()
	if len(diags) == 0 {
		return []SessionProcess{}
	}
	processes := make([]SessionProcess, 0, len(diags))
	for _, diag := range diags {
		if !strings.EqualFold(strings.TrimSpace(diag.Target), target) {
			continue
		}
		processes = append(processes, SessionProcess{
			PID:   strconv.FormatUint(uint64(diag.ID), 10),
			Name:  streamProcessName(diag.Kind),
			User:  streamProcessUser(diag.Metadata, diag.SessionID),
			State: streamProcessState(diag.Outbound, diag.Inflight, diag.Pending),
			Path:  streamProcessPath(diag.Metadata, diag.Target),
			Since: diag.LastActivity,
		})
	}
	return processes
}

func (admin *Admin) listenerProcessesForSession(target string) []SessionProcess {
	listeners := admin.ListListeners(ListenerFilter{Targets: []string{target}})
	if len(listeners) == 0 {
		return nil
	}
	processes := make([]SessionProcess, 0, len(listeners))
	for _, listener := range listeners {
		name := strings.TrimSpace(listener.Protocol)
		if name == "" {
			name = "listener"
		} else {
			name = "listener:" + strings.ToLower(name)
		}
		path := strings.TrimSpace(listener.Bind)
		processes = append(processes, SessionProcess{
			PID:   strings.TrimSpace(listener.ID),
			Name:  name,
			State: strings.ToLower(strings.TrimSpace(string(listener.Status))),
			Path:  path,
			Since: listener.UpdatedAt,
		})
	}
	return processes
}

func (admin *Admin) proxyProcessesForSession(target string) []SessionProcess {
	proxies := admin.ListProxies()
	if len(proxies) == 0 {
		return nil
	}
	processes := make([]SessionProcess, 0, len(proxies))
	for _, proxy := range proxies {
		if proxy == nil || !strings.EqualFold(strings.TrimSpace(proxy.Target()), target) {
			continue
		}
		opts := proxy.Options()
		path := strings.TrimSpace(opts["bind"])
		if path == "" {
			path = strings.TrimSpace(opts["remote"])
		}
		if path == "" {
			path = strings.TrimSpace(opts["remote_addr"])
		}
		if path == "" {
			path = strings.TrimSpace(opts["local_port"])
		}
		processes = append(processes, SessionProcess{
			PID:   strings.TrimSpace(proxy.ID()),
			Name:  "proxy:" + strings.ToLower(strings.TrimSpace(proxy.Kind())),
			State: "running",
			Path:  path,
		})
	}
	return processes
}

func (admin *Admin) remoteProcessSnapshot(target string) []SessionProcess {
	target = strings.TrimSpace(target)
	if target == "" || admin == nil {
		return nil
	}
	info := admin.buildSessionInfo(target)
	if !info.Connected {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	sessionID := "diag-proc-" + utils.GenerateUUID()
	stream, err := admin.OpenStream(ctx, target, sessionID, map[string]string{
		"kind": "shell",
		"mode": strconv.Itoa(int(protocol.ShellModePipe)),
	})
	if err != nil || stream == nil {
		return nil
	}
	defer stream.Close()

	var raw bytes.Buffer
	done := make(chan struct{})
	go func() {
		_, _ = io.Copy(&raw, stream)
		close(done)
	}()
	_, _ = io.WriteString(
		stream,
		"echo __SHEPHERD_PS_BEGIN__\n"+
			"ps -eo pid,user,comm,args\n"+
			"tasklist /FO CSV /NH\n"+
			"echo __SHEPHERD_PS_END__\n"+
			"exit\n",
	)

	select {
	case <-done:
	case <-ctx.Done():
		_ = stream.Close()
		select {
		case <-done:
		case <-time.After(300 * time.Millisecond):
		}
	}

	return parseRemoteProcessOutput(raw.String())
}

func parseRemoteProcessOutput(raw string) []SessionProcess {
	raw = strings.ReplaceAll(raw, "\r\n", "\n")
	raw = strings.ReplaceAll(raw, "\r", "\n")
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	const (
		begin = "__SHEPHERD_PS_BEGIN__"
		end   = "__SHEPHERD_PS_END__"
	)
	start := strings.Index(raw, begin)
	if start < 0 {
		return nil
	}
	raw = raw[start+len(begin):]
	if finish := strings.Index(raw, end); finish >= 0 {
		raw = raw[:finish]
	}
	scanner := bufio.NewScanner(strings.NewReader(raw))
	processes := make([]SessionProcess, 0, 64)
	seen := make(map[string]struct{})
	skippingPSHeader := false
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if looksLikeTasklistError(line) || looksLikePSNoise(line) {
			continue
		}
		if strings.HasPrefix(strings.ToUpper(line), "PID ") || strings.HasPrefix(strings.ToUpper(line), "PID\t") {
			skippingPSHeader = true
			continue
		}
		if strings.HasPrefix(line, "\"") {
			proc := parseTasklistCSV(line)
			if proc == nil {
				continue
			}
			key := proc.PID + "|" + proc.Name
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			processes = append(processes, *proc)
			continue
		}
		if skippingPSHeader || isLikelyPSData(line) {
			proc := parsePSLine(line)
			if proc == nil {
				continue
			}
			key := proc.PID + "|" + proc.Name
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			processes = append(processes, *proc)
		}
	}
	return processes
}

func parseTasklistCSV(line string) *SessionProcess {
	reader := csv.NewReader(strings.NewReader(line))
	record, err := reader.Read()
	if err != nil || len(record) < 2 {
		return nil
	}
	name := strings.TrimSpace(record[0])
	pid := strings.TrimSpace(record[1])
	if name == "" || pid == "" {
		return nil
	}
	return &SessionProcess{
		PID:   pid,
		Name:  name,
		State: "running",
		Path:  name,
	}
}

func parsePSLine(line string) *SessionProcess {
	fields := strings.Fields(line)
	if len(fields) < 3 {
		return nil
	}
	pid := strings.TrimSpace(fields[0])
	if _, err := strconv.Atoi(pid); err != nil {
		return nil
	}
	user := strings.TrimSpace(fields[1])
	name := strings.TrimSpace(fields[2])
	path := name
	if len(fields) > 3 {
		path = strings.TrimSpace(strings.Join(fields[3:], " "))
	}
	base := filepath.Base(path)
	if base != "" {
		name = base
	}
	return &SessionProcess{
		PID:   pid,
		Name:  name,
		User:  user,
		State: "running",
		Path:  path,
	}
}

func looksLikeTasklistError(line string) bool {
	lower := strings.ToLower(line)
	return strings.Contains(lower, "tasklist") && strings.Contains(lower, "not found")
}

func looksLikePSNoise(line string) bool {
	lower := strings.ToLower(line)
	if strings.Contains(lower, "is not recognized as an internal or external command") {
		return true
	}
	if strings.Contains(lower, "command not found") {
		return true
	}
	if strings.HasPrefix(lower, "error:") {
		return true
	}
	return false
}

func isLikelyPSData(line string) bool {
	fields := strings.Fields(line)
	if len(fields) < 3 {
		return false
	}
	_, err := strconv.Atoi(fields[0])
	return err == nil
}

func streamProcessName(kind string) string {
	kind = strings.TrimSpace(kind)
	if kind == "" {
		return "stream"
	}
	return kind
}

func streamProcessUser(meta map[string]string, sessionID string) string {
	for _, key := range []string{"operator", "username", "user", "session"} {
		if value := strings.TrimSpace(meta[key]); value != "" {
			return value
		}
	}
	return strings.TrimSpace(sessionID)
}

func streamProcessState(outbound bool, inflight, pending int) string {
	direction := "inbound"
	if outbound {
		direction = "outbound"
	}
	if inflight > 0 {
		return direction + ":inflight"
	}
	if pending > 0 {
		return direction + ":pending"
	}
	return direction + ":idle"
}

func streamProcessPath(meta map[string]string, target string) string {
	for _, key := range []string{"path", "addr", "remote", "bind", "lport", "rport", "port"} {
		if value := strings.TrimSpace(meta[key]); value != "" {
			return value
		}
	}
	return strings.TrimSpace(target)
}

func formatSleepProfile(sleep, work int) string {
	if sleep <= 0 && work <= 0 {
		return ""
	}
	if sleep <= 0 {
		return "long"
	}
	if work > 0 {
		return fmt.Sprintf("%ds/%ds", sleep, work)
	}
	return fmt.Sprintf("%ds", sleep)
}
