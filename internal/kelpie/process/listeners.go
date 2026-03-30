package process

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"codeberg.org/agnoie/shepherd/internal/kelpie/printer"
	"codeberg.org/agnoie/shepherd/pkg/config/defaults"
	"codeberg.org/agnoie/shepherd/pkg/utils"
)

// ListenerStatus 标记监听当前状态。
type ListenerStatus string

const (
	ListenerStatusPending ListenerStatus = "pending"
	ListenerStatusRunning ListenerStatus = "running"
	ListenerStatusFailed  ListenerStatus = "failed"
	ListenerStatusStopped ListenerStatus = "stopped"
)

// ListenerRecord 表示一条 Kelpie 监听的持久化/运行时状态。
type ListenerRecord struct {
	ID          string
	TargetUUID  string
	Route       string
	Protocol    string
	Bind        string
	Mode        int
	Status      ListenerStatus
	LastError   string
	Metadata    map[string]string
	AuthPayload []byte
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// ListenerSpec 描述监听期望配置。
type ListenerSpec struct {
	Protocol    string
	Bind        string
	Mode        int
	Metadata    map[string]string
	AuthPayload []byte
}

// ListenerFilter 用于 List 时的筛选条件。
type ListenerFilter struct {
	Targets   []string
	Protocols []string
	Statuses  []ListenerStatus
}

// ListenerPersistence 抽象监听持久化接口，供 sqlite 等实现。
type ListenerPersistence interface {
	SaveListener(record ListenerRecord) error
	DeleteListener(id string) error
	LoadListeners() ([]ListenerRecord, error)
}

type listenerRegistry struct {
	mu      sync.RWMutex
	items   map[string]ListenerRecord
	persist ListenerPersistence
}

const (
	maxListenerAuthPayload = 8 * 1024
)

const (
	listenerModeNormal = iota
	listenerModeIptables
	listenerModeSoReuse
)

const (
	ListenerModeNormal   = listenerModeNormal
	ListenerModeIptables = listenerModeIptables
	ListenerModeSoReuse  = listenerModeSoReuse
)

var listenerProtocolModes = map[string]int{
	"tcp":      listenerModeNormal,
	"raw":      listenerModeNormal,
	"http":     listenerModeNormal,
	"https":    listenerModeNormal,
	"ws":       listenerModeNormal,
	"wss":      listenerModeNormal,
	"iptables": listenerModeIptables,
	"soreuse":  listenerModeSoReuse,
}

func newListenerRegistry(persist ListenerPersistence, seed []ListenerRecord) *listenerRegistry {
	reg := &listenerRegistry{
		items:   make(map[string]ListenerRecord),
		persist: persist,
	}
	for _, rec := range seed {
		if rec.ID == "" {
			continue
		}
		cp := rec
		if cp.Metadata == nil {
			cp.Metadata = map[string]string{}
		}
		reg.items[rec.ID] = cp
	}
	return reg
}

func (r *listenerRegistry) list(filter ListenerFilter) []ListenerRecord {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	var (
		targets   = normalizeFilter(filter.Targets)
		protos    = normalizeFilter(filter.Protocols)
		statusMap = make(map[ListenerStatus]struct{})
	)
	for _, st := range filter.Statuses {
		statusMap[st] = struct{}{}
	}
	result := make([]ListenerRecord, 0, len(r.items))
	for _, rec := range r.items {
		if len(targets) > 0 {
			if _, ok := targets[strings.ToLower(rec.TargetUUID)]; !ok {
				continue
			}
		}
		if len(protos) > 0 {
			if _, ok := protos[strings.ToLower(rec.Protocol)]; !ok {
				continue
			}
		}
		if len(statusMap) > 0 {
			if _, ok := statusMap[rec.Status]; !ok {
				continue
			}
		}
		result = append(result, cloneListener(rec))
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].CreatedAt.Before(result[j].CreatedAt)
	})
	return result
}

func (r *listenerRegistry) get(id string) (ListenerRecord, bool) {
	if r == nil {
		return ListenerRecord{}, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	rec, ok := r.items[id]
	return cloneListener(rec), ok
}

func (r *listenerRegistry) save(rec ListenerRecord) error {
	if r == nil || rec.ID == "" {
		return fmt.Errorf("listener registry unavailable")
	}
	r.mu.Lock()
	r.items[rec.ID] = cloneListener(rec)
	r.mu.Unlock()
	if r.persist != nil {
		return r.persist.SaveListener(rec)
	}
	return nil
}

func (r *listenerRegistry) delete(id string) (ListenerRecord, error) {
	if r == nil || id == "" {
		return ListenerRecord{}, fmt.Errorf("listener registry unavailable")
	}
	r.mu.Lock()
	rec, ok := r.items[id]
	if ok {
		delete(r.items, id)
	}
	r.mu.Unlock()
	if !ok {
		return ListenerRecord{}, fmt.Errorf("listener %s not found", id)
	}
	if r.persist != nil {
		if err := r.persist.DeleteListener(id); err != nil {
			return ListenerRecord{}, err
		}
	}
	return rec, nil
}

func normalizeFilter(values []string) map[string]struct{} {
	if len(values) == 0 {
		return nil
	}
	result := make(map[string]struct{}, len(values))
	for _, val := range values {
		val = strings.ToLower(strings.TrimSpace(val))
		if val == "" {
			continue
		}
		result[val] = struct{}{}
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

func cloneListener(rec ListenerRecord) ListenerRecord {
	copyMeta := make(map[string]string, len(rec.Metadata))
	for k, v := range rec.Metadata {
		copyMeta[k] = v
	}
	rec.Metadata = copyMeta
	if len(rec.AuthPayload) > 0 {
		rec.AuthPayload = append([]byte(nil), rec.AuthPayload...)
	}
	return rec
}

func copyMetadata(m map[string]string) map[string]string {
	if len(m) == 0 {
		return map[string]string{}
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func normalizeListenerProtocol(proto string, mode int) (string, int, error) {
	proto = strings.ToLower(strings.TrimSpace(proto))
	resolved, ok := listenerProtocolModes[proto]
	switch {
	case proto == "":
		return "tcp", listenerModeNormal, nil
	case ok:
		return proto, resolved, nil
	case isValidListenerMode(mode):
		return proto, mode, nil
	default:
		return "", mode, fmt.Errorf("unsupported listener protocol %s", proto)
	}
}

func isValidListenerMode(mode int) bool {
	switch mode {
	case listenerModeNormal, listenerModeIptables, listenerModeSoReuse:
		return true
	default:
		return false
	}
}

func applyAuthPayload(meta map[string]string, payload []byte) (map[string]string, error) {
	if len(payload) == 0 {
		if meta != nil {
			delete(meta, "auth_payload_sha256")
		}
		return meta, nil
	}
	if len(payload) > maxListenerAuthPayload {
		return meta, fmt.Errorf("auth payload exceeds %d bytes", maxListenerAuthPayload)
	}
	if meta == nil {
		meta = make(map[string]string)
	}
	sum := sha256.Sum256(payload)
	meta["auth_payload_sha256"] = hex.EncodeToString(sum[:])
	return meta, nil
}

func (admin *Admin) initListeners(store ListenerPersistence) {
	if admin == nil {
		return
	}
	var seed []ListenerRecord
	if store != nil {
		var err error
		seed, err = store.LoadListeners()
		if err != nil {
			printer.Warning("\r\n[*] Failed to load listeners: %v\r\n", err)
		}
	}
	admin.listenerStore = store
	admin.listeners = newListenerRegistry(store, seed)
}

// ListListeners 返回监听列表。
func (admin *Admin) ListListeners(filter ListenerFilter) []ListenerRecord {
	if admin == nil || admin.listeners == nil {
		return nil
	}
	return admin.listeners.list(filter)
}

// CreateListener 新建监听实例并立刻尝试在目标节点上启用。
func (admin *Admin) CreateListener(ctx context.Context, targetUUID string, spec ListenerSpec) (ListenerRecord, error) {
	_ = ctx
	if admin == nil {
		return ListenerRecord{}, fmt.Errorf("admin unavailable")
	}
	if admin.listeners == nil {
		admin.initListeners(admin.listenerStore)
	}
	targetUUID = strings.TrimSpace(targetUUID)
	if targetUUID == "" {
		return ListenerRecord{}, fmt.Errorf("missing target uuid")
	}
	mode := spec.Mode
	meta := copyMetadata(spec.Metadata)
	var err error
	if meta, err = applyAuthPayload(meta, spec.AuthPayload); err != nil {
		return ListenerRecord{}, err
	}
	protocol, mode, err := normalizeListenerProtocol(spec.Protocol, mode)
	if err != nil {
		return ListenerRecord{}, err
	}
	rec := ListenerRecord{
		ID:         fmt.Sprintf("lis-%s", utils.GenerateUUID()),
		TargetUUID: targetUUID,
		Protocol:   protocol,
		Bind:       strings.TrimSpace(spec.Bind),
		Mode:       mode,
		Status:     ListenerStatusPending,
		Metadata:   meta,
		CreatedAt:  time.Now().UTC(),
		UpdatedAt:  time.Now().UTC(),
	}
	if err := admin.listeners.save(rec); err != nil {
		return ListenerRecord{}, err
	}
	route, err := admin.startListenerInternal(targetUUID, rec.Bind, rec.Mode, rec.ID)
	if err != nil {
		rec.Status = ListenerStatusFailed
		rec.LastError = err.Error()
	} else {
		rec.Status = ListenerStatusRunning
		rec.LastError = ""
		rec.Route = route
	}
	rec.UpdatedAt = time.Now().UTC()
	if err := admin.listeners.save(rec); err != nil {
		return rec, err
	}
	return rec, nil
}

// UpdateListener 更新监听配置或执行控制指令。
func (admin *Admin) UpdateListener(ctx context.Context, id string, spec *ListenerSpec, desiredStatus string) (ListenerRecord, error) {
	_ = ctx
	if admin == nil || admin.listeners == nil {
		return ListenerRecord{}, fmt.Errorf("listener registry unavailable")
	}
	current, ok := admin.listeners.get(id)
	if !ok {
		return ListenerRecord{}, fmt.Errorf("listener %s not found", id)
	}
	changed := false
	if spec != nil {
		if bind := strings.TrimSpace(spec.Bind); bind != "" && bind != current.Bind {
			current.Bind = bind
			changed = true
		}
		if proto := strings.TrimSpace(spec.Protocol); proto != "" || isValidListenerMode(spec.Mode) {
			inputProto := proto
			if inputProto == "" {
				inputProto = current.Protocol
			}
			targetMode := current.Mode
			if isValidListenerMode(spec.Mode) {
				targetMode = spec.Mode
			}
			normalized, mode, err := normalizeListenerProtocol(inputProto, targetMode)
			if err != nil {
				return ListenerRecord{}, err
			}
			if normalized != current.Protocol {
				current.Protocol = normalized
				changed = true
			}
			if mode != current.Mode {
				current.Mode = mode
				changed = true
			}
		}
		if len(spec.Metadata) > 0 {
			current.Metadata = copyMetadata(spec.Metadata)
			changed = true
		}
		if spec.AuthPayload != nil {
			old := ""
			if current.Metadata != nil {
				old = current.Metadata["auth_payload_sha256"]
			}
			meta, err := applyAuthPayload(current.Metadata, spec.AuthPayload)
			if err != nil {
				return ListenerRecord{}, err
			}
			current.Metadata = meta
			if current.Metadata != nil && current.Metadata["auth_payload_sha256"] != old {
				changed = true
			}
		}
	}
	switch strings.ToLower(strings.TrimSpace(desiredStatus)) {
	case "":
	case "restart":
		if err := admin.stopListenerInternal(current.TargetUUID, current.ID); err != nil {
			current.Status = ListenerStatusFailed
			current.LastError = err.Error()
			return current, err
		}
		route, err := admin.startListenerInternal(current.TargetUUID, current.Bind, current.Mode, current.ID)
		if err != nil {
			current.Status = ListenerStatusFailed
			current.LastError = err.Error()
			current.Route = ""
			return current, err
		}
		current.Status = ListenerStatusRunning
		current.LastError = ""
		current.Route = route
		changed = true
	case "stop", "stopped", "pause", "paused":
		if err := admin.stopListenerInternal(current.TargetUUID, current.ID); err != nil {
			current.LastError = err.Error()
			current.Status = ListenerStatusFailed
			return current, err
		}
		current.Status = ListenerStatusStopped
		current.LastError = ""
		current.Route = ""
		changed = true
	case "start", "resume", "running":
		route, err := admin.startListenerInternal(current.TargetUUID, current.Bind, current.Mode, current.ID)
		if err != nil {
			current.Status = ListenerStatusFailed
			current.LastError = err.Error()
			current.Route = ""
			return current, err
		}
		current.Status = ListenerStatusRunning
		current.LastError = ""
		current.Route = route
		changed = true
	default:
		return ListenerRecord{}, fmt.Errorf("unsupported desired status %s", desiredStatus)
	}
	if !changed {
		return current, nil
	}
	current.UpdatedAt = time.Now().UTC()
	if err := admin.listeners.save(current); err != nil {
		return current, err
	}
	return current, nil
}

// DeleteListener 移除监听记录。
func (admin *Admin) DeleteListener(id string) (ListenerRecord, error) {
	if admin == nil || admin.listeners == nil {
		return ListenerRecord{}, fmt.Errorf("listener registry unavailable")
	}
	current, ok := admin.listeners.get(id)
	if !ok {
		return ListenerRecord{}, fmt.Errorf("listener %s not found", id)
	}
	_ = admin.stopListenerInternal(current.TargetUUID, current.ID)
	return admin.listeners.delete(id)
}

func (admin *Admin) startListenerInternal(targetUUID, bind string, mode int, listenerID string) (string, error) {
	if admin == nil {
		return "", fmt.Errorf("admin unavailable")
	}
	if admin.listenerStartOverride != nil {
		return admin.listenerStartOverride(targetUUID, bind, mode, listenerID)
	}
	return admin.StartListener(targetUUID, bind, mode, listenerID)
}

func (admin *Admin) stopListenerInternal(targetUUID, listenerID string) error {
	if admin == nil {
		return fmt.Errorf("admin unavailable")
	}
	if admin.listenerStopOverride != nil {
		return admin.listenerStopOverride(targetUUID, listenerID)
	}
	return admin.StopListener(targetUUID, listenerID)
}

func (admin *Admin) startListenerReconciler(ctx context.Context) {
	if admin == nil || admin.listeners == nil {
		return
	}
	go admin.runListenerReconciler(ctx)
}

func (admin *Admin) runListenerReconciler(ctx context.Context) {
	admin.reconcileListeners(true)
	interval := defaults.ListenerReconcileInterval
	if interval <= 0 {
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			admin.reconcileListeners(false)
		}
	}
}

func (admin *Admin) reconcileListeners(allowRunning bool) {
	if admin == nil || admin.listeners == nil {
		return
	}
	records := admin.listeners.list(ListenerFilter{})
	for _, rec := range records {
		admin.reconcileListenerRecord(rec, allowRunning)
	}
}

func (admin *Admin) reconcileListenerRecord(rec ListenerRecord, allowRunning bool) {
	if admin == nil || admin.listeners == nil || rec.ID == "" {
		return
	}
	current, ok := admin.listeners.get(rec.ID)
	if !ok {
		return
	}
	switch current.Status {
	case ListenerStatusPending:
		admin.startListenerRecord(current)
	case ListenerStatusFailed:
		if admin.listenerRetryDue(current) {
			admin.startListenerRecord(current)
		}
	case ListenerStatusRunning:
		if allowRunning {
			admin.startListenerRecord(current)
		}
	}
}

func (admin *Admin) listenerRetryDue(rec ListenerRecord) bool {
	if rec.Status != ListenerStatusFailed {
		return false
	}
	interval := defaults.ListenerRetryInterval
	if interval <= 0 {
		return true
	}
	if rec.UpdatedAt.IsZero() {
		return true
	}
	return time.Since(rec.UpdatedAt) >= interval
}

func (admin *Admin) startListenerRecord(rec ListenerRecord) {
	if admin == nil || admin.listeners == nil {
		return
	}
	current, ok := admin.listeners.get(rec.ID)
	if !ok {
		return
	}
	rec = current
	if rec.TargetUUID == "" || strings.TrimSpace(rec.ID) == "" {
		return
	}
	route, err := admin.startListenerInternal(rec.TargetUUID, rec.Bind, rec.Mode, rec.ID)
	now := time.Now().UTC()
	if err != nil {
		rec.Status = ListenerStatusFailed
		rec.LastError = err.Error()
		rec.Route = ""
	} else {
		rec.Status = ListenerStatusRunning
		rec.LastError = ""
		rec.Route = route
	}
	rec.UpdatedAt = now
	if err := admin.listeners.save(rec); err != nil {
		printer.Warning("\r\n[*] Failed to persist listener %s: %v\r\n", rec.ID, err)
	}
}
