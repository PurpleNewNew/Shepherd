package process

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"codeberg.org/agnoie/shepherd/internal/kelpie/printer"
	"codeberg.org/agnoie/shepherd/internal/kelpie/topology"
	"codeberg.org/agnoie/shepherd/internal/kelpie/uipb"
	"codeberg.org/agnoie/shepherd/pkg/config/defaults"
	"codeberg.org/agnoie/shepherd/pkg/utils"
	"codeberg.org/agnoie/shepherd/protocol"
)

// SleepUpdateParams 表示运行期 sleep 更新中的可选字段。
type SleepUpdateParams struct {
	SleepSeconds  *int
	WorkSeconds   *int
	JitterPercent *float64
}

type pendingSleepUpdate struct {
	params      SleepUpdateParams
	enqueuedAt  time.Time
	nextAttempt time.Time
	backoff     time.Duration
	attempts    int
	lastError   string
}

const (
	pendingSleepFlushInterval  = 250 * time.Millisecond
	pendingSleepInitialBackoff = 250 * time.Millisecond
	pendingSleepMaxBackoff     = 5 * time.Second
	pendingSleepMaxAge         = 30 * time.Minute
)

// UpdateNodeMemo 为指定节点设置 memo 字段，并通知对应 agent。
func (admin *Admin) UpdateNodeMemo(targetUUID, memo string) error {
	if admin == nil || admin.mgr == nil {
		return fmt.Errorf("admin unavailable")
	}
	targetUUID = strings.TrimSpace(targetUUID)
	if targetUUID == "" {
		return fmt.Errorf("missing target uuid")
	}
	task := &topology.TopoTask{Mode: topology.UPDATEMEMO, UUID: targetUUID, Memo: memo}
	if _, err := admin.topoRequest(task); err != nil {
		return err
	}
	route, ok := admin.fetchRoute(targetUUID)
	if !ok || route == "" {
		return fmt.Errorf("route unavailable for %s", targetUUID)
	}
	sess := admin.sessionForRoute(route)
	if sess == nil || sess.Conn() == nil {
		return fmt.Errorf("session unavailable for %s", targetUUID)
	}
	msg := protocol.NewDownMsg(sess.Conn(), sess.Secret(), sess.UUID())
	protocol.SetMessageMeta(msg, sess.ProtocolVersion(), sess.ProtocolFlags())
	header := &protocol.Header{
		Sender:      protocol.ADMIN_UUID,
		Accepter:    protocol.TEMP_UUID,
		MessageType: protocol.MYMEMO,
		RouteLen:    uint32(len([]byte(route))),
		Route:       route,
	}
	payload := &protocol.MyMemo{MemoLen: uint64(len(memo)), Memo: memo}
	protocol.ConstructMessage(msg, header, payload, false)
	msg.SendMessage()
	return nil
}

// ConnectNode 请求目标节点主动连接到一个下游地址。
func (admin *Admin) ConnectNode(ctx context.Context, targetUUID, addr string) error {
	if admin == nil || admin.mgr == nil {
		return fmt.Errorf("admin unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	targetUUID = strings.TrimSpace(targetUUID)
	addr = strings.TrimSpace(addr)
	if targetUUID == "" {
		return fmt.Errorf("missing target uuid")
	}
	if addr == "" {
		return fmt.Errorf("missing connect address")
	}
	normalized, _, err := utils.CheckIPPort(addr)
	if err != nil {
		return err
	}
	route, ok := admin.fetchRoute(targetUUID)
	if !ok || route == "" {
		return fmt.Errorf("route unavailable for %s", targetUUID)
	}
	ackCh := admin.mgr.ConnectManager.RegisterAck()
	if err := admin.sendConnectStart(route, targetUUID, normalized); err != nil {
		admin.mgr.ConnectManager.UnregisterAck(ackCh)
		return err
	}
	timer := time.NewTimer(defaults.ConnectAckTimeout)
	defer timer.Stop()
	consumed := false
	defer func() {
		if !consumed {
			admin.mgr.ConnectManager.UnregisterAck(ackCh)
		}
	}()
	select {
	case ok, open := <-ackCh:
		consumed = true
		if !open {
			return fmt.Errorf("connect acknowledgement channel closed")
		}
		if !ok {
			return fmt.Errorf("connect request rejected by %s", targetUUID)
		}
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return fmt.Errorf("connect request timeout for %s", targetUUID)
	}
	return nil
}

var (
	ErrSSHTunnelMissingTarget   = errors.New("missing target uuid")
	ErrSSHTunnelMissingAddr     = errors.New("missing server address")
	ErrSSHTunnelMissingPort     = errors.New("missing agent port")
	ErrSSHTunnelMissingUser     = errors.New("missing username")
	ErrSSHTunnelMissingPass     = errors.New("missing password")
	ErrSSHTunnelMissingCert     = errors.New("missing certificate payload")
	ErrSSHTunnelUnsupportedAuth = errors.New("unsupported auth method")
	ErrSleepUpdateMissingTarget = errors.New("missing target uuid")
	ErrSleepUpdateNoFields      = errors.New("at least one sleep field required")
	ErrSleepUpdateInvalidSleep  = errors.New("sleep seconds must be >= 0")
	ErrSleepUpdateInvalidWork   = errors.New("work seconds must be >= 0")
	ErrShutdownMissingTarget    = errors.New("missing target uuid")
)

func (admin *Admin) StartSSHTunnel(ctx context.Context, targetUUID, serverAddr, agentPort string, method uipb.SshTunnelAuthMethod, username, password string, privateKey []byte) error {
	if admin == nil {
		return fmt.Errorf("admin unavailable")
	}
	targetUUID = strings.TrimSpace(targetUUID)
	if targetUUID == "" {
		return ErrSSHTunnelMissingTarget
	}
	serverAddr = strings.TrimSpace(serverAddr)
	if serverAddr == "" {
		return ErrSSHTunnelMissingAddr
	}
	agentPort = strings.TrimSpace(agentPort)
	if agentPort == "" {
		return ErrSSHTunnelMissingPort
	}
	username = strings.TrimSpace(username)
	if username == "" {
		return ErrSSHTunnelMissingUser
	}
	authMethod := method
	switch authMethod {
	case uipb.SshTunnelAuthMethod_SSH_TUNNEL_AUTH_METHOD_PASSWORD:
		if strings.TrimSpace(password) == "" {
			return ErrSSHTunnelMissingPass
		}
	case uipb.SshTunnelAuthMethod_SSH_TUNNEL_AUTH_METHOD_CERT:
		if len(privateKey) == 0 {
			return ErrSSHTunnelMissingCert
		}
	default:
		return ErrSSHTunnelUnsupportedAuth
	}
	opts := map[string]string{
		"kind":     "ssh-tunnel",
		"addr":     serverAddr,
		"port":     agentPort,
		"method":   "1",
		"username": username,
	}
	if authMethod == uipb.SshTunnelAuthMethod_SSH_TUNNEL_AUTH_METHOD_CERT {
		opts["method"] = "2"
	} else {
		opts["password"] = password
	}
	openFn := admin.streamOpenOverride
	if openFn == nil {
		openFn = admin.OpenStream
	}
	streamHandle, err := openFn(ctx, targetUUID, "", opts)
	if err != nil {
		return err
	}
	defer streamHandle.Close()
	if authMethod == uipb.SshTunnelAuthMethod_SSH_TUNNEL_AUTH_METHOD_CERT {
		if _, err := streamHandle.Write(privateKey); err != nil {
			return fmt.Errorf("send cert failed: %w", err)
		}
	}
	return nil
}

func (admin *Admin) UpdateSleep(targetUUID string, params SleepUpdateParams) error {
	if admin == nil || admin.topology == nil || admin.mgr == nil {
		return fmt.Errorf("admin unavailable")
	}
	targetUUID = strings.TrimSpace(targetUUID)
	if targetUUID == "" {
		return ErrSleepUpdateMissingTarget
	}
	if params.SleepSeconds == nil && params.WorkSeconds == nil && params.JitterPercent == nil {
		return ErrSleepUpdateNoFields
	}
	// 尽早校验载荷，避免把无效更新排进后续发送队列。
	if _, err := buildSleepUpdatePayload(params); err != nil {
		return err
	}

	// 即使节点暂时不可达，也要先在拓扑中更新期望的 sleep/work 配置。
	// 这样可以避免 Kelpie 把 duty-cycle 节点误判成“常在线”，
	// 从而触发不必要的自愈抖动。
	admin.updateSleepTopology(targetUUID, params, false)

	// 会话元数据表达的是“期望状态”，因此无论是否能立即下发都先更新。
	state := admin.sessionState(targetUUID)
	now := time.Now().UTC()
	if params.JitterPercent != nil {
		val := *params.JitterPercent
		state.JitterPercent = &val
	}
	state.LastCommand = "sleep_update"
	state.LastCommandAt = now
	admin.setSessionState(targetUUID, state)

	var (
		route string
		ok    bool
	)
	if fn := admin.routeOverride; fn != nil {
		route, ok = fn(targetUUID)
	} else {
		route, ok = admin.fetchRoute(targetUUID)
	}
	if ok && strings.TrimSpace(route) != "" {
		if err := admin.sendSleepUpdate(targetUUID, route, params); err == nil {
			return nil
		} else {
			admin.enqueuePendingSleepUpdate(targetUUID, params, err)
			return nil
		}
	}
	admin.enqueuePendingSleepUpdate(targetUUID, params, fmt.Errorf("route unavailable for %s", targetUUID))
	return nil
}

func (admin *Admin) sendSleepUpdate(targetUUID, route string, params SleepUpdateParams) error {
	payload, err := buildSleepUpdatePayload(params)
	if err != nil {
		return err
	}
	msg, err := admin.newDownstreamMessageForRoute(targetUUID, route)
	if err != nil {
		return err
	}
	header := &protocol.Header{
		Sender:      protocol.ADMIN_UUID,
		Accepter:    protocol.TEMP_UUID,
		MessageType: protocol.SLEEP_UPDATE,
		RouteLen:    uint32(len([]byte(route))),
		Route:       route,
	}
	protocol.ConstructMessage(msg, header, payload, false)
	msg.SendMessage()
	admin.updateSleepTopology(targetUUID, params, true)
	printer.Success("\r\n[*] Sleep update dispatched.\r\n")
	return nil
}

func buildSleepUpdatePayload(params SleepUpdateParams) (*protocol.SleepUpdate, error) {
	update := &protocol.SleepUpdate{}
	if params.SleepSeconds != nil {
		if *params.SleepSeconds < 0 {
			return nil, ErrSleepUpdateInvalidSleep
		}
		update.Flags |= protocol.SleepUpdateFlagSleepSeconds
		update.SleepSeconds = int32(*params.SleepSeconds)
	}
	if params.WorkSeconds != nil {
		if *params.WorkSeconds < 0 {
			return nil, ErrSleepUpdateInvalidWork
		}
		update.Flags |= protocol.SleepUpdateFlagWorkSeconds
		update.WorkSeconds = int32(*params.WorkSeconds)
	}
	if params.JitterPercent != nil {
		jitter := *params.JitterPercent
		if jitter < 0 {
			jitter = 0
		}
		if jitter > 50 {
			jitter = 50
		}
		update.Flags |= protocol.SleepUpdateFlagJitter
		update.JitterPermille = uint16(math.Round(jitter * 10))
	}
	if update.Flags == 0 {
		return nil, ErrSleepUpdateNoFields
	}
	return update, nil
}

func (admin *Admin) updateSleepTopology(targetUUID string, params SleepUpdateParams, touchLiveness bool) {
	if admin == nil || admin.topology == nil {
		return
	}
	if params.SleepSeconds == nil && params.WorkSeconds == nil {
		return
	}
	task := &topology.TopoTask{
		Mode:         topology.UPDATEDETAIL,
		UUID:         targetUUID,
		SleepSeconds: -1,
		WorkSeconds:  -1,
		SkipLiveness: !touchLiveness,
	}
	if params.SleepSeconds != nil {
		task.SleepSeconds = *params.SleepSeconds
	}
	if params.WorkSeconds != nil {
		task.WorkSeconds = *params.WorkSeconds
	}
	admin.topology.Execute(task)
}

func copySleepUpdateParams(params SleepUpdateParams) SleepUpdateParams {
	out := SleepUpdateParams{}
	if params.SleepSeconds != nil {
		v := *params.SleepSeconds
		out.SleepSeconds = &v
	}
	if params.WorkSeconds != nil {
		v := *params.WorkSeconds
		out.WorkSeconds = &v
	}
	if params.JitterPercent != nil {
		v := *params.JitterPercent
		out.JitterPercent = &v
	}
	return out
}

func mergeSleepUpdateParams(base SleepUpdateParams, override SleepUpdateParams) SleepUpdateParams {
	out := copySleepUpdateParams(base)
	if override.SleepSeconds != nil {
		v := *override.SleepSeconds
		out.SleepSeconds = &v
	}
	if override.WorkSeconds != nil {
		v := *override.WorkSeconds
		out.WorkSeconds = &v
	}
	if override.JitterPercent != nil {
		v := *override.JitterPercent
		out.JitterPercent = &v
	}
	return out
}

func (admin *Admin) enqueuePendingSleepUpdate(targetUUID string, params SleepUpdateParams, lastErr error) {
	if admin == nil {
		return
	}
	key := strings.ToLower(strings.TrimSpace(targetUUID))
	if key == "" {
		return
	}
	now := time.Now().UTC()
	rec := pendingSleepUpdate{
		params:      copySleepUpdateParams(params),
		enqueuedAt:  now,
		nextAttempt: now,
		backoff:     pendingSleepInitialBackoff,
	}
	if lastErr != nil {
		rec.lastError = lastErr.Error()
	}
	admin.pendingSleepMu.Lock()
	if admin.pendingSleepUpdates == nil {
		admin.pendingSleepUpdates = make(map[string]pendingSleepUpdate)
	}
	if existing, ok := admin.pendingSleepUpdates[key]; ok {
		existing.params = mergeSleepUpdateParams(existing.params, params)
		if existing.enqueuedAt.IsZero() {
			existing.enqueuedAt = rec.enqueuedAt
		}
		if existing.backoff <= 0 {
			existing.backoff = pendingSleepInitialBackoff
		}
		existing.nextAttempt = now
		if rec.lastError != "" {
			existing.lastError = rec.lastError
		}
		admin.pendingSleepUpdates[key] = existing
	} else {
		admin.pendingSleepUpdates[key] = rec
	}
	admin.pendingSleepMu.Unlock()
}

func (admin *Admin) runPendingSleepUpdateFlusher(ctx context.Context) {
	if admin == nil || ctx == nil {
		return
	}
	ticker := time.NewTicker(pendingSleepFlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			admin.flushPendingSleepUpdates()
		}
	}
}

func (admin *Admin) flushPendingSleepUpdates() {
	if admin == nil {
		return
	}
	now := time.Now().UTC()

	type dueUpdate struct {
		target string
		rec    pendingSleepUpdate
	}

	var due []dueUpdate
	admin.pendingSleepMu.Lock()
	for target, rec := range admin.pendingSleepUpdates {
		if !rec.enqueuedAt.IsZero() && pendingSleepMaxAge > 0 && now.Sub(rec.enqueuedAt) > pendingSleepMaxAge {
			delete(admin.pendingSleepUpdates, target)
			continue
		}
		if !rec.nextAttempt.IsZero() && now.Before(rec.nextAttempt) {
			continue
		}
		delete(admin.pendingSleepUpdates, target)
		due = append(due, dueUpdate{target: target, rec: rec})
	}
	admin.pendingSleepMu.Unlock()

	for _, item := range due {
		targetUUID := item.target
		params := item.rec.params
		var (
			route string
			ok    bool
		)
		if fn := admin.routeOverride; fn != nil {
			route, ok = fn(targetUUID)
		} else {
			route, ok = admin.fetchRoute(targetUUID)
		}
		if ok && strings.TrimSpace(route) != "" {
			if err := admin.sendSleepUpdate(targetUUID, route, params); err == nil {
				continue
			} else {
				item.rec.lastError = err.Error()
			}
		} else {
			item.rec.lastError = fmt.Sprintf("route unavailable for %s", targetUUID)
		}

		item.rec.attempts++
		if item.rec.backoff <= 0 {
			item.rec.backoff = pendingSleepInitialBackoff
		} else {
			item.rec.backoff *= 2
		}
		if item.rec.backoff > pendingSleepMaxBackoff {
			item.rec.backoff = pendingSleepMaxBackoff
		}
		item.rec.nextAttempt = now.Add(item.rec.backoff)
		if item.rec.enqueuedAt.IsZero() {
			item.rec.enqueuedAt = now
		}

		admin.pendingSleepMu.Lock()
		if existing, ok := admin.pendingSleepUpdates[targetUUID]; ok {
			// 较新的更新优先；在本次取出后，保留队列里已经排好的其他字段。
			existing.params = mergeSleepUpdateParams(item.rec.params, existing.params)
			if existing.enqueuedAt.IsZero() || (!item.rec.enqueuedAt.IsZero() && item.rec.enqueuedAt.Before(existing.enqueuedAt)) {
				existing.enqueuedAt = item.rec.enqueuedAt
			}
			if existing.backoff < item.rec.backoff {
				existing.backoff = item.rec.backoff
			}
			if existing.nextAttempt.Before(item.rec.nextAttempt) {
				existing.nextAttempt = item.rec.nextAttempt
			}
			existing.lastError = item.rec.lastError
			existing.attempts += item.rec.attempts
			admin.pendingSleepUpdates[targetUUID] = existing
		} else {
			admin.pendingSleepUpdates[targetUUID] = item.rec
		}
		admin.pendingSleepMu.Unlock()
	}
}

func (admin *Admin) ShutdownNode(targetUUID string) error {
	if admin == nil || admin.mgr == nil {
		return fmt.Errorf("admin unavailable")
	}
	targetUUID = strings.TrimSpace(targetUUID)
	if targetUUID == "" {
		return ErrShutdownMissingTarget
	}
	var (
		route string
		ok    bool
	)
	if fn := admin.routeOverride; fn != nil {
		route, ok = fn(targetUUID)
	} else {
		route, ok = admin.fetchRoute(targetUUID)
	}
	if !ok || strings.TrimSpace(route) == "" {
		return fmt.Errorf("route unavailable for %s", targetUUID)
	}
	if fn := admin.shutdownOverride; fn != nil {
		fn(route, targetUUID)
		return nil
	}
	msg, err := admin.newDownstreamMessageForRoute(targetUUID, route)
	if err != nil {
		return err
	}
	header := &protocol.Header{
		Sender:      protocol.ADMIN_UUID,
		Accepter:    protocol.TEMP_UUID,
		MessageType: protocol.SHUTDOWN,
		RouteLen:    uint32(len([]byte(route))),
		Route:       route,
	}
	payload := &protocol.Shutdown{OK: 1}
	protocol.ConstructMessage(msg, header, payload, false)
	msg.SendMessage()
	return nil
}

func (admin *Admin) StartForwardProxy(ctx context.Context, targetUUID, bindAddr, remoteAddr string) (*ProxyDescriptor, error) {
	if admin == nil {
		return nil, fmt.Errorf("admin unavailable")
	}
	mgr := admin.ensurePortProxyManager()
	if mgr == nil {
		return nil, fmt.Errorf("proxy manager unavailable")
	}
	return mgr.StartForward(ctx, targetUUID, bindAddr, remoteAddr)
}

func (admin *Admin) StopForwardProxy(targetUUID, proxyID string) ([]*ProxyDescriptor, error) {
	if admin == nil {
		return nil, fmt.Errorf("admin unavailable")
	}
	mgr := admin.ensurePortProxyManager()
	if mgr == nil {
		return nil, fmt.Errorf("proxy manager unavailable")
	}
	return mgr.StopForward(targetUUID, proxyID)
}

func (admin *Admin) StartBackwardProxy(ctx context.Context, targetUUID, remotePort, localPort string) (*ProxyDescriptor, error) {
	if admin == nil {
		return nil, fmt.Errorf("admin unavailable")
	}
	mgr := admin.ensurePortProxyManager()
	if mgr == nil {
		return nil, fmt.Errorf("proxy manager unavailable")
	}
	return mgr.StartBackward(ctx, targetUUID, remotePort, localPort)
}

func (admin *Admin) StopBackwardProxy(targetUUID, proxyID string) ([]*ProxyDescriptor, error) {
	if admin == nil {
		return nil, fmt.Errorf("admin unavailable")
	}
	mgr := admin.ensurePortProxyManager()
	if mgr == nil {
		return nil, fmt.Errorf("proxy manager unavailable")
	}
	return mgr.StopBackward(targetUUID, proxyID)
}

func (admin *Admin) ListProxies() []*ProxyDescriptor {
	if admin == nil {
		return nil
	}
	mgr := admin.ensurePortProxyManager()
	if mgr == nil {
		return nil
	}
	return mgr.List()
}

// StartListener 通过当前 DTN 控制路径，在指定节点上触发一个被动监听器。
func (admin *Admin) StartListener(targetUUID, bind string, mode int, listenerID string) (string, error) {
	if admin == nil || admin.mgr == nil {
		return "", fmt.Errorf("admin unavailable")
	}
	targetUUID = strings.TrimSpace(targetUUID)
	if targetUUID == "" {
		return "", fmt.Errorf("missing target uuid")
	}
	listenerID = strings.TrimSpace(listenerID)
	if listenerID == "" {
		return "", fmt.Errorf("listener id required")
	}
	bind = strings.TrimSpace(bind)
	if mode == listenerModeNormal {
		if bind == "" {
			return "", fmt.Errorf("missing bind address")
		}
		validated, _, err := utils.CheckIPPort(bind)
		if err != nil {
			return "", err
		}
		bind = validated
	}
	route, ok := admin.fetchRoute(targetUUID)
	if !ok || route == "" {
		return "", fmt.Errorf("route unavailable for %s", targetUUID)
	}
	ackCh := admin.mgr.ListenManager.RegisterAck(listenerID)
	if err := admin.sendListenerStart(route, targetUUID, listenerID, mode, bind); err != nil {
		return "", err
	}
	select {
	case ok := <-ackCh:
		if !ok {
			return "", fmt.Errorf("listener %s rejected", shortID(listenerID))
		}
	case <-time.After(defaults.ListenerAckTimeout):
		return "", fmt.Errorf("listener %s start timeout", shortID(listenerID))
	}
	return route, nil
}

func (admin *Admin) StopListener(targetUUID, listenerID string) error {
	if admin == nil || admin.mgr == nil {
		return fmt.Errorf("admin unavailable")
	}
	targetUUID = strings.TrimSpace(targetUUID)
	listenerID = strings.TrimSpace(listenerID)
	if targetUUID == "" || listenerID == "" {
		return fmt.Errorf("target uuid and listener id required")
	}
	route, ok := admin.fetchRoute(targetUUID)
	if !ok || strings.TrimSpace(route) == "" {
		return fmt.Errorf("route unavailable for %s", targetUUID)
	}
	ackCh := admin.mgr.ListenManager.RegisterAck(listenerID)
	if err := admin.sendListenerStop(route, targetUUID, listenerID); err != nil {
		return err
	}
	select {
	case ok := <-ackCh:
		if !ok {
			return fmt.Errorf("listener %s stop rejected", shortID(listenerID))
		}
	case <-time.After(defaults.ListenerAckTimeout):
		return fmt.Errorf("listener %s stop timeout", shortID(listenerID))
	}
	return nil
}

func (admin *Admin) sendListenerStart(route, targetUUID, listenerID string, mode int, addr string) error {
	msg, err := admin.newDownstreamMessageForRoute(targetUUID, route)
	if err != nil {
		return err
	}
	header := &protocol.Header{
		Sender:      protocol.ADMIN_UUID,
		Accepter:    protocol.TEMP_UUID,
		MessageType: protocol.LISTENREQ,
		RouteLen:    uint32(len([]byte(route))),
		Route:       route,
	}
	listenReq := &protocol.ListenReq{
		Method:     uint16(mode),
		AddrLen:    uint64(len(addr)),
		Addr:       addr,
		ListenerID: listenerID,
	}
	protocol.ConstructMessage(msg, header, listenReq, false)
	msg.SendMessage()
	return nil
}

func (admin *Admin) sendListenerStop(route, targetUUID, listenerID string) error {
	msg, err := admin.newDownstreamMessageForRoute(targetUUID, route)
	if err != nil {
		return err
	}
	header := &protocol.Header{
		Sender:      protocol.ADMIN_UUID,
		Accepter:    protocol.TEMP_UUID,
		MessageType: protocol.LISTENSTOP,
		RouteLen:    uint32(len([]byte(route))),
		Route:       route,
	}
	stopReq := &protocol.ListenStop{ListenerID: listenerID}
	protocol.ConstructMessage(msg, header, stopReq, false)
	msg.SendMessage()
	return nil
}

func (admin *Admin) sendConnectStart(route, targetUUID, addr string) error {
	msg, err := admin.newDownstreamMessageForRoute(targetUUID, route)
	if err != nil {
		return err
	}
	header := &protocol.Header{
		Sender:      protocol.ADMIN_UUID,
		Accepter:    protocol.TEMP_UUID,
		MessageType: protocol.CONNECTSTART,
		RouteLen:    uint32(len([]byte(route))),
		Route:       route,
	}
	req := &protocol.ConnectStart{
		AddrLen: uint16(len(addr)),
		Addr:    addr,
	}
	protocol.ConstructMessage(msg, header, req, false)
	msg.SendMessage()
	return nil
}
