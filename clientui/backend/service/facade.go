package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	wruntime "github.com/wailsapp/wails/v2/pkg/runtime"

	"codeberg.org/agnoie/shepherd/clientui/backend/config"
	"codeberg.org/agnoie/shepherd/clientui/backend/kelpie"
	"codeberg.org/agnoie/shepherd/internal/kelpie/uipb"
)

// API 是绑定给前端（Wails）的门面对象。所有由 Vue 组件发起的调用都会经过这里。
//
// 线程模型：
//   - Wails 可能在多个 goroutine 里调用方法，mu 用于保护 client/stream 字段。
//   - EventStream goroutine 独立运行，通过 wruntime.EventsEmit 推送事件给前端。
type API struct {
	store *config.Store
	ctx   context.Context

	mu         sync.Mutex
	client     *kelpie.Client
	stream     *kelpie.EventStream
	status     ConnectionStatus
	ringMu     sync.Mutex
	eventRing  []kelpie.Event
	eventRingN int
}

const (
	eventTopic       = "kelpie:event"
	statusTopic      = "kelpie:status"
	eventRingCap     = 300
	defaultDialTime  = 6 * time.Second
	snapshotTimeout  = 8 * time.Second
)

// New 创建 API；store 不可为 nil。
func New(store *config.Store) *API {
	return &API{
		store:     store,
		status:    ConnectionStatus{Phase: PhaseDisconnected},
		eventRing: make([]kelpie.Event, eventRingCap),
	}
}

// BindContext 在 Wails.Startup 里调用，保存运行时上下文。
func (a *API) BindContext(ctx context.Context) { a.ctx = ctx }

// Shutdown 在 Wails OnBeforeClose / OnShutdown 里调用。
func (a *API) Shutdown(context.Context) {
	_ = a.Disconnect()
}

// ---------- 连接生命周期 ----------

// ListRecentConnections 返回最近连接历史。
func (a *API) ListRecentConnections() []RecentConnectionDTO {
	records := a.store.ListRecent()
	out := make([]RecentConnectionDTO, 0, len(records))
	for _, r := range records {
		out = append(out, RecentConnectionDTO{
			ID:          r.ID,
			Label:       r.Label,
			Endpoint:    r.Endpoint,
			UseTLS:      r.TLSEnabled,
			ServerName:  r.ServerName,
			Fingerprint: r.Fingerprint,
			LastUsedAt:  r.LastUsedAt,
		})
	}
	return out
}

// RemoveRecentConnection 删除一条历史。
func (a *API) RemoveRecentConnection(id string) error {
	return a.store.RemoveRecent(id)
}

// GetConnectionStatus 给前端查询当前连接状态。
func (a *API) GetConnectionStatus() ConnectionStatus {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.status
}

// Connect 尝试连接 Kelpie。语义：
//   - 未开启 TLS：直接 dial。
//   - 开启 TLS：按 TOFU 查本地指纹，若空则首次握手并返回 PhaseAwaitingTrust；
//     用户同意后再次调用时设置 req.ForceAccept=true，此时会保存指纹并重试。
func (a *API) Connect(req ConnectRequest) (ConnectResult, error) {
	req.Endpoint = strings.TrimSpace(req.Endpoint)
	if req.Endpoint == "" {
		return ConnectResult{Phase: PhaseDisconnected, Message: "endpoint is required"}, errors.New("endpoint is required")
	}

	// 若已经有连接，先断开，避免状态错乱。
	_ = a.Disconnect()

	a.setPhase(PhaseConnecting, "")

	opts := kelpie.ConnectOptions{
		Endpoint:    req.Endpoint,
		Token:       req.Token,
		UseTLS:      req.UseTLS,
		ServerName:  req.ServerName,
		DialTimeout: defaultDialTime,
	}
	if req.UseTLS {
		if trusted, ok := a.store.LookupFingerprint(req.Endpoint); ok {
			opts.ExpectedFingerprint = trusted.Fingerprint
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultDialTime+2*time.Second)
	defer cancel()
	client, verifier, err := kelpie.Dial(ctx, opts)

	// 处理 TOFU 决策未完成的情况
	if err != nil {
		if decision, ok := kelpie.IsTOFUDecisionRequired(err); ok {
			// 首次握手成功，但用户尚未信任。
			if req.ForceAccept {
				// 用户在前端点了“信任”，这里立刻写入并重试。
				if err2 := a.store.TrustFingerprint(req.Endpoint, decision.Fingerprint); err2 != nil {
					a.setPhase(PhaseDisconnected, err2.Error())
					return ConnectResult{Phase: PhaseDisconnected, Message: err2.Error()}, err2
				}
				// 修正 opts 后二次 dial。
				opts.ExpectedFingerprint = decision.Fingerprint
				ctx2, cancel2 := context.WithTimeout(context.Background(), defaultDialTime+2*time.Second)
				defer cancel2()
				client, verifier, err = kelpie.Dial(ctx2, opts)
			} else {
				a.setPhase(PhaseAwaitingTrust, "fingerprint not trusted yet")
				return ConnectResult{
					Phase:              PhaseAwaitingTrust,
					PendingFingerprint: decision.Fingerprint,
					Endpoint:           req.Endpoint,
					Message:            "server fingerprint unrecorded; please confirm",
				}, nil
			}
		}
		if mismatch, ok := kelpie.IsTOFUMismatch(err); ok {
			a.setPhase(PhaseDisconnected, "fingerprint mismatch")
			return ConnectResult{
				Phase:              PhaseDisconnected,
				PendingFingerprint: mismatch.Got,
				MismatchExpected:   mismatch.Expected,
				Endpoint:           req.Endpoint,
				Message:            "server fingerprint does not match the recorded one",
			}, err
		}
	}

	if err != nil {
		a.setPhase(PhaseDisconnected, err.Error())
		return ConnectResult{Phase: PhaseDisconnected, Message: err.Error(), Endpoint: req.Endpoint}, err
	}

	// 到这里，TLS/明文连接都算成功。启动事件订阅。
	streamCtx := context.Background()
	if a.ctx != nil {
		streamCtx = a.ctx
	}
	stream, err := client.Subscribe(streamCtx, 256)
	if err != nil {
		_ = client.Close()
		a.setPhase(PhaseDisconnected, err.Error())
		return ConnectResult{Phase: PhaseDisconnected, Message: err.Error()}, err
	}

	a.mu.Lock()
	a.client = client
	a.stream = stream
	now := time.Now()
	fp := ""
	if verifier != nil {
		fp = verifier.LastSeen()
	}
	a.status = ConnectionStatus{
		Phase:       PhaseConnected,
		Endpoint:    req.Endpoint,
		UseTLS:      req.UseTLS,
		Fingerprint: fp,
		ConnectedAt: now,
	}
	snapshot := a.status
	a.mu.Unlock()

	// 持久化最近连接。Token 默认不落盘。
	if req.Remember {
		rec := config.RecentConnection{
			Label:       req.Label,
			Endpoint:    req.Endpoint,
			TLSEnabled:  req.UseTLS,
			ServerName:  req.ServerName,
			Fingerprint: fp,
		}
		_ = a.store.UpsertRecent(rec)
	}

	// 启动 goroutine 把事件转发给前端
	go a.pumpEvents(stream)

	a.emitStatus()

	return ConnectResult{
		Phase:    PhaseConnected,
		Endpoint: req.Endpoint,
		Status:   &snapshot,
	}, nil
}

// Disconnect 主动断开，不删除 TOFU 指纹。
func (a *API) Disconnect() error {
	a.mu.Lock()
	stream := a.stream
	client := a.client
	a.stream = nil
	a.client = nil
	a.status = ConnectionStatus{Phase: PhaseDisconnected}
	a.mu.Unlock()

	if stream != nil {
		stream.Close()
	}
	if client != nil {
		_ = client.Close()
	}
	a.emitStatus()
	return nil
}

// ForgetFingerprint 让 UI 能"忘记信任"。
func (a *API) ForgetFingerprint(endpoint string) error {
	return a.store.ForgetFingerprint(endpoint)
}

// ---------- 数据面 API ----------

// GetSnapshot 取当前拓扑、会话、sleep 一份快照（用于 UI 初始化/手动刷新）。
func (a *API) GetSnapshot() (Snapshot, error) {
	client, err := a.mustClient()
	if err != nil {
		return Snapshot{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), snapshotTimeout)
	defer cancel()

	raw, err := client.Snapshot(ctx)
	if err != nil {
		return Snapshot{}, err
	}
	profiles, _ := client.ListSleepProfiles(ctx)

	out := Snapshot{FetchedAt: time.Now()}
	if raw != nil {
		for _, n := range raw.GetNodes() {
			out.Nodes = append(out.Nodes, nodeFromPB(n))
		}
		for _, e := range raw.GetEdges() {
			out.Edges = append(out.Edges, EdgeSummary{
				ParentUUID:   e.GetParentUuid(),
				ChildUUID:    e.GetChildUuid(),
				Supplemental: e.GetSupplemental(),
			})
		}
		for _, s := range raw.GetStreams() {
			out.Streams = append(out.Streams, streamFromPB(s))
		}
		for _, ss := range raw.GetSessions() {
			out.Sessions = append(out.Sessions, sessionFromPB(ss))
		}
	}
	for _, p := range profiles {
		out.SleepProfiles = append(out.SleepProfiles, sleepFromPB(p))
	}
	return out, nil
}

// GetNodeDetail 返回某个节点的详情视图。
func (a *API) GetNodeDetail(uuid string) (NodeDetail, error) {
	client, err := a.mustClient()
	if err != nil {
		return NodeDetail{}, err
	}
	if strings.TrimSpace(uuid) == "" {
		return NodeDetail{}, errors.New("uuid required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), snapshotTimeout)
	defer cancel()

	resp, err := client.NodeStatus(ctx, uuid)
	if err != nil {
		return NodeDetail{}, err
	}
	detail := NodeDetail{FetchedAt: time.Now()}
	if node := resp.GetNode(); node != nil {
		detail.Node = nodeFromPB(node)
	} else {
		detail.Node.UUID = uuid
	}
	for _, s := range resp.GetStreams() {
		detail.Streams = append(detail.Streams, streamFromPB(s))
	}
	for _, l := range resp.GetPivotListeners() {
		detail.PivotListeners = append(detail.PivotListeners, PivotListenerDTO{
			ListenerID: l.GetListenerId(),
			Protocol:   l.GetProtocol(),
			Bind:       l.GetBind(),
			Status:     l.GetStatus(),
			Mode:       l.GetMode().String(),
		})
	}
	// sleep 配置单独查一次，避免全量 ListSleepProfiles。
	profiles, _ := client.ListSleepProfiles(ctx)
	for _, p := range profiles {
		if p.GetTargetUuid() == uuid {
			sp := sleepFromPB(p)
			detail.Sleep = &sp
			break
		}
	}
	return detail, nil
}

// GetMetrics 取聚合指标。
func (a *API) GetMetrics() (MetricsDTO, error) {
	client, err := a.mustClient()
	if err != nil {
		return MetricsDTO{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), snapshotTimeout)
	defer cancel()
	resp, err := client.Metrics(ctx)
	if err != nil {
		return MetricsDTO{}, err
	}
	suppStatus, _ := client.SupplementalStatus(ctx)
	suppMetrics, _ := client.SupplementalMetrics(ctx)

	out := MetricsDTO{CapturedAt: time.Now()}
	if dtn := resp.GetDtnMetrics(); dtn != nil {
		out.DTN.Enqueued = dtn.GetEnqueued()
		out.DTN.Delivered = dtn.GetDelivered()
		out.DTN.Failed = dtn.GetFailed()
		out.DTN.Retried = dtn.GetRetried()
		if g := dtn.GetGlobalQueue(); g != nil {
			out.DTN.Global.Total = g.GetTotal()
			out.DTN.Global.Ready = g.GetReady()
			out.DTN.Global.Held = g.GetHeld()
			out.DTN.Global.Capacity = g.GetCapacity()
			out.DTN.Global.HighWatermark = g.GetHighWatermark()
			out.DTN.Global.AverageWait = g.GetAverageWait()
			out.DTN.Global.DroppedTotal = g.GetDroppedTotal()
			out.DTN.Global.ExpiredTotal = g.GetExpiredTotal()
		}
	}
	if r := resp.GetReconnectMetrics(); r != nil {
		out.Reconnect.Attempts = r.GetAttempts()
		out.Reconnect.Success = r.GetSuccess()
		out.Reconnect.Failures = r.GetFailures()
		out.Reconnect.LastError = r.GetLastError()
	}
	if suppStatus != nil {
		out.Supplemental.Enabled = suppStatus.GetEnabled()
		out.Supplemental.QueueLength = suppStatus.GetQueueLength()
		out.Supplemental.PendingActions = suppStatus.GetPendingActions()
		out.Supplemental.ActiveLinks = suppStatus.GetActiveLinks()
	}
	if suppMetrics != nil {
		out.Supplemental.Dispatched = suppMetrics.GetDispatched()
		out.Supplemental.Success = suppMetrics.GetSuccess()
		out.Supplemental.Failures = suppMetrics.GetFailures()
		out.Supplemental.Dropped = suppMetrics.GetDropped()
		out.Supplemental.Recycled = suppMetrics.GetRecycled()
		out.Supplemental.QueueHigh = suppMetrics.GetQueueHigh()
		out.Supplemental.LastFailure = suppMetrics.GetLastFailure()
	}
	return out, nil
}

// ListSupplementalEvents 查最近 N 条补链事件。
func (a *API) ListSupplementalEvents(limit int32) ([]SupplementalEventDTO, error) {
	client, err := a.mustClient()
	if err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 80
	}
	ctx, cancel := context.WithTimeout(context.Background(), snapshotTimeout)
	defer cancel()
	evs, err := client.SupplementalEvents(ctx, limit)
	if err != nil {
		return nil, err
	}
	out := make([]SupplementalEventDTO, 0, len(evs))
	for _, e := range evs {
		out = append(out, SupplementalEventDTO{
			Seq:       e.GetSeq(),
			Kind:      e.GetKind(),
			Action:    e.GetAction(),
			Source:    e.GetSourceUuid(),
			Target:    e.GetTargetUuid(),
			Detail:    e.GetDetail(),
			Timestamp: e.GetTimestamp(),
		})
	}
	return out, nil
}

// EnqueueDTN 发一条 DTN payload。
func (a *API) EnqueueDTN(req EnqueueDTNRequest) (EnqueueDTNResult, error) {
	client, err := a.mustClient()
	if err != nil {
		return EnqueueDTNResult{}, err
	}
	prio := parsePriority(req.Priority)
	if req.TTLSeconds <= 0 {
		req.TTLSeconds = 600
	}
	ctx, cancel := context.WithTimeout(context.Background(), snapshotTimeout)
	defer cancel()
	id, err := client.EnqueueDTN(ctx, req.Target, req.Payload, prio, req.TTLSeconds)
	if err != nil {
		return EnqueueDTNResult{}, err
	}
	return EnqueueDTNResult{BundleID: id, EnqueuedAt: time.Now()}, nil
}

// UpdateSleep 调整 sleep 配置。
func (a *API) UpdateSleep(req UpdateSleepRequest) error {
	client, err := a.mustClient()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), snapshotTimeout)
	defer cancel()
	return client.UpdateSleep(ctx, req.Target, req.SleepSeconds, req.WorkSeconds, req.Jitter)
}

// PruneOffline 清理离线节点。
func (a *API) PruneOffline() (PruneOfflineResult, error) {
	client, err := a.mustClient()
	if err != nil {
		return PruneOfflineResult{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), snapshotTimeout)
	defer cancel()
	n, err := client.PruneOffline(ctx)
	if err != nil {
		return PruneOfflineResult{}, err
	}
	return PruneOfflineResult{Removed: n}, nil
}

// RecentEvents 供前端在首次进入时拉一段历史（ring buffer，最多 eventRingCap 条）。
func (a *API) RecentEvents(limit int) []kelpie.Event {
	a.ringMu.Lock()
	defer a.ringMu.Unlock()
	n := a.eventRingN
	if n > eventRingCap {
		n = eventRingCap
	}
	if limit > 0 && limit < n {
		n = limit
	}
	out := make([]kelpie.Event, 0, n)
	// 环形读取：从最旧的有效下标取到最新。
	start := 0
	if a.eventRingN > eventRingCap {
		start = a.eventRingN % eventRingCap
	}
	for i := 0; i < n; i++ {
		idx := (start + i) % eventRingCap
		out = append(out, a.eventRing[idx])
	}
	return out
}

// ---------- 内部辅助 ----------

func (a *API) mustClient() (*kelpie.Client, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.client == nil {
		return nil, errors.New("not connected to any Kelpie server")
	}
	return a.client, nil
}

func (a *API) setPhase(p ConnectionPhase, errMsg string) {
	a.mu.Lock()
	a.status.Phase = p
	a.status.Error = errMsg
	a.mu.Unlock()
	a.emitStatus()
}

func (a *API) emitStatus() {
	if a.ctx == nil {
		return
	}
	a.mu.Lock()
	snapshot := a.status
	a.mu.Unlock()
	wruntime.EventsEmit(a.ctx, statusTopic, snapshot)
}

func (a *API) pumpEvents(stream *kelpie.EventStream) {
	for ev := range stream.Out() {
		a.pushEventRing(ev)
		if a.ctx != nil {
			wruntime.EventsEmit(a.ctx, eventTopic, ev)
		}
	}
	// 流结束，若是异常，则向前端报告连接掉线。
	if err := stream.Err(); err != nil {
		a.setPhase(PhaseDisconnected, fmt.Sprintf("event stream closed: %v", err))
		a.mu.Lock()
		client := a.client
		a.client = nil
		a.stream = nil
		a.mu.Unlock()
		if client != nil {
			_ = client.Close()
		}
	}
}

func (a *API) pushEventRing(ev kelpie.Event) {
	a.ringMu.Lock()
	defer a.ringMu.Unlock()
	idx := a.eventRingN % eventRingCap
	a.eventRing[idx] = ev
	a.eventRingN++
}

func nodeFromPB(n *uipb.NodeInfo) NodeSummary {
	if n == nil {
		return NodeSummary{}
	}
	return NodeSummary{
		UUID:          n.GetUuid(),
		Alias:         n.GetAlias(),
		ParentUUID:    n.GetParentUuid(),
		Status:        n.GetStatus(),
		Network:       n.GetNetwork(),
		Sleep:         n.GetSleep(),
		Memo:          n.GetMemo(),
		Depth:         n.GetDepth(),
		Tags:          append([]string(nil), n.GetTags()...),
		ActiveStreams: n.GetActiveStreams(),
		WorkProfile:   n.GetWorkProfile(),
	}
}

func streamFromPB(s *uipb.StreamDiag) StreamDiagDTO {
	if s == nil {
		return StreamDiagDTO{}
	}
	return StreamDiagDTO{
		StreamID:     s.GetStreamId(),
		TargetUUID:   s.GetTargetUuid(),
		Kind:         s.GetKind(),
		Outbound:     s.GetOutbound(),
		Pending:      s.GetPending(),
		InFlight:     s.GetInflight(),
		Window:       s.GetWindow(),
		Seq:          s.GetSeq(),
		Ack:          s.GetAck(),
		Rto:          s.GetRto(),
		LastActivity: s.GetLastActivity(),
		SessionID:    s.GetSessionId(),
	}
}

func sessionFromPB(s *uipb.SessionInfo) SessionSummary {
	if s == nil {
		return SessionSummary{}
	}
	return SessionSummary{
		TargetUUID:   s.GetTargetUuid(),
		Status:       s.GetStatus().String(),
		Active:       s.GetActive(),
		Connected:    s.GetConnected(),
		RemoteAddr:   s.GetRemoteAddr(),
		Upstream:     s.GetUpstream(),
		Downstream:   s.GetDownstream(),
		NetworkID:    s.GetNetworkId(),
		LastSeen:     s.GetLastSeen(),
		LastError:    s.GetLastError(),
		SleepSeconds: s.SleepSeconds,
		WorkSeconds:  s.WorkSeconds,
		Jitter:       s.JitterPercent,
	}
}

func sleepFromPB(p *uipb.SleepProfile) SleepProfileDTO {
	if p == nil {
		return SleepProfileDTO{}
	}
	return SleepProfileDTO{
		TargetUUID:   p.GetTargetUuid(),
		SleepSeconds: p.SleepSeconds,
		WorkSeconds:  p.WorkSeconds,
		Jitter:       p.JitterPercent,
		Profile:      p.GetProfile(),
		LastUpdated:  p.GetLastUpdated(),
		NextWakeAt:   p.GetNextWakeAt(),
		Status:       p.GetStatus(),
	}
}

func parsePriority(s string) uipb.DtnPriority {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "low":
		return uipb.DtnPriority_DTN_PRIORITY_LOW
	case "high":
		return uipb.DtnPriority_DTN_PRIORITY_HIGH
	case "", "normal":
		return uipb.DtnPriority_DTN_PRIORITY_NORMAL
	}
	return uipb.DtnPriority_DTN_PRIORITY_NORMAL
}
