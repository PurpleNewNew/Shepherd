package grpcserver

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"codeberg.org/agnoie/shepherd/internal/dataplanepb"
	"codeberg.org/agnoie/shepherd/internal/kelpie/collab"
	"codeberg.org/agnoie/shepherd/internal/kelpie/dataplane"
	"codeberg.org/agnoie/shepherd/internal/kelpie/dtn"
	"codeberg.org/agnoie/shepherd/internal/kelpie/printer"
	"codeberg.org/agnoie/shepherd/internal/kelpie/process"
	"codeberg.org/agnoie/shepherd/internal/kelpie/storage/sqlite"
	"codeberg.org/agnoie/shepherd/internal/kelpie/stream"
	"codeberg.org/agnoie/shepherd/internal/kelpie/supp"
	"codeberg.org/agnoie/shepherd/internal/kelpie/topology"
	"codeberg.org/agnoie/shepherd/internal/kelpie/uipb"
	"codeberg.org/agnoie/shepherd/pkg/bus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// Server 封装了一个向 GUI 客户端暴露 Kelpie 状态的 gRPC 服务端。
type Server struct {
	grpcServer *grpc.Server
	service    *service
}

// New 使用给定的 admin 实例创建一个 UI gRPC 服务端。
func New(admin *process.Admin, auditor *collab.Recorder, chatSvc *collab.Service, dpManager *dataplane.Manager, bootstrapToken string, opts ...grpc.ServerOption) *Server {
	svc := &service{
		admin:          admin,
		auditor:        auditor,
		chatService:    chatSvc,
		dataplane:      dpManager,
		bootstrapToken: strings.TrimSpace(bootstrapToken),
		subscribers:    make(map[uint64]chan *uipb.UiEvent),
	}
	grpcSrv := grpc.NewServer(opts...)
	uipb.RegisterKelpieUIServiceServer(grpcSrv, svc)
	uipb.RegisterPivotListenerAdminServiceServer(grpcSrv, svc)
	uipb.RegisterControllerListenerAdminServiceServer(grpcSrv, svc)
	uipb.RegisterProxyAdminServiceServer(grpcSrv, svc)
	uipb.RegisterSleepAdminServiceServer(grpcSrv, svc)
	uipb.RegisterSupplementalAdminServiceServer(grpcSrv, svc)
	uipb.RegisterConnectAdminServiceServer(grpcSrv, svc)
	dataplanepb.RegisterDataplaneAdminServer(grpcSrv, svc)
	svc.logHookCancel = printer.RegisterHook(svc.handlePrinterHook)
	svc.registerSupplementalHooks()
	if auditor != nil {
		auditor.RegisterSink(svc.handleAuditRecord)
	}
	if chatSvc != nil {
		chatSvc.RegisterSink(svc.handleChatRecord)
	}
	return &Server{grpcServer: grpcSrv, service: svc}
}

// Serve 在给定 listener 上开始提供服务。
func (s *Server) Serve(lis net.Listener) error {
	if s == nil || s.grpcServer == nil {
		return fmt.Errorf("grpc server not initialized")
	}
	return s.grpcServer.Serve(lis)
}

// Stop 会优雅地停止 gRPC 服务端。
func (s *Server) Stop() {
	if s == nil {
		return
	}
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}
	if s.service != nil {
		s.service.close()
	}
}

type service struct {
	dataplanepb.UnimplementedDataplaneAdminServer
	uipb.UnimplementedKelpieUIServiceServer
	uipb.UnimplementedPivotListenerAdminServiceServer
	uipb.UnimplementedProxyAdminServiceServer
	uipb.UnimplementedSleepAdminServiceServer
	uipb.UnimplementedSupplementalAdminServiceServer
	uipb.UnimplementedConnectAdminServiceServer
	uipb.UnimplementedControllerListenerAdminServiceServer
	admin          *process.Admin
	subMu          sync.Mutex
	subscribers    map[uint64]chan *uipb.UiEvent
	nextSubID      atomic.Uint64
	logSeq         atomic.Uint64
	logHookCancel  func()
	suppHookCancel []func()
	dropCount      atomic.Uint64
	auditor        *collab.Recorder
	chatService    *collab.Service
	dataplane      *dataplane.Manager
	bootstrapToken string
	// 测试钩子
	startForwardProxyOverride  func(context.Context, string, string, string) (*process.ProxyDescriptor, error)
	stopForwardProxyOverride   func(string, string) ([]*process.ProxyDescriptor, error)
	startBackwardProxyOverride func(context.Context, string, string, string) (*process.ProxyDescriptor, error)
	stopBackwardProxyOverride  func(string, string) ([]*process.ProxyDescriptor, error)
	updateSleepOverride        func(string, process.SleepUpdateParams) error
	connectNodeOverride        func(context.Context, string, string) error
	pruneOfflineOverride       func() (int, error)
	topologySnapshotOverride   func(target, network string) topology.UISnapshot
	routerStatsOverride        func() map[uint16]bus.RouterCounter
	reconnectStatsOverride     func() process.ReconnectStatsView
	queueStatsOverride         func(target string) (dtn.QueueStats, error)
	listBundlesOverride        func(target string, limit int) ([]dtn.BundleSummary, error)
	enqueueDtnOverride         func(target, data string, priority dtn.Priority, ttl time.Duration) (string, error)
	dtnPolicyOverride          func() map[string]string
	setDtnPolicyOverride       func(key, value string) error
	dtnMetricsOverride         func() (dtn.QueueStats, time.Time)
	dtnStatsOverride           func() (uint64, uint64, uint64, uint64)
	sessionsOverride           func(process.SessionFilter) []process.SessionInfo
	terminateSessionOverride   func(target, reason string) (bool, error)
	closeStreamOverride        func(uint32, string) error
	dialMu                     sync.Mutex
	dials                      map[string]*dialTracker
	suppEventSeq               atomic.Uint64
	listProxiesOverride        func() []*process.ProxyDescriptor
	listSleepProfilesOverride  func() []process.SessionInfo
	listRepairsOverride        func() []process.RepairStatusSnapshot
}

type dialTracker struct {
	id       string
	target   string
	address  string
	reason   string
	operator string
	state    uipb.DialState
	err      string
	started  time.Time
	updated  time.Time
	ctx      context.Context
	cancel   context.CancelFunc
}

func (t *dialTracker) snapshot() *dialTracker {
	if t == nil {
		return nil
	}
	clone := *t
	return &clone
}

func (s *service) snapshotView(target, network string) topology.UISnapshot {
	if s != nil && s.topologySnapshotOverride != nil {
		return s.topologySnapshotOverride(target, network)
	}
	if s == nil || s.admin == nil {
		return topology.UISnapshot{}
	}
	return s.admin.TopologySnapshot(target, network)
}

func (s *service) pruneOfflineCount() (int, error) {
	if s == nil {
		return 0, fmt.Errorf("service unavailable")
	}
	if s.pruneOfflineOverride != nil {
		return s.pruneOfflineOverride()
	}
	if s.admin == nil {
		return 0, fmt.Errorf("admin unavailable")
	}
	return s.admin.PruneOffline()
}

func (s *service) routerStats() map[uint16]bus.RouterCounter {
	if s == nil {
		return nil
	}
	if s.routerStatsOverride != nil {
		return s.routerStatsOverride()
	}
	if s.admin == nil {
		return nil
	}
	return s.admin.RouterStats()
}

func (s *service) reconnectStats() process.ReconnectStatsView {
	if s == nil {
		return process.ReconnectStatsView{}
	}
	if s.reconnectStatsOverride != nil {
		return s.reconnectStatsOverride()
	}
	if s.admin == nil {
		return process.ReconnectStatsView{}
	}
	return s.admin.ReconnectStatsView()
}

func (s *service) queueStats(target string) (dtn.QueueStats, error) {
	if s == nil {
		return dtn.QueueStats{}, fmt.Errorf("service unavailable")
	}
	if s.queueStatsOverride != nil {
		return s.queueStatsOverride(target)
	}
	if s.admin == nil {
		return dtn.QueueStats{}, fmt.Errorf("admin unavailable")
	}
	return s.admin.QueueStats(target)
}

func (s *service) listBundles(target string, limit int) ([]dtn.BundleSummary, error) {
	if s == nil {
		return nil, fmt.Errorf("service unavailable")
	}
	if s.listBundlesOverride != nil {
		return s.listBundlesOverride(target, limit)
	}
	if s.admin == nil {
		return nil, fmt.Errorf("admin unavailable")
	}
	return s.admin.ListBundles(target, limit)
}

func (s *service) enqueueDtnPayload(target, payload string, priority dtn.Priority, ttl time.Duration) (string, error) {
	if s == nil {
		return "", fmt.Errorf("service unavailable")
	}
	if s.enqueueDtnOverride != nil {
		return s.enqueueDtnOverride(target, payload, priority, ttl)
	}
	if s.admin == nil {
		return "", fmt.Errorf("admin unavailable")
	}
	return s.admin.EnqueueDiagnostic(target, payload, priority, ttl)
}

func (s *service) dtnPolicy() map[string]string {
	if s == nil {
		return nil
	}
	if s.dtnPolicyOverride != nil {
		return s.dtnPolicyOverride()
	}
	if s.admin == nil {
		return nil
	}
	return s.admin.DTNPolicy()
}

func (s *service) setDtnPolicy(key, value string) error {
	if s == nil {
		return fmt.Errorf("service unavailable")
	}
	if s.setDtnPolicyOverride != nil {
		return s.setDtnPolicyOverride(key, value)
	}
	if s.admin == nil {
		return fmt.Errorf("admin unavailable")
	}
	return s.admin.SetDTNPolicy(key, value)
}

func (s *service) dtnMetricsSnapshot() (dtn.QueueStats, time.Time) {
	if s == nil {
		return dtn.QueueStats{}, time.Time{}
	}
	if s.dtnMetricsOverride != nil {
		return s.dtnMetricsOverride()
	}
	if s.admin == nil {
		return dtn.QueueStats{}, time.Time{}
	}
	return s.admin.DTNMetricsSnapshot()
}

func (s *service) dtnStats() (uint64, uint64, uint64, uint64) {
	if s == nil {
		return 0, 0, 0, 0
	}
	if s.dtnStatsOverride != nil {
		return s.dtnStatsOverride()
	}
	if s.admin == nil {
		return 0, 0, 0, 0
	}
	return s.admin.DTNStats()
}

func (s *service) SendChatMessage(ctx context.Context, req *uipb.SendChatMessageRequest) (*uipb.SendChatMessageResponse, error) {
	if s == nil || s.chatService == nil {
		return nil, status.Error(codes.Unavailable, "chat unavailable")
	}
	operator := s.currentOperator(ctx)
	if operator == "" {
		return nil, status.Error(codes.Unauthenticated, "unauthenticated")
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "missing request")
	}
	rec, err := s.chatService.Append(operator, s.operatorRole(ctx), req.GetMessage())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &uipb.SendChatMessageResponse{Message: convertChatRecord(rec)}, nil
}

func (s *service) ListChatMessages(ctx context.Context, req *uipb.ListChatMessagesRequest) (*uipb.ListChatMessagesResponse, error) {
	if s == nil || s.chatService == nil {
		return nil, status.Error(codes.Unavailable, "chat unavailable")
	}
	limit := int(req.GetLimit())
	before := time.Time{}
	if ts := strings.TrimSpace(req.GetBeforeId()); ts != "" {
		if parsed, err := time.Parse(time.RFC3339Nano, ts); err == nil {
			before = parsed
		}
	}
	recs, err := s.chatService.List(limit, before)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	resp := &uipb.ListChatMessagesResponse{Messages: make([]*uipb.ChatMessage, 0, len(recs))}
	for _, rec := range recs {
		resp.Messages = append(resp.Messages, convertChatRecord(rec))
	}
	return resp, nil
}

func (s *service) ListAuditLogs(ctx context.Context, req *uipb.ListAuditLogsRequest) (*uipb.ListAuditLogsResponse, error) {
	if s == nil || s.auditor == nil {
		return nil, status.Error(codes.Unavailable, "audit unavailable")
	}
	filter := collab.Filter{}
	// 默认按当前租户隔离视图，实现按人多租。
	if tenant := s.currentTenant(ctx); tenant != "" {
		filter.Tenant = tenant
	}
	if req != nil {
		filter.Username = strings.TrimSpace(req.GetUsername())
		filter.Method = strings.TrimSpace(req.GetMethod())
		if from := strings.TrimSpace(req.GetFromTime()); from != "" {
			if parsed, err := time.Parse(time.RFC3339Nano, from); err == nil {
				filter.From = parsed
			}
		}
		if to := strings.TrimSpace(req.GetToTime()); to != "" {
			if parsed, err := time.Parse(time.RFC3339Nano, to); err == nil {
				filter.To = parsed
			}
		}
		filter.Limit = int(req.GetLimit())
	}
	recs, err := s.auditor.List(filter)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	resp := &uipb.ListAuditLogsResponse{Entries: make([]*uipb.AuditLogEntry, 0, len(recs))}
	for _, rec := range recs {
		resp.Entries = append(resp.Entries, convertAuditRecord(rec))
	}
	return resp, nil
}

func (s *service) UpdateNodeMemo(ctx context.Context, req *uipb.UpdateNodeMemoRequest) (*uipb.UpdateNodeMemoResponse, error) {
	if s == nil || s.admin == nil {
		return nil, status.Error(codes.Unavailable, "admin unavailable")
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "missing request")
	}
	target := strings.TrimSpace(req.GetTargetUuid())
	if target == "" {
		return nil, status.Error(codes.InvalidArgument, "missing target uuid")
	}
	if err := s.admin.UpdateNodeMemo(target, req.GetMemo()); err != nil {
		return &uipb.UpdateNodeMemoResponse{}, status.Errorf(codes.Internal, "update memo failed: %v", err)
	}
	return &uipb.UpdateNodeMemoResponse{}, nil
}

func (s *service) UpdateSleep(ctx context.Context, req *uipb.UpdateSleepRequest) (*uipb.UpdateSleepResponse, error) {
	if s == nil || (s.admin == nil && s.updateSleepOverride == nil) {
		return nil, status.Error(codes.Unavailable, "admin unavailable")
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "missing request")
	}
	target := strings.TrimSpace(req.GetTargetUuid())
	if target == "" {
		return nil, status.Error(codes.InvalidArgument, "missing target uuid")
	}
	params := process.SleepUpdateParams{}
	if req.SleepSeconds != nil {
		val := int(req.GetSleepSeconds())
		params.SleepSeconds = &val
	}
	if req.WorkSeconds != nil {
		val := int(req.GetWorkSeconds())
		params.WorkSeconds = &val
	}
	if req.JitterPercent != nil {
		val := req.GetJitterPercent()
		params.JitterPercent = &val
	}
	if params.SleepSeconds == nil && params.WorkSeconds == nil && params.JitterPercent == nil {
		return nil, status.Error(codes.InvalidArgument, "at least one sleep field required")
	}
	var err error
	if s.updateSleepOverride != nil {
		err = s.updateSleepOverride(target, params)
	} else {
		err = s.admin.UpdateSleep(target, params)
	}
	if err != nil {
		return nil, mapSleepUpdateError(err)
	}
	s.broadcastSleepEvent(uipb.SleepEvent_SLEEP_EVENT_UPDATED, target, params, "", s.currentOperator(ctx))
	return &uipb.UpdateSleepResponse{}, nil
}

func (s *service) ListSleepProfiles(ctx context.Context, _ *uipb.ListSleepProfilesRequest) (*uipb.ListSleepProfilesResponse, error) {
	if s == nil || (s.admin == nil && s.listSleepProfilesOverride == nil && s.sessionsOverride == nil) {
		return nil, status.Error(codes.Unavailable, "admin unavailable")
	}
	filter := process.SessionFilter{IncludeInactive: true}
	var sessions []process.SessionInfo
	switch {
	case s.listSleepProfilesOverride != nil:
		sessions = s.listSleepProfilesOverride()
	case s.sessionsOverride != nil:
		sessions = s.sessionsOverride(filter)
	default:
		sessions = s.admin.Sessions(filter)
	}
	profiles := make([]*uipb.SleepProfile, 0, len(sessions))
	for _, sess := range sessions {
		if sess.SleepSeconds == nil && sess.WorkSeconds == nil && sess.JitterPercent == nil && strings.TrimSpace(sess.SleepProfile) == "" {
			continue
		}
		if profile := convertSleepProfile(sess); profile != nil {
			profiles = append(profiles, profile)
		}
	}
	sort.Slice(profiles, func(i, j int) bool {
		return profiles[i].GetTargetUuid() < profiles[j].GetTargetUuid()
	})
	return &uipb.ListSleepProfilesResponse{Profiles: profiles}, nil
}

func (s *service) StartSshTunnel(ctx context.Context, req *uipb.StartSshTunnelRequest) (*uipb.StartSshTunnelResponse, error) {
	if s == nil || s.admin == nil {
		return nil, status.Error(codes.Unavailable, "admin unavailable")
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "missing request")
	}
	method := req.GetAuthMethod()
	if method == uipb.SshTunnelAuthMethod_SSH_TUNNEL_AUTH_METHOD_UNSPECIFIED {
		method = uipb.SshTunnelAuthMethod_SSH_TUNNEL_AUTH_METHOD_PASSWORD
	}
	if err := s.admin.StartSSHTunnel(ctx, req.GetTargetUuid(), req.GetServerAddr(), req.GetAgentPort(), method, req.GetUsername(), req.GetPassword(), req.GetPrivateKey()); err != nil {
		return nil, mapSshTunnelError(err)
	}
	return &uipb.StartSshTunnelResponse{}, nil
}

func (s *service) ShutdownNode(ctx context.Context, req *uipb.ShutdownNodeRequest) (*uipb.ShutdownNodeResponse, error) {
	if s == nil || s.admin == nil {
		return nil, status.Error(codes.Unavailable, "admin unavailable")
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "missing request")
	}
	target := strings.TrimSpace(req.GetTargetUuid())
	if target == "" {
		return nil, status.Error(codes.InvalidArgument, "missing target uuid")
	}
	if err := s.admin.ShutdownNode(target); err != nil {
		return nil, mapShutdownError(err)
	}
	return &uipb.ShutdownNodeResponse{}, nil
}

func (s *service) NodeStatus(ctx context.Context, req *uipb.NodeStatusRequest) (*uipb.NodeStatusResponse, error) {
	if s == nil || s.admin == nil {
		return nil, status.Error(codes.Unavailable, "admin unavailable")
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "missing request")
	}
	target := strings.TrimSpace(req.GetTargetUuid())
	if target == "" {
		return nil, status.Error(codes.InvalidArgument, "missing target uuid")
	}
	snapshot := s.admin.TopologySnapshot("", "")
	var nodeInfo *uipb.NodeInfo
	for _, node := range snapshot.Nodes {
		if strings.EqualFold(node.UUID, target) {
			nodeInfo = convertNodeInfo(node)
			break
		}
	}
	if nodeInfo == nil {
		return nil, status.Error(codes.NotFound, "node not found")
	}
	listeners := s.admin.ListListeners(process.ListenerFilter{Targets: []string{target}})
	streamDiags := s.admin.StreamDiagnostics()
	resp := &uipb.NodeStatusResponse{Node: nodeInfo}
	if len(listeners) > 0 {
		resp.PivotListeners = convertPivotListeners(listeners)
	}
	if len(streamDiags) > 0 {
		resp.Streams = make([]*uipb.StreamDiag, 0, len(streamDiags))
		for _, diag := range streamDiags {
			if strings.EqualFold(diag.Target, target) {
				resp.Streams = append(resp.Streams, convertStreamDiag(diag))
			}
		}
	}
	return resp, nil
}

func (s *service) ListNetworks(ctx context.Context, _ *uipb.ListNetworksRequest) (*uipb.ListNetworksResponse, error) {
	if s == nil || s.admin == nil {
		return nil, status.Error(codes.Unavailable, "admin unavailable")
	}
	infos, active, err := s.admin.ListNetworks()
	if err != nil {
		return nil, mapSupplementalError(err)
	}
	return &uipb.ListNetworksResponse{
		Networks:        convertNetworkInfos(infos, active),
		ActiveNetworkId: active,
	}, nil
}

func (s *service) UseNetwork(ctx context.Context, req *uipb.UseNetworkRequest) (*uipb.UseNetworkResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "missing request")
	}
	if s == nil || s.admin == nil {
		return nil, status.Error(codes.Unavailable, "admin unavailable")
	}
	id := strings.TrimSpace(req.GetNetworkId())
	if id == "" {
		return nil, status.Error(codes.InvalidArgument, "network id required")
	}
	active, err := s.admin.SetActiveNetwork(id)
	if err != nil {
		return nil, mapSupplementalError(err)
	}
	return &uipb.UseNetworkResponse{ActiveNetworkId: active}, nil
}

func (s *service) ResetNetwork(ctx context.Context, _ *uipb.ResetNetworkRequest) (*uipb.ResetNetworkResponse, error) {
	if s == nil || s.admin == nil {
		return nil, status.Error(codes.Unavailable, "admin unavailable")
	}
	active := s.admin.ResetActiveNetwork()
	return &uipb.ResetNetworkResponse{ActiveNetworkId: active}, nil
}

func (s *service) SetNodeNetwork(ctx context.Context, req *uipb.SetNodeNetworkRequest) (*uipb.SetNodeNetworkResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "missing request")
	}
	if s == nil || s.admin == nil {
		return nil, status.Error(codes.Unavailable, "admin unavailable")
	}
	target := strings.TrimSpace(req.GetTargetUuid())
	network := strings.TrimSpace(req.GetNetworkId())
	if target == "" || network == "" {
		return nil, status.Error(codes.InvalidArgument, "target and network required")
	}
	if err := s.admin.SetNodeNetwork(target, network); err != nil {
		return nil, mapSupplementalError(err)
	}
	return &uipb.SetNodeNetworkResponse{}, nil
}

func (s *service) PruneOffline(ctx context.Context, _ *uipb.PruneOfflineRequest) (*uipb.PruneOfflineResponse, error) {
	if s == nil {
		return nil, status.Error(codes.Unavailable, "admin unavailable")
	}
	if s.admin == nil && s.pruneOfflineOverride == nil {
		return nil, status.Error(codes.Unavailable, "admin unavailable")
	}
	count, err := s.pruneOfflineCount()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "prune failed: %v", err)
	}
	return &uipb.PruneOfflineResponse{Removed: int32(count)}, nil
}

func (s *service) GetSupplementalStatus(ctx context.Context, _ *uipb.SupplementalEmpty) (*uipb.SupplementalStatus, error) {
	if s == nil || s.admin == nil {
		return nil, status.Error(codes.Unavailable, "admin unavailable")
	}
	statusSnap, err := s.admin.SupplementalStatus()
	if err != nil {
		return nil, mapSupplementalError(err)
	}
	return convertSupplementalStatus(statusSnap), nil
}

func (s *service) GetSupplementalMetrics(ctx context.Context, _ *uipb.SupplementalEmpty) (*uipb.SupplementalMetrics, error) {
	if s == nil || s.admin == nil {
		return nil, status.Error(codes.Unavailable, "admin unavailable")
	}
	snapshot, err := s.admin.SupplementalMetrics()
	if err != nil {
		return nil, mapSupplementalError(err)
	}
	return convertSupplementalMetrics(snapshot), nil
}

func (s *service) ListSupplementalEvents(ctx context.Context, req *uipb.ListSupplementalEventsRequest) (*uipb.ListSupplementalEventsResponse, error) {
	if s == nil || s.admin == nil {
		return nil, status.Error(codes.Unavailable, "admin unavailable")
	}
	limit := int(req.GetLimit())
	if limit <= 0 {
		limit = 50
	}
	events, err := s.admin.SupplementalEvents(limit)
	if err != nil {
		return nil, mapSupplementalError(err)
	}
	return &uipb.ListSupplementalEventsResponse{Events: convertSupplementalEvents(events)}, nil
}

func (s *service) ListSupplementalQuality(ctx context.Context, req *uipb.ListSupplementalQualityRequest) (*uipb.ListSupplementalQualityResponse, error) {
	if s == nil || s.admin == nil {
		return nil, status.Error(codes.Unavailable, "admin unavailable")
	}
	limit := int(req.GetLimit())
	qualities, err := s.admin.SupplementalQuality(limit, req.GetNodeUuids())
	if err != nil {
		return nil, mapSupplementalError(err)
	}
	return &uipb.ListSupplementalQualityResponse{Qualities: convertSupplementalQuality(qualities)}, nil
}

func (s *service) GetMetrics(ctx context.Context, req *uipb.GetMetricsRequest) (*uipb.GetMetricsResponse, error) {
	if s == nil || s.admin == nil {
		return nil, status.Error(codes.Unavailable, "admin unavailable")
	}
	includeRouter := true
	includeReconnect := true
	includeDTN := true
	if req != nil {
		includeRouter = req.GetIncludeRouter()
		includeReconnect = req.GetIncludeReconnect()
		includeDTN = true
		if !includeRouter && !includeReconnect {
			includeRouter = true
			includeReconnect = true
		}
	}
	resp := &uipb.GetMetricsResponse{}
	if includeRouter {
		resp.RouterMetrics = convertRouterMetrics(s.routerStats())
	}
	if includeReconnect {
		resp.ReconnectMetrics = convertReconnectMetrics(s.reconnectStats())
	}
	if includeDTN {
		stats, captured := s.dtnMetricsSnapshot()
		enq, del, fail, retry := s.dtnStats()
		resp.DtnMetrics = convertDtnMetrics(stats, captured, enq, del, fail, retry)
	}
	return resp, nil
}

func (s *service) GetDtnQueueStats(ctx context.Context, req *uipb.GetDtnQueueStatsRequest) (*uipb.GetDtnQueueStatsResponse, error) {
	if s == nil || s.admin == nil {
		return nil, status.Error(codes.Unavailable, "admin unavailable")
	}
	target := ""
	if req != nil {
		target = strings.TrimSpace(req.GetTargetUuid())
	}
	stats, err := s.queueStats(target)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "queue stats failed: %v", err)
	}
	return &uipb.GetDtnQueueStatsResponse{Stats: convertDtnQueueStats(stats)}, nil
}

func (s *service) ListDtnBundles(ctx context.Context, req *uipb.ListDtnBundlesRequest) (*uipb.ListDtnBundlesResponse, error) {
	if s == nil || s.admin == nil {
		return nil, status.Error(codes.Unavailable, "admin unavailable")
	}
	target := ""
	if req != nil {
		target = strings.TrimSpace(req.GetTargetUuid())
	}
	limit := 10
	if req != nil && req.GetLimit() > 0 {
		limit = int(req.GetLimit())
	}
	summaries, err := s.listBundles(target, limit)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list bundles failed: %v", err)
	}
	resp := &uipb.ListDtnBundlesResponse{Bundles: make([]*uipb.DtnBundle, 0, len(summaries))}
	for _, summary := range summaries {
		resp.Bundles = append(resp.Bundles, convertDtnBundle(summary))
	}
	return resp, nil
}

func (s *service) EnqueueDtnPayload(ctx context.Context, req *uipb.EnqueueDtnPayloadRequest) (*uipb.EnqueueDtnPayloadResponse, error) {
	if s == nil || s.admin == nil {
		return nil, status.Error(codes.Unavailable, "admin unavailable")
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "missing request")
	}
	target := strings.TrimSpace(req.GetTargetUuid())
	if target == "" {
		return nil, status.Error(codes.InvalidArgument, "target uuid required")
	}
	payload := req.GetPayload()
	if strings.TrimSpace(payload) == "" {
		return nil, status.Error(codes.InvalidArgument, "payload required")
	}
	priority := mapProtoDtnPriority(req.GetPriority())
	ttl := time.Duration(req.GetTtlSeconds()) * time.Second
	id, err := s.enqueueDtnPayload(target, payload, priority, ttl)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "enqueue failed: %v", err)
	}
	return &uipb.EnqueueDtnPayloadResponse{BundleId: id}, nil
}

func (s *service) GetDtnPolicy(ctx context.Context, _ *uipb.GetDtnPolicyRequest) (*uipb.GetDtnPolicyResponse, error) {
	if s == nil || s.admin == nil {
		return nil, status.Error(codes.Unavailable, "admin unavailable")
	}
	entries := s.dtnPolicy()
	return &uipb.GetDtnPolicyResponse{Entries: entries}, nil
}

func (s *service) UpdateDtnPolicy(ctx context.Context, req *uipb.UpdateDtnPolicyRequest) (*uipb.UpdateDtnPolicyResponse, error) {
	if s == nil || s.admin == nil {
		return nil, status.Error(codes.Unavailable, "admin unavailable")
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "missing request")
	}
	key := strings.TrimSpace(req.GetKey())
	value := strings.TrimSpace(req.GetValue())
	if key == "" {
		return nil, status.Error(codes.InvalidArgument, "policy key required")
	}
	if value == "" {
		return nil, status.Error(codes.InvalidArgument, "policy value required")
	}
	if err := s.setDtnPolicy(key, value); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "update failed: %v", err)
	}
	return &uipb.UpdateDtnPolicyResponse{Entries: s.dtnPolicy()}, nil
}

func (s *service) GetRoutingStrategy(ctx context.Context, _ *uipb.GetRoutingStrategyRequest) (*uipb.GetRoutingStrategyResponse, error) {
	if s == nil || s.admin == nil {
		return nil, status.Error(codes.Unavailable, "admin unavailable")
	}
	strategy := mapRoutingStrategyToProto(s.admin.RoutingStrategy())
	return &uipb.GetRoutingStrategyResponse{Strategy: strategy}, nil
}

func (s *service) SetRoutingStrategy(ctx context.Context, req *uipb.SetRoutingStrategyRequest) (*uipb.SetRoutingStrategyResponse, error) {
	if s == nil || s.admin == nil {
		return nil, status.Error(codes.Unavailable, "admin unavailable")
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "missing request")
	}
	strategy, err := mapProtoRoutingStrategy(req.GetStrategy())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := s.admin.SetRoutingStrategy(strategy); err != nil {
		return nil, status.Errorf(codes.Internal, "routing update failed: %v", err)
	}
	return &uipb.SetRoutingStrategyResponse{Strategy: mapRoutingStrategyToProto(strategy)}, nil
}

func (s *service) registerSubscriber() (uint64, chan *uipb.UiEvent) {
	id := s.nextSubID.Add(1)
	ch := make(chan *uipb.UiEvent, 256)
	s.subMu.Lock()
	if s.subscribers == nil {
		s.subscribers = make(map[uint64]chan *uipb.UiEvent)
	}
	s.subscribers[id] = ch
	s.subMu.Unlock()
	return id, ch
}

func (s *service) unregisterSubscriber(id uint64) {
	s.subMu.Lock()
	ch, ok := s.subscribers[id]
	if ok {
		delete(s.subscribers, id)
	}
	s.subMu.Unlock()
	if ok {
		close(ch)
	}
}

func (s *service) broadcast(event *uipb.UiEvent) {
	if event == nil {
		return
	}
	s.subMu.Lock()
	defer s.subMu.Unlock()
	for _, ch := range s.subscribers {
		select {
		case ch <- event:
		default:
			dropped := s.dropCount.Add(1)
			if dropped%100 == 1 {
				printer.Warning("[ui-grpc] dropped %d events to slow subscriber\n", dropped)
			}
		}
	}
}

func (s *service) broadcastSleepEvent(kind uipb.SleepEvent_Kind, target string, params process.SleepUpdateParams, reason, operator string) {
	if target == "" {
		return
	}
	evt := &uipb.SleepEvent{
		Kind:       kind,
		TargetUuid: target,
		Reason:     reason,
		Operator:   operator,
	}
	if params.SleepSeconds != nil {
		val := int32(*params.SleepSeconds)
		evt.SleepSeconds = &val
	}
	if params.WorkSeconds != nil {
		val := int32(*params.WorkSeconds)
		evt.WorkSeconds = &val
	}
	if params.JitterPercent != nil {
		val := *params.JitterPercent
		evt.JitterPercent = &val
	}
	s.broadcast(&uipb.UiEvent{
		Payload: &uipb.UiEvent_SleepEvent{
			SleepEvent: evt,
		},
	})
}

func (s *service) broadcastSupplementalEvent(kind, action, source, target, detail string) {
	if s == nil {
		return
	}
	seq := s.suppEventSeq.Add(1)
	evt := &uipb.SupplementalEvent{
		Seq:        seq,
		Kind:       kind,
		Action:     action,
		SourceUuid: source,
		TargetUuid: target,
		Detail:     detail,
		Timestamp:  time.Now().UTC().Format(time.RFC3339Nano),
	}
	s.broadcast(&uipb.UiEvent{
		Payload: &uipb.UiEvent_SupplementalEvent{
			SupplementalEvent: evt,
		},
	})
}

func (s *service) registerSupplementalHooks() {
	if s == nil {
		return
	}
	s.suppHookCancel = append(s.suppHookCancel, supp.RegisterSuppFailoverHook(func(uuid string) {
		s.broadcastSupplementalEvent("failover", "trigger", uuid, "", "")
	}))
	s.suppHookCancel = append(s.suppHookCancel, supp.RegisterSuppLinkFailedHook(func(linkUUID string, endpoints []string) {
		s.broadcastSupplementalEvent("link", "failed", linkUUID, strings.Join(endpoints, ","), "")
	}))
	s.suppHookCancel = append(s.suppHookCancel, supp.RegisterSuppLinkRetiredHook(func(linkUUID string, endpoints []string, reason string) {
		s.broadcastSupplementalEvent("link", "retired", linkUUID, strings.Join(endpoints, ","), reason)
	}))
	s.suppHookCancel = append(s.suppHookCancel, supp.RegisterSuppLinkPromotedHook(func(linkUUID, parentUUID, childUUID string) {
		detail := fmt.Sprintf("parent=%s", parentUUID)
		s.broadcastSupplementalEvent("link", "promoted", linkUUID, childUUID, detail)
	}))
	s.suppHookCancel = append(s.suppHookCancel, supp.RegisterSuppHeartbeatHook(func(linkUUID, endpoint string, status supp.SuppLinkHealth, ts time.Time) {
		detail := status.String()
		if !ts.IsZero() {
			detail = fmt.Sprintf("%s@%s", detail, ts.UTC().Format(time.RFC3339Nano))
		}
		s.broadcastSupplementalEvent("heartbeat", "sample", linkUUID, endpoint, detail)
	}))
}

func convertChatRecord(rec sqlite.ChatRecord) *uipb.ChatMessage {
	return &uipb.ChatMessage{
		Id:        rec.ID,
		Username:  rec.Username,
		Role:      rec.Role,
		Message:   rec.Message,
		Timestamp: rec.CreatedAt.UTC().Format(time.RFC3339Nano),
	}
}

func convertAuditRecord(rec sqlite.AuditRecord) *uipb.AuditLogEntry {
	return &uipb.AuditLogEntry{
		Id:         rec.ID,
		Username:   rec.Username,
		Role:       rec.Role,
		Method:     rec.Method,
		Target:     rec.Target,
		Parameters: rec.Params,
		Status:     rec.Status,
		Error:      rec.Error,
		Peer:       rec.Peer,
		Timestamp:  rec.CreatedAt.UTC().Format(time.RFC3339Nano),
	}
}

func convertSleepProfile(info process.SessionInfo) *uipb.SleepProfile {
	if strings.TrimSpace(info.TargetUUID) == "" {
		return nil
	}
	profile := &uipb.SleepProfile{
		TargetUuid: info.TargetUUID,
		Profile:    info.SleepProfile,
		Status:     string(info.Status),
		Operator:   info.LastOperator,
		Reason:     info.StatusReason,
	}
	if info.SleepSeconds != nil {
		val := int32(*info.SleepSeconds)
		profile.SleepSeconds = &val
	}
	if info.WorkSeconds != nil {
		val := int32(*info.WorkSeconds)
		profile.WorkSeconds = &val
	}
	if info.JitterPercent != nil {
		val := *info.JitterPercent
		profile.JitterPercent = &val
	}
	if memo := info.Metadata["memo"]; memo != "" {
		profile.Memo = memo
	}
	if ts := info.Metadata["status_updated_at"]; ts != "" {
		profile.LastUpdated = ts
	}
	if ts := info.Metadata["next_wake_at"]; ts != "" {
		profile.NextWakeAt = ts
	}
	return profile
}

// -------- Dataplane Admin (实验性控制面) ----------
func (s *service) PrepareTransfer(ctx context.Context, req *dataplanepb.PrepareTransferRequest) (*dataplanepb.PrepareTransferResponse, error) {
	if s == nil || s.dataplane == nil {
		return nil, status.Error(codes.Unavailable, "dataplane unavailable")
	}
	if req == nil || strings.TrimSpace(req.GetTargetUuid()) == "" {
		return nil, status.Error(codes.InvalidArgument, "missing target uuid")
	}
	dir := mapDpDirection(req.GetDirection())
	if dir == "" {
		return nil, status.Error(codes.InvalidArgument, "invalid direction")
	}
	if req.GetOffset() < 0 {
		return nil, status.Error(codes.InvalidArgument, "offset must be >= 0")
	}
	if req.GetSizeHint() < 0 {
		return nil, status.Error(codes.InvalidArgument, "size_hint must be >= 0")
	}
	if req.GetSizeHint() > 0 && req.GetOffset() > req.GetSizeHint() {
		return nil, status.Error(codes.InvalidArgument, "offset exceeds size_hint")
	}
	hash := strings.ToLower(strings.TrimSpace(req.GetHash()))
	if !validSHA256Hex(hash) {
		return nil, status.Error(codes.InvalidArgument, "hash must be 64-char hex (sha256)")
	}
	ttl := time.Duration(req.GetTtlSeconds()) * time.Second
	claims, _ := collab.ClaimsFromContext(ctx)
	extras := req.GetMetadata()
	if extras == nil {
		extras = make(map[string]string)
	}
	if p := strings.TrimSpace(req.GetPath()); p != "" {
		extras["path"] = p
	}
	if req.GetSizeHint() > 0 {
		extras["size_hint"] = fmt.Sprintf("%d", req.GetSizeHint())
	}
	token, endpoint, meta := s.dataplane.PrepareTransfer(
		claims.Tenant,
		claims.Username,
		req.GetTargetUuid(),
		dir,
		req.GetMaxSize(),
		req.GetMaxRateBps(),
		req.GetSizeHint(),
		hash,
		req.GetOffset(),
		ttl,
		extras,
	)
	return &dataplanepb.PrepareTransferResponse{
		Token:         token,
		Endpoint:      endpoint,
		ExpiresAtUnix: meta.ExpiresAt.Unix(),
	}, nil
}

func (s *service) CompleteTransfer(ctx context.Context, req *dataplanepb.CompleteTransferRequest) (*dataplanepb.CompleteTransferResponse, error) {
	if s == nil || s.dataplane == nil {
		return nil, status.Error(codes.Unavailable, "dataplane unavailable")
	}
	if req == nil || strings.TrimSpace(req.GetToken()) == "" {
		return nil, status.Error(codes.InvalidArgument, "missing token")
	}
	target := strings.TrimSpace(req.GetTargetUuid())
	if target == "" {
		return nil, status.Error(codes.InvalidArgument, "missing target uuid")
	}
	if req.GetBytes() < 0 {
		return nil, status.Error(codes.InvalidArgument, "bytes must be >= 0")
	}
	statusText := strings.TrimSpace(req.GetStatus())
	if statusText == "" {
		statusText = codes.OK.String()
	}
	s.recordDataplaneAudit(ctx, "CompleteTransfer", target, statusText, req.GetError(), req.GetBytes())
	return &dataplanepb.CompleteTransferResponse{}, nil
}

func (s *service) PrepareProxy(ctx context.Context, req *dataplanepb.PrepareProxyRequest) (*dataplanepb.PrepareProxyResponse, error) {
	if s == nil || s.dataplane == nil {
		return nil, status.Error(codes.Unavailable, "dataplane unavailable")
	}
	if req == nil || strings.TrimSpace(req.GetTargetUuid()) == "" {
		return nil, status.Error(codes.InvalidArgument, "missing target uuid")
	}
	ttl := time.Duration(req.GetTtlSeconds()) * time.Second
	claims, _ := collab.ClaimsFromContext(ctx)
	token, endpoint, meta := s.dataplane.PrepareProxy(
		claims.Tenant,
		claims.Username,
		req.GetTargetUuid(),
		ttl,
		req.GetMetadata(),
	)
	return &dataplanepb.PrepareProxyResponse{
		Token:         token,
		ListenAddr:    endpoint,
		ExpiresAtUnix: meta.ExpiresAt.Unix(),
	}, nil
}

func (s *service) CompleteProxy(ctx context.Context, req *dataplanepb.CompleteProxyRequest) (*dataplanepb.CompleteProxyResponse, error) {
	if s == nil || s.dataplane == nil {
		return nil, status.Error(codes.Unavailable, "dataplane unavailable")
	}
	if req == nil || strings.TrimSpace(req.GetToken()) == "" {
		return nil, status.Error(codes.InvalidArgument, "missing token")
	}
	target := strings.TrimSpace(req.GetTargetUuid())
	if target == "" {
		return nil, status.Error(codes.InvalidArgument, "missing target uuid")
	}
	statusText := strings.TrimSpace(req.GetStatus())
	if statusText == "" {
		statusText = codes.OK.String()
	}
	s.recordDataplaneAudit(ctx, "CompleteProxy", target, statusText, req.GetError(), 0)
	return &dataplanepb.CompleteProxyResponse{}, nil
}

func mapDpDirection(d dataplanepb.Direction) string {
	switch d {
	case dataplanepb.Direction_DIRECTION_UPLOAD:
		return "upload"
	case dataplanepb.Direction_DIRECTION_DOWNLOAD:
		return "download"
	case dataplanepb.Direction_DIRECTION_PROXY:
		return "proxy"
	default:
		return ""
	}
}

func validSHA256Hex(s string) bool {
	if strings.TrimSpace(s) == "" {
		return true
	}
	if len(s) != 64 {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}

func (s *service) recordDataplaneAudit(ctx context.Context, method, target, statusText, errMsg string, bytes int64) {
	if s == nil || s.auditor == nil {
		return
	}
	claims, _ := collab.ClaimsFromContext(ctx)
	entry := collab.Entry{
		Tenant:   strings.TrimSpace(claims.Tenant),
		Username: claims.Username,
		Role:     claims.Role,
		Method:   method,
		Status:   statusText,
		Error:    errMsg,
		Peer:     peerAddress(ctx),
	}
	if entry.Status == "" {
		entry.Status = codes.OK.String()
	}
	entry.Target = target
	if bytes > 0 {
		entry.Params = fmt.Sprintf("bytes=%d", bytes)
	}
	s.auditor.Record(entry)
}

func convertSupplementalStatus(status process.SupplementalStatusSnapshot) *uipb.SupplementalStatus {
	return &uipb.SupplementalStatus{
		Enabled:        status.Enabled,
		QueueLength:    int32(status.QueueLength),
		PendingActions: int32(status.PendingActions),
		ActiveLinks:    int32(status.ActiveLinks),
	}
}

func convertSupplementalMetrics(snapshot process.SupplementalMetricsSnapshot) *uipb.SupplementalMetrics {
	return &uipb.SupplementalMetrics{
		Dispatched:      snapshot.Dispatched,
		Success:         snapshot.Success,
		Failures:        snapshot.Failures,
		Dropped:         snapshot.Dropped,
		Recycled:        snapshot.Recycled,
		QueueHigh:       uint64(snapshot.QueueHigh),
		LastFailure:     snapshot.LastFailure,
		EventSeq:        snapshot.EventSeq,
		RepairAttempts:  snapshot.RepairAttempts,
		RepairSuccess:   snapshot.RepairSuccess,
		RepairFailures:  snapshot.RepairFailures,
		LastGraphReport: append([]string(nil), snapshot.LastGraphReport...),
	}
}

func convertSupplementalEvents(events []process.SupplementalPlannerEvent) []*uipb.SupplementalEvent {
	if len(events) == 0 {
		return nil
	}
	result := make([]*uipb.SupplementalEvent, 0, len(events))
	for _, evt := range events {
		ts := ""
		if !evt.Timestamp.IsZero() {
			ts = evt.Timestamp.UTC().Format(time.RFC3339Nano)
		}
		result = append(result, &uipb.SupplementalEvent{
			Seq:        evt.Seq,
			Kind:       evt.Kind,
			Action:     evt.Action,
			SourceUuid: evt.SourceUUID,
			TargetUuid: evt.TargetUUID,
			Detail:     evt.Detail,
			Timestamp:  ts,
		})
	}
	return result
}

func convertSupplementalQuality(qualities []process.SupplementalQualitySnapshot) []*uipb.SupplementalQuality {
	if len(qualities) == 0 {
		return nil
	}
	result := make([]*uipb.SupplementalQuality, 0, len(qualities))
	for _, q := range qualities {
		last := ""
		if !q.LastHeartbeat.IsZero() {
			last = q.LastHeartbeat.UTC().Format(time.RFC3339Nano)
		}
		result = append(result, &uipb.SupplementalQuality{
			NodeUuid:       q.NodeUUID,
			HealthScore:    q.HealthScore,
			LatencyScore:   q.LatencyScore,
			FailureScore:   q.FailureScore,
			QueueScore:     q.QueueScore,
			StalenessScore: q.StalenessScore,
			TotalSuccess:   q.TotalSuccess,
			TotalFailures:  q.TotalFailures,
			LastHeartbeat:  last,
		})
	}
	return result
}

func mapSupplementalError(err error) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	lower := strings.ToLower(msg)
	if strings.Contains(lower, "unavailable") {
		return status.Error(codes.Unavailable, msg)
	}
	return status.Error(codes.InvalidArgument, msg)
}

func convertNetworkInfos(infos []process.NetworkInfo, active string) []*uipb.NetworkInfo {
	if len(infos) == 0 {
		return nil
	}
	result := make([]*uipb.NetworkInfo, 0, len(infos))
	for _, info := range infos {
		copyTargets := append([]string(nil), info.Entries...)
		result = append(result, &uipb.NetworkInfo{
			NetworkId:   info.ID,
			TargetUuids: copyTargets,
			Active:      active != "" && strings.EqualFold(active, info.ID),
		})
	}
	return result
}

func mapSshTunnelError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, process.ErrSSHTunnelMissingTarget),
		errors.Is(err, process.ErrSSHTunnelMissingAddr),
		errors.Is(err, process.ErrSSHTunnelMissingPort),
		errors.Is(err, process.ErrSSHTunnelMissingUser),
		errors.Is(err, process.ErrSSHTunnelMissingPass),
		errors.Is(err, process.ErrSSHTunnelMissingCert),
		errors.Is(err, process.ErrSSHTunnelUnsupportedAuth):
		return status.Error(codes.InvalidArgument, err.Error())
	default:
		return status.Errorf(codes.Internal, "start ssh tunnel failed: %v", err)
	}
}

func mapSleepUpdateError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, process.ErrSleepUpdateMissingTarget),
		errors.Is(err, process.ErrSleepUpdateNoFields),
		errors.Is(err, process.ErrSleepUpdateInvalidSleep),
		errors.Is(err, process.ErrSleepUpdateInvalidWork):
		return status.Error(codes.InvalidArgument, err.Error())
	default:
		return status.Errorf(codes.Internal, "update sleep failed: %v", err)
	}
}

func mapShutdownError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, process.ErrShutdownMissingTarget):
		return status.Error(codes.InvalidArgument, err.Error())
	default:
		return status.Errorf(codes.Internal, "shutdown failed: %v", err)
	}
}

func mapStreamPingError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, process.ErrStreamPingMissingTarget),
		errors.Is(err, process.ErrStreamPingInvalidCount),
		errors.Is(err, process.ErrStreamPingInvalidSize):
		return status.Error(codes.InvalidArgument, err.Error())
	default:
		return status.Errorf(codes.Internal, "stream ping failed: %v", err)
	}
}

func protoPivotListenerMode(mode int) uipb.PivotListenerMode {
	switch mode {
	case process.ListenerModeIptables:
		return uipb.PivotListenerMode_PIVOT_LISTENER_MODE_IPTABLES
	case process.ListenerModeSoReuse:
		return uipb.PivotListenerMode_PIVOT_LISTENER_MODE_SOREUSE
	default:
		return uipb.PivotListenerMode_PIVOT_LISTENER_MODE_NORMAL
	}
}

func (s *service) close() {
	if s == nil {
		return
	}
	if s.logHookCancel != nil {
		s.logHookCancel()
		s.logHookCancel = nil
	}
	for _, cancel := range s.suppHookCancel {
		if cancel != nil {
			cancel()
		}
	}
	s.suppHookCancel = nil
}

func convertNodeInfo(node topology.UINodeSnapshot) *uipb.NodeInfo {
	status := "offline"
	if node.IsAlive {
		status = "online"
	}
	sleep := ""
	if node.SleepSecond > 0 || node.WorkSecond > 0 {
		sleep = fmt.Sprintf("%ds/%ds", node.SleepSecond, node.WorkSecond)
	}
	workProfile := ""
	if node.Username != "" && node.Hostname != "" {
		workProfile = fmt.Sprintf("%s@%s", node.Username, node.Hostname)
	} else if node.Hostname != "" {
		workProfile = node.Hostname
	} else {
		workProfile = node.Username
	}
	return &uipb.NodeInfo{
		Uuid:          node.UUID,
		Alias:         node.Alias,
		ParentUuid:    node.ParentUUID,
		Status:        status,
		Network:       node.Network,
		Sleep:         sleep,
		Memo:          node.Memo,
		Depth:         int32(node.Depth),
		WorkProfile:   workProfile,
		ActiveStreams: 0,
	}
}

func (s *service) handlePrinterHook(level, msg string) {
	if s == nil {
		return
	}
	entry := &uipb.LogEntry{
		Id:        fmt.Sprintf("log-%d", s.logSeq.Add(1)),
		Level:     level,
		Message:   msg,
		Timestamp: time.Now().Format(time.RFC3339),
	}
	s.broadcast(&uipb.UiEvent{
		Payload: &uipb.UiEvent_LogEvent{
			LogEvent: entry,
		},
	})
}

func (s *service) currentOperator(ctx context.Context) string {
	claims, ok := collab.ClaimsFromContext(ctx)
	if !ok {
		return ""
	}
	return claims.Username
}

func (s *service) operatorRole(ctx context.Context) string {
	claims, ok := collab.ClaimsFromContext(ctx)
	if !ok {
		return ""
	}
	return claims.Role
}

// currentTenant 返回当前请求所属的租户标识。
// 在 teamserver 模式下，我们直接用 Client Name（即 Claims.Username）
// 作为租户，将审计视图按人隔离。
func (s *service) currentTenant(ctx context.Context) string {
	claims, ok := collab.ClaimsFromContext(ctx)
	if !ok {
		return ""
	}
	if t := strings.ToLower(strings.TrimSpace(claims.Tenant)); t != "" {
		return t
	}
	return strings.ToLower(strings.TrimSpace(claims.Username))
}

func peerAddress(ctx context.Context) string {
	if p, ok := peer.FromContext(ctx); ok && p.Addr != nil {
		return p.Addr.String()
	}
	return ""
}

func (s *service) handleChatRecord(rec sqlite.ChatRecord) {
	if s == nil {
		return
	}
	s.broadcast(&uipb.UiEvent{
		Payload: &uipb.UiEvent_ChatEvent{
			ChatEvent: &uipb.ChatEvent{Message: convertChatRecord(rec)},
		},
	})
}

func (s *service) handleAuditRecord(rec sqlite.AuditRecord) {
	if s == nil {
		return
	}
	s.broadcast(&uipb.UiEvent{
		Payload: &uipb.UiEvent_AuditEvent{
			AuditEvent: &uipb.AuditEvent{Entry: convertAuditRecord(rec)},
		},
	})
}

func (s *service) ListRepairs(ctx context.Context, _ *uipb.ListRepairsRequest) (*uipb.ListRepairsResponse, error) {
	if s == nil || (s.admin == nil && s.listRepairsOverride == nil && s.sessionsOverride == nil) {
		return nil, status.Error(codes.Unavailable, "admin unavailable")
	}
	filter := process.SessionFilter{IncludeInactive: true}
	var sessions []process.SessionInfo
	switch {
	case s.sessionsOverride != nil:
		sessions = s.sessionsOverride(filter)
	default:
		sessions = s.admin.Sessions(filter)
	}
	statusMap := make(map[string]*uipb.RepairStatus)
	for _, sess := range sessions {
		if sess.Status != process.SessionStatusRepairing {
			continue
		}
		key := strings.ToLower(sess.TargetUUID)
		entry := &uipb.RepairStatus{
			TargetUuid: sess.TargetUUID,
			LastError:  sess.LastError,
			Reason:     sess.StatusReason,
		}
		statusMap[key] = entry
	}
	var snapshots []process.RepairStatusSnapshot
	switch {
	case s.listRepairsOverride != nil:
		snapshots = s.listRepairsOverride()
	default:
		var err error
		snapshots, err = s.admin.SupplementalRepairs()
		if err != nil {
			return nil, mapSupplementalError(err)
		}
	}
	for _, snap := range snapshots {
		key := strings.ToLower(snap.TargetUUID)
		entry := statusMap[key]
		if entry == nil {
			entry = &uipb.RepairStatus{TargetUuid: snap.TargetUUID}
			statusMap[key] = entry
		}
		entry.Attempts = int32(snap.Attempts)
		entry.Broken = snap.Broken
		if !snap.NextAttempt.IsZero() {
			entry.NextAttempt = snap.NextAttempt.UTC().Format(time.RFC3339Nano)
		}
	}
	repairs := make([]*uipb.RepairStatus, 0, len(statusMap))
	for _, entry := range statusMap {
		repairs = append(repairs, entry)
	}
	sort.Slice(repairs, func(i, j int) bool {
		return repairs[i].GetTargetUuid() < repairs[j].GetTargetUuid()
	})
	return &uipb.ListRepairsResponse{Repairs: repairs}, nil
}

func convertRouterMetrics(stats map[uint16]bus.RouterCounter) []*uipb.RouterMetric {
	if len(stats) == 0 {
		return nil
	}
	keys := make([]int, 0, len(stats))
	for msgType := range stats {
		keys = append(keys, int(msgType))
	}
	sort.Ints(keys)
	metrics := make([]*uipb.RouterMetric, 0, len(keys))
	for _, key := range keys {
		mt := uint16(key)
		counter := stats[mt]
		metrics = append(metrics, &uipb.RouterMetric{
			MessageType: uint32(mt),
			Dispatched:  counter.Dispatched,
			Errors:      counter.Errors,
			Drops:       counter.Drops,
		})
	}
	return metrics
}

func convertReconnectMetrics(view process.ReconnectStatsView) *uipb.ReconnectMetrics {
	if view.Attempts == 0 && view.Success == 0 && view.Failures == 0 && view.LastError == "" {
		return nil
	}
	return &uipb.ReconnectMetrics{
		Attempts:  view.Attempts,
		Success:   view.Success,
		Failures:  view.Failures,
		LastError: view.LastError,
	}
}

func convertDtnQueueStats(stats dtn.QueueStats) *uipb.DtnQueueStats {
	result := &uipb.DtnQueueStats{
		Total:            int32(stats.Total),
		Ready:            int32(stats.Ready),
		Held:             int32(stats.Held),
		Capacity:         int32(stats.Capacity),
		HighWatermark:    int32(stats.HighWatermark),
		AverageWait:      stats.AverageWait.String(),
		OldestBundleId:   stats.OldestID,
		OldestTargetUuid: shorten(stats.OldestTarget),
		OldestAge:        stats.OldestAge.String(),
		DroppedTotal:     stats.DroppedTotal,
		ExpiredTotal:     stats.ExpiredTotal,
		DropByPriority:   make(map[string]uint64),
	}
	for priority, count := range stats.DropByPriority {
		result.DropByPriority[dtnPriorityLabel(priority)] = count
	}
	return result
}

func convertDtnBundle(summary dtn.BundleSummary) *uipb.DtnBundle {
	holdu := ""
	if !summary.HoldUntil.IsZero() {
		holdu = summary.HoldUntil.UTC().Format(time.RFC3339Nano)
	}
	deliver := ""
	if !summary.DeliverBy.IsZero() {
		deliver = summary.DeliverBy.UTC().Format(time.RFC3339Nano)
	}
	return &uipb.DtnBundle{
		BundleId:   summary.ID,
		TargetUuid: summary.Target,
		Priority:   mapDtnPriorityToProto(summary.Priority),
		Attempts:   int32(summary.Attempts),
		Age:        summary.Age.String(),
		HoldUntil:  holdu,
		DeliverBy:  deliver,
		Preview:    summary.Preview,
	}
}

func convertDtnMetrics(stats dtn.QueueStats, captured time.Time, enqueued, delivered, failed, retried uint64) *uipb.DtnMetrics {
	metrics := &uipb.DtnMetrics{
		GlobalQueue: convertDtnQueueStats(stats),
		Enqueued:    enqueued,
		Delivered:   delivered,
		Failed:      failed,
		Retried:     retried,
	}
	if !captured.IsZero() {
		metrics.CapturedAt = captured.UTC().Format(time.RFC3339Nano)
	}
	return metrics
}

func mapDtnPriorityToProto(p dtn.Priority) uipb.DtnPriority {
	switch p {
	case dtn.PriorityHigh:
		return uipb.DtnPriority_DTN_PRIORITY_HIGH
	case dtn.PriorityLow:
		return uipb.DtnPriority_DTN_PRIORITY_LOW
	case dtn.PriorityNormal:
		fallthrough
	default:
		return uipb.DtnPriority_DTN_PRIORITY_NORMAL
	}
}

func mapProtoDtnPriority(p uipb.DtnPriority) dtn.Priority {
	switch p {
	case uipb.DtnPriority_DTN_PRIORITY_HIGH:
		return dtn.PriorityHigh
	case uipb.DtnPriority_DTN_PRIORITY_LOW:
		return dtn.PriorityLow
	default:
		return dtn.PriorityNormal
	}
}

func mapRoutingStrategyToProto(strategy topology.RoutingStrategy) uipb.RoutingStrategy {
	switch strategy {
	case topology.RoutingByWeight:
		return uipb.RoutingStrategy_ROUTING_STRATEGY_WEIGHT
	case topology.RoutingByLatency:
		return uipb.RoutingStrategy_ROUTING_STRATEGY_LATENCY
	case topology.RoutingByHops:
		fallthrough
	default:
		return uipb.RoutingStrategy_ROUTING_STRATEGY_HOPS
	}
}

func mapProtoRoutingStrategy(strategy uipb.RoutingStrategy) (topology.RoutingStrategy, error) {
	switch strategy {
	case uipb.RoutingStrategy_ROUTING_STRATEGY_HOPS:
		return topology.RoutingByHops, nil
	case uipb.RoutingStrategy_ROUTING_STRATEGY_WEIGHT:
		return topology.RoutingByWeight, nil
	case uipb.RoutingStrategy_ROUTING_STRATEGY_LATENCY:
		return topology.RoutingByLatency, nil
	case uipb.RoutingStrategy_ROUTING_STRATEGY_UNSPECIFIED:
		return topology.RoutingByHops, nil
	default:
		return topology.RoutingByHops, fmt.Errorf("unknown routing strategy")
	}
}

func dtnPriorityLabel(p dtn.Priority) string {
	switch p {
	case dtn.PriorityHigh:
		return "high"
	case dtn.PriorityLow:
		return "low"
	default:
		return "normal"
	}
}

func convertEdge(edge topology.UIEdgeSnapshot) *uipb.Edge {
	return &uipb.Edge{
		ParentUuid:   edge.ParentUUID,
		ChildUuid:    edge.ChildUUID,
		Supplemental: edge.Supplemental,
	}
}

func shorten(uuid string) string {
	if len(uuid) <= 8 {
		return uuid
	}
	return uuid[:8]
}

func convertStreamDiag(diag stream.SessionDiag) *uipb.StreamDiag {
	lastActivity := ""
	if !diag.LastActivity.IsZero() {
		lastActivity = diag.LastActivity.UTC().Format(time.RFC3339)
	}
	streamDiag := &uipb.StreamDiag{
		StreamId:     diag.ID,
		TargetUuid:   diag.Target,
		Kind:         diag.Kind,
		Outbound:     diag.Outbound,
		Pending:      uint32(diag.Pending),
		Inflight:     uint32(diag.Inflight),
		Window:       uint32(diag.Window),
		Seq:          diag.Seq,
		Ack:          diag.Ack,
		Rto:          diag.RTO.String(),
		LastActivity: lastActivity,
		SessionId:    diag.SessionID,
	}
	if len(diag.Metadata) > 0 {
		sanitized := sanitizeStreamMetadata(diag.Metadata)
		if len(sanitized) > 0 {
			streamDiag.Metadata = sanitized
		}
	}
	return streamDiag
}

func sanitizeStreamMetadata(meta map[string]string) map[string]string {
	if len(meta) == 0 {
		return nil
	}
	sanitized := make(map[string]string, len(meta))
	for key, value := range meta {
		cleanKey := strings.TrimSpace(key)
		if cleanKey == "" || isSensitiveMetadataKey(cleanKey) {
			continue
		}
		sanitized[cleanKey] = value
	}
	return sanitized
}

func isSensitiveMetadataKey(key string) bool {
	lower := strings.ToLower(strings.TrimSpace(key))
	if lower == "" {
		return true
	}
	sensitiveTokens := []string{
		"password",
		"passwd",
		"secret",
		"token",
		"private",
		"credential",
		"apikey",
		"api_key",
		"auth_payload",
	}
	for _, token := range sensitiveTokens {
		if strings.Contains(lower, token) {
			return true
		}
	}
	return false
}

func nodeChanged(prev, next topology.UINodeSnapshot) bool {
	if prev.IsAlive != next.IsAlive {
		return true
	}
	if prev.ParentUUID != next.ParentUUID {
		return true
	}
	if prev.Network != next.Network {
		return true
	}
	if prev.Alias != next.Alias {
		return true
	}
	if prev.Memo != next.Memo {
		return true
	}
	if prev.Depth != next.Depth {
		return true
	}
	if prev.SleepSecond != next.SleepSecond || prev.WorkSecond != next.WorkSecond {
		return true
	}
	if !prev.LastSeen.Equal(next.LastSeen) {
		return true
	}
	return false
}

func streamChanged(prev, next stream.SessionDiag) bool {
	if prev.Outbound != next.Outbound {
		return true
	}
	if prev.Kind != next.Kind {
		return true
	}
	if prev.SessionID != next.SessionID {
		return true
	}
	if prev.Pending != next.Pending || prev.Inflight != next.Inflight {
		return true
	}
	if prev.Window != next.Window {
		return true
	}
	if prev.Ack != next.Ack || prev.Seq != next.Seq {
		return true
	}
	if prev.RTO != next.RTO {
		return true
	}
	if !prev.LastActivity.Equal(next.LastActivity) {
		return true
	}
	if len(prev.Metadata) != len(next.Metadata) {
		return true
	}
	for k, v := range prev.Metadata {
		if next.Metadata[k] != v {
			return true
		}
	}
	return false
}

func listenerChanged(prev, next process.ListenerRecord) bool {
	if prev.TargetUUID != next.TargetUUID {
		return true
	}
	if prev.Protocol != next.Protocol || prev.Bind != next.Bind || prev.Mode != next.Mode {
		return true
	}
	if prev.Status != next.Status {
		return true
	}
	if prev.LastError != next.LastError {
		return true
	}
	if prev.Route != next.Route {
		return true
	}
	if len(prev.Metadata) != len(next.Metadata) {
		return true
	}
	for k, v := range prev.Metadata {
		if next.Metadata == nil {
			return true
		}
		if next.Metadata[k] != v {
			return true
		}
	}
	return false
}
