package grpcserver

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"codeberg.org/agnoie/shepherd/internal/dataplanepb"
	"codeberg.org/agnoie/shepherd/internal/kelpie/collab/audit"
	"codeberg.org/agnoie/shepherd/internal/kelpie/collab/auth"
	"codeberg.org/agnoie/shepherd/internal/kelpie/collab/chat"
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
	"codeberg.org/agnoie/shepherd/protocol"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// Server wraps a gRPC server that exposes Kelpie state to GUI clients.
type Server struct {
	grpcServer *grpc.Server
	service    *service
}

// New creates a UI gRPC server using the provided admin instance.
func New(admin *process.Admin, auditor *audit.Recorder, chatSvc *chat.Service, dpManager *dataplane.Manager, bootstrapToken string, opts ...grpc.ServerOption) *Server {
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

// Serve starts serving on the given listener.
func (s *Server) Serve(lis net.Listener) error {
	if s == nil || s.grpcServer == nil {
		return fmt.Errorf("grpc server not initialized")
	}
	return s.grpcServer.Serve(lis)
}

// Stop gracefully stops the gRPC server.
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
	auditor        *audit.Recorder
	chatService    *chat.Service
	dataplane      *dataplane.Manager
	bootstrapToken string
	// test hooks
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

func (s *service) GetSnapshot(ctx context.Context, _ *uipb.SnapshotRequest) (*uipb.SnapshotResponse, error) {
	if s == nil || s.admin == nil {
		return nil, status.Error(codes.Unavailable, "admin unavailable")
	}
	topoSnap := s.admin.TopologySnapshot("", "")
	streams := s.admin.StreamDiagnostics()
	snapshot := &uipb.Snapshot{
		Nodes:               make([]*uipb.NodeInfo, 0, len(topoSnap.Nodes)),
		Edges:               make([]*uipb.Edge, 0, len(topoSnap.Edges)),
		Streams:             make([]*uipb.StreamDiag, 0, len(streams)),
		RecentLogs:          []*uipb.LogEntry{},
		PivotListeners:      nil,
		Sessions:            nil,
		ControllerListeners: nil,
	}
	for _, node := range topoSnap.Nodes {
		snapshot.Nodes = append(snapshot.Nodes, convertNodeInfo(node))
	}
	for _, edge := range topoSnap.Edges {
		snapshot.Edges = append(snapshot.Edges, convertEdge(edge))
	}
	for _, diag := range streams {
		snapshot.Streams = append(snapshot.Streams, convertStreamDiag(diag))
	}
	if listeners := s.admin.ListListeners(process.ListenerFilter{}); len(listeners) > 0 {
		snapshot.PivotListeners = convertPivotListeners(listeners)
	}
	if ctrl := s.admin.ListControllerListeners(); len(ctrl) > 0 {
		snapshot.ControllerListeners = convertControllerListeners(ctrl)
	}
	if sessions := s.admin.Sessions(process.SessionFilter{}); len(sessions) > 0 {
		snapshot.Sessions = convertSessions(sessions)
	}
	return &uipb.SnapshotResponse{Snapshot: snapshot}, nil
}

func (s *service) ListNodes(ctx context.Context, req *uipb.ListNodesRequest) (*uipb.ListNodesResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "missing request")
	}
	if s == nil || s.admin == nil {
		return nil, status.Error(codes.Unavailable, "admin unavailable")
	}
	target := strings.TrimSpace(req.GetTargetUuid())
	network := strings.TrimSpace(req.GetNetworkId())
	snapshot := s.snapshotView(target, network)
	resp := &uipb.ListNodesResponse{Nodes: make([]*uipb.NodeInfo, 0, len(snapshot.Nodes))}
	for _, node := range snapshot.Nodes {
		resp.Nodes = append(resp.Nodes, convertNodeInfo(node))
	}
	return resp, nil
}

func (s *service) GetTopology(ctx context.Context, req *uipb.GetTopologyRequest) (*uipb.GetTopologyResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "missing request")
	}
	if s == nil || s.admin == nil {
		return nil, status.Error(codes.Unavailable, "admin unavailable")
	}
	target := strings.TrimSpace(req.GetTargetUuid())
	network := strings.TrimSpace(req.GetNetworkId())
	snapshot := s.snapshotView(target, network)
	resp := &uipb.GetTopologyResponse{
		Nodes: make([]*uipb.NodeInfo, 0, len(snapshot.Nodes)),
		Edges: make([]*uipb.Edge, 0, len(snapshot.Edges)),
	}
	for _, node := range snapshot.Nodes {
		resp.Nodes = append(resp.Nodes, convertNodeInfo(node))
	}
	for _, edge := range snapshot.Edges {
		resp.Edges = append(resp.Edges, convertEdge(edge))
	}
	if !snapshot.LastUpdated.IsZero() {
		resp.LastUpdated = snapshot.LastUpdated.UTC().Format(time.RFC3339Nano)
	}
	return resp, nil
}

func (s *service) WatchEvents(_ *uipb.WatchEventsRequest, srv uipb.KelpieUIService_WatchEventsServer) error {
	if s == nil || s.admin == nil {
		return status.Error(codes.Unavailable, "admin unavailable")
	}
	subID, subCh := s.registerSubscriber()
	defer s.unregisterSubscriber(subID)
	prevNodes := make(map[string]topology.UINodeSnapshot)
	prevStreams := make(map[uint32]stream.SessionDiag)
	prevListeners := make(map[string]process.ListenerRecord)
	prevSessions := make(map[string]process.SessionInfo)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	emitNodeEvent := func(kind uipb.NodeEvent_Kind, node topology.UINodeSnapshot) error {
		event := &uipb.UiEvent{
			Payload: &uipb.UiEvent_NodeEvent{
				NodeEvent: &uipb.NodeEvent{
					Kind: kind,
					Node: convertNodeInfo(node),
				},
			},
		}
		return srv.Send(event)
	}
	emitStreamEvent := func(kind uipb.StreamEvent_Kind, diag stream.SessionDiag, reason string) error {
		event := &uipb.UiEvent{
			Payload: &uipb.UiEvent_StreamEvent{
				StreamEvent: &uipb.StreamEvent{
					Kind:   kind,
					Stream: convertStreamDiag(diag),
					Reason: reason,
				},
			},
		}
		return srv.Send(event)
	}
	emitListenerEvent := func(kind uipb.PivotListenerEvent_Kind, rec process.ListenerRecord) error {
		event := &uipb.UiEvent{
			Payload: &uipb.UiEvent_ListenerEvent{
				ListenerEvent: &uipb.PivotListenerEvent{
					Kind:     kind,
					Listener: convertPivotListener(rec),
				},
			},
		}
		return srv.Send(event)
	}
	sendSession := func(kind uipb.SessionEvent_Kind, session process.SessionInfo, reason string) error {
		event := &uipb.UiEvent{
			Payload: &uipb.UiEvent_SessionEvent{
				SessionEvent: &uipb.SessionEvent{
					Kind:    kind,
					Session: convertSession(session),
					Reason:  reason,
				},
			},
		}
		return srv.Send(event)
	}

	// Initial snapshot as ADDED events.
	initialSnap := s.admin.TopologySnapshot("", "")
	for _, node := range initialSnap.Nodes {
		prevNodes[node.UUID] = node
		if err := emitNodeEvent(uipb.NodeEvent_ADDED, node); err != nil {
			return err
		}
	}
	for _, diag := range s.admin.StreamDiagnostics() {
		prevStreams[diag.ID] = diag
		if err := emitStreamEvent(uipb.StreamEvent_STREAM_OPENED, diag, ""); err != nil {
			return err
		}
	}
	for _, rec := range s.admin.ListListeners(process.ListenerFilter{}) {
		prevListeners[rec.ID] = rec
		if err := emitListenerEvent(uipb.PivotListenerEvent_PIVOT_LISTENER_ADDED, rec); err != nil {
			return err
		}
	}
	for _, sess := range s.admin.Sessions(process.SessionFilter{}) {
		prevSessions[strings.ToLower(sess.TargetUUID)] = sess
		if err := sendSession(uipb.SessionEvent_SESSION_EVENT_ADDED, sess, ""); err != nil {
			return err
		}
	}

	for {
		select {
		case <-srv.Context().Done():
			return srv.Context().Err()
		case event := <-subCh:
			if event != nil {
				if err := srv.Send(event); err != nil {
					return err
				}
			}
		case <-ticker.C:
			snap := s.admin.TopologySnapshot("", "")
			currentNodes := make(map[string]topology.UINodeSnapshot, len(snap.Nodes))
			for _, node := range snap.Nodes {
				currentNodes[node.UUID] = node
				prev, ok := prevNodes[node.UUID]
				if !ok {
					if err := emitNodeEvent(uipb.NodeEvent_ADDED, node); err != nil {
						return err
					}
					continue
				}
				if nodeChanged(prev, node) {
					if err := emitNodeEvent(uipb.NodeEvent_UPDATED, node); err != nil {
						return err
					}
				}
			}
			for uuid, prev := range prevNodes {
				if _, ok := currentNodes[uuid]; !ok {
					if err := emitNodeEvent(uipb.NodeEvent_REMOVED, prev); err != nil {
						return err
					}
				}
			}
			prevNodes = currentNodes

			streamDiags := s.admin.StreamDiagnostics()
			currentStreams := make(map[uint32]stream.SessionDiag, len(streamDiags))
			for _, diag := range streamDiags {
				currentStreams[diag.ID] = diag
				if prev, ok := prevStreams[diag.ID]; ok {
					if streamChanged(prev, diag) {
						if err := emitStreamEvent(uipb.StreamEvent_STREAM_UPDATED, diag, ""); err != nil {
							return err
						}
					}
				} else {
					if err := emitStreamEvent(uipb.StreamEvent_STREAM_OPENED, diag, ""); err != nil {
						return err
					}
				}
			}
			for id, diag := range prevStreams {
				if _, ok := currentStreams[id]; !ok {
					reason := s.admin.StreamCloseReason(id)
					if err := emitStreamEvent(uipb.StreamEvent_STREAM_CLOSED, diag, reason); err != nil {
						return err
					}
				}
			}
			prevStreams = currentStreams

			listenerList := s.admin.ListListeners(process.ListenerFilter{})
			currentListeners := make(map[string]process.ListenerRecord, len(listenerList))
			for _, rec := range listenerList {
				currentListeners[rec.ID] = rec
				if prev, ok := prevListeners[rec.ID]; ok {
					if listenerChanged(prev, rec) {
						if err := emitListenerEvent(uipb.PivotListenerEvent_PIVOT_LISTENER_UPDATED, rec); err != nil {
							return err
						}
					}
				} else {
					if err := emitListenerEvent(uipb.PivotListenerEvent_PIVOT_LISTENER_ADDED, rec); err != nil {
						return err
					}
				}
			}
			for id, prev := range prevListeners {
				if _, ok := currentListeners[id]; !ok {
					if err := emitListenerEvent(uipb.PivotListenerEvent_PIVOT_LISTENER_REMOVED, prev); err != nil {
						return err
					}
				}
			}
			prevListeners = currentListeners

			sessionList := s.admin.Sessions(process.SessionFilter{})
			currentSessions := make(map[string]process.SessionInfo, len(sessionList))
			for _, sess := range sessionList {
				key := strings.ToLower(sess.TargetUUID)
				currentSessions[key] = sess
				if prev, ok := prevSessions[key]; ok {
					if sessionChanged(prev, sess) {
						if err := sendSession(uipb.SessionEvent_SESSION_EVENT_UPDATED, sess, ""); err != nil {
							return err
						}
					}
				} else {
					if err := sendSession(uipb.SessionEvent_SESSION_EVENT_ADDED, sess, ""); err != nil {
						return err
					}
				}
			}
			for id, prev := range prevSessions {
				if _, ok := currentSessions[id]; !ok {
					if err := sendSession(uipb.SessionEvent_SESSION_EVENT_REMOVED, prev, ""); err != nil {
						return err
					}
				}
			}
			prevSessions = currentSessions
		}
	}
}

func (s *service) ProxyStream(streamSrv uipb.KelpieUIService_ProxyStreamServer) error {
	if s == nil || s.admin == nil {
		return status.Error(codes.Unavailable, "admin unavailable")
	}
	first, err := streamSrv.Recv()
	if err != nil {
		if err == io.EOF {
			return status.Error(codes.InvalidArgument, "empty stream request")
		}
		return err
	}
	targetUUID := strings.TrimSpace(first.GetTargetUuid())
	if targetUUID == "" {
		return status.Error(codes.InvalidArgument, "missing target uuid")
	}
	sessionID := strings.TrimSpace(first.GetSessionId())
	if sessionID == "" {
		sessionID = s.admin.ShellSessionID(targetUUID)
	}
	meta := sanitizeStreamOptions(first.GetOptions())
	kind := strings.ToLower(strings.TrimSpace(meta["kind"]))
	if kind == "" {
		kind = "stream"
	}
	ctx, cancel := context.WithCancel(streamSrv.Context())
	defer cancel()
	upstream, err := s.admin.OpenStream(ctx, targetUUID, sessionID, meta)
	if err != nil {
		return status.Errorf(codes.Internal, "open stream failed: %v", err)
	}
	defer upstream.Close()
	streamID := uint32(0)
	if idProvider, ok := upstream.(interface{ ID() uint32 }); ok && idProvider != nil {
		streamID = idProvider.ID()
	}
	if err := streamSrv.Send(streamOpenResponse(sessionID, targetUUID, streamID, kind)); err != nil {
		return err
	}
	upstreamErrCh := make(chan error, 1)
	go func() {
		buf := make([]byte, 32*1024)
		for {
			n, readErr := upstream.Read(buf)
			if n > 0 {
				data := append([]byte(nil), buf[:n]...)
				resp := &uipb.StreamResponse{
					SessionId:  sessionID,
					TargetUuid: targetUUID,
					Data:       data,
				}
				if sendErr := streamSrv.Send(resp); sendErr != nil {
					upstreamErrCh <- sendErr
					return
				}
			}
			if readErr != nil {
				upstreamErrCh <- readErr
				return
			}
		}
	}()
	handleReq := func(req *uipb.StreamRequest) error {
		if req == nil {
			return nil
		}
		if ctrl := req.GetControl(); ctrl != nil {
			switch ctrl.GetKind() {
			case uipb.StreamControl_CLOSE:
				return io.EOF
			case uipb.StreamControl_RESIZE:
				rows := uint16(ctrl.GetRows())
				cols := uint16(ctrl.GetCols())
				ctrlBuf := []byte(fmt.Sprintf("WIN %d %d", rows, cols))
				payload := append([]byte{0x00}, ctrlBuf...)
				payload = append(payload, '\n')
				if _, err := upstream.Write(payload); err != nil {
					return status.Errorf(codes.Internal, "stream resize send failed: %v", err)
				}
				return nil
			case uipb.StreamControl_ERROR:
				if msg := strings.TrimSpace(ctrl.GetError()); msg != "" {
					return status.Error(codes.Aborted, msg)
				}
				return status.Error(codes.Aborted, "stream aborted")
			}
		}
		if data := req.GetData(); len(data) > 0 {
			if _, err := upstream.Write(data); err != nil {
				return status.Errorf(codes.Internal, "stream write failed: %v", err)
			}
		}
		return nil
	}
	if err := handleReq(first); err != nil {
		if err == io.EOF {
			_ = streamSrv.Send(streamCloseResponse(sessionID, targetUUID, streamID, kind, "", uipb.ProxyStreamError_PROXYSTREAM_ERROR_UNSPECIFIED))
			return nil
		}
		return err
	}
	clientReqCh := make(chan *uipb.StreamRequest)
	clientErrCh := make(chan error, 1)
	go func() {
		defer close(clientReqCh)
		for {
			req, recvErr := streamSrv.Recv()
			if recvErr != nil {
				clientErrCh <- recvErr
				return
			}
			clientReqCh <- req
		}
	}()
	for {
		select {
		case err := <-upstreamErrCh:
			if err == nil || err == io.EOF {
				_ = streamSrv.Send(streamCloseResponse(sessionID, targetUUID, streamID, kind, "", uipb.ProxyStreamError_PROXYSTREAM_ERROR_UNSPECIFIED))
				return nil
			}
			_ = streamSrv.Send(streamCloseResponse(sessionID, targetUUID, streamID, kind, err.Error(), uipb.ProxyStreamError_PROXYSTREAM_ERROR_REMOTE))
			return nil
		case req, ok := <-clientReqCh:
			if !ok {
				var recvErr error
				select {
				case recvErr = <-clientErrCh:
				default:
				}
				if recvErr == nil || recvErr == io.EOF {
					_ = streamSrv.Send(streamCloseResponse(sessionID, targetUUID, streamID, kind, "", uipb.ProxyStreamError_PROXYSTREAM_ERROR_UNSPECIFIED))
					return nil
				}
				return recvErr
			}
			if err := handleReq(req); err != nil {
				if err == io.EOF {
					_ = streamSrv.Send(streamCloseResponse(sessionID, targetUUID, streamID, kind, "", uipb.ProxyStreamError_PROXYSTREAM_ERROR_UNSPECIFIED))
					return nil
				}
				_ = streamSrv.Send(streamCloseResponse(sessionID, targetUUID, streamID, kind, err.Error(), uipb.ProxyStreamError_PROXYSTREAM_ERROR_REMOTE))
				return err
			}
		}
	}
}

func (s *service) StartShell(ctx context.Context, req *uipb.StartShellRequest) (*uipb.StartShellResponse, error) {
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
	sessionID := strings.TrimSpace(req.GetResumeSessionId())
	if sessionID == "" {
		sessionID = s.admin.ShellSessionID(target)
	}
	mode := req.GetMode()
	shellMode := protocol.ShellModePipe
	switch mode {
	case uipb.ShellMode_SHELL_MODE_UNSPECIFIED, uipb.ShellMode_SHELL_MODE_PIPE:
		shellMode = protocol.ShellModePipe
	case uipb.ShellMode_SHELL_MODE_PTY:
		shellMode = protocol.ShellModePTY
	default:
		return nil, status.Errorf(codes.InvalidArgument, "unsupported shell mode: %v", mode)
	}
	ops := map[string]string{"mode": strconv.Itoa(int(shellMode))}
	handle := buildProxyStreamHandle(target, sessionID, "shell", ops)
	return &uipb.StartShellResponse{Handle: handle}, nil
}

func (s *service) StartSocksProxy(ctx context.Context, req *uipb.StartSocksProxyRequest) (*uipb.StartSocksProxyResponse, error) {
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
	opts := map[string]string{}
	switch req.GetAuth() {
	case uipb.SocksProxyAuth_SOCKS_PROXY_AUTH_UNSPECIFIED, uipb.SocksProxyAuth_SOCKS_PROXY_AUTH_NONE:
		opts["auth"] = "none"
	case uipb.SocksProxyAuth_SOCKS_PROXY_AUTH_USERPASS:
		username := strings.TrimSpace(req.GetUsername())
		password := req.GetPassword()
		if username == "" {
			return nil, status.Error(codes.InvalidArgument, "username required for userpass auth")
		}
		if password == "" {
			return nil, status.Error(codes.InvalidArgument, "password required for userpass auth")
		}
		opts["auth"] = "userpass"
		opts["username"] = username
		opts["password"] = password
	default:
		return nil, status.Errorf(codes.InvalidArgument, "unsupported socks auth method: %v", req.GetAuth())
	}
	return &uipb.StartSocksProxyResponse{
		Handle: buildProxyStreamHandle(target, uuid.NewString(), "socks", opts),
	}, nil
}

func (s *service) StartSshSession(ctx context.Context, req *uipb.StartSshSessionRequest) (*uipb.StartSshSessionResponse, error) {
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
	serverAddr := strings.TrimSpace(req.GetServerAddr())
	if serverAddr == "" {
		return nil, status.Error(codes.InvalidArgument, "missing server address")
	}
	username := strings.TrimSpace(req.GetUsername())
	if username == "" {
		return nil, status.Error(codes.InvalidArgument, "missing username")
	}

	authMethod := req.GetAuthMethod()
	if authMethod == uipb.SshSessionAuthMethod_SSH_SESSION_AUTH_METHOD_UNSPECIFIED {
		authMethod = uipb.SshSessionAuthMethod_SSH_SESSION_AUTH_METHOD_PASSWORD
	}
	opts := map[string]string{
		"addr":     serverAddr,
		"username": username,
	}
	switch authMethod {
	case uipb.SshSessionAuthMethod_SSH_SESSION_AUTH_METHOD_PASSWORD:
		password := req.GetPassword()
		if password == "" {
			return nil, status.Error(codes.InvalidArgument, "missing password")
		}
		opts["method"] = "1"
		opts["password"] = password
	default:
		return nil, status.Errorf(codes.InvalidArgument, "unsupported auth method: %v", authMethod)
	}

	return &uipb.StartSshSessionResponse{
		Handle: buildProxyStreamHandle(target, uuid.NewString(), "ssh", opts),
	}, nil
}

func convertLootCategory(cat process.LootCategory) uipb.LootCategory {
	switch cat {
	case process.LootCategoryFile:
		return uipb.LootCategory_LOOT_CATEGORY_FILE
	case process.LootCategoryScreenshot:
		return uipb.LootCategory_LOOT_CATEGORY_SCREENSHOT
	case process.LootCategoryTicket:
		return uipb.LootCategory_LOOT_CATEGORY_TICKET
	default:
		return uipb.LootCategory_LOOT_CATEGORY_UNSPECIFIED
	}
}

func convertProtoLootCategory(cat uipb.LootCategory) process.LootCategory {
	switch cat {
	case uipb.LootCategory_LOOT_CATEGORY_FILE:
		return process.LootCategoryFile
	case uipb.LootCategory_LOOT_CATEGORY_SCREENSHOT:
		return process.LootCategoryScreenshot
	case uipb.LootCategory_LOOT_CATEGORY_TICKET:
		return process.LootCategoryTicket
	default:
		return ""
	}
}

func normalizeTags(tags []string) []string {
	var result []string
	seen := make(map[string]struct{})
	for _, t := range tags {
		t = strings.TrimSpace(strings.ToLower(t))
		if t == "" {
			continue
		}
		if _, ok := seen[t]; ok {
			continue
		}
		seen[t] = struct{}{}
		result = append(result, t)
	}
	return result
}

func buildLootItem(rec process.LootRecord) *uipb.LootItem {
	item := &uipb.LootItem{
		LootId:     rec.ID,
		TargetUuid: rec.TargetUUID,
		Operator:   rec.Operator,
		Category:   convertLootCategory(rec.Category),
		Name:       rec.Name,
		StorageRef: rec.StorageRef,
		OriginPath: rec.OriginPath,
		Hash:       rec.Hash,
		Size:       rec.Size,
		Mime:       rec.Mime,
		Metadata:   rec.Metadata,
		Tags:       rec.Tags,
	}
	if !rec.CreatedAt.IsZero() {
		item.CreatedAt = rec.CreatedAt.UTC().Format(time.RFC3339Nano)
	}
	return item
}

func (s *service) broadcastLootAdded(rec process.LootRecord) {
	if s == nil {
		return
	}
	s.broadcast(&uipb.UiEvent{
		Payload: &uipb.UiEvent_LootEvent{
			LootEvent: &uipb.LootEvent{
				Kind: uipb.LootEvent_LOOT_EVENT_ADDED,
				Item: buildLootItem(rec),
			},
		},
	})
}

func (s *service) ListLoot(ctx context.Context, req *uipb.ListLootRequest) (*uipb.ListLootResponse, error) {
	if s == nil || s.admin == nil {
		return nil, status.Error(codes.Unavailable, "admin unavailable")
	}
	limit := int(req.GetLimit())
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	filter := process.LootFilter{
		TargetUUID: strings.TrimSpace(req.GetTargetUuid()),
		Limit:      limit,
		BeforeID:   strings.TrimSpace(req.GetBeforeId()),
		Tags:       normalizeTags(req.GetTags()),
	}
	if req.GetCategory() != uipb.LootCategory_LOOT_CATEGORY_UNSPECIFIED {
		filter.Category = convertProtoLootCategory(req.GetCategory())
	}
	records := s.admin.ListLoot(filter)
	resp := &uipb.ListLootResponse{}
	for _, rec := range records {
		resp.Items = append(resp.Items, buildLootItem(rec))
	}
	return resp, nil
}

func (s *service) SubmitLoot(ctx context.Context, req *uipb.SubmitLootRequest) (*uipb.SubmitLootResponse, error) {
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
	name := strings.TrimSpace(req.GetName())
	if name == "" {
		return nil, status.Error(codes.InvalidArgument, "name required")
	}
	content := req.GetContent()
	storageRef := strings.TrimSpace(req.GetStorageRef())
	if len(content) == 0 && storageRef == "" {
		return nil, status.Error(codes.InvalidArgument, "content or storage_ref required")
	}
	category := convertProtoLootCategory(req.GetCategory())
	operator := s.currentOperator(ctx)
	rec := process.LootRecord{
		TargetUUID: target,
		Operator:   operator,
		Category:   category,
		Name:       name,
		StorageRef: storageRef,
		OriginPath: strings.TrimSpace(req.GetOriginPath()),
		Hash:       strings.TrimSpace(req.GetHash()),
		Size:       req.GetSize(),
		Mime:       strings.TrimSpace(req.GetMime()),
		Metadata:   req.GetMetadata(),
		Tags:       normalizeTags(req.GetTags()),
	}
	saved, err := s.admin.SubmitLoot(rec, content)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "submit loot failed: %v", err)
	}
	s.broadcastLootAdded(saved)
	return &uipb.SubmitLootResponse{Item: buildLootItem(saved)}, nil
}

func (s *service) GetLoot(ctx context.Context, req *uipb.GetLootRequest) (*uipb.GetLootResponse, error) {
	if s == nil || s.admin == nil {
		return nil, status.Error(codes.Unavailable, "admin unavailable")
	}
	if req == nil || strings.TrimSpace(req.GetLootId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "loot_id required")
	}
	rec, content, err := s.admin.GetLootContent(req.GetLootId())
	if err != nil {
		if rec.ID == "" {
			return nil, status.Errorf(codes.NotFound, "get loot: %v", err)
		}
		return nil, status.Errorf(codes.Internal, "get loot content: %v", err)
	}
	return &uipb.GetLootResponse{Item: buildLootItem(rec), Content: content}, nil
}

func (s *service) ListPivotListeners(ctx context.Context, req *uipb.ListPivotListenersRequest) (*uipb.ListPivotListenersResponse, error) {
	if s == nil || s.admin == nil {
		return nil, status.Error(codes.Unavailable, "admin unavailable")
	}
	filter := process.ListenerFilter{}
	if req != nil {
		filter.Targets = req.GetTargetUuids()
		filter.Protocols = req.GetProtocols()
		for _, st := range req.GetStatuses() {
			filter.Statuses = append(filter.Statuses, process.ListenerStatus(strings.ToLower(strings.TrimSpace(st))))
		}
	}
	items := s.admin.ListListeners(filter)
	return &uipb.ListPivotListenersResponse{Listeners: convertPivotListeners(items)}, nil
}

func (s *service) CreatePivotListener(ctx context.Context, req *uipb.CreatePivotListenerRequest) (*uipb.CreatePivotListenerResponse, error) {
	if s == nil || s.admin == nil {
		return nil, status.Error(codes.Unavailable, "admin unavailable")
	}
	if req == nil || req.GetSpec() == nil {
		return nil, status.Error(codes.InvalidArgument, "missing request")
	}
	mode, err := mapPivotListenerMode(req.GetSpec().GetMode())
	if err != nil {
		return nil, err
	}
	rec, err := s.admin.CreateListener(ctx, req.GetTargetUuid(), process.ListenerSpec{
		Protocol:    req.GetSpec().GetProtocol(),
		Bind:        req.GetSpec().GetBind(),
		Mode:        mode,
		Metadata:    req.GetSpec().GetMetadata(),
		AuthPayload: req.GetSpec().GetAuthPayload(),
	})
	if err != nil {
		return nil, err
	}
	listener := convertPivotListener(rec)
	s.broadcastPivotListenerEvent(uipb.PivotListenerEvent_PIVOT_LISTENER_ADDED, listener)
	return &uipb.CreatePivotListenerResponse{Listener: listener}, nil
}

func (s *service) UpdatePivotListener(ctx context.Context, req *uipb.UpdatePivotListenerRequest) (*uipb.UpdatePivotListenerResponse, error) {
	if s == nil || s.admin == nil {
		return nil, status.Error(codes.Unavailable, "admin unavailable")
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "missing request")
	}
	var spec *process.ListenerSpec
	if req.GetSpec() != nil {
		mode, err := mapPivotListenerMode(req.GetSpec().GetMode())
		if err != nil {
			return nil, err
		}
		spec = &process.ListenerSpec{
			Protocol:    req.GetSpec().GetProtocol(),
			Bind:        req.GetSpec().GetBind(),
			Mode:        mode,
			Metadata:    req.GetSpec().GetMetadata(),
			AuthPayload: req.GetSpec().GetAuthPayload(),
		}
	}
	rec, err := s.admin.UpdateListener(ctx, req.GetListenerId(), spec, req.GetDesiredStatus())
	if err != nil {
		return nil, err
	}
	listener := convertPivotListener(rec)
	s.broadcastPivotListenerEvent(uipb.PivotListenerEvent_PIVOT_LISTENER_UPDATED, listener)
	return &uipb.UpdatePivotListenerResponse{Listener: listener}, nil
}

func (s *service) DeletePivotListener(ctx context.Context, req *uipb.DeletePivotListenerRequest) (*uipb.DeletePivotListenerResponse, error) {
	if s == nil || s.admin == nil {
		return nil, status.Error(codes.Unavailable, "admin unavailable")
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "missing request")
	}
	rec, err := s.admin.DeleteListener(req.GetListenerId())
	if err != nil {
		return nil, err
	}
	s.broadcastPivotListenerEvent(uipb.PivotListenerEvent_PIVOT_LISTENER_REMOVED, convertPivotListener(rec))
	return &uipb.DeletePivotListenerResponse{}, nil
}

func (s *service) ListControllerListeners(ctx context.Context, _ *uipb.ListControllerListenersRequest) (*uipb.ListControllerListenersResponse, error) {
	if s == nil || s.admin == nil {
		return nil, status.Error(codes.Unavailable, "admin unavailable")
	}
	records := s.admin.ListControllerListeners()
	return &uipb.ListControllerListenersResponse{Listeners: convertControllerListeners(records)}, nil
}

func (s *service) CreateControllerListener(ctx context.Context, req *uipb.CreateControllerListenerRequest) (*uipb.CreateControllerListenerResponse, error) {
	if s == nil || s.admin == nil {
		return nil, status.Error(codes.Unavailable, "admin unavailable")
	}
	if req == nil || req.GetSpec() == nil {
		return nil, status.Error(codes.InvalidArgument, "missing request")
	}
	spec := controllerListenerSpecFromProto(req.GetSpec())
	rec, err := s.admin.CreateControllerListener(spec)
	if err != nil {
		return nil, err
	}
	// 新增监听器后立即尝试绑定，以免需要手动重启 Kelpie。
	s.admin.StartControllerListenerAsync(rec.ID)
	return &uipb.CreateControllerListenerResponse{Listener: convertControllerListener(rec)}, nil
}

func (s *service) UpdateControllerListener(ctx context.Context, req *uipb.UpdateControllerListenerRequest) (*uipb.UpdateControllerListenerResponse, error) {
	if s == nil || s.admin == nil {
		return nil, status.Error(codes.Unavailable, "admin unavailable")
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "missing request")
	}
	var spec *process.ControllerListenerSpec
	if req.GetSpec() != nil {
		value := controllerListenerSpecFromProto(req.GetSpec())
		spec = &value
	}
	desired := convertProtoControllerListenerStatus(req.GetDesiredStatus())
	var rec process.ControllerListenerRecord
	var err error
	if desired == process.ControllerListenerStopped {
		rec, err = s.admin.StopControllerListener(req.GetListenerId())
	} else {
		rec, err = s.admin.UpdateControllerListener(req.GetListenerId(), spec, desired)
	}
	if err != nil {
		return nil, err
	}
	// 若更新为 pending/running 或修改了绑定，尝试重新启动监听。
	if rec.Status == process.ControllerListenerPending || rec.Status == process.ControllerListenerRunning {
		s.admin.StartControllerListenerAsync(rec.ID)
	}
	return &uipb.UpdateControllerListenerResponse{Listener: convertControllerListener(rec)}, nil
}

func (s *service) DeleteControllerListener(ctx context.Context, req *uipb.DeleteControllerListenerRequest) (*uipb.DeleteControllerListenerResponse, error) {
	if s == nil || s.admin == nil {
		return nil, status.Error(codes.Unavailable, "admin unavailable")
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "missing request")
	}
	rec, err := s.admin.DeleteControllerListener(req.GetListenerId())
	if err != nil {
		return nil, err
	}
	return &uipb.DeleteControllerListenerResponse{Listener: convertControllerListener(rec)}, nil
}

func (s *service) StartDial(ctx context.Context, req *uipb.StartDialRequest) (*uipb.StartDialResponse, error) {
	if s == nil || (s.admin == nil && s.connectNodeOverride == nil) {
		return nil, status.Error(codes.Unavailable, "admin unavailable")
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "missing request")
	}
	target := strings.TrimSpace(req.GetTargetUuid())
	addr := strings.TrimSpace(req.GetAddress())
	if target == "" || addr == "" {
		return nil, status.Error(codes.InvalidArgument, "missing target uuid or address")
	}
	reason := strings.TrimSpace(req.GetReason())
	dialID := uuid.NewString()
	dialCtx, cancel := context.WithCancel(context.Background())
	tracker := &dialTracker{
		id:       dialID,
		target:   target,
		address:  addr,
		reason:   reason,
		operator: s.currentOperator(ctx),
		state:    uipb.DialState_DIAL_STATE_ENQUEUED,
		started:  time.Now().UTC(),
		updated:  time.Now().UTC(),
		ctx:      dialCtx,
		cancel:   cancel,
	}
	s.dialMu.Lock()
	if s.dials == nil {
		s.dials = make(map[string]*dialTracker)
	}
	s.dials[dialID] = tracker
	snapshot := tracker.snapshot()
	s.dialMu.Unlock()
	s.broadcastDialEvent(uipb.DialEvent_DIAL_EVENT_ENQUEUED, snapshot)
	go s.executeDial(tracker)
	return &uipb.StartDialResponse{
		DialId:   dialID,
		Accepted: true,
		Message:  "dial scheduled",
	}, nil
}

func (s *service) CancelDial(ctx context.Context, req *uipb.CancelDialRequest) (*uipb.CancelDialResponse, error) {
	if s == nil {
		return nil, status.Error(codes.Unavailable, "service unavailable")
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "missing request")
	}
	dialID := strings.TrimSpace(req.GetDialId())
	if dialID == "" {
		return nil, status.Error(codes.InvalidArgument, "missing dial id")
	}
	// dialMu protects both the dials map and the tracker fields; take a snapshot of
	// state/cancel under the lock to avoid races with executeDial/updateDialState.
	s.dialMu.Lock()
	tracker := s.dials[dialID]
	state := uipb.DialState_DIAL_STATE_UNSPECIFIED
	cancel := (context.CancelFunc)(nil)
	if tracker != nil {
		state = tracker.state
		cancel = tracker.cancel
	}
	s.dialMu.Unlock()
	if tracker == nil {
		return &uipb.CancelDialResponse{Canceled: false, Error: "dial not found"}, nil
	}
	if isDialTerminal(state) {
		return &uipb.CancelDialResponse{Canceled: false, Error: "dial already completed"}, nil
	}
	if cancel != nil {
		cancel()
	}
	return &uipb.CancelDialResponse{Canceled: true}, nil
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
	filter := audit.Filter{}
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

func (s *service) ListDials(ctx context.Context, _ *uipb.ListDialRequest) (*uipb.ListDialResponse, error) {
	if s == nil {
		return nil, status.Error(codes.Unavailable, "service unavailable")
	}
	s.dialMu.Lock()
	items := make([]*uipb.DialStatus, 0, len(s.dials))
	for _, tracker := range s.dials {
		items = append(items, convertDialStatus(tracker.snapshot()))
	}
	s.dialMu.Unlock()
	return &uipb.ListDialResponse{Dials: items}, nil
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

func (s *service) ListSessions(ctx context.Context, req *uipb.ListSessionsRequest) (*uipb.ListSessionsResponse, error) {
	if s == nil || s.admin == nil {
		return nil, status.Error(codes.Unavailable, "admin unavailable")
	}
	filter := process.SessionFilter{}
	if req != nil {
		filter.Targets = req.GetTargetUuids()
		for _, st := range req.GetStatuses() {
			filter.Statuses = append(filter.Statuses, mapProtoSessionStatus(st))
		}
		filter.IncludeInactive = req.GetIncludeInactive()
	}
	var sessions []process.SessionInfo
	if s.sessionsOverride != nil {
		sessions = s.sessionsOverride(filter)
	} else {
		sessions = s.admin.Sessions(filter)
	}
	return &uipb.ListSessionsResponse{Sessions: convertSessions(sessions)}, nil
}

func (s *service) MarkSession(ctx context.Context, req *uipb.MarkSessionRequest) (*uipb.MarkSessionResponse, error) {
	if s == nil || s.admin == nil {
		return nil, status.Error(codes.Unavailable, "admin unavailable")
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "missing request")
	}
	action := req.GetAction()
	if action == uipb.SessionMarkAction_SESSION_MARK_ACTION_UNSPECIFIED {
		return nil, status.Error(codes.InvalidArgument, "missing mark action")
	}
	info, err := s.admin.MarkSession(req.GetTargetUuid(), mapProtoSessionMarkAction(action), "", strings.TrimSpace(req.GetReason()))
	if err != nil {
		return nil, mapSessionError(err)
	}
	converted := convertSession(info)
	s.broadcastSessionEvent(uipb.SessionEvent_SESSION_EVENT_MARKED, converted, req.GetReason(), s.currentOperator(ctx))
	return &uipb.MarkSessionResponse{Session: converted}, nil
}

func (s *service) RepairSession(ctx context.Context, req *uipb.RepairSessionRequest) (*uipb.RepairSessionResponse, error) {
	if s == nil || s.admin == nil {
		return nil, status.Error(codes.Unavailable, "admin unavailable")
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "missing request")
	}
	info, err := s.admin.RepairSession(req.GetTargetUuid(), req.GetForce(), strings.TrimSpace(req.GetReason()))
	if err != nil {
		return nil, mapSessionError(err)
	}
	s.broadcastSessionEvent(uipb.SessionEvent_SESSION_EVENT_REPAIR_STARTED, convertSession(info), req.GetReason(), s.currentOperator(ctx))
	return &uipb.RepairSessionResponse{Enqueued: true, Message: "repair requested"}, nil
}

func (s *service) ReconnectSession(ctx context.Context, req *uipb.ReconnectSessionRequest) (*uipb.ReconnectSessionResponse, error) {
	if s == nil || s.admin == nil {
		return nil, status.Error(codes.Unavailable, "admin unavailable")
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "missing request")
	}
	info, err := s.admin.ReconnectSession(req.GetTargetUuid(), strings.TrimSpace(req.GetReason()))
	if err != nil {
		return nil, mapSessionError(err)
	}
	s.broadcastSessionEvent(uipb.SessionEvent_SESSION_EVENT_UPDATED, convertSession(info), req.GetReason(), s.currentOperator(ctx))
	return &uipb.ReconnectSessionResponse{Accepted: true, Message: "reconnect triggered"}, nil
}

func (s *service) TerminateSession(ctx context.Context, req *uipb.TerminateSessionRequest) (*uipb.TerminateSessionResponse, error) {
	if s == nil || s.admin == nil {
		return nil, status.Error(codes.Unavailable, "admin unavailable")
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "missing request")
	}
	var (
		ok  bool
		err error
	)
	if s.terminateSessionOverride != nil {
		ok, err = s.terminateSessionOverride(req.GetTargetUuid(), strings.TrimSpace(req.GetReason()))
	} else {
		ok, err = s.admin.TerminateSession(req.GetTargetUuid(), strings.TrimSpace(req.GetReason()))
	}
	if err != nil {
		return nil, mapSessionError(err)
	}
	if ok {
		s.broadcastSessionEvent(uipb.SessionEvent_SESSION_EVENT_TERMINATED, convertSession(process.SessionInfo{TargetUUID: req.GetTargetUuid()}), req.GetReason(), s.currentOperator(ctx))
	}
	return &uipb.TerminateSessionResponse{Terminated: ok}, nil
}

func (s *service) GetSessionDiagnostics(ctx context.Context, req *uipb.SessionDiagnosticsRequest) (*uipb.SessionDiagnosticsResponse, error) {
	if s == nil || s.admin == nil {
		return nil, status.Error(codes.Unavailable, "admin unavailable")
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "missing request")
	}
	diag, err := s.admin.SessionDiagnostics(req.GetTargetUuid(), req.GetIncludeProcesses(), req.GetIncludeMetrics())
	if err != nil {
		return nil, mapSessionError(err)
	}
	return convertSessionDiagnostics(diag), nil
}

func (s *service) StartForwardProxy(ctx context.Context, req *uipb.StartForwardProxyRequest) (*uipb.StartForwardProxyResponse, error) {
	if s == nil || (s.admin == nil && s.startForwardProxyOverride == nil) {
		return nil, status.Error(codes.Unavailable, "admin unavailable")
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "missing request")
	}
	var (
		desc *process.ProxyDescriptor
		err  error
	)
	if s.startForwardProxyOverride != nil {
		desc, err = s.startForwardProxyOverride(ctx, req.GetTargetUuid(), req.GetLocalBind(), req.GetRemoteAddr())
	} else {
		desc, err = s.admin.StartForwardProxy(ctx, req.GetTargetUuid(), req.GetLocalBind(), req.GetRemoteAddr())
	}
	if err != nil {
		return nil, mapProxyError(err)
	}
	options := desc.Options()
	h := buildProxyStreamHandle(desc.Target(), "", desc.Kind(), options)
	s.broadcastProxyEvent(uipb.ProxyEvent_PROXY_EVENT_STARTED, convertProxyDescriptor(desc), "")
	return &uipb.StartForwardProxyResponse{
		Handle:     h,
		ProxyId:    desc.ID(),
		Bind:       options["bind"],
		RemoteAddr: options["remote"],
	}, nil
}

func (s *service) StopForwardProxy(ctx context.Context, req *uipb.StopForwardProxyRequest) (*uipb.StopForwardProxyResponse, error) {
	if s == nil || (s.admin == nil && s.stopForwardProxyOverride == nil) {
		return nil, status.Error(codes.Unavailable, "admin unavailable")
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "missing request")
	}
	var (
		descs []*process.ProxyDescriptor
		err   error
	)
	if s.stopForwardProxyOverride != nil {
		descs, err = s.stopForwardProxyOverride(req.GetTargetUuid(), req.GetProxyId())
	} else {
		descs, err = s.admin.StopForwardProxy(req.GetTargetUuid(), req.GetProxyId())
	}
	if err != nil {
		return nil, mapProxyError(err)
	}
	for _, desc := range descs {
		s.broadcastProxyEvent(uipb.ProxyEvent_PROXY_EVENT_STOPPED, convertProxyDescriptor(desc), "")
	}
	return &uipb.StopForwardProxyResponse{Stopped: int32(len(descs))}, nil
}

func (s *service) StartBackwardProxy(ctx context.Context, req *uipb.StartBackwardProxyRequest) (*uipb.StartBackwardProxyResponse, error) {
	if s == nil || (s.admin == nil && s.startBackwardProxyOverride == nil) {
		return nil, status.Error(codes.Unavailable, "admin unavailable")
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "missing request")
	}
	var (
		desc *process.ProxyDescriptor
		err  error
	)
	if s.startBackwardProxyOverride != nil {
		desc, err = s.startBackwardProxyOverride(ctx, req.GetTargetUuid(), req.GetRemotePort(), req.GetLocalPort())
	} else {
		desc, err = s.admin.StartBackwardProxy(ctx, req.GetTargetUuid(), req.GetRemotePort(), req.GetLocalPort())
	}
	if err != nil {
		return nil, mapProxyError(err)
	}
	options := desc.Options()
	h := buildProxyStreamHandle(desc.Target(), "", desc.Kind(), options)
	s.broadcastProxyEvent(uipb.ProxyEvent_PROXY_EVENT_STARTED, convertProxyDescriptor(desc), "")
	return &uipb.StartBackwardProxyResponse{
		Handle:     h,
		ProxyId:    desc.ID(),
		RemotePort: options["remote_port"],
		LocalPort:  options["local_port"],
	}, nil
}

func (s *service) StopBackwardProxy(ctx context.Context, req *uipb.StopBackwardProxyRequest) (*uipb.StopBackwardProxyResponse, error) {
	if s == nil || (s.admin == nil && s.stopBackwardProxyOverride == nil) {
		return nil, status.Error(codes.Unavailable, "admin unavailable")
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "missing request")
	}
	var (
		descs []*process.ProxyDescriptor
		err   error
	)
	if s.stopBackwardProxyOverride != nil {
		descs, err = s.stopBackwardProxyOverride(req.GetTargetUuid(), req.GetProxyId())
	} else {
		descs, err = s.admin.StopBackwardProxy(req.GetTargetUuid(), req.GetProxyId())
	}
	if err != nil {
		return nil, mapProxyError(err)
	}
	for _, desc := range descs {
		s.broadcastProxyEvent(uipb.ProxyEvent_PROXY_EVENT_STOPPED, convertProxyDescriptor(desc), "")
	}
	return &uipb.StopBackwardProxyResponse{Stopped: int32(len(descs))}, nil
}

func (s *service) ListProxies(ctx context.Context, _ *uipb.ListProxiesRequest) (*uipb.ListProxiesResponse, error) {
	if s == nil || (s.admin == nil && s.listProxiesOverride == nil) {
		return nil, status.Error(codes.Unavailable, "admin unavailable")
	}
	var descs []*process.ProxyDescriptor
	if s.listProxiesOverride != nil {
		descs = s.listProxiesOverride()
	} else {
		descs = s.admin.ListProxies()
	}
	proxies := make([]*uipb.ProxyInfo, 0, len(descs))
	for _, desc := range descs {
		if info := convertProxyDescriptor(desc); info != nil {
			proxies = append(proxies, info)
		}
	}
	sort.Slice(proxies, func(i, j int) bool {
		if proxies[i].GetTargetUuid() == proxies[j].GetTargetUuid() {
			return proxies[i].GetProxyId() < proxies[j].GetProxyId()
		}
		return proxies[i].GetTargetUuid() < proxies[j].GetTargetUuid()
	})
	return &uipb.ListProxiesResponse{Proxies: proxies}, nil
}

func (s *service) CloseStream(ctx context.Context, req *uipb.CloseStreamRequest) (*uipb.CloseStreamResponse, error) {
	if s == nil {
		return nil, status.Error(codes.Unavailable, "admin unavailable")
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "missing request")
	}
	if req.GetStreamId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "stream id required")
	}
	var err error
	if s.closeStreamOverride != nil {
		err = s.closeStreamOverride(req.GetStreamId(), strings.TrimSpace(req.GetReason()))
	} else {
		if s.admin == nil {
			return nil, status.Error(codes.Unavailable, "admin unavailable")
		}
		err = s.admin.CloseStream(req.GetStreamId(), strings.TrimSpace(req.GetReason()))
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "close stream failed: %v", err)
	}
	return &uipb.CloseStreamResponse{}, nil
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

func (s *service) StreamStats(ctx context.Context, _ *uipb.StreamStatsRequest) (*uipb.StreamStatsResponse, error) {
	if s == nil || s.admin == nil {
		return nil, status.Error(codes.Unavailable, "admin unavailable")
	}
	stats := s.admin.StreamStats()
	resp := &uipb.StreamStatsResponse{Stats: make([]*uipb.StreamStatInfo, 0, len(stats))}
	for _, st := range stats {
		info := &uipb.StreamStatInfo{
			Kind:       st.Kind,
			Opened:     st.Opened,
			Closed:     st.Closed,
			Active:     st.Active,
			LastReason: st.LastReason,
		}
		if !st.LastClosed.IsZero() {
			info.LastClosed = st.LastClosed.UTC().Format(time.RFC3339Nano)
		}
		resp.Stats = append(resp.Stats, info)
	}
	return resp, nil
}

func (s *service) StreamDiagnostics(ctx context.Context, _ *uipb.StreamDiagnosticsRequest) (*uipb.StreamDiagnosticsResponse, error) {
	if s == nil || s.admin == nil {
		return nil, status.Error(codes.Unavailable, "admin unavailable")
	}
	diags := s.admin.StreamDiagnostics()
	resp := &uipb.StreamDiagnosticsResponse{Streams: make([]*uipb.StreamDiag, 0, len(diags))}
	for _, diag := range diags {
		resp.Streams = append(resp.Streams, convertStreamDiag(diag))
	}
	return resp, nil
}

func (s *service) StreamPing(ctx context.Context, req *uipb.StreamPingRequest) (*uipb.StreamPingResponse, error) {
	if s == nil || s.admin == nil {
		return nil, status.Error(codes.Unavailable, "admin unavailable")
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "missing request")
	}
	count := int(req.GetCount())
	size := int(req.GetPayloadSize())
	if err := s.admin.StreamPing(req.GetTargetUuid(), count, size); err != nil {
		return nil, mapStreamPingError(err)
	}
	return &uipb.StreamPingResponse{}, nil
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

func (s *service) broadcastPivotListenerEvent(kind uipb.PivotListenerEvent_Kind, listener *uipb.PivotListener) {
	if listener == nil {
		return
	}
	s.broadcast(&uipb.UiEvent{
		Payload: &uipb.UiEvent_ListenerEvent{
			ListenerEvent: &uipb.PivotListenerEvent{
				Kind:     kind,
				Listener: listener,
			},
		},
	})
}

func (s *service) broadcastSessionEvent(kind uipb.SessionEvent_Kind, session *uipb.SessionInfo, reason, operator string) {
	if session == nil {
		return
	}
	s.broadcast(&uipb.UiEvent{
		Payload: &uipb.UiEvent_SessionEvent{
			SessionEvent: &uipb.SessionEvent{
				Kind:     kind,
				Session:  session,
				Reason:   reason,
				Operator: operator,
			},
		},
	})
}

func (s *service) broadcastDialEvent(kind uipb.DialEvent_Kind, tracker *dialTracker) {
	if tracker == nil {
		return
	}
	status := convertDialStatus(tracker)
	if status == nil {
		return
	}
	s.broadcast(&uipb.UiEvent{
		Payload: &uipb.UiEvent_DialEvent{
			DialEvent: &uipb.DialEvent{
				Kind:   kind,
				Status: status,
			},
		},
	})
}

func (s *service) broadcastProxyEvent(kind uipb.ProxyEvent_Kind, info *uipb.ProxyInfo, reason string) {
	if info == nil {
		return
	}
	s.broadcast(&uipb.UiEvent{
		Payload: &uipb.UiEvent_ProxyEvent{
			ProxyEvent: &uipb.ProxyEvent{
				Kind:   kind,
				Proxy:  info,
				Reason: reason,
			},
		},
	})
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

func convertPivotListeners(records []process.ListenerRecord) []*uipb.PivotListener {
	if len(records) == 0 {
		return nil
	}
	out := make([]*uipb.PivotListener, 0, len(records))
	for _, rec := range records {
		out = append(out, convertPivotListener(rec))
	}
	return out
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

func convertPivotListener(rec process.ListenerRecord) *uipb.PivotListener {
	meta := make(map[string]string, len(rec.Metadata))
	for k, v := range rec.Metadata {
		meta[k] = v
	}
	created := ""
	if !rec.CreatedAt.IsZero() {
		created = rec.CreatedAt.UTC().Format(time.RFC3339Nano)
	}
	updated := ""
	if !rec.UpdatedAt.IsZero() {
		updated = rec.UpdatedAt.UTC().Format(time.RFC3339Nano)
	}
	return &uipb.PivotListener{
		ListenerId: rec.ID,
		TargetUuid: rec.TargetUUID,
		Route:      rec.Route,
		Protocol:   rec.Protocol,
		Bind:       rec.Bind,
		Mode:       protoPivotListenerMode(rec.Mode),
		Status:     string(rec.Status),
		LastError:  rec.LastError,
		Metadata:   meta,
		CreatedAt:  created,
		UpdatedAt:  updated,
	}
}

func controllerListenerSpecFromProto(spec *uipb.ControllerListenerSpec) process.ControllerListenerSpec {
	if spec == nil {
		return process.ControllerListenerSpec{}
	}
	protocol := strings.TrimSpace(spec.GetProtocol())
	if protocol == "" {
		protocol = string(process.ControllerListenerProtocolTCP)
	}
	return process.ControllerListenerSpec{
		Bind:     spec.GetBind(),
		Protocol: process.ControllerListenerProtocol(strings.ToLower(protocol)),
	}
}

func convertControllerListeners(records []process.ControllerListenerRecord) []*uipb.ControllerListener {
	if len(records) == 0 {
		return nil
	}
	out := make([]*uipb.ControllerListener, 0, len(records))
	for _, rec := range records {
		out = append(out, convertControllerListener(rec))
	}
	return out
}

func convertControllerListener(rec process.ControllerListenerRecord) *uipb.ControllerListener {
	created := ""
	updated := ""
	if !rec.CreatedAt.IsZero() {
		created = rec.CreatedAt.UTC().Format(time.RFC3339Nano)
	}
	if !rec.UpdatedAt.IsZero() {
		updated = rec.UpdatedAt.UTC().Format(time.RFC3339Nano)
	}
	return &uipb.ControllerListener{
		ListenerId: rec.ID,
		Bind:       rec.Bind,
		Protocol:   string(rec.Protocol),
		Status:     convertProcessControllerListenerStatus(rec.Status),
		LastError:  rec.LastError,
		CreatedAt:  created,
		UpdatedAt:  updated,
	}
}

func convertProcessControllerListenerStatus(status process.ControllerListenerStatus) uipb.ControllerListenerStatus {
	switch status {
	case process.ControllerListenerPending:
		return uipb.ControllerListenerStatus_CONTROLLER_LISTENER_STATUS_PENDING
	case process.ControllerListenerRunning:
		return uipb.ControllerListenerStatus_CONTROLLER_LISTENER_STATUS_RUNNING
	case process.ControllerListenerFailed:
		return uipb.ControllerListenerStatus_CONTROLLER_LISTENER_STATUS_FAILED
	case process.ControllerListenerStopped:
		return uipb.ControllerListenerStatus_CONTROLLER_LISTENER_STATUS_STOPPED
	default:
		return uipb.ControllerListenerStatus_CONTROLLER_LISTENER_STATUS_UNSPECIFIED
	}
}

func convertProtoControllerListenerStatus(status uipb.ControllerListenerStatus) process.ControllerListenerStatus {
	switch status {
	case uipb.ControllerListenerStatus_CONTROLLER_LISTENER_STATUS_PENDING:
		return process.ControllerListenerPending
	case uipb.ControllerListenerStatus_CONTROLLER_LISTENER_STATUS_RUNNING:
		return process.ControllerListenerRunning
	case uipb.ControllerListenerStatus_CONTROLLER_LISTENER_STATUS_FAILED:
		return process.ControllerListenerFailed
	case uipb.ControllerListenerStatus_CONTROLLER_LISTENER_STATUS_STOPPED:
		return process.ControllerListenerStopped
	default:
		return ""
	}
}

func convertProxyDescriptor(desc *process.ProxyDescriptor) *uipb.ProxyInfo {
	if desc == nil {
		return nil
	}
	info := &uipb.ProxyInfo{
		ProxyId:    desc.ID(),
		TargetUuid: desc.Target(),
		Kind:       desc.Kind(),
		Metadata:   make(map[string]string),
	}
	for k, v := range desc.Options() {
		info.Metadata[k] = v
		switch k {
		case "bind":
			info.Bind = v
		case "remote", "remote_addr":
			info.Remote = v
		case "remote_port":
			if info.Remote == "" {
				info.Remote = v
			}
		}
	}
	return info
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

func (s *service) executeDial(tracker *dialTracker) {
	if tracker == nil {
		return
	}
	snapshot := s.updateDialState(tracker.id, uipb.DialState_DIAL_STATE_RUNNING, "", false)
	s.broadcastDialEvent(uipb.DialEvent_DIAL_EVENT_RUNNING, snapshot)
	err := s.invokeConnectNode(tracker.ctx, tracker.target, tracker.address)
	switch {
	case err == nil:
		snapshot = s.updateDialState(tracker.id, uipb.DialState_DIAL_STATE_SUCCEEDED, "", true)
		s.broadcastDialEvent(uipb.DialEvent_DIAL_EVENT_COMPLETED, snapshot)
	case errors.Is(err, context.Canceled):
		snapshot = s.updateDialState(tracker.id, uipb.DialState_DIAL_STATE_CANCELED, err.Error(), true)
		s.broadcastDialEvent(uipb.DialEvent_DIAL_EVENT_CANCELED, snapshot)
	default:
		snapshot = s.updateDialState(tracker.id, uipb.DialState_DIAL_STATE_FAILED, err.Error(), true)
		s.broadcastDialEvent(uipb.DialEvent_DIAL_EVENT_FAILED, snapshot)
	}
}

func (s *service) updateDialState(id string, state uipb.DialState, errMsg string, remove bool) *dialTracker {
	if s == nil {
		return nil
	}
	s.dialMu.Lock()
	defer s.dialMu.Unlock()
	tracker := s.dials[id]
	if tracker == nil {
		return nil
	}
	tracker.state = state
	tracker.err = errMsg
	tracker.updated = time.Now().UTC()
	snapshot := tracker.snapshot()
	if remove {
		delete(s.dials, id)
	}
	return snapshot
}

func convertDialStatus(tracker *dialTracker) *uipb.DialStatus {
	if tracker == nil {
		return nil
	}
	status := &uipb.DialStatus{
		DialId:     tracker.id,
		TargetUuid: tracker.target,
		Address:    tracker.address,
		Reason:     tracker.reason,
		State:      tracker.state,
		Error:      tracker.err,
		Operator:   tracker.operator,
	}
	if !tracker.started.IsZero() {
		status.StartedAt = tracker.started.UTC().Format(time.RFC3339Nano)
	}
	if !tracker.updated.IsZero() {
		status.UpdatedAt = tracker.updated.UTC().Format(time.RFC3339Nano)
	}
	return status
}

func isDialTerminal(state uipb.DialState) bool {
	switch state {
	case uipb.DialState_DIAL_STATE_SUCCEEDED,
		uipb.DialState_DIAL_STATE_FAILED,
		uipb.DialState_DIAL_STATE_CANCELED:
		return true
	default:
		return false
	}
}

func (s *service) invokeConnectNode(ctx context.Context, target, addr string) error {
	if s != nil && s.connectNodeOverride != nil {
		return s.connectNodeOverride(ctx, target, addr)
	}
	if s == nil || s.admin == nil {
		return fmt.Errorf("admin unavailable")
	}
	return s.admin.ConnectNode(ctx, target, addr)
}

func convertSessions(records []process.SessionInfo) []*uipb.SessionInfo {
	if len(records) == 0 {
		return nil
	}
	out := make([]*uipb.SessionInfo, 0, len(records))
	for _, sess := range records {
		out = append(out, convertSession(sess))
	}
	return out
}

func convertSession(info process.SessionInfo) *uipb.SessionInfo {
	meta := make(map[string]string, len(info.Metadata))
	for k, v := range info.Metadata {
		meta[k] = v
	}
	result := &uipb.SessionInfo{
		TargetUuid:   info.TargetUUID,
		Active:       info.Active,
		Connected:    info.Connected,
		RemoteAddr:   info.RemoteAddr,
		Upstream:     info.Upstream,
		Downstream:   info.Downstream,
		NetworkId:    info.NetworkID,
		Status:       protoSessionStatus(info.Status),
		LastError:    info.LastError,
		LastOperator: info.LastOperator,
		SleepProfile: info.SleepProfile,
		LastCommand:  info.LastCommand,
		Metadata:     meta,
	}
	if !info.LastSeen.IsZero() {
		result.LastSeen = info.LastSeen.UTC().Format(time.RFC3339Nano)
	}
	if info.SleepSeconds != nil {
		val := int32(*info.SleepSeconds)
		result.SleepSeconds = &val
	}
	if info.WorkSeconds != nil {
		val := int32(*info.WorkSeconds)
		result.WorkSeconds = &val
	}
	if info.JitterPercent != nil {
		val := *info.JitterPercent
		result.JitterPercent = &val
	}
	return result
}

func convertSessionDiagnostics(diag process.SessionDiagnostics) *uipb.SessionDiagnosticsResponse {
	resp := &uipb.SessionDiagnosticsResponse{
		Session: convertSession(diag.Session),
	}
	if len(diag.Metrics) > 0 {
		resp.Metrics = make([]*uipb.SessionMetric, 0, len(diag.Metrics))
		for _, metric := range diag.Metrics {
			resp.Metrics = append(resp.Metrics, &uipb.SessionMetric{Name: metric.Name, Value: metric.Value})
		}
	}
	if len(diag.Issues) > 0 {
		resp.Issues = make([]*uipb.SessionIssue, 0, len(diag.Issues))
		for _, issue := range diag.Issues {
			resp.Issues = append(resp.Issues, &uipb.SessionIssue{Code: issue.Code, Message: issue.Message, Detail: issue.Detail})
		}
	}
	if len(diag.Process) > 0 {
		resp.Processes = make([]*uipb.SessionProcess, 0, len(diag.Process))
		for _, proc := range diag.Process {
			started := ""
			if !proc.Since.IsZero() {
				started = proc.Since.UTC().Format(time.RFC3339Nano)
			}
			resp.Processes = append(resp.Processes, &uipb.SessionProcess{
				Pid:       proc.PID,
				Name:      proc.Name,
				User:      proc.User,
				Status:    proc.State,
				Path:      proc.Path,
				StartedAt: started,
			})
		}
	}
	return resp
}

func sessionChanged(prev, next process.SessionInfo) bool {
	if prev.Status != next.Status {
		return true
	}
	if prev.Active != next.Active || prev.Connected != next.Connected {
		return true
	}
	if prev.RemoteAddr != next.RemoteAddr || prev.NetworkID != next.NetworkID {
		return true
	}
	if prev.LastError != next.LastError {
		return true
	}
	if prev.SleepProfile != next.SleepProfile {
		return true
	}
	if !intPtrEqual(prev.SleepSeconds, next.SleepSeconds) || !intPtrEqual(prev.WorkSeconds, next.WorkSeconds) {
		return true
	}
	if !floatPtrEqual(prev.JitterPercent, next.JitterPercent) {
		return true
	}
	if prev.LastCommand != next.LastCommand {
		return true
	}
	return false
}

func intPtrEqual(a, b *int) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
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
	claims, _ := auth.ClaimsFromContext(ctx)
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
	claims, _ := auth.ClaimsFromContext(ctx)
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
	claims, _ := auth.ClaimsFromContext(ctx)
	entry := audit.Entry{
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

func floatPtrEqual(a, b *float64) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

func protoSessionStatus(status process.SessionStatus) uipb.SessionStatus {
	switch status {
	case process.SessionStatusActive:
		return uipb.SessionStatus_SESSION_STATUS_ACTIVE
	case process.SessionStatusDegraded:
		return uipb.SessionStatus_SESSION_STATUS_DEGRADED
	case process.SessionStatusFailed:
		return uipb.SessionStatus_SESSION_STATUS_FAILED
	case process.SessionStatusMarkedDead:
		return uipb.SessionStatus_SESSION_STATUS_MARKED_DEAD
	case process.SessionStatusRepairing:
		return uipb.SessionStatus_SESSION_STATUS_REPAIRING
	default:
		return uipb.SessionStatus_SESSION_STATUS_UNSPECIFIED
	}
}

func mapProtoSessionStatus(status uipb.SessionStatus) process.SessionStatus {
	switch status {
	case uipb.SessionStatus_SESSION_STATUS_ACTIVE:
		return process.SessionStatusActive
	case uipb.SessionStatus_SESSION_STATUS_DEGRADED:
		return process.SessionStatusDegraded
	case uipb.SessionStatus_SESSION_STATUS_FAILED:
		return process.SessionStatusFailed
	case uipb.SessionStatus_SESSION_STATUS_MARKED_DEAD:
		return process.SessionStatusMarkedDead
	case uipb.SessionStatus_SESSION_STATUS_REPAIRING:
		return process.SessionStatusRepairing
	default:
		return process.SessionStatusUnknown
	}
}

func mapProtoSessionMarkAction(action uipb.SessionMarkAction) process.SessionMarkAction {
	switch action {
	case uipb.SessionMarkAction_SESSION_MARK_ACTION_ALIVE:
		return process.SessionMarkAlive
	case uipb.SessionMarkAction_SESSION_MARK_ACTION_DEAD:
		return process.SessionMarkDead
	case uipb.SessionMarkAction_SESSION_MARK_ACTION_MAINTENANCE:
		return process.SessionMarkMaintenance
	default:
		return ""
	}
}

func mapSessionError(err error) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	lower := strings.ToLower(msg)
	if strings.Contains(msg, "missing target") || strings.Contains(msg, "required") {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if strings.Contains(lower, "not found") {
		return status.Error(codes.NotFound, err.Error())
	}
	return status.Errorf(codes.Internal, "%v", err)
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

func mapProxyError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, process.ErrProxyMissingTarget),
		errors.Is(err, process.ErrProxyMissingBind),
		errors.Is(err, process.ErrProxyInvalidRemote),
		errors.Is(err, process.ErrProxyInvalidPort):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, process.ErrProxyNotFound):
		return status.Error(codes.NotFound, err.Error())
	default:
		return status.Errorf(codes.Internal, "proxy operation failed: %v", err)
	}
}

func mapPivotListenerMode(mode uipb.PivotListenerMode) (int, error) {
	switch mode {
	case uipb.PivotListenerMode_PIVOT_LISTENER_MODE_UNSPECIFIED, uipb.PivotListenerMode_PIVOT_LISTENER_MODE_NORMAL:
		return process.ListenerModeNormal, nil
	case uipb.PivotListenerMode_PIVOT_LISTENER_MODE_IPTABLES:
		return process.ListenerModeIptables, nil
	case uipb.PivotListenerMode_PIVOT_LISTENER_MODE_SOREUSE:
		return process.ListenerModeSoReuse, nil
	default:
		return 0, status.Errorf(codes.InvalidArgument, "unsupported listener mode: %v", mode)
	}
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
	claims, ok := auth.ClaimsFromContext(ctx)
	if !ok {
		return ""
	}
	return claims.Username
}

func (s *service) operatorRole(ctx context.Context) string {
	claims, ok := auth.ClaimsFromContext(ctx)
	if !ok {
		return ""
	}
	return claims.Role
}

// currentTenant 返回当前请求所属的租户标识。
// 在 teamserver 模式下，我们直接用 Client Name（即 Claims.Username）
// 作为租户，将审计视图按人隔离。
func (s *service) currentTenant(ctx context.Context) string {
	claims, ok := auth.ClaimsFromContext(ctx)
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

func buildProxyStreamHandle(target, sessionID, kind string, opts map[string]string) *uipb.ProxyStreamHandle {
	clean := sanitizeStreamOptions(opts)
	if clean == nil {
		clean = map[string]string{}
	}
	clean["kind"] = kind
	return &uipb.ProxyStreamHandle{TargetUuid: target, SessionId: sessionID, Kind: kind, Options: clean}
}

func sanitizeStreamOptions(opts map[string]string) map[string]string {
	if len(opts) == 0 {
		return map[string]string{}
	}
	clean := make(map[string]string, len(opts))
	for k, v := range opts {
		key := strings.TrimSpace(k)
		if key == "" {
			continue
		}
		clean[key] = v
	}
	return clean
}

func streamOpenResponse(sessionID, target string, streamID uint32, kind string) *uipb.StreamResponse {
	return &uipb.StreamResponse{
		SessionId:  sessionID,
		TargetUuid: target,
		Control: &uipb.StreamControl{
			Kind:       uipb.StreamControl_OPEN,
			StreamId:   streamID,
			StreamKind: kind,
		},
	}
}

func streamCloseResponse(sessionID, target string, streamID uint32, kind, errMsg string, code uipb.ProxyStreamError) *uipb.StreamResponse {
	ctrl := &uipb.StreamControl{Kind: uipb.StreamControl_CLOSE, StreamId: streamID, StreamKind: kind, ErrorCode: code}
	if strings.TrimSpace(errMsg) != "" {
		ctrl.Kind = uipb.StreamControl_ERROR
		ctrl.Error = errMsg
		if ctrl.ErrorCode == uipb.ProxyStreamError_PROXYSTREAM_ERROR_UNSPECIFIED {
			ctrl.ErrorCode = uipb.ProxyStreamError_PROXYSTREAM_ERROR_REMOTE
		}
	}
	return &uipb.StreamResponse{
		SessionId:  sessionID,
		TargetUuid: target,
		Control:    ctrl,
	}
}
