package grpcserver

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"codeberg.org/agnoie/shepherd/internal/kelpie/process"
	"codeberg.org/agnoie/shepherd/internal/kelpie/uipb"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

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
	// dialMu 同时保护 dials 映射与 tracker 字段；这里在锁内拍下
	// state/cancel 快照，避免与 executeDial/updateDialState 竞争。
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
