package grpcserver

import (
	"context"
	"strings"
	"time"

	"codeberg.org/agnoie/shepherd/internal/kelpie/process"
	"codeberg.org/agnoie/shepherd/internal/kelpie/uipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

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
