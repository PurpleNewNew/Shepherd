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
