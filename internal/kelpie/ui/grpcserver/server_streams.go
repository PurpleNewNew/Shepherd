package grpcserver

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"codeberg.org/agnoie/shepherd/internal/kelpie/uipb"
	"codeberg.org/agnoie/shepherd/protocol"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

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
