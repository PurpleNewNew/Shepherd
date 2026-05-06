package kelpie

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"codeberg.org/agnoie/shepherd/internal/kelpie/uipb"
)

// ConnectOptions 描述一次 gRPC 连接所需的参数。
type ConnectOptions struct {
	Endpoint            string
	Token               string
	UseTLS              bool
	ServerName          string
	ExpectedFingerprint string
	DialTimeout         time.Duration
}

// Client 封装到 Kelpie 的 gRPC 连接以及几个常用 service 的 stub。
// 本客户端被设计为"一次连一个 Kelpie"：Stockman 不会同时持有多个 Kelpie 会话。
type Client struct {
	conn         *grpc.ClientConn
	opts         ConnectOptions
	verifier     *TOFUVerifier
	uiClient     uipb.KelpieUIServiceClient
	supp         uipb.SupplementalAdminServiceClient
	sleepAdmin   uipb.SleepAdminServiceClient
	connectAdmin uipb.ConnectAdminServiceClient
	proxyAdmin   uipb.ProxyAdminServiceClient
	pivot        uipb.PivotListenerAdminServiceClient
	controller   uipb.ControllerListenerAdminServiceClient

	token string
	mu    sync.Mutex
}

// Dial 建立一个新的 Kelpie 客户端。
// 对 TLS 连接，会使用 TOFU 校验；对明文连接，等同于 grpc.WithInsecure。
//
// 返回的 error 语义：
//   - 若 UseTLS 且首次连接未记录指纹，返回 *TOFUDecisionRequired，并保留 verifier 方便上层读取。
//   - 若 UseTLS 且记录指纹与实际不匹配，返回 *TOFUMismatch。
//   - 其它错误按 gRPC 原样返回。
func Dial(ctx context.Context, opts ConnectOptions) (*Client, *TOFUVerifier, error) {
	if strings.TrimSpace(opts.Endpoint) == "" {
		return nil, nil, errors.New("endpoint required")
	}
	if opts.DialTimeout <= 0 {
		opts.DialTimeout = 6 * time.Second
	}
	var (
		tc       credentials.TransportCredentials
		verifier *TOFUVerifier
	)
	if opts.UseTLS {
		serverName := strings.TrimSpace(opts.ServerName)
		if serverName == "" {
			serverName = hostFromEndpoint(opts.Endpoint)
		}
		verifier = NewTOFUVerifier(opts.ExpectedFingerprint)
		tc = credentials.NewTLS(verifier.TLSConfig(serverName))
	} else {
		tc = insecure.NewCredentials()
	}

	dialCtx, cancel := context.WithTimeout(ctx, opts.DialTimeout)
	defer cancel()
	conn, err := grpc.DialContext(
		dialCtx,
		opts.Endpoint,
		grpc.WithTransportCredentials(tc),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, verifier, wrapDialError(err, verifier)
	}

	cli := &Client{
		conn:         conn,
		opts:         opts,
		verifier:     verifier,
		token:        strings.TrimSpace(opts.Token),
		uiClient:     uipb.NewKelpieUIServiceClient(conn),
		supp:         uipb.NewSupplementalAdminServiceClient(conn),
		sleepAdmin:   uipb.NewSleepAdminServiceClient(conn),
		connectAdmin: uipb.NewConnectAdminServiceClient(conn),
		proxyAdmin:   uipb.NewProxyAdminServiceClient(conn),
		pivot:        uipb.NewPivotListenerAdminServiceClient(conn),
		controller:   uipb.NewControllerListenerAdminServiceClient(conn),
	}
	return cli, verifier, nil
}

// wrapDialError 提取被 grpc 封装的 TLS 错误，暴露 TOFU 决策信号给上层。
func wrapDialError(err error, verifier *TOFUVerifier) error {
	if err == nil {
		return nil
	}
	// 检查 gRPC 的 Unavailable 错误中是否包含 TOFU 决策信号。
	if st, ok := status.FromError(err); ok && st.Code() == codes.Unavailable {
		// gRPC 把 VerifyPeerCertificate 的 error 包装在消息里，我们尝试 unwrap。
		var decision *TOFUDecisionRequired
		if errors.As(err, &decision) {
			return decision
		}
		var mismatch *TOFUMismatch
		if errors.As(err, &mismatch) {
			return mismatch
		}
		// 对于某些 TLS 握手失败，gRPC 只留一条 message。若 verifier 有 LastSeen，
		// 我们仍视为待确认，便于 UI 给出友好提示。
		if verifier != nil {
			if fp := verifier.LastSeen(); fp != "" && verifier.expected == "" {
				return &TOFUDecisionRequired{Fingerprint: fp}
			}
		}
	}
	return err
}

// hostFromEndpoint 取 "host:port" 中的 host，用作 TLS SNI。
func hostFromEndpoint(endpoint string) string {
	host := endpoint
	if i := strings.LastIndex(endpoint, ":"); i > 0 {
		host = endpoint[:i]
	}
	host = strings.TrimPrefix(host, "[")
	host = strings.TrimSuffix(host, "]")
	if host == "" {
		host = "localhost"
	}
	return host
}

// Close 关闭底层连接。
func (c *Client) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

// attachAuth 在 gRPC 调用上下文里附上 token。
func (c *Client) attachAuth(ctx context.Context) context.Context {
	if c == nil || strings.TrimSpace(c.token) == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx,
		"authorization", "Bearer "+c.token,
		"x-kelpie-token", c.token,
	)
}

// --- 对外暴露的 RPC 封装 ---

// Snapshot 一次性取回初始快照（nodes/edges/sessions/pivot/controller 等）。
func (c *Client) Snapshot(ctx context.Context) (*uipb.Snapshot, error) {
	resp, err := c.uiClient.GetSnapshot(c.attachAuth(ctx), &uipb.SnapshotRequest{})
	if err != nil {
		return nil, err
	}
	return resp.GetSnapshot(), nil
}

// Topology 返回当前拓扑（节点 + 边 + 最后更新时间）。
func (c *Client) Topology(ctx context.Context) (*uipb.GetTopologyResponse, error) {
	return c.uiClient.GetTopology(c.attachAuth(ctx), &uipb.GetTopologyRequest{})
}

// NodeStatus 查询单节点的详细诊断（streams/listeners）。
func (c *Client) NodeStatus(ctx context.Context, uuid string) (*uipb.NodeStatusResponse, error) {
	return c.uiClient.NodeStatus(c.attachAuth(ctx), &uipb.NodeStatusRequest{TargetUuid: uuid})
}

// Metrics 返回聚合指标（DTN/router/reconnect）。
func (c *Client) Metrics(ctx context.Context) (*uipb.GetMetricsResponse, error) {
	return c.uiClient.GetMetrics(c.attachAuth(ctx), &uipb.GetMetricsRequest{
		IncludeRouter:    true,
		IncludeReconnect: true,
	})
}

// EnqueueDTN 向指定节点投递一条 DTN payload；返回 bundle_id。
func (c *Client) EnqueueDTN(ctx context.Context, target, payload string, priority uipb.DtnPriority, ttlSeconds int64) (string, error) {
	if strings.TrimSpace(target) == "" {
		return "", errors.New("target uuid required")
	}
	resp, err := c.uiClient.EnqueueDtnPayload(c.attachAuth(ctx), &uipb.EnqueueDtnPayloadRequest{
		TargetUuid: target,
		Payload:    payload,
		Priority:   priority,
		TtlSeconds: ttlSeconds,
	})
	if err != nil {
		return "", err
	}
	return resp.GetBundleId(), nil
}

// UpdateSleep 调整节点 sleep 配置（任一字段为负表示不更新）。
func (c *Client) UpdateSleep(ctx context.Context, target string, sleepSec, workSec *int32, jitter *float64) error {
	if strings.TrimSpace(target) == "" {
		return errors.New("target uuid required")
	}
	req := &uipb.UpdateSleepRequest{TargetUuid: target}
	if sleepSec != nil {
		req.SleepSeconds = sleepSec
	}
	if workSec != nil {
		req.WorkSeconds = workSec
	}
	if jitter != nil {
		req.JitterPercent = jitter
	}
	_, err := c.sleepAdmin.UpdateSleep(c.attachAuth(ctx), req)
	return err
}

// PruneOffline 移除长期离线节点；返回删除的数量。
func (c *Client) PruneOffline(ctx context.Context) (int32, error) {
	resp, err := c.uiClient.PruneOffline(c.attachAuth(ctx), &uipb.PruneOfflineRequest{})
	if err != nil {
		return 0, err
	}
	return resp.GetRemoved(), nil
}

func (c *Client) StartShell(ctx context.Context, target, mode, resumeSessionID string) (*uipb.ProxyStreamHandle, error) {
	reqMode := uipb.ShellMode_SHELL_MODE_PIPE
	if strings.EqualFold(strings.TrimSpace(mode), "pty") {
		reqMode = uipb.ShellMode_SHELL_MODE_PTY
	}
	resp, err := c.uiClient.StartShell(c.attachAuth(ctx), &uipb.StartShellRequest{
		TargetUuid:      strings.TrimSpace(target),
		Mode:            reqMode,
		ResumeSessionId: strings.TrimSpace(resumeSessionID),
	})
	if err != nil {
		return nil, err
	}
	return resp.GetHandle(), nil
}

func (c *Client) StartSocksProxy(ctx context.Context, target, auth, username, password string) (*uipb.ProxyStreamHandle, error) {
	reqAuth := uipb.SocksProxyAuth_SOCKS_PROXY_AUTH_NONE
	if strings.EqualFold(strings.TrimSpace(auth), "userpass") {
		reqAuth = uipb.SocksProxyAuth_SOCKS_PROXY_AUTH_USERPASS
	}
	resp, err := c.uiClient.StartSocksProxy(c.attachAuth(ctx), &uipb.StartSocksProxyRequest{
		TargetUuid: strings.TrimSpace(target),
		Auth:       reqAuth,
		Username:   strings.TrimSpace(username),
		Password:   password,
	})
	if err != nil {
		return nil, err
	}
	return resp.GetHandle(), nil
}

func (c *Client) StartSshSession(ctx context.Context, target, serverAddr, username, password string) (*uipb.ProxyStreamHandle, error) {
	resp, err := c.uiClient.StartSshSession(c.attachAuth(ctx), &uipb.StartSshSessionRequest{
		TargetUuid: strings.TrimSpace(target),
		ServerAddr: strings.TrimSpace(serverAddr),
		AuthMethod: uipb.SshSessionAuthMethod_SSH_SESSION_AUTH_METHOD_PASSWORD,
		Username:   strings.TrimSpace(username),
		Password:   password,
	})
	if err != nil {
		return nil, err
	}
	return resp.GetHandle(), nil
}

func (c *Client) StartSSHTunnel(ctx context.Context, target, serverAddr, agentPort, authMethod, username, password string, privateKey []byte) error {
	method := uipb.SshTunnelAuthMethod_SSH_TUNNEL_AUTH_METHOD_PASSWORD
	if strings.EqualFold(strings.TrimSpace(authMethod), "cert") {
		method = uipb.SshTunnelAuthMethod_SSH_TUNNEL_AUTH_METHOD_CERT
	}
	_, err := c.uiClient.StartSshTunnel(c.attachAuth(ctx), &uipb.StartSshTunnelRequest{
		TargetUuid: strings.TrimSpace(target),
		ServerAddr: strings.TrimSpace(serverAddr),
		AgentPort:  strings.TrimSpace(agentPort),
		AuthMethod: method,
		Username:   strings.TrimSpace(username),
		Password:   password,
		PrivateKey: privateKey,
	})
	return err
}

func (c *Client) ProxyStream(ctx context.Context) (uipb.KelpieUIService_ProxyStreamClient, error) {
	return c.uiClient.ProxyStream(c.attachAuth(ctx))
}

func (c *Client) ListLoot(ctx context.Context, target string, limit int32) ([]*uipb.LootItem, error) {
	resp, err := c.uiClient.ListLoot(c.attachAuth(ctx), &uipb.ListLootRequest{
		TargetUuid: strings.TrimSpace(target),
		Category:   uipb.LootCategory_LOOT_CATEGORY_FILE,
		Limit:      limit,
	})
	if err != nil {
		return nil, err
	}
	return resp.GetItems(), nil
}

func (c *Client) SyncLoot(ctx context.Context, lootID string, w io.Writer) (*uipb.LootItem, uint64, error) {
	if w == nil {
		return nil, 0, errors.New("writer required")
	}
	stream, err := c.uiClient.SyncLoot(c.attachAuth(ctx), &uipb.SyncLootRequest{LootId: strings.TrimSpace(lootID)})
	if err != nil {
		return nil, 0, err
	}
	var (
		item    *uipb.LootItem
		written uint64
	)
	for {
		chunk, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return item, written, nil
			}
			return item, written, err
		}
		if chunk.GetItem() != nil {
			item = chunk.GetItem()
		}
		if data := chunk.GetData(); len(data) > 0 {
			n, writeErr := w.Write(data)
			written += uint64(n)
			if writeErr != nil {
				return item, written, writeErr
			}
			if n != len(data) {
				return item, written, io.ErrShortWrite
			}
		}
	}
}

func (c *Client) ListRemoteFiles(ctx context.Context, target, path string) (*uipb.ListRemoteFilesResponse, error) {
	return c.uiClient.ListRemoteFiles(c.attachAuth(ctx), &uipb.ListRemoteFilesRequest{
		TargetUuid: strings.TrimSpace(target),
		Path:       path,
	})
}

func (c *Client) CollectLootFile(ctx context.Context, target, remotePath string, tags []string) (*uipb.CollectLootFileResponse, error) {
	return c.uiClient.CollectLootFile(c.attachAuth(ctx), &uipb.CollectLootFileRequest{
		TargetUuid: strings.TrimSpace(target),
		RemotePath: strings.TrimSpace(remotePath),
		Tags:       tags,
	})
}

func (c *Client) UploadRemoteFile(ctx context.Context, target, localPath, remotePath string) (*uipb.UploadRemoteFileResponse, error) {
	localPath = strings.TrimSpace(localPath)
	if localPath == "" {
		return nil, errors.New("local path required")
	}
	f, err := os.Open(localPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if info.IsDir() {
		return nil, errors.New("local path is a directory")
	}
	hasher := sha256.New()
	if _, err := io.Copy(hasher, f); err != nil {
		return nil, err
	}
	sum := hex.EncodeToString(hasher.Sum(nil))
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	stream, err := c.uiClient.UploadRemoteFile(c.attachAuth(ctx))
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 64*1024)
	first := true
	for {
		n, readErr := f.Read(buf)
		if n > 0 || first {
			req := &uipb.UploadRemoteFileRequest{
				TargetUuid: strings.TrimSpace(target),
				RemotePath: strings.TrimSpace(remotePath),
				Size:       uint64(info.Size()),
				Sha256:     sum,
			}
			if n > 0 {
				req.Data = append([]byte(nil), buf[:n]...)
			}
			if !first {
				req.TargetUuid = ""
				req.RemotePath = ""
				req.Size = 0
				req.Sha256 = ""
			}
			if err := stream.Send(req); err != nil {
				return nil, err
			}
			first = false
		}
		if readErr != nil {
			if readErr == io.EOF {
				return stream.CloseAndRecv()
			}
			return nil, readErr
		}
	}
}

func (c *Client) CloseStream(ctx context.Context, streamID uint32, reason string) error {
	_, err := c.uiClient.CloseStream(c.attachAuth(ctx), &uipb.CloseStreamRequest{
		StreamId: streamID,
		Reason:   strings.TrimSpace(reason),
	})
	return err
}

func (c *Client) StreamDiagnostics(ctx context.Context) ([]*uipb.StreamDiag, error) {
	resp, err := c.uiClient.StreamDiagnostics(c.attachAuth(ctx), &uipb.StreamDiagnosticsRequest{})
	if err != nil {
		return nil, err
	}
	return resp.GetStreams(), nil
}

func (c *Client) StreamPing(ctx context.Context, target string, count, payloadSize int32) error {
	_, err := c.uiClient.StreamPing(c.attachAuth(ctx), &uipb.StreamPingRequest{
		TargetUuid:  strings.TrimSpace(target),
		Count:       count,
		PayloadSize: payloadSize,
	})
	return err
}

func (c *Client) StartForwardProxy(ctx context.Context, target, localBind, remoteAddr string) (*uipb.StartForwardProxyResponse, error) {
	return c.proxyAdmin.StartForwardProxy(c.attachAuth(ctx), &uipb.StartForwardProxyRequest{
		TargetUuid: strings.TrimSpace(target),
		LocalBind:  strings.TrimSpace(localBind),
		RemoteAddr: strings.TrimSpace(remoteAddr),
	})
}

func (c *Client) StopForwardProxy(ctx context.Context, target, proxyID string) (int32, error) {
	resp, err := c.proxyAdmin.StopForwardProxy(c.attachAuth(ctx), &uipb.StopForwardProxyRequest{
		TargetUuid: strings.TrimSpace(target),
		ProxyId:    strings.TrimSpace(proxyID),
	})
	if err != nil {
		return 0, err
	}
	return resp.GetStopped(), nil
}

func (c *Client) StartBackwardProxy(ctx context.Context, target, remotePort, localPort string) (*uipb.StartBackwardProxyResponse, error) {
	return c.proxyAdmin.StartBackwardProxy(c.attachAuth(ctx), &uipb.StartBackwardProxyRequest{
		TargetUuid: strings.TrimSpace(target),
		RemotePort: strings.TrimSpace(remotePort),
		LocalPort:  strings.TrimSpace(localPort),
	})
}

func (c *Client) StopBackwardProxy(ctx context.Context, target, proxyID string) (int32, error) {
	resp, err := c.proxyAdmin.StopBackwardProxy(c.attachAuth(ctx), &uipb.StopBackwardProxyRequest{
		TargetUuid: strings.TrimSpace(target),
		ProxyId:    strings.TrimSpace(proxyID),
	})
	if err != nil {
		return 0, err
	}
	return resp.GetStopped(), nil
}

func (c *Client) MarkSession(ctx context.Context, target, action, reason string) (*uipb.SessionInfo, error) {
	reqAction := uipb.SessionMarkAction_SESSION_MARK_ACTION_ALIVE
	switch strings.ToLower(strings.TrimSpace(action)) {
	case "dead":
		reqAction = uipb.SessionMarkAction_SESSION_MARK_ACTION_DEAD
	case "maintenance":
		reqAction = uipb.SessionMarkAction_SESSION_MARK_ACTION_MAINTENANCE
	}
	resp, err := c.uiClient.MarkSession(c.attachAuth(ctx), &uipb.MarkSessionRequest{
		TargetUuid: strings.TrimSpace(target),
		Action:     reqAction,
		Reason:     strings.TrimSpace(reason),
	})
	if err != nil {
		return nil, err
	}
	return resp.GetSession(), nil
}

func (c *Client) RepairSession(ctx context.Context, target string, force bool, reason string) (*uipb.RepairSessionResponse, error) {
	return c.uiClient.RepairSession(c.attachAuth(ctx), &uipb.RepairSessionRequest{
		TargetUuid: strings.TrimSpace(target),
		Force:      force,
		Reason:     strings.TrimSpace(reason),
	})
}

func (c *Client) ReconnectSession(ctx context.Context, target, reason string) (*uipb.ReconnectSessionResponse, error) {
	return c.uiClient.ReconnectSession(c.attachAuth(ctx), &uipb.ReconnectSessionRequest{
		TargetUuid: strings.TrimSpace(target),
		Reason:     strings.TrimSpace(reason),
	})
}

func (c *Client) TerminateSession(ctx context.Context, target, reason string) (*uipb.TerminateSessionResponse, error) {
	return c.uiClient.TerminateSession(c.attachAuth(ctx), &uipb.TerminateSessionRequest{
		TargetUuid: strings.TrimSpace(target),
		Reason:     strings.TrimSpace(reason),
	})
}

func (c *Client) SessionDiagnostics(ctx context.Context, target string, includeProcesses, includeMetrics bool) (*uipb.SessionDiagnosticsResponse, error) {
	return c.uiClient.GetSessionDiagnostics(c.attachAuth(ctx), &uipb.SessionDiagnosticsRequest{
		TargetUuid:       strings.TrimSpace(target),
		IncludeProcesses: includeProcesses,
		IncludeMetrics:   includeMetrics,
	})
}

func (c *Client) ListPivotListeners(ctx context.Context, target string) ([]*uipb.PivotListener, error) {
	req := &uipb.ListPivotListenersRequest{}
	if strings.TrimSpace(target) != "" {
		req.TargetUuids = []string{strings.TrimSpace(target)}
	}
	resp, err := c.pivot.ListPivotListeners(c.attachAuth(ctx), req)
	if err != nil {
		return nil, err
	}
	return resp.GetListeners(), nil
}

func (c *Client) CreatePivotListener(ctx context.Context, target, protocol, bind, mode string) (*uipb.PivotListener, error) {
	resp, err := c.pivot.CreatePivotListener(c.attachAuth(ctx), &uipb.CreatePivotListenerRequest{
		TargetUuid: strings.TrimSpace(target),
		Spec: &uipb.PivotListenerSpec{
			Protocol: strings.TrimSpace(protocol),
			Bind:     strings.TrimSpace(bind),
			Mode:     pivotMode(mode),
		},
	})
	if err != nil {
		return nil, err
	}
	return resp.GetListener(), nil
}

func (c *Client) UpdatePivotListener(ctx context.Context, listenerID, target, protocol, bind, mode, desiredStatus string, includeSpec bool) (*uipb.PivotListener, error) {
	req := &uipb.UpdatePivotListenerRequest{
		ListenerId:    strings.TrimSpace(listenerID),
		DesiredStatus: strings.TrimSpace(desiredStatus),
	}
	if includeSpec {
		req.Spec = &uipb.PivotListenerSpec{
			Protocol: strings.TrimSpace(protocol),
			Bind:     strings.TrimSpace(bind),
			Mode:     pivotMode(mode),
		}
		_ = target
	}
	resp, err := c.pivot.UpdatePivotListener(c.attachAuth(ctx), req)
	if err != nil {
		return nil, err
	}
	return resp.GetListener(), nil
}

func (c *Client) DeletePivotListener(ctx context.Context, listenerID string) error {
	_, err := c.pivot.DeletePivotListener(c.attachAuth(ctx), &uipb.DeletePivotListenerRequest{ListenerId: strings.TrimSpace(listenerID)})
	return err
}

func (c *Client) ListControllerListeners(ctx context.Context) ([]*uipb.ControllerListener, error) {
	resp, err := c.controller.ListControllerListeners(c.attachAuth(ctx), &uipb.ListControllerListenersRequest{})
	if err != nil {
		return nil, err
	}
	return resp.GetListeners(), nil
}

func (c *Client) CreateControllerListener(ctx context.Context, protocol, bind string) (*uipb.ControllerListener, error) {
	resp, err := c.controller.CreateControllerListener(c.attachAuth(ctx), &uipb.CreateControllerListenerRequest{
		Spec: &uipb.ControllerListenerSpec{
			Bind:     strings.TrimSpace(bind),
			Protocol: strings.TrimSpace(protocol),
		},
	})
	if err != nil {
		return nil, err
	}
	return resp.GetListener(), nil
}

func (c *Client) UpdateControllerListener(ctx context.Context, listenerID, protocol, bind, desiredStatus string, includeSpec bool) (*uipb.ControllerListener, error) {
	req := &uipb.UpdateControllerListenerRequest{
		ListenerId:    strings.TrimSpace(listenerID),
		DesiredStatus: controllerStatus(desiredStatus),
	}
	if includeSpec {
		req.Spec = &uipb.ControllerListenerSpec{
			Bind:     strings.TrimSpace(bind),
			Protocol: strings.TrimSpace(protocol),
		}
	}
	resp, err := c.controller.UpdateControllerListener(c.attachAuth(ctx), req)
	if err != nil {
		return nil, err
	}
	return resp.GetListener(), nil
}

func (c *Client) DeleteControllerListener(ctx context.Context, listenerID string) (*uipb.ControllerListener, error) {
	resp, err := c.controller.DeleteControllerListener(c.attachAuth(ctx), &uipb.DeleteControllerListenerRequest{ListenerId: strings.TrimSpace(listenerID)})
	if err != nil {
		return nil, err
	}
	return resp.GetListener(), nil
}

func pivotMode(mode string) uipb.PivotListenerMode {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "iptables":
		return uipb.PivotListenerMode_PIVOT_LISTENER_MODE_IPTABLES
	case "soreuse", "so_reuse", "so-reuse":
		return uipb.PivotListenerMode_PIVOT_LISTENER_MODE_SOREUSE
	default:
		return uipb.PivotListenerMode_PIVOT_LISTENER_MODE_NORMAL
	}
}

func controllerStatus(status string) uipb.ControllerListenerStatus {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "pending":
		return uipb.ControllerListenerStatus_CONTROLLER_LISTENER_STATUS_PENDING
	case "running", "resume", "start":
		return uipb.ControllerListenerStatus_CONTROLLER_LISTENER_STATUS_RUNNING
	case "failed":
		return uipb.ControllerListenerStatus_CONTROLLER_LISTENER_STATUS_FAILED
	case "stopped", "stop", "pause":
		return uipb.ControllerListenerStatus_CONTROLLER_LISTENER_STATUS_STOPPED
	default:
		return uipb.ControllerListenerStatus_CONTROLLER_LISTENER_STATUS_UNSPECIFIED
	}
}

// SupplementalStatus 返回补链调度器状态。
func (c *Client) SupplementalStatus(ctx context.Context) (*uipb.SupplementalStatus, error) {
	return c.supp.GetSupplementalStatus(c.attachAuth(ctx), &uipb.SupplementalEmpty{})
}

// SupplementalMetrics 返回补链调度器累计指标。
func (c *Client) SupplementalMetrics(ctx context.Context) (*uipb.SupplementalMetrics, error) {
	return c.supp.GetSupplementalMetrics(c.attachAuth(ctx), &uipb.SupplementalEmpty{})
}

// SupplementalEvents 返回最近补链事件（由 admin 侧内存缓存）。
func (c *Client) SupplementalEvents(ctx context.Context, limit int32) ([]*uipb.SupplementalEvent, error) {
	resp, err := c.supp.ListSupplementalEvents(c.attachAuth(ctx), &uipb.ListSupplementalEventsRequest{Limit: limit})
	if err != nil {
		return nil, err
	}
	return resp.GetEvents(), nil
}

// WatchEvents 订阅一个 UiEvent 流。caller 负责 cancel 上下文以关闭流。
func (c *Client) WatchEvents(ctx context.Context) (uipb.KelpieUIService_WatchEventsClient, error) {
	return c.uiClient.WatchEvents(c.attachAuth(ctx), &uipb.WatchEventsRequest{})
}

// ListSleepProfiles 返回所有节点当前 sleep 配置快照（便于 UI 初始化）。
func (c *Client) ListSleepProfiles(ctx context.Context) ([]*uipb.SleepProfile, error) {
	resp, err := c.sleepAdmin.ListSleepProfiles(c.attachAuth(ctx), &uipb.ListSleepProfilesRequest{})
	if err != nil {
		return nil, err
	}
	return resp.GetProfiles(), nil
}

// Endpoint 返回当前连接的地址（用于 UI 显示）。
func (c *Client) Endpoint() string {
	if c == nil {
		return ""
	}
	return c.opts.Endpoint
}

// IsTLS 报告连接是否为 TLS。
func (c *Client) IsTLS() bool {
	if c == nil {
		return false
	}
	return c.opts.UseTLS
}

// LastSeenFingerprint 返回 TOFU verifier 观察到的指纹（握手后写入）。
func (c *Client) LastSeenFingerprint() string {
	if c == nil || c.verifier == nil {
		return ""
	}
	return c.verifier.LastSeen()
}

// Ensure client.Close doesn't race with later calls. mu 暂未使用，
// 预留给后续"重建连接"逻辑（比如重连时的原子替换）。
var _ = fmt.Sprintf
