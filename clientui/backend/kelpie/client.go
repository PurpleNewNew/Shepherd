package kelpie

import (
	"context"
	"errors"
	"fmt"
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
