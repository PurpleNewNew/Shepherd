package main

import (
	"context"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"math"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"codeberg.org/agnoie/shepherd/internal/kelpie/collab/audit"
	"codeberg.org/agnoie/shepherd/internal/kelpie/collab/auth"
	"codeberg.org/agnoie/shepherd/internal/kelpie/collab/chat"
	"codeberg.org/agnoie/shepherd/internal/kelpie/dataplane"
	"codeberg.org/agnoie/shepherd/internal/kelpie/initial"
	"codeberg.org/agnoie/shepherd/internal/kelpie/printer"
	"codeberg.org/agnoie/shepherd/internal/kelpie/process"
	"codeberg.org/agnoie/shepherd/internal/kelpie/storage/lootfs"
	"codeberg.org/agnoie/shepherd/internal/kelpie/storage/sqlite"
	"codeberg.org/agnoie/shepherd/internal/kelpie/supp"
	"codeberg.org/agnoie/shepherd/internal/kelpie/topology"
	"codeberg.org/agnoie/shepherd/internal/kelpie/ui/grpcserver"
	"codeberg.org/agnoie/shepherd/pkg/config/defaults"
	"codeberg.org/agnoie/shepherd/pkg/global"
	"codeberg.org/agnoie/shepherd/pkg/share"
	"codeberg.org/agnoie/shepherd/pkg/share/transport"
	"codeberg.org/agnoie/shepherd/protocol"

	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func main() {
	printer.InitPrinter()

	options, err := initial.ParseOptions()
	if err != nil {
		printer.Fail("[*] Options err: %v\r\n", err)
		return
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	printer.Success("[*] Kelpie gRPC service starting\r\n")

	token := share.GeneratePreAuthToken(options.Secret)

	// 显示 TLS 证书指纹（类似 Cobalt Strike 的 TOFU 模式）
	_, fingerprint, err := transport.GetCertificatePEM(token, options.Domain)
	if err != nil {
		printer.Fail("[*] Failed to generate TLS certificate: %v\r\n", err)
		return
	}
	printer.Success("[*] TLS Certificate SHA256 Fingerprint:\r\n")
	printer.Warning("    %s\r\n", formatFingerprint(fingerprint))
	printer.Success("[*] Stockman clients should verify this fingerprint on first connect\r\n")

	transports := protocol.NewTransports("raw", options.Downstream)
	protocol.SetDefaultTransports("raw", options.Downstream)

	store := global.NewStoreWithTransports(transports)
	if err := store.SetPreAuthToken(token); err != nil {
		printer.Fail("[*] Failed to set pre-auth token: %v\r\n", err)
		return
	}
	options.PreAuthToken = token

	protocol.ConfigureHTTP(
		protocol.HTTPConfig{},
		protocol.HTTPConfig{
			Host:      options.HTTPHost,
			Path:      options.HTTPPath,
			UserAgent: options.HTTPUserAgent,
		},
	)

	topo := topology.NewTopology()
	dbPath := filepath.Join("db", "admin.db")
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		printer.Fail("[*] Failed to prepare database directory: %v\r\n", err)
		return
	}
	sqlStore, err := sqlite.New(dbPath)
	if err != nil {
		printer.Fail("[*] Failed to initialize sqlite store: %v\r\n", err)
		return
	}
	defer sqlStore.Close()
	var lootContentStore process.LootContentStore
	if store, err := lootfs.New(filepath.Join(filepath.Dir(dbPath), "loot")); err != nil {
		printer.Warning("[*] Failed to initialize loot content store: %v\r\n", err)
	} else {
		lootContentStore = store
	}

	// Teamserver 启动时优先从 controller_listeners 表中选择监听地址；
	// 若暂无 pending 监听器，则仅启动 UI，等待后续配置。
	if records, err := sqlStore.LoadControllerListeners(); err != nil {
		printer.Fail("[*] Failed to load controller listeners: %v\r\n", err)
		return
	} else {
		for _, rec := range records {
			if rec.Status == process.ControllerListenerPending && strings.TrimSpace(rec.Bind) != "" {
				options.Listen = rec.Bind
				options.Mode = initial.NORMAL_PASSIVE
				printer.Warning("[*] Teamserver: using Controller Listener %s on %s as admin entrypoint\r\n", rec.ID, rec.Bind)
				break
			}
		}
	}
	printer.Warning("[*] Kelpie running in TEAMSERVER mode (no per-user auth)\r\n")
	auditRecorder := audit.NewRecorder(sqlStore)
	chatService := chat.NewService(sqlStore)
	topo.SetPersistence(sqlStore)
	snapshot, err := sqlStore.Load()
	if err != nil {
		printer.Fail("[*] Failed to load persisted topology: %v\r\n", err)
		return
	}
	if snapshot != nil {
		topo.ApplySnapshot(snapshot)
		// 启动前统一将快照中的在线标记重置为离线，等待新的上线消息重新置位。
		topo.MarkAllOffline()
		supp.RestoreSupplementalLinks(snapshot.SupplementalLinks)
	}
	// 基于 sleep 参数配置离线判定策略：
	// sleep=0 使用默认离线容忍窗口；sleep>0 在默认容忍之外叠加周期预算。
	if options.SleepSeconds >= 0 { // 允许 0 值
		// sleep 未知或为 0 时保留默认容忍，避免短暂静默就被判离线，
		// 留给节点后续自报 sleep profile。
		zeroGrace := defaults.NodeStaleTimeout
		sleep := time.Duration(options.SleepSeconds) * time.Second
		topo.ConfigureStalePolicy(sleep, zeroGrace)
	}
	go topo.Run()
	defer topo.Stop()

	var (
		conn        net.Conn
		negotiation *protocol.Negotiation
	)
	// 优先使用 Controller Listener 提供的监听地址；没有初始监听地址时只启动 UI。
	if options.Mode == initial.NORMAL_PASSIVE || options.Mode == initial.NORMAL_ACTIVE ||
		options.Mode == initial.SOCKS5_PROXY_ACTIVE || options.Mode == initial.HTTP_PROXY_ACTIVE {
		if strings.TrimSpace(options.Listen) == "" && options.Mode == initial.NORMAL_PASSIVE {
			// 没有初始监听地址时，只启动 UI，等待后续通过 Controller Listener 配置再建链。
			printer.Warning("[*] No admin listener configured; Kelpie UI will start without upstream connection\r\n")
		} else {
			printer.Warning("[*] Waiting for new connection...\r\n")
			conn, negotiation, err = connectWithRetry(options, topo)
			if err != nil {
				printer.Fail("[*] Failed to establish initial connection: %s\r\n", err)
				return
			}
			store.InitializeComponent(conn, options.Secret, protocol.ADMIN_UUID, transports.Upstream(), options.Downstream)
			if negotiation != nil {
				store.UpdateProtocol(protocol.ADMIN_UUID, negotiation.Version, negotiation.Flags)
			}
		}
	} else {
		printer.Warning("[*] No admin listener configured; Kelpie UI will start without upstream connection\r\n")
	}
	var plannerMetrics topology.PlannerMetricsSnapshot
	if snapshot != nil {
		plannerMetrics = snapshot.PlannerMetrics
	}
	admin := process.NewAdmin(ctx, options, topo, store, nil, plannerMetrics, sqlStore, sqlStore, sqlStore, sqlStore, lootContentStore)
	defer admin.Stop()

	topoTask := &topology.TopoTask{
		Mode: topology.CALCULATE,
	}
	if _, err := topo.Execute(topoTask); err != nil {
		printer.Fail("[*] Initial topology calculation failed: %v\r\n", err)
	}

	admin.Start()
	if err := serveGRPCUI(ctx, admin, options, auditRecorder, chatService); err != nil && !errors.Is(err, context.Canceled) {
		printer.Fail("[*] gRPC UI server error: %v\r\n", err)
	}
}

func recordDataplaneAuditHook(recorder *audit.Recorder, meta dataplane.TokenMeta, bytes int64, err error) {
	if recorder == nil {
		return
	}
	statusText := codes.OK.String()
	errMsg := ""
	if err != nil {
		statusText = codes.Internal.String()
		errMsg = err.Error()
	}
	params := ""
	if bytes > 0 {
		params = fmt.Sprintf("bytes=%d", bytes)
	}
	recorder.Record(audit.Entry{
		Tenant:   strings.TrimSpace(meta.Tenant),
		Username: strings.TrimSpace(meta.Operator),
		Role:     auth.RoleAdmin,
		Method:   fmt.Sprintf("dataplane/%s", meta.Direction),
		Target:   meta.Target,
		Params:   params,
		Status:   statusText,
		Error:    errMsg,
	})
}

const (
	maxActiveRetries   = 5
	retryBackoffFactor = 2
)

func connectWithRetry(options *initial.Options, topo *topology.Topology) (net.Conn, *protocol.Negotiation, error) {
	var (
		conn    net.Conn
		nego    *protocol.Negotiation
		err     error
		backoff = time.Second
	)

	switch options.Mode {
	case initial.NORMAL_PASSIVE:
		return initial.NormalPassive(options, topo)
	case initial.NORMAL_ACTIVE:
		for attempt := 1; attempt <= maxActiveRetries; attempt++ {
			conn, nego, err = initial.NormalActive(options, topo, nil)
			if err == nil {
				return conn, nego, nil
			}
			printer.Fail("[*] Active connection attempt %d failed: %s\r\n", attempt, err)
			time.Sleep(backoff)
			backoff *= retryBackoffFactor
		}
	case initial.SOCKS5_PROXY_ACTIVE:
		proxy := share.NewSocks5Proxy(options.Connect, options.Socks5Proxy, options.Socks5ProxyU, options.Socks5ProxyP)
		for attempt := 1; attempt <= maxActiveRetries; attempt++ {
			conn, nego, err = initial.NormalActive(options, topo, proxy)
			if err == nil {
				return conn, nego, nil
			}
			printer.Fail("[*] Active connection via socks5 attempt %d failed: %s\r\n", attempt, err)
			time.Sleep(backoff)
			backoff *= retryBackoffFactor
		}
	case initial.HTTP_PROXY_ACTIVE:
		proxy := share.NewHTTPProxy(options.Connect, options.HttpProxy)
		for attempt := 1; attempt <= maxActiveRetries; attempt++ {
			conn, nego, err = initial.NormalActive(options, topo, proxy)
			if err == nil {
				return conn, nego, nil
			}
			printer.Fail("[*] Active connection via http proxy attempt %d failed: %s\r\n", attempt, err)
			time.Sleep(backoff)
			backoff *= retryBackoffFactor
		}
	default:
		return nil, nil, errors.New("unknown mode")
	}
	return nil, nil, err
}

func serveGRPCUI(ctx context.Context, admin *process.Admin, opts *initial.Options, auditor *audit.Recorder, chatSvc *chat.Service) error {
	if admin == nil {
		return fmt.Errorf("admin unavailable")
	}
	if opts == nil {
		return fmt.Errorf("options unavailable")
	}
	addr := opts.UIListen
	if strings.TrimSpace(addr) == "" {
		return fmt.Errorf("grpc listen address missing")
	}
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer lis.Close()
	serverOpts, err := grpcServerOptionsFromConfig(opts, auditor)
	if err != nil {
		return err
	}
	dpTokens := dataplane.NewTokenStore(opts.DataTokenTTL)
	dpCfg := dataplane.Config{
		Listen:       opts.DataListen,
		EnableTLS:    opts.DataEnableTLS,
		TLSCert:      opts.DataTLSCert,
		TLSKey:       opts.DataTLSKey,
		TLSClientCA:  opts.DataTLSClientCA,
		MaxRateBps:   opts.DataMaxRateBps,
		MaxSize:      opts.DataMaxSize,
		TokenRetries: opts.DataTokenRetries,
		Admin:        admin,
		CompleteHook: func(meta dataplane.TokenMeta, bytes int64, err error) {
			recordDataplaneAuditHook(auditor, meta, bytes, err)
		},
	}
	dpManager := dataplane.NewManager(dpCfg, dpTokens, opts.DataTokenTTL)
	// 新的 TCP 多路复用 dataplane（实验性，替代 HTTP）。
	dpSrv := dataplane.NewTCPServer(dpCfg, dpTokens)

	srv := grpcserver.New(admin, auditor, chatSvc, dpManager, opts.UIAuthToken, serverOpts...)
	go func() {
		printer.Success("[*] Kelpie dataplane listening on %s\r\n", opts.DataListen)
		if err := dpSrv.ListenAndServe(ctx); err != nil && !errors.Is(err, context.Canceled) {
			printer.Warning("[*] dataplane server stopped: %v\r\n", err)
		}
	}()
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Serve(lis)
	}()
	printer.Success("[*] Kelpie gRPC UI listening on %s\r\n", addr)
	select {
	case <-ctx.Done():
		srv.Stop()
		if err := <-errCh; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			return err
		}
		return ctx.Err()
	case err := <-errCh:
		srv.Stop()
		if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			return err
		}
		return nil
	}
}

func grpcServerOptionsFromConfig(opts *initial.Options, auditor *audit.Recorder) ([]grpc.ServerOption, error) {
	serverOpts := make([]grpc.ServerOption, 0, 2)
	if opts == nil {
		return serverOpts, nil
	}
	if opts.UIEnableTLS {
		creds, err := loadTLSCredentials(opts.UITLSCert, opts.UITLSKey, opts.UITLSClientCA)
		if err != nil {
			return nil, err
		}
		serverOpts = append(serverOpts, grpc.Creds(creds))
	}
	var unary []grpc.UnaryServerInterceptor
	var stream []grpc.StreamServerInterceptor
	if limiter := newRateLimiter(opts.UIRateLimitRPS); limiter != nil {
		unary = append(unary, rateLimitUnaryInterceptor(limiter))
		stream = append(stream, rateLimitStreamInterceptor(limiter))
	}
	// 先校验 UI token，再附加 teamserver 鉴权上下文。
	unary = append(unary, bootstrapUnaryInterceptor(opts.UIAuthToken))
	stream = append(stream, bootstrapStreamInterceptor(opts.UIAuthToken))
	// teamserver 拦截器附加鉴权上下文
	unary = append(unary, teamserverUnaryInterceptor(opts.UIAuthToken))
	stream = append(stream, teamserverStreamInterceptor(opts.UIAuthToken))
	if auditor != nil {
		unary = append(unary, auditUnaryInterceptor(auditor))
		stream = append(stream, auditStreamInterceptor(auditor))
	}
	if len(unary) > 0 {
		serverOpts = append(serverOpts, grpc.ChainUnaryInterceptor(unary...))
	}
	if len(stream) > 0 {
		serverOpts = append(serverOpts, grpc.ChainStreamInterceptor(stream...))
	}
	return serverOpts, nil
}

func loadTLSCredentials(certFile, keyFile, clientCA string) (credentials.TransportCredentials, error) {
	if strings.TrimSpace(certFile) == "" || strings.TrimSpace(keyFile) == "" {
		return nil, fmt.Errorf("tls cert/key missing")
	}
	certificate, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load TLS cert/key: %w", err)
	}
	config := &tls.Config{Certificates: []tls.Certificate{certificate}}
	if strings.TrimSpace(clientCA) != "" {
		caBytes, err := os.ReadFile(clientCA)
		if err != nil {
			return nil, fmt.Errorf("failed to read client CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caBytes) {
			return nil, fmt.Errorf("failed to append client CA")
		}
		config.ClientAuth = tls.RequireAndVerifyClientCert
		config.ClientCAs = pool
	}
	return credentials.NewTLS(config), nil
}

func extractAuthToken(md metadata.MD) string {
	if md == nil {
		return ""
	}
	for _, header := range md.Get("authorization") {
		parts := strings.Fields(header)
		if len(parts) == 2 && strings.EqualFold(parts[0], "bearer") {
			return parts[1]
		}
	}
	if vals := md.Get("x-kelpie-token"); len(vals) > 0 {
		return vals[0]
	}
	return ""
}

// operatorNameFromMetadata 会从传入的 metadata 中提取一个便于阅读的操作者名称。
// 在 teamserver 模式下，这个名称只用于归属标记。
// (chat/audit) 且不信任客户端自报的用户名，以防伪造。
func operatorNameFromMetadata(ctx context.Context) string {
	name := "teamserver"
	if p, ok := peer.FromContext(ctx); ok && p.Addr != nil {
		name = fmt.Sprintf("teamserver@%s", p.Addr.String())
	}
	return name
}

func newRateLimiter(maxRPS float64) *rate.Limiter {
	if maxRPS <= 0 {
		return nil
	}
	burst := int(math.Ceil(maxRPS))
	if burst < 1 {
		burst = 1
	}
	if burst > 100 {
		burst = 100
	}
	return rate.NewLimiter(rate.Limit(maxRPS), burst)
}

func rateLimitUnaryInterceptor(limiter *rate.Limiter) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if limiter != nil && !limiter.Allow() {
			return nil, status.Error(codes.ResourceExhausted, "gRPC UI rate limit exceeded")
		}
		return handler(ctx, req)
	}
}

func rateLimitStreamInterceptor(limiter *rate.Limiter) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if limiter != nil && !limiter.Allow() {
			return status.Error(codes.ResourceExhausted, "gRPC UI rate limit exceeded")
		}
		return handler(srv, ss)
	}
}

// bootstrap token 拦截器负责在 teamserver 拦截器之前验证 UI token。
func bootstrapUnaryInterceptor(token string) grpc.UnaryServerInterceptor {
	token = strings.TrimSpace(token)
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if token == "" {
			return nil, status.Error(codes.Unauthenticated, "server ui token is not configured")
		}
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "missing auth metadata")
		}
		provided := strings.TrimSpace(extractAuthToken(md))
		if provided == "" {
			return nil, status.Error(codes.Unauthenticated, "missing bootstrap token")
		}
		if subtle.ConstantTimeCompare([]byte(provided), []byte(token)) != 1 {
			return nil, status.Error(codes.Unauthenticated, "invalid bootstrap token")
		}
		return handler(ctx, req)
	}
}

func bootstrapStreamInterceptor(token string) grpc.StreamServerInterceptor {
	token = strings.TrimSpace(token)
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if token == "" {
			return status.Error(codes.Unauthenticated, "server ui token is not configured")
		}
		md, ok := metadata.FromIncomingContext(ss.Context())
		if !ok {
			return status.Error(codes.Unauthenticated, "missing auth metadata")
		}
		provided := strings.TrimSpace(extractAuthToken(md))
		if provided == "" {
			return status.Error(codes.Unauthenticated, "missing bootstrap token")
		}
		if subtle.ConstantTimeCompare([]byte(provided), []byte(token)) != 1 {
			return status.Error(codes.Unauthenticated, "invalid bootstrap token")
		}
		return handler(srv, ss)
	}
}

func teamserverUnaryInterceptor(bootstrap string) grpc.UnaryServerInterceptor {
	bootstrap = strings.TrimSpace(bootstrap)
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if bootstrap == "" {
			return nil, status.Error(codes.Unauthenticated, "server ui token is not configured")
		}
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "missing auth metadata")
		}
		token := strings.TrimSpace(extractAuthToken(md))
		if token == "" {
			return nil, status.Error(codes.Unauthenticated, "missing teamserver token")
		}
		if subtle.ConstantTimeCompare([]byte(token), []byte(bootstrap)) != 1 {
			return nil, status.Error(codes.Unauthenticated, "invalid teamserver token")
		}
		username := operatorNameFromMetadata(ctx)
		tenant := strings.ToLower(strings.TrimSpace(username))
		claims := auth.Claims{
			Username: username,
			Role:     auth.RoleAdmin,
			Tenant:   tenant,
		}
		ctx = auth.ContextWithClaims(ctx, claims)
		return handler(ctx, req)
	}
}

type wrappedTeamserverStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedTeamserverStream) Context() context.Context { return w.ctx }

func teamserverStreamInterceptor(bootstrap string) grpc.StreamServerInterceptor {
	bootstrap = strings.TrimSpace(bootstrap)
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := ss.Context()
		if bootstrap == "" {
			return status.Error(codes.Unauthenticated, "server ui token is not configured")
		}
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return status.Error(codes.Unauthenticated, "missing auth metadata")
		}
		token := strings.TrimSpace(extractAuthToken(md))
		if token == "" {
			return status.Error(codes.Unauthenticated, "missing teamserver token")
		}
		if subtle.ConstantTimeCompare([]byte(token), []byte(bootstrap)) != 1 {
			return status.Error(codes.Unauthenticated, "invalid teamserver token")
		}
		username := operatorNameFromMetadata(ctx)
		tenant := strings.ToLower(strings.TrimSpace(username))
		claims := auth.Claims{
			Username: username,
			Role:     auth.RoleAdmin,
			Tenant:   tenant,
		}
		ctx = auth.ContextWithClaims(ctx, claims)
		return handler(srv, &wrappedTeamserverStream{ServerStream: ss, ctx: ctx})
	}
}

func auditUnaryInterceptor(recorder *audit.Recorder) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		resp, err := handler(ctx, req)
		recordAudit(ctx, recorder, info.FullMethod, err)
		return resp, err
	}
}

func auditStreamInterceptor(recorder *audit.Recorder) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		err := handler(srv, ss)
		recordAudit(ss.Context(), recorder, info.FullMethod, err)
		return err
	}
}

func recordAudit(ctx context.Context, recorder *audit.Recorder, method string, err error) {
	if recorder == nil || skipAudit(method) {
		return
	}
	claims, _ := auth.ClaimsFromContext(ctx)
	tenant := strings.TrimSpace(claims.Tenant)
	if tenant == "" {
		tenant = strings.TrimSpace(claims.Username)
	}
	statusText := codes.OK.String()
	msg := ""
	if err != nil {
		statusText = status.Code(err).String()
		msg = err.Error()
	}
	recorder.Record(audit.Entry{
		Tenant:   tenant,
		Username: claims.Username,
		Role:     claims.Role,
		Method:   method,
		Status:   statusText,
		Error:    msg,
		Peer:     peerAddress(ctx),
	})
}

func skipAudit(method string) bool {
	switch method {
	case "/kelpieui.v1.KelpieUIService/WatchEvents":
		return true
	default:
		return false
	}
}

func peerAddress(ctx context.Context) string {
	if p, ok := peer.FromContext(ctx); ok && p.Addr != nil {
		return p.Addr.String()
	}
	return ""
}

// formatFingerprint 将十六进制指纹格式化为易读的冠号分隔形式 (AA:BB:CC:DD...)
func formatFingerprint(hex string) string {
	if len(hex) == 0 {
		return hex
	}
	var parts []string
	for i := 0; i < len(hex); i += 2 {
		end := i + 2
		if end > len(hex) {
			end = len(hex)
		}
		parts = append(parts, strings.ToUpper(hex[i:end]))
	}
	return strings.Join(parts, ":")
}
