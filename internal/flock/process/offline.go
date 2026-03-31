package process

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"codeberg.org/agnoie/shepherd/internal/flock/initial"
	"codeberg.org/agnoie/shepherd/pkg/config/defaults"
	"codeberg.org/agnoie/shepherd/pkg/share"
	"codeberg.org/agnoie/shepherd/pkg/share/handshake"
	reconn "codeberg.org/agnoie/shepherd/pkg/share/reconnect"
	"codeberg.org/agnoie/shepherd/pkg/share/transport"
	"codeberg.org/agnoie/shepherd/pkg/utils"
	"codeberg.org/agnoie/shepherd/protocol"

	reuseport "github.com/libp2p/go-reuseport"
)

const (
	manualReconnectAttempts = 5
	manualReconnectDelay    = 5 * time.Second
)

func (agent *Agent) protocolFlags() uint16 {
	if agent != nil {
		return sessionFlagsOrDefault(agent.currentSession(), protocol.DefaultProtocolFlags)
	}
	return protocol.DefaultProtocolFlags
}

func reconnectStrategy(options *initial.Options) reconn.Strategy {
	strategy := reconn.DefaultStrategy
	if options != nil && options.Reconnect > 0 {
		base := time.Duration(options.Reconnect) * time.Second
		if base > 0 {
			strategy.BaseDelay = base
			if base*8 > strategy.MaxDelay {
				strategy.MaxDelay = base * 8
			}
		}
	}
	return strategy
}

func (agent *Agent) reconHandshakeSecret(options *initial.Options) string {
	if options == nil {
		return ""
	}
	// 握手（HI 交换）必须使用原始的基础密钥。
	// 首条链路建立后，Options.Secret 可能会被派生出的会话密钥覆盖，
	// 这会破坏那些用 BaseSecret 解密 HI 的监听端的重连握手。
	if base := options.BaseSecret(); base != "" {
		return base
	}
	return options.Secret
}

func applyProtocolFlags(options *initial.Options, flags uint16) error {
	if options == nil {
		return nil
	}
	effectiveUp := strings.ToLower(strings.TrimSpace(options.Upstream))
	if effectiveUp == "" {
		effectiveUp = protocol.DefaultTransports().Upstream()
	}
	if effectiveUp == "http" && flags&protocol.FlagSupportChunked == 0 {
		return fmt.Errorf("upstream does not support HTTP chunked transfer")
	}
	return nil
}

func (agent *Agent) finalizePassiveHandshake(conn net.Conn, options *initial.Options, hiTemplate *protocol.HIMess, header *protocol.Header, activeUUID string) (net.Conn, error) {
	if conn == nil || options == nil || hiTemplate == nil || header == nil {
		return nil, fmt.Errorf("invalid handshake parameters")
	}

	handshakeSecret := agent.reconHandshakeSecret(options)
	transport := options.Upstream
	if strings.TrimSpace(transport) == "" {
		transport = agent.upstreamTransport()
	}
	rMessage := protocol.NewUpMsgWithTransport(conn, handshakeSecret, protocol.TEMP_UUID, transport)
	fHeader, fMessage, err := protocol.DestructMessage(rMessage)
	if err != nil {
		return nil, err
	}

	if fHeader.MessageType != protocol.HI {
		return nil, fmt.Errorf("unexpected handshake response type %d", fHeader.MessageType)
	}

	mmess, ok := fMessage.(*protocol.HIMess)
	if !ok || !handshake.ValidGreeting(handshake.RoleAgent, mmess.Greeting) || mmess.IsAdmin != 1 {
		return nil, fmt.Errorf("invalid handshake greeting")
	}

	meta := protocol.ResolveProtocolMeta(hiTemplate.ProtoFlags, mmess.ProtoFlags)

	if err := applyProtocolFlags(options, meta.Flags); err != nil {
		WarnRuntime(agent.mgr, "AGENT_RECONNECT_NEGOTIATION", true, err, "apply protocol flags failed")
		return nil, err
	}

	if agent.store != nil {
		agent.store.UpdateProtocolFlags(activeUUID, meta.Flags)
	}

	resp := *hiTemplate
	resp.ProtoFlags = meta.Flags

	sMessage := protocol.NewUpMsgWithTransport(conn, agent.reconHandshakeSecret(options), protocol.TEMP_UUID, transport)
	protocol.SetMessageMeta(sMessage, meta.Flags)
	header.Flags = meta.Flags
	protocol.ConstructMessage(sMessage, header, &resp, false)
	sMessage.SendMessage()
	return conn, nil
}

func upstreamOffline(agent *Agent) {
	mgr := agent.mgr
	options := agent.options
	if mgr == nil || options == nil {
		return
	}
	// 记录触发离线处理的那条上游连接指针。如果 repair/rescue 连接并发到达，
	// 它会替换当前活跃连接；此时不能继续围绕过期的 options.Connect 阻塞在睡眠或重连逻辑里。
	oldConn := func() net.Conn {
		if sess := agent.currentSession(); sess != nil {
			return sess.Conn()
		}
		return nil
	}()
	// 关键点：当上游消失时，必须避免 session/store 继续持有已关闭的 net.Conn 指针。
	// 否则依赖“当前会话连接”的子系统（如 stream/dataplane ACK）会反复向死连接写数据，
	// 即使请求已经通过 supplemental 链路送达，也可能因此卡死。
	//
	// 只有当 session 仍然指向 oldConn 时才清理，避免与并发建立的 repair/rescue 连接竞争。
	if oldConn != nil {
		if sess := agent.currentSession(); sess != nil && sess.Conn() == oldConn {
			if agent.store != nil {
				agent.store.UpdateActiveConn(nil)
			}
			agent.updateSessionConn(nil)
		}
	}
	// 如果我们感知到的 ParentUUID 已经变化（例如 rescue/reparent），
	// 应优先重连到父节点通过 gossip/监听公布的地址，而不是继续使用过期的静态 options.Connect。
	//
	// 这对短连接睡眠节点尤其重要：它们会在睡眠期间主动关闭上游会话，并依赖重连恢复。
	// 如果 Connect 仍指向旧父节点，节点就可能在拓扑里显示在线、但实际上不可达，
	// 最终导致 DTN ACK 超时。
	if connect := agent.preferredUpstreamConnect(options); connect != "" && strings.TrimSpace(connect) != strings.TrimSpace(options.Connect) {
		clone := *options
		clone.Connect = connect
		options = &clone
	}
	ctx := agent.context()
	// 如果已经安排了睡眠，则等到唤醒时刻。
	agent.sleepMu.Lock()
	until := agent.sleepingUntil
	agent.sleepMu.Unlock()
	if !until.IsZero() {
		now := time.Now()
		if until.After(now) {
			timer := time.NewTimer(until.Sub(now))
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			case <-agent.connChanged:
				timer.Stop()
				// 在等待唤醒期间，repair/rescue 连接可能已经替换了上游会话；
				// 这里让读循环去接管新的连接。
				if sess := agent.currentSession(); sess != nil && sess.Conn() != nil && sess.Conn() != oldConn {
					agent.finalizeUpstreamRecovery(ctx)
					return
				}
			}
		}
		// 清除睡眠状态。
		agent.sleepMu.Lock()
		agent.sleepingUntil = time.Time{}
		agent.sleepMu.Unlock()
	}
	if agent.tryAdoptFailoverParent(ctx) {
		agent.finalizeUpstreamRecovery(ctx)
		return
	}

	broadcastOfflineMess(ctx, mgr)

	var (
		newConn net.Conn
		err     error
	)
	switch options.Mode {
	case initial.NORMAL_PASSIVE:
		newConn, err = agent.normalPassiveReconn(agent.context(), options)
	case initial.IPTABLES_REUSE_PASSIVE:
		newConn, err = agent.ipTableReusePassiveReconn(agent.context(), options)
	case initial.SO_REUSE_PASSIVE:
		newConn, err = agent.soReusePassiveReconn(agent.context(), options)
	case initial.NORMAL_RECONNECT_ACTIVE:
		newConn = agent.normalReconnActiveReconn(agent.context(), options, nil)
	case initial.SOCKS5_PROXY_RECONNECT_ACTIVE:
		proxy := share.NewSocks5Proxy(options.Connect, options.Socks5Proxy, options.Socks5ProxyU, options.Socks5ProxyP)
		newConn = agent.normalReconnActiveReconn(agent.context(), options, proxy)
	case initial.HTTP_PROXY_RECONNECT_ACTIVE:
		proxy := share.NewHTTPProxy(options.Connect, options.HttpProxy)
		newConn = agent.normalReconnActiveReconn(agent.context(), options, proxy)
	case initial.NORMAL_ACTIVE:
		newConn, err = agent.manualActiveReconnect(agent.context(), options, nil, manualReconnectAttempts, manualReconnectDelay)
	case initial.SOCKS5_PROXY_ACTIVE:
		proxy := share.NewSocks5Proxy(options.Connect, options.Socks5Proxy, options.Socks5ProxyU, options.Socks5ProxyP)
		newConn, err = agent.manualActiveReconnect(agent.context(), options, proxy, manualReconnectAttempts, manualReconnectDelay)
	case initial.HTTP_PROXY_ACTIVE:
		proxy := share.NewHTTPProxy(options.Connect, options.HttpProxy)
		newConn, err = agent.manualActiveReconnect(agent.context(), options, proxy, manualReconnectAttempts, manualReconnectDelay)
	default:
		err = fmt.Errorf("unsupported reconnect mode %d", options.Mode)
	}

	if err != nil {
		WarnRuntime(agent.mgr, "AGENT_RECONNECT_RESTORE", true, err, "failed to restore upstream connection")
		return
	}
	if newConn == nil {
		WarnRuntime(agent.mgr, "AGENT_RECONNECT_RESTORE", true, nil, "upstream connection could not be restored")
		return
	}

	agent.setActiveConnection(newConn)

	agent.finalizeUpstreamRecovery(ctx)
}

func (agent *Agent) finalizeUpstreamRecovery(ctx context.Context) {
	if agent == nil || agent.mgr == nil {
		return
	}
	tellAdminReonline(agent.mgr)
	broadcastReonlineMess(ctx, agent.mgr)
	// 重连后顺便主动拉取一次 DTN。
	agent.requestDTNPull(8)
}

// preferredUpstreamConnect 会根据当前 ParentUUID 与该父节点最近一次 gossip NodeInfo，
// 返回一个替代的连接地址（host:port）。
//
// 如果没有可用的覆盖地址，则返回空字符串。
func (agent *Agent) preferredUpstreamConnect(options *initial.Options) string {
	if agent == nil || options == nil {
		return ""
	}
	// 只有在主动重连模式且已经存在 connect 目标时，这个覆盖地址才有意义。
	if strings.TrimSpace(options.Connect) == "" {
		return ""
	}
	parent := agent.ParentUUID()
	if parent == "" || parent == protocol.ADMIN_UUID {
		return ""
	}

	agent.knownMu.RLock()
	info := agent.knownNodes[parent]
	agent.knownMu.RUnlock()
	if info == nil || info.Port <= 0 {
		return ""
	}
	ip := strings.TrimSpace(info.IP)
	if ip == "" {
		return ""
	}
	return net.JoinHostPort(ip, strconv.Itoa(info.Port))
}

func (agent *Agent) normalPassiveReconn(ctx context.Context, options *initial.Options) (net.Conn, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	listenAddr, _, err := utils.CheckIPPort(options.Listen)
	if err != nil {
		return nil, err
	}

	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, err
	}

	closeListener := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = listener.Close()
		case <-closeListener:
		}
	}()
	defer func() {
		close(closeListener)
		listener.Close()
	}()

	activeUUID := agent.activeUUID()

	flags := agent.protocolFlags()
	greet := handshake.RandomGreeting(handshake.RoleAdmin)
	hiTemplate := &protocol.HIMess{
		GreetingLen: uint16(len(greet)),
		Greeting:    greet,
		UUIDLen:     uint16(len(activeUUID)),
		UUID:        activeUUID,
		IsAdmin:     0,
		IsReconnect: 1,
		ProtoFlags:  flags,
	}

	var lastErr error
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				WarnRuntime(agent.mgr, "AGENT_PASSIVE_ACCEPT", true, err, "temporary passive accept error")
				continue
			}
			lastErr = err
			break
		}

		if agent.tlsEnabled() {
			var tlsConfig *tls.Config
			tlsConfig, err = transport.NewServerTLSConfig(agent.preAuthToken(), agent.options.Domain)
			if err != nil {
				WarnRuntime(agent.mgr, "AGENT_TLS_CONFIG", false, err, "failed to prepare TLS server config")
				conn.Close()
				continue
			}
			conn = transport.WrapTLSServerConn(conn, tlsConfig)
		}

		param := new(protocol.NegParam)
		param.Conn = conn
		proto := protocol.NewUpProto(param)
		if err := proto.SNegotiate(); err != nil {
			conn.Close()
			continue
		}
		conn = param.Conn

		token := agent.preAuthToken()
		if err := share.PassivePreAuth(conn, token); err != nil {
			conn.Close()
			continue
		}

		header := &protocol.Header{
			Flags:       flags,
			Sender:      activeUUID,
			Accepter:    protocol.ADMIN_UUID,
			MessageType: protocol.HI,
			RouteLen:    uint32(len([]byte(protocol.TEMP_ROUTE))),
			Route:       protocol.TEMP_ROUTE,
		}

		conn, err = agent.finalizePassiveHandshake(conn, options, hiTemplate, header, activeUUID)
		if err != nil {
			conn.Close()
			continue
		}

		return conn, nil
	}

	return nil, lastErr
}

func (agent *Agent) ipTableReusePassiveReconn(ctx context.Context, options *initial.Options) (net.Conn, error) {
	return agent.normalPassiveReconn(ctx, options)
}

func (agent *Agent) soReusePassiveReconn(ctx context.Context, options *initial.Options) (net.Conn, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	listenAddr := fmt.Sprintf("%s:%s", options.ReuseHost, options.ReusePort)

	listener, err := reuseport.Listen("tcp", listenAddr)
	if err != nil {
		return nil, err
	}

	closeListener := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = listener.Close()
		case <-closeListener:
		}
	}()
	defer func() {
		close(closeListener)
		listener.Close()
	}()

	activeUUID := agent.activeUUID()

	flags := agent.protocolFlags()
	greet := handshake.RandomGreeting(handshake.RoleAdmin)
	hiTemplate := &protocol.HIMess{
		GreetingLen: uint16(len(greet)),
		Greeting:    greet,
		UUIDLen:     uint16(len(activeUUID)),
		UUID:        activeUUID,
		IsAdmin:     0,
		IsReconnect: 1,
		ProtoFlags:  flags,
	}

	var reuseLastErr error
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				WarnRuntime(agent.mgr, "AGENT_PASSIVE_SOREUSE_ACCEPT", true, err, "temporary soReuse accept error")
				continue
			}
			reuseLastErr = err
			break
		}

		if agent.tlsEnabled() {
			var tlsConfig *tls.Config
			tlsConfig, err = transport.NewServerTLSConfig(agent.preAuthToken(), agent.options.Domain)
			if err != nil {
				WarnRuntime(agent.mgr, "AGENT_TLS_CONFIG", false, err, "failed to prepare TLS server config")
				conn.Close()
				continue
			}
			conn = transport.WrapTLSServerConn(conn, tlsConfig)
		}

		param := new(protocol.NegParam)
		param.Conn = conn
		proto := protocol.NewUpProto(param)
		if err := proto.SNegotiate(); err != nil {
			conn.Close()
			continue
		}
		conn = param.Conn

		ok, err := initial.PassivePreAuthOrProxy(conn, agent.preAuthToken(), options.ReusePort, 2*time.Second)
		if err != nil {
			conn.Close()
			continue
		}
		if !ok {
			// 非 Shepherd 流量已经被代理转发。
			continue
		}

		header := &protocol.Header{
			Flags:       flags,
			Sender:      activeUUID,
			Accepter:    protocol.ADMIN_UUID,
			MessageType: protocol.HI,
			RouteLen:    uint32(len([]byte(protocol.TEMP_ROUTE))),
			Route:       protocol.TEMP_ROUTE,
		}

		conn, err = agent.finalizePassiveHandshake(conn, options, hiTemplate, header, activeUUID)
		if err != nil {
			conn.Close()
			continue
		}

		return conn, nil
	}

	return nil, reuseLastErr
}

func (agent *Agent) manualActiveReconnect(ctx context.Context, options *initial.Options, proxy share.Proxy, attempts int, delay time.Duration) (net.Conn, error) {
	if options == nil {
		return nil, fmt.Errorf("nil options")
	}
	if attempts <= 0 {
		attempts = 1
	}
	if delay <= 0 {
		delay = manualReconnectDelay
	}

	strategy := reconnectStrategy(options)
	strategy.MaxAttempts = attempts
	strategy.BaseDelay = delay
	strategy.MaxDelay = delay * 8
	sched := reconn.NewScheduler(strategy)

	var lastErr error
	attemptsCh := sched.Attempts(ctx)
	failoverCh := agent.failoverConnChan
	for {
		select {
		case <-ctx.Done():
			goto done
		case candidate := <-failoverCh:
			if agent.adoptFailoverCandidate(candidate) {
				return agent.currentSession().Conn(), nil
			}
		case attempt, ok := <-attemptsCh:
			if !ok {
				goto done
			}
			conn, err := agent.reconnectOnce(ctx, options, proxy)
			if err == nil {
				return conn, nil
			}
			lastErr = err
			WarnRuntime(agent.mgr, "AGENT_RECONNECT_ACTIVE_ATTEMPT", true, err, "active reconnect attempt %d failed", attempt.Index)
		}
	}
done:
	if lastErr == nil {
		lastErr = fmt.Errorf("reconnect attempts cancelled")
	}
	return nil, lastErr
}

func (agent *Agent) reconnectOnce(ctx context.Context, options *initial.Options, proxy share.Proxy) (net.Conn, error) {
	if options == nil {
		return nil, fmt.Errorf("nil options")
	}
	sess := agent.currentSession()
	if sess == nil || sess.UUID() == "" {
		return nil, fmt.Errorf("missing global component state")
	}

	var (
		conn net.Conn
		err  error
	)

	if proxy == nil {
		if ctx == nil {
			ctx = context.Background()
		}
		d := &net.Dialer{}
		conn, err = d.DialContext(ctx, "tcp", options.Connect)
	} else {
		// Proxy 拨号本身不感知 context；这里保留该行为，但确保后续读取都有 deadline。
		conn, err = proxy.Dial()
	}
	if err != nil {
		return nil, err
	}

	if agent.tlsEnabled() {
		var tlsConfig *tls.Config
		tlsConfig, err = transport.NewClientTLSConfig(options.Domain, options.PreAuthToken)
		if err != nil {
			conn.Close()
			return nil, err
		}
		conn = transport.WrapTLSClientConn(conn, tlsConfig)
	}

	param := &protocol.NegParam{
		Conn:   conn,
		Domain: options.Domain,
	}
	proto := protocol.NewUpProto(param)
	if err := proto.CNegotiate(); err != nil {
		conn.Close()
		return nil, err
	}
	conn = param.Conn

	token := agent.preAuthToken()
	if err := share.ActivePreAuth(conn, token); err != nil {
		conn.Close()
		return nil, err
	}

	activeUUID := sess.UUID()
	flags := agent.protocolFlags()
	hiMess := &protocol.HIMess{
		GreetingLen: uint16(len("Shhh...")),
		Greeting:    "Shhh...",
		UUIDLen:     uint16(len(activeUUID)),
		UUID:        activeUUID,
		IsAdmin:     0,
		IsReconnect: 1,
		ProtoFlags:  flags,
	}
	header := &protocol.Header{
		Sender:      activeUUID,
		Accepter:    protocol.ADMIN_UUID,
		MessageType: protocol.HI,
		RouteLen:    uint32(len([]byte(protocol.TEMP_ROUTE))),
		Route:       protocol.TEMP_ROUTE,
	}

	handshakeSecret := agent.reconHandshakeSecret(options)
	transport := options.Upstream
	if strings.TrimSpace(transport) == "" {
		transport = agent.upstreamTransport()
	}
	if handshakeDebugEnabled() {
		base := ""
		if options != nil {
			base = options.BaseSecret()
		}
		logger.Infof("handshake-debug reconnectOnce uuid=%s connect=%s tls=%t transport=%s hs(len=%d fp=%s) base(len=%d fp=%s) opt(len=%d fp=%s)",
			activeUUID,
			strings.TrimSpace(options.Connect),
			agent.tlsEnabled(),
			strings.TrimSpace(transport),
			len(handshakeSecret),
			secretFingerprint(handshakeSecret),
			len(base),
			secretFingerprint(base),
			len(options.Secret),
			secretFingerprint(options.Secret),
		)
	}
	sMessage := protocol.NewUpMsgWithTransport(conn, handshakeSecret, protocol.TEMP_UUID, transport)
	protocol.SetMessageMeta(sMessage, flags)
	protocol.ConstructMessage(sMessage, header, hiMess, false)
	sMessage.SendMessage()

	_ = conn.SetReadDeadline(time.Now().Add(defaults.HandshakeReadTimeout))
	rMessage := protocol.NewUpMsgWithTransport(conn, handshakeSecret, protocol.TEMP_UUID, transport)
	fHeader, fMessage, err := protocol.DestructMessage(rMessage)
	_ = conn.SetReadDeadline(time.Time{})
	if err != nil {
		conn.Close()
		return nil, err
	}

	if fHeader.MessageType == protocol.HI {
		if mmess, ok := fMessage.(*protocol.HIMess); ok {
			if handshake.ValidGreeting(handshake.RoleAdmin, mmess.Greeting) && mmess.IsAdmin == 1 {
				meta := protocol.ResolveProtocolMeta(flags, mmess.ProtoFlags)
				if err := applyProtocolFlags(options, meta.Flags); err != nil {
					conn.Close()
					return nil, err
				}
				if agent.store != nil {
					agent.store.UpdateProtocolFlags(activeUUID, meta.Flags)
				}
				return conn, nil
			}
		}
	}

	conn.Close()
	return nil, fmt.Errorf("unexpected handshake response")
}

func (agent *Agent) activeUUID() string {
	if agent == nil {
		return ""
	}
	if sess := agent.currentSession(); sess != nil {
		return sess.UUID()
	}
	return ""
}

func (agent *Agent) tlsEnabled() bool {
	if agent == nil || agent.store == nil {
		return false
	}
	return agent.store.TLSEnabled()
}

func (agent *Agent) setActiveConnection(conn net.Conn) {
	if agent == nil || conn == nil {
		return
	}
	// 在更新 session/store 之前，先保存旧的上游连接。如果要切换到新的上游
	// （repair/rescue/failover），必须强制主读循环（handleDataFromUpstream）
	// 退出旧连接上的 DestructMessage() 阻塞读取。最简单也最可靠的方式，
	// 就是关闭之前那条活跃连接。
	var oldConn net.Conn
	if sess := agent.currentSession(); sess != nil {
		oldConn = sess.Conn()
	}
	if agent.store != nil {
		agent.store.UpdateActiveConn(conn)
	}
	agent.updateSessionConn(conn)
	// 尽力发一个信号：如果外部 repair/rescue 连接已经更新了活跃会话，
	// 让重连循环可以尽早退出。
	if agent.connChanged != nil {
		select {
		case agent.connChanged <- struct{}{}:
		default:
		}
	}
	// 如果底层连接已经切换，就关闭旧连接，让上游读循环立刻绑定到新的会话。
	if oldConn != nil && oldConn != conn {
		// 调用方有时会交替传入 SafeConn 包装层和其底层裸连接，这里要避免误关新连接。
		sameUnderlying := false
		if sc, ok := oldConn.(*utils.SafeConn); ok && sc != nil && sc.Conn == conn {
			sameUnderlying = true
		} else if sc, ok := conn.(*utils.SafeConn); ok && sc != nil && sc.Conn == oldConn {
			sameUnderlying = true
		} else if osc, ok := oldConn.(*utils.SafeConn); ok && osc != nil {
			if nsc, ok := conn.(*utils.SafeConn); ok && nsc != nil && osc.Conn == nsc.Conn {
				sameUnderlying = true
			}
		}
		if !sameUnderlying {
			_ = oldConn.Close()
		}
	}
}

func (agent *Agent) normalReconnActiveReconn(ctx context.Context, options *initial.Options, proxy share.Proxy) net.Conn {
	strategy := reconnectStrategy(options)
	sched := reconn.NewScheduler(strategy)
	initialConn := func() net.Conn {
		if sess := agent.currentSession(); sess != nil {
			return sess.Conn()
		}
		return nil
	}()
	var lastErr error
	attemptsCh := sched.Attempts(ctx)
	failoverCh := agent.failoverConnChan
	for {
		select {
		case <-ctx.Done():
			goto done
		case <-agent.connChanged:
			// repair/rescue 连接已经替换了当前活跃的上游会话。
			if sess := agent.currentSession(); sess != nil {
				if conn := sess.Conn(); conn != nil && conn != initialConn {
					return conn
				}
			}
		case candidate := <-failoverCh:
			if agent.adoptFailoverCandidate(candidate) {
				return agent.currentSession().Conn()
			}
		case attempt, ok := <-attemptsCh:
			if !ok {
				goto done
			}
			conn, err := agent.reconnectOnce(ctx, options, proxy)
			if err == nil {
				return conn
			}
			lastErr = err
			WarnRuntime(agent.mgr, "AGENT_RECONNECT_ATTEMPT", true, err, "reconnect attempt %d failed", attempt.Index)
		}
	}
done:
	if lastErr != nil {
		WarnRuntime(agent.mgr, "AGENT_RECONNECT_EXHAUSTED", true, lastErr, "reconnect attempts exhausted")
	}
	return nil
}
