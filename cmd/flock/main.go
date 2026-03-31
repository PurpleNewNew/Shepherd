package main

import (
	"context"
	"net"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"codeberg.org/agnoie/shepherd/internal/flock/initial"
	"codeberg.org/agnoie/shepherd/internal/flock/process"
	"codeberg.org/agnoie/shepherd/pkg/global"
	"codeberg.org/agnoie/shepherd/pkg/logging"
	"codeberg.org/agnoie/shepherd/pkg/share"
	"codeberg.org/agnoie/shepherd/pkg/utils/runtimeerr"
	"codeberg.org/agnoie/shepherd/protocol"
)

var logger = logging.New("agent/main")

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	options, err := initial.ParseOptions()
	if err != nil {
		logger.Errorf("failed to parse options: %v", err)
		return
	}

	transports := protocol.NewTransports(options.Upstream, options.Downstream)
	protocol.SetDefaultTransports(options.Upstream, options.Downstream)

	token := share.GeneratePreAuthToken(options.Secret)

	store := global.NewStoreWithTransports(transports)
	if err := store.SetPreAuthToken(token); err != nil {
		logger.Errorf("failed to set pre-auth token: %v", err)
		return
	}
	options.PreAuthToken = token

	agent := process.NewAgent(ctx, options, store, nil)
	defer agent.Stop()

	protocol.ConfigureHTTP(
		protocol.HTTPConfig{
			Host:      options.HTTPHost,
			Path:      options.HTTPPath,
			UserAgent: options.HTTPUserAgent,
		},
		protocol.HTTPConfig{
			Host:      options.HTTPHost,
			Path:      options.HTTPPath,
			UserAgent: options.HTTPUserAgent,
		},
	)

	var (
		conn net.Conn
		uuid string
		meta *protocol.ProtocolMeta
	)

	switch options.Mode {
	case initial.NORMAL_PASSIVE:
		conn, uuid, meta, err = initial.NormalPassive(options)
	case initial.NORMAL_RECONNECT_ACTIVE:
		fallthrough
	case initial.NORMAL_ACTIVE:
		conn, uuid, meta, err = initial.NormalActive(ctx, options, nil)
	case initial.SOCKS5_PROXY_RECONNECT_ACTIVE:
		fallthrough
	case initial.SOCKS5_PROXY_ACTIVE:
		proxy := share.NewSocks5Proxy(options.Connect, options.Socks5Proxy, options.Socks5ProxyU, options.Socks5ProxyP)
		conn, uuid, meta, err = initial.NormalActive(ctx, options, proxy)
	case initial.HTTP_PROXY_RECONNECT_ACTIVE:
		fallthrough
	case initial.HTTP_PROXY_ACTIVE:
		proxy := share.NewHTTPProxy(options.Connect, options.HttpProxy)
		conn, uuid, meta, err = initial.NormalActive(ctx, options, proxy)
	case initial.IPTABLES_REUSE_PASSIVE:
		defer initial.DeletePortReuseRules(options.Listen, options.ReusePort)
		conn, uuid, meta, err = initial.IPTableReusePassive(options)
	case initial.SO_REUSE_PASSIVE:
		conn, uuid, meta, err = initial.SoReusePassive(options)
	default:
		err = runtimeerr.New("AGENT_UNKNOWN_MODE", runtimeerr.SeverityFatal, false, "unknown mode %d", options.Mode)
	}

	if err != nil {
		logger.Errorf("agent initialization failed: %s", runtimeerr.Format(err))
		return
	}

	agent.UUID = uuid

	store.InitializeComponent(conn, options.Secret, agent.UUID, options.Upstream, options.Downstream)
	if meta != nil {
		store.UpdateProtocolFlags(agent.UUID, meta.Flags)
	}
	store.SetTLSEnabled(options.TlsEnable)
	agent.BindSession(store.ActiveSession())
	agent.SetStore(store)

	agent.Run()
}
