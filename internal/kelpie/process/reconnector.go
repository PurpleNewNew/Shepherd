package process

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"codeberg.org/agnoie/shepherd/internal/kelpie/initial"
	"codeberg.org/agnoie/shepherd/internal/kelpie/printer"
	"codeberg.org/agnoie/shepherd/internal/kelpie/topology"
	"codeberg.org/agnoie/shepherd/pkg/share"
	reconn "codeberg.org/agnoie/shepherd/pkg/share/reconnect"
	"codeberg.org/agnoie/shepherd/protocol"
)

const reconnectMaxAttempts = 5

type reconnMetrics struct {
	mu       sync.Mutex
	attempts uint64
	success  uint64
	failures uint64
	lastErr  string
}

type ReconnectStats struct {
	Attempts  uint64
	Success   uint64
	Failures  uint64
	LastError string
}

func (m *reconnMetrics) recordAttempt() {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.attempts++
	m.mu.Unlock()
}

func (m *reconnMetrics) recordSuccess() {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.success++
	m.lastErr = ""
	m.mu.Unlock()
}

func (m *reconnMetrics) recordFailure(err error) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.failures++
	if err != nil {
		m.lastErr = err.Error()
	}
	m.mu.Unlock()
}

type Reconnector struct {
	ctx      context.Context
	options  *initial.Options
	topo     *topology.Topology
	metrics  reconnMetrics
	strategy reconn.Strategy

	tryConnect  func(*initial.Options) (net.Conn, *protocol.ProtocolMeta, error)
	updateConn  func(net.Conn)
	updateTopo  func() error
	updateProto func(*protocol.ProtocolMeta)
}

func NewReconnector(ctx context.Context, opt *initial.Options, topo *topology.Topology) *Reconnector {
	if ctx == nil {
		ctx = context.Background()
	}
	return &Reconnector{
		ctx:      ctx,
		options:  opt,
		topo:     topo,
		strategy: reconn.DefaultStrategy,
	}
}

func (r *Reconnector) WithDialer(fn func(*initial.Options) (net.Conn, *protocol.ProtocolMeta, error)) *Reconnector {
	if r != nil {
		r.tryConnect = fn
	}
	return r
}

func (r *Reconnector) WithConnUpdater(fn func(net.Conn)) *Reconnector {
	if r != nil {
		r.updateConn = fn
	}
	return r
}

func (r *Reconnector) WithTopoUpdater(fn func() error) *Reconnector {
	if r != nil {
		r.updateTopo = fn
	}
	return r
}

func (r *Reconnector) WithProtocolUpdater(fn func(*protocol.ProtocolMeta)) *Reconnector {
	if r != nil {
		r.updateProto = fn
	}
	return r
}

func (r *Reconnector) Stats() ReconnectStats {
	if r == nil {
		return ReconnectStats{}
	}
	r.metrics.mu.Lock()
	defer r.metrics.mu.Unlock()
	return ReconnectStats{
		Attempts:  r.metrics.attempts,
		Success:   r.metrics.success,
		Failures:  r.metrics.failures,
		LastError: r.metrics.lastErr,
	}
}

func (r *Reconnector) Attempt(adminCtx context.Context, options *initial.Options) (net.Conn, *protocol.ProtocolMeta, error) {
	if r == nil {
		return nil, nil, fmt.Errorf("reconnector not initialised")
	}
	if options == nil {
		options = r.options
	}
	if options == nil {
		return nil, nil, fmt.Errorf("missing reconnect options")
	}
	if r.tryConnect == nil {
		r.tryConnect = func(opt *initial.Options) (net.Conn, *protocol.ProtocolMeta, error) {
			switch opt.Mode {
			case initial.NORMAL_PASSIVE:
				return initial.NormalPassive(opt, r.topo)
			case initial.NORMAL_ACTIVE:
				return initial.NormalActive(opt, r.topo, nil)
			case initial.SOCKS5_PROXY_ACTIVE:
				proxy := share.NewSocks5Proxy(opt.Connect, opt.Socks5Proxy, opt.Socks5ProxyU, opt.Socks5ProxyP)
				return initial.NormalActive(opt, r.topo, proxy)
			case initial.HTTP_PROXY_ACTIVE:
				proxy := share.NewHTTPProxy(opt.Connect, opt.HttpProxy)
				return initial.NormalActive(opt, r.topo, proxy)
			default:
				return nil, nil, fmt.Errorf("unsupported reconnect mode %d", opt.Mode)
			}
		}
	}
	ctx := adminCtx
	if ctx == nil {
		ctx = r.ctx
	}
	strategy := r.strategy
	strategy.MaxAttempts = reconnectMaxAttempts
	strategy.BaseDelay = time.Second
	strategy.MaxDelay = 30 * time.Second
	strategy.ImmediateFirst = true
	sched := reconn.NewScheduler(strategy)
	for attempt := range sched.Attempts(ctx) {
		r.metrics.recordAttempt()
		conn, nego, err := r.tryConnect(options)
		if err == nil {
			if r.updateConn != nil {
				r.updateConn(conn)
			}
			if r.updateTopo != nil {
				if topoErr := r.updateTopo(); topoErr != nil {
					printer.Warning("\r\n[*] Topology recalculation failed after reconnect: %v\r\n", topoErr)
				}
			}
			if r.updateProto != nil && nego != nil {
				r.updateProto(nego)
			}
			r.metrics.recordSuccess()
			return conn, nego, nil
		}
		r.metrics.recordFailure(err)
		if options.Mode == initial.NORMAL_PASSIVE {
			printer.Fail("\r\n[*] Passive reconnect failed: %s\r\n", err)
			return nil, nil, err
		}
		printer.Fail("\r\n[*] Reconnect attempt %d failed: %v\r\n", attempt.Index, err)
	}
	return nil, nil, fmt.Errorf("reconnect attempts exhausted: %s", r.metrics.lastErr)
}

func (r *Reconnector) Close() {
	if r == nil {
		return
	}
}
