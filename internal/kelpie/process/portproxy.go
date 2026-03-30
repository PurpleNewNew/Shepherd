package process

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"

	"codeberg.org/agnoie/shepherd/internal/kelpie/printer"
)

var (
	// ErrProxyMissingTarget indicates the caller did not provide a node UUID.
	ErrProxyMissingTarget = errors.New("missing target uuid")
	// ErrProxyMissingBind indicates no local bind address/port was provided.
	ErrProxyMissingBind = errors.New("missing local bind address")
	// ErrProxyInvalidRemote indicates the remote address is malformed.
	ErrProxyInvalidRemote = errors.New("invalid remote address")
	// ErrProxyInvalidPort indicates a provided port is not valid.
	ErrProxyInvalidPort = errors.New("invalid port")
	// ErrProxyNotFound indicates the requested proxy handle was not found.
	ErrProxyNotFound = errors.New("proxy entry not found")
)

const (
	kindForwardProxy  = "proxy" // 复用 proxy kind，Kelpie 端监听 -> Flock 端拨目标
	kindBackwardProxy = "backward-proxy"
)

type streamOpenFunc func(ctx context.Context, target, sessionID string, meta map[string]string) (io.ReadWriteCloser, error)

// ProxyDescriptor 描述一个长生命周期的本地代理（forward/backward）。
type ProxyDescriptor struct {
	id      string
	target  string
	kind    string
	options map[string]string
}

func newProxyDescriptor(id, target, kind string, options map[string]string) *ProxyDescriptor {
	copyOpts := make(map[string]string, len(options))
	for k, v := range options {
		copyOpts[k] = v
	}
	return &ProxyDescriptor{id: id, target: strings.ToLower(strings.TrimSpace(target)), kind: kind, options: copyOpts}
}

// NewProxyDescriptor creates a proxy descriptor using the provided fields.
func NewProxyDescriptor(id, target, kind string, options map[string]string) *ProxyDescriptor {
	return newProxyDescriptor(id, target, kind, options)
}

func (d *ProxyDescriptor) ID() string {
	if d == nil {
		return ""
	}
	return d.id
}

func (d *ProxyDescriptor) Target() string {
	if d == nil {
		return ""
	}
	return d.target
}

func (d *ProxyDescriptor) Kind() string {
	if d == nil {
		return ""
	}
	return d.kind
}

func (d *ProxyDescriptor) Options() map[string]string {
	if d == nil || len(d.options) == 0 {
		return nil
	}
	copyOpts := make(map[string]string, len(d.options))
	for k, v := range d.options {
		copyOpts[k] = v
	}
	return copyOpts
}

type portProxyManager struct {
	ctxFn      func() context.Context
	openStream streamOpenFunc

	mu        sync.Mutex
	nextID    uint64
	forwards  map[string]*forwardEntry
	backwards map[string]*backwardEntry
}

func newPortProxyManager(ctxFn func() context.Context, opener streamOpenFunc) *portProxyManager {
	return &portProxyManager{
		ctxFn:      ctxFn,
		openStream: opener,
		forwards:   make(map[string]*forwardEntry),
		backwards:  make(map[string]*backwardEntry),
	}
}

func contextOrBackground(ctx context.Context) context.Context {
	if ctx != nil {
		return ctx
	}
	return context.Background()
}

func (m *portProxyManager) StartForward(ctx context.Context, target, bind, remote string) (*ProxyDescriptor, error) {
	if m == nil {
		return nil, fmt.Errorf("port proxy manager unavailable")
	}
	target = strings.TrimSpace(target)
	if target == "" {
		return nil, ErrProxyMissingTarget
	}
	bindAddr, err := normalizeBind(bind)
	if err != nil {
		return nil, err
	}
	remoteAddr, err := normalizeRemote(remote)
	if err != nil {
		return nil, err
	}
	ln, err := net.Listen("tcp", bindAddr)
	if err != nil {
		return nil, err
	}
	actualBind := ln.Addr().String()
	entryCtx, cancel := context.WithCancel(contextOrBackground(ctx))
	entry := &forwardEntry{
		target:     strings.ToLower(target),
		bind:       actualBind,
		remote:     remoteAddr,
		listener:   ln,
		ctx:        entryCtx,
		cancel:     cancel,
		openStream: m.openStream,
	}
	m.mu.Lock()
	m.nextID++
	entry.id = fmt.Sprintf("fwd-%d", m.nextID)
	m.forwards[entry.id] = entry
	m.mu.Unlock()
	go entry.run()
	options := map[string]string{
		"proxy_id": entry.id,
		"bind":     actualBind,
		"remote":   remoteAddr,
	}
	return newProxyDescriptor(entry.id, entry.target, kindForwardProxy, options), nil
}

func (fe *forwardEntry) descriptor() *ProxyDescriptor {
	if fe == nil {
		return nil
	}
	return newProxyDescriptor(fe.id, fe.target, kindForwardProxy, map[string]string{
		"proxy_id": fe.id,
		"bind":     fe.bind,
		"remote":   fe.remote,
	})
}

func (be *backwardEntry) descriptor() *ProxyDescriptor {
	if be == nil {
		return nil
	}
	return newProxyDescriptor(be.id, be.target, kindBackwardProxy, map[string]string{
		"proxy_id":    be.id,
		"remote_port": be.remote,
		"local_port":  be.local,
	})
}

func (m *portProxyManager) StopForward(target, id string) ([]*ProxyDescriptor, error) {
	if m == nil {
		return nil, nil
	}
	target = strings.TrimSpace(strings.ToLower(target))
	if target == "" {
		return nil, ErrProxyMissingTarget
	}
	m.mu.Lock()
	var entries []*forwardEntry
	if id != "" {
		entry, ok := m.forwards[id]
		if !ok || entry == nil || entry.target != target {
			m.mu.Unlock()
			return nil, ErrProxyNotFound
		}
		entries = append(entries, entry)
		delete(m.forwards, id)
	} else {
		for fid, entry := range m.forwards {
			if entry != nil && entry.target == target {
				entries = append(entries, entry)
				delete(m.forwards, fid)
			}
		}
	}
	m.mu.Unlock()
	descriptors := make([]*ProxyDescriptor, 0, len(entries))
	for _, entry := range entries {
		if desc := entry.descriptor(); desc != nil {
			descriptors = append(descriptors, desc)
		}
		entry.stop()
	}
	return descriptors, nil
}

func (m *portProxyManager) StartBackward(ctx context.Context, target, remotePort, localPort string) (*ProxyDescriptor, error) {
	if m == nil {
		return nil, fmt.Errorf("port proxy manager unavailable")
	}
	target = strings.TrimSpace(target)
	if target == "" {
		return nil, ErrProxyMissingTarget
	}
	rport, err := normalizePort(remotePort)
	if err != nil {
		return nil, err
	}
	lport, err := normalizePort(localPort)
	if err != nil {
		return nil, err
	}
	entryCtx, cancel := context.WithCancel(contextOrBackground(ctx))
	opts := map[string]string{
		"kind":  kindBackwardProxy,
		"rport": rport,
		"lport": lport,
	}
	stream, err := m.openStream(entryCtx, target, "", opts)
	if err != nil {
		cancel()
		return nil, err
	}
	entry := &backwardEntry{
		target: strings.ToLower(target),
		remote: rport,
		local:  lport,
		ctx:    entryCtx,
		cancel: cancel,
		stream: stream,
	}
	m.mu.Lock()
	m.nextID++
	entry.id = fmt.Sprintf("bwd-%d", m.nextID)
	m.backwards[entry.id] = entry
	m.mu.Unlock()
	options := map[string]string{
		"proxy_id":    entry.id,
		"remote_port": rport,
		"local_port":  lport,
	}
	return newProxyDescriptor(entry.id, entry.target, kindBackwardProxy, options), nil
}

func (m *portProxyManager) StopBackward(target, id string) ([]*ProxyDescriptor, error) {
	if m == nil {
		return nil, nil
	}
	target = strings.TrimSpace(strings.ToLower(target))
	if target == "" {
		return nil, ErrProxyMissingTarget
	}
	m.mu.Lock()
	var entries []*backwardEntry
	if id != "" {
		entry, ok := m.backwards[id]
		if !ok || entry == nil || entry.target != target {
			m.mu.Unlock()
			return nil, ErrProxyNotFound
		}
		entries = append(entries, entry)
		delete(m.backwards, id)
	} else {
		for bid, entry := range m.backwards {
			if entry != nil && entry.target == target {
				entries = append(entries, entry)
				delete(m.backwards, bid)
			}
		}
	}
	m.mu.Unlock()
	descriptors := make([]*ProxyDescriptor, 0, len(entries))
	for _, entry := range entries {
		if desc := entry.descriptor(); desc != nil {
			descriptors = append(descriptors, desc)
		}
		entry.stop()
	}
	return descriptors, nil
}

func (m *portProxyManager) StopAll() {
	if m == nil {
		return
	}
	m.mu.Lock()
	forwards := make([]*forwardEntry, 0, len(m.forwards))
	for id, entry := range m.forwards {
		if entry != nil {
			forwards = append(forwards, entry)
		}
		delete(m.forwards, id)
	}
	backwards := make([]*backwardEntry, 0, len(m.backwards))
	for id, entry := range m.backwards {
		if entry != nil {
			backwards = append(backwards, entry)
		}
		delete(m.backwards, id)
	}
	m.mu.Unlock()
	for _, entry := range forwards {
		entry.stop()
	}
	for _, entry := range backwards {
		entry.stop()
	}
}

func (m *portProxyManager) List() []*ProxyDescriptor {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	proxies := make([]*ProxyDescriptor, 0, len(m.forwards)+len(m.backwards))
	for _, entry := range m.forwards {
		if entry == nil {
			continue
		}
		if desc := entry.descriptor(); desc != nil {
			proxies = append(proxies, desc)
		}
	}
	for _, entry := range m.backwards {
		if entry == nil {
			continue
		}
		if desc := entry.descriptor(); desc != nil {
			proxies = append(proxies, desc)
		}
	}
	return proxies
}

type forwardEntry struct {
	id         string
	target     string
	bind       string
	remote     string
	listener   net.Listener
	ctx        context.Context
	cancel     context.CancelFunc
	openStream streamOpenFunc
	wg         sync.WaitGroup
}

func (fe *forwardEntry) run() {
	if fe == nil || fe.listener == nil {
		return
	}
	for {
		conn, err := fe.listener.Accept()
		if err != nil {
			select {
			case <-fe.ctx.Done():
				return
			default:
			}
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			printer.Warning("\r\n[*] forward(%s) listener error: %v\r\n", fe.id, err)
			return
		}
		fe.wg.Add(1)
		go func(c net.Conn) {
			defer fe.wg.Done()
			fe.handleConn(c)
		}(conn)
	}
}

func (fe *forwardEntry) handleConn(conn net.Conn) {
	if fe == nil || conn == nil {
		return
	}
	streamCtx, cancel := context.WithCancel(fe.ctx)
	defer cancel()
	opts := map[string]string{
		// 使用通用 proxy kind，由 Flock 侧发起到目标地址的 TCP 连接。
		"kind":   "proxy",
		"target": fe.remote,
	}
	stream, err := fe.openStream(streamCtx, fe.target, "", opts)
	if err != nil {
		printer.Fail("\r\n[*] forward(%s) open stream failed: %v\r\n", fe.id, err)
		conn.Close()
		return
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, _ = io.Copy(stream, conn)
	}()
	go func() {
		defer wg.Done()
		_, _ = io.Copy(conn, stream)
	}()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-fe.ctx.Done():
		_ = stream.Close()
		_ = conn.Close()
		<-done
	case <-done:
		_ = stream.Close()
		_ = conn.Close()
	}
}

func (fe *forwardEntry) stop() {
	if fe == nil {
		return
	}
	if fe.cancel != nil {
		fe.cancel()
	}
	if fe.listener != nil {
		_ = fe.listener.Close()
	}
	fe.wg.Wait()
}

type backwardEntry struct {
	id     string
	target string
	remote string
	local  string
	ctx    context.Context
	cancel context.CancelFunc
	stream io.Closer
}

func (be *backwardEntry) stop() {
	if be == nil {
		return
	}
	if be.cancel != nil {
		be.cancel()
	}
	if be.stream != nil {
		_ = be.stream.Close()
	}
}

func normalizeBind(bind string) (string, error) {
	bind = strings.TrimSpace(bind)
	if bind == "" {
		return "", ErrProxyMissingBind
	}
	if !strings.Contains(bind, ":") {
		bind = net.JoinHostPort("127.0.0.1", bind)
	}
	return bind, nil
}

func normalizeRemote(remote string) (string, error) {
	remote = strings.TrimSpace(remote)
	if remote == "" {
		return "", ErrProxyInvalidRemote
	}
	if _, _, err := net.SplitHostPort(remote); err != nil {
		return "", fmt.Errorf("%w: %v", ErrProxyInvalidRemote, err)
	}
	return remote, nil
}

func normalizePort(port string) (string, error) {
	port = strings.TrimSpace(port)
	if port == "" {
		return "", fmt.Errorf("%w: empty", ErrProxyInvalidPort)
	}
	val, err := strconv.Atoi(port)
	if err != nil || val <= 0 || val > 65535 {
		return "", fmt.Errorf("%w: %s", ErrProxyInvalidPort, port)
	}
	return strconv.Itoa(val), nil
}
