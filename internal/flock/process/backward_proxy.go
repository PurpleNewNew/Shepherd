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

	"codeberg.org/agnoie/shepherd/pkg/share/streamopts"
	"codeberg.org/agnoie/shepherd/pkg/streamid"
	"codeberg.org/agnoie/shepherd/protocol"
)

const (
	streamKindBackwardProxy = "backward-proxy"
	streamKindBackwardConn  = "backward-conn"
)

type backwardProxy struct {
	controlStreamID uint32
	remotePort      string
	localPort       string
	listener        net.Listener
	ctx             context.Context
	cancel          context.CancelFunc
	wg              sync.WaitGroup

	mu       sync.Mutex
	children map[uint32]struct{}
}

func (agent *Agent) backwardProxyOnOpen(streamID uint32, opts map[string]string) {
	if agent == nil {
		return
	}
	rport, err := parseProxyPort(opts["rport"], "remote port")
	if err != nil {
		agent.rejectStreamOpen(streamID, err.Error())
		return
	}
	lport, err := parseProxyPort(opts["lport"], "local port")
	if err != nil {
		agent.rejectStreamOpen(streamID, err.Error())
		return
	}
	ln, err := net.Listen("tcp", net.JoinHostPort("0.0.0.0", rport))
	if err != nil {
		agent.rejectStreamOpen(streamID, fmt.Sprintf("backward listen failed: %v", err))
		return
	}
	ctx, cancel := context.WithCancel(agent.context())
	bp := &backwardProxy{
		controlStreamID: streamID,
		remotePort:      rport,
		localPort:       lport,
		listener:        ln,
		ctx:             ctx,
		cancel:          cancel,
		children:        make(map[uint32]struct{}),
	}
	agent.fwdMu.Lock()
	if agent.backwardByID == nil {
		agent.backwardByID = make(map[uint32]*backwardProxy)
	}
	agent.backwardByID[streamID] = bp
	agent.fwdMu.Unlock()

	agent.sendStreamData(streamID, []byte("ready "+ln.Addr().String()))
	go bp.run(agent)
}

func (bp *backwardProxy) run(agent *Agent) {
	if bp == nil || bp.listener == nil || agent == nil {
		return
	}
	for {
		conn, err := bp.listener.Accept()
		if err != nil {
			select {
			case <-bp.ctx.Done():
				return
			default:
			}
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			go agent.closeBackwardProxy(bp.controlStreamID, 1, fmt.Sprintf("backward accept failed: %v", err))
			return
		}
		bp.wg.Add(1)
		go func(c net.Conn) {
			defer bp.wg.Done()
			agent.handleBackwardConn(bp, c)
		}(conn)
	}
}

func (agent *Agent) handleBackwardConn(bp *backwardProxy, conn net.Conn) {
	if agent == nil || bp == nil || conn == nil {
		if conn != nil {
			_ = conn.Close()
		}
		return
	}
	streamID := streamid.Next()
	meta := map[string]string{
		"kind":  streamKindBackwardConn,
		"lport": bp.localPort,
	}
	options := streamopts.Encode(meta)
	agent.streamMu.Lock()
	if agent.streams == nil {
		agent.streams = make(map[uint32]*streamState)
	}
	agent.streams[streamID] = &streamState{
		options:  options,
		kind:     streamKindBackwardConn,
		meta:     meta,
		txWindow: streamDefaultWindow,
	}
	agent.streamMu.Unlock()

	agent.fwdMu.Lock()
	if agent.backwardConnByID == nil {
		agent.backwardConnByID = make(map[uint32]net.Conn)
	}
	agent.backwardConnByID[streamID] = conn
	agent.fwdMu.Unlock()
	bp.addChild(streamID)

	if err := agent.sendStreamOpenToAdmin(streamID, options); err != nil {
		bp.removeChild(streamID)
		agent.backwardConnClose(streamID, 1, err.Error())
		return
	}

	buf := make([]byte, 32*1024)
	for {
		n, err := conn.Read(buf)
		if n > 0 {
			agent.sendStreamData(streamID, append([]byte(nil), buf[:n]...))
		}
		if err != nil {
			code := uint16(0)
			reason := "backward connection closed"
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				code = 1
				reason = err.Error()
			}
			bp.removeChild(streamID)
			agent.backwardConnClose(streamID, code, reason)
			return
		}
	}
}

func (agent *Agent) backwardConnOnData(streamID uint32, data []byte) {
	if agent == nil || len(data) == 0 {
		return
	}
	agent.fwdMu.Lock()
	conn := agent.backwardConnByID[streamID]
	agent.fwdMu.Unlock()
	if conn != nil {
		_, _ = conn.Write(data)
	}
}

func (agent *Agent) backwardProxyOnClose(streamID uint32) {
	agent.closeBackwardProxy(streamID, 0, "backward proxy stopped")
}

func (agent *Agent) closeBackwardProxy(streamID uint32, code uint16, reason string) {
	if agent == nil {
		return
	}
	agent.fwdMu.Lock()
	var bp *backwardProxy
	if agent.backwardByID != nil {
		bp = agent.backwardByID[streamID]
		delete(agent.backwardByID, streamID)
	}
	agent.fwdMu.Unlock()
	if bp == nil {
		return
	}
	childIDs := bp.childStreamIDs()
	if bp.cancel != nil {
		bp.cancel()
	}
	if bp.listener != nil {
		_ = bp.listener.Close()
	}
	for _, childID := range childIDs {
		agent.backwardConnClose(childID, code, reason)
	}
	bp.wg.Wait()
	agent.sendStreamClose(streamID, code, reason)
}

func (agent *Agent) backwardConnOnClose(streamID uint32) {
	agent.backwardConnClose(streamID, 0, "backward connection closed")
}

func (agent *Agent) backwardConnClose(streamID uint32, code uint16, reason string) {
	if agent == nil {
		return
	}
	var (
		conn    net.Conn
		removed bool
	)
	agent.fwdMu.Lock()
	if agent.backwardConnByID != nil {
		conn = agent.backwardConnByID[streamID]
		if conn != nil {
			delete(agent.backwardConnByID, streamID)
			removed = true
		}
	}
	agent.fwdMu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
	agent.streamMu.Lock()
	if agent.streams != nil {
		if _, ok := agent.streams[streamID]; ok {
			delete(agent.streams, streamID)
			removed = true
		}
	}
	agent.streamMu.Unlock()
	if removed {
		agent.sendStreamClose(streamID, code, reason)
	}
}

func (agent *Agent) sendStreamOpenToAdmin(streamID uint32, options string) error {
	if agent == nil {
		return fmt.Errorf("agent unavailable")
	}
	agent.noteActivity()
	conn, secret, ok := agent.streamUplinkConn(streamID)
	if !ok || conn == nil || secret == "" {
		return fmt.Errorf("active session unavailable")
	}
	up := protocol.NewUpMsg(conn, secret, agent.UUID)
	header := &protocol.Header{
		Sender:      agent.UUID,
		Accepter:    protocol.ADMIN_UUID,
		MessageType: uint16(protocol.STREAM_OPEN),
		RouteLen:    uint32(len([]byte(protocol.TEMP_ROUTE))),
		Route:       protocol.TEMP_ROUTE,
	}
	open := &protocol.StreamOpen{StreamID: streamID, Options: options}
	protocol.ConstructMessage(up, header, open, false)
	up.SendMessage()
	return nil
}

func (bp *backwardProxy) addChild(streamID uint32) {
	if bp == nil {
		return
	}
	bp.mu.Lock()
	if bp.children == nil {
		bp.children = make(map[uint32]struct{})
	}
	bp.children[streamID] = struct{}{}
	bp.mu.Unlock()
}

func (bp *backwardProxy) removeChild(streamID uint32) {
	if bp == nil {
		return
	}
	bp.mu.Lock()
	delete(bp.children, streamID)
	bp.mu.Unlock()
}

func (bp *backwardProxy) childStreamIDs() []uint32 {
	if bp == nil {
		return nil
	}
	bp.mu.Lock()
	defer bp.mu.Unlock()
	ids := make([]uint32, 0, len(bp.children))
	for id := range bp.children {
		ids = append(ids, id)
	}
	return ids
}

func parseProxyPort(value, label string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", fmt.Errorf("%s missing", label)
	}
	port, err := strconv.Atoi(value)
	if err != nil || port <= 0 || port > 65535 {
		return "", fmt.Errorf("invalid %s: %s", label, value)
	}
	return strconv.Itoa(port), nil
}
