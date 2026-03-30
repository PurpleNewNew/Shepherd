package process

import (
	"fmt"
	"net"
	"strings"
	"time"
)

// proxyOnOpen handles STREAM kind "proxy" (Kelpie dataplane HTTP CONNECT 继电).
func (agent *Agent) proxyOnOpen(streamID uint32, opts map[string]string) {
	if agent == nil {
		return
	}
	target := strings.TrimSpace(opts["target"])
	if target == "" {
		agent.rejectStreamOpen(streamID, "proxy target missing")
		return
	}
	conn, err := net.DialTimeout("tcp", target, 10*time.Second)
	if err != nil {
		agent.rejectStreamOpen(streamID, fmt.Sprintf("proxy dial failed: %v", err))
		return
	}
	agent.fwdMu.Lock()
	if agent.fwdByID == nil {
		agent.fwdByID = make(map[uint32]net.Conn)
	}
	agent.fwdByID[streamID] = conn
	agent.fwdMu.Unlock()
	// pump target -> stream
	go func(id uint32, c net.Conn) {
		buf := make([]byte, 32*1024)
		for {
			n, err := c.Read(buf)
			if n > 0 {
				agent.sendStreamData(id, append([]byte(nil), buf[:n]...))
			}
			if err != nil {
				agent.proxyOnClose(id)
				return
			}
		}
	}(streamID, conn)
}

func (agent *Agent) proxyOnData(streamID uint32, data []byte) {
	if agent == nil || len(data) == 0 {
		return
	}
	agent.fwdMu.Lock()
	conn := agent.fwdByID[streamID]
	agent.fwdMu.Unlock()
	if conn != nil {
		_, _ = conn.Write(data)
	}
}

func (agent *Agent) proxyOnClose(streamID uint32) {
	if agent == nil {
		return
	}
	agent.fwdMu.Lock()
	if conn := agent.fwdByID[streamID]; conn != nil {
		_ = conn.Close()
		delete(agent.fwdByID, streamID)
	}
	agent.fwdMu.Unlock()
	agent.sendStreamClose(streamID, 0, "proxy closed")
}
