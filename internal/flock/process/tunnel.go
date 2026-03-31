package process

import "strings"

// startSSHTunnel 根据 options 与可选证书启动隧道，并通过 STREAM_CLOSE 通知完成。
func (agent *Agent) startSSHTunnel(streamID uint32, opts map[string]string, cert []byte) {
	if agent == nil || agent.mgr == nil {
		return
	}
	method := strings.TrimSpace(opts["method"]) // "1" or "2"
	addr := strings.TrimSpace(opts["addr"])     // ssh server
	port := strings.TrimSpace(opts["port"])     // local port to expose (on remote 127.0.0.1:<port>)
	user := strings.TrimSpace(opts["username"])
	pass := opts["password"]
	m := 1
	if method == "2" {
		m = 2
	}
	sshTunnel := NewSSHTunnelForStream(m, addr, port, user, pass, cert)
	// 运行
	sshTunnel.Start(agent.mgr)
	// 结束时关闭流
	agent.sendStreamClose(streamID, 0, "ok")
}
