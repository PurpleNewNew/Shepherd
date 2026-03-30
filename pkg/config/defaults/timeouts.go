package defaults

import "time"

// 通用超时与截止时间常量，供 Kelpie 与 Flock 复用。
const (
	// 控制台等待节点 OK 信号
	ConsoleOKWait = 10 * time.Second

	// 管理端对信息类查询（如 forward/backward/socks info）的等待时间
	InfoQueryTimeout = 3 * time.Second

	// 初始/重连握手后的读取超时（防止握手卡死）
	HandshakeReadTimeout = 5 * time.Second
	// 初始连接建立的拨号超时
	HandshakeDialTimeout = 10 * time.Second

	// 广播写入截止时间（上下线通知等）
	BroadcastWriteDeadline = 1 * time.Second

	// 代理握手（HTTP/SOCKS5 CONNECT）总超时
	ProxyHandshakeDeadline = 5 * time.Second

	// AutoSOCKS 周期检查间隔
	AutoSOCKSMonitorInterval = 10 * time.Second

	// NodeStaleTimeout 超过该时间未收到任何更新，则将节点标记为离线。
	// 该值应不小于 Flock 的心跳离线阈值（默认约 60s）。
	NodeStaleTimeout = 60 * time.Second

	// StalenessSweepInterval 扫描并标记离线状态的周期。
	StalenessSweepInterval = 10 * time.Second

	// ListenerAckTimeout 等待节点反馈监听状态的超时时间。
	ListenerAckTimeout = 5 * time.Second

	// ListenerReconcileInterval 周期性检查监听状态的时间间隔。
	ListenerReconcileInterval = 30 * time.Second

	// ListenerRetryInterval 监听启动失败后的最少重试间隔。
	ListenerRetryInterval = 1 * time.Minute

	// ConnectAckTimeout 等待节点响应 CONNECTSTART 的时间。
	ConnectAckTimeout = 5 * time.Second

	// AdminHeartbeatInterval Kelpie 对上游节点发送心跳的间隔。
	AdminHeartbeatInterval = 10 * time.Second
)
