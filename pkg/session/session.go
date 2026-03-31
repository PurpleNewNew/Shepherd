package session

import "net"

// Session 描述一个连接上下文，封装当前传输层、加密密钥和逻辑 UUID。
type Session interface {
	Conn() net.Conn
	Secret() string
	UUID() string
	UpdateConn(net.Conn)
	ProtocolFlags() uint16
	SetProtocolFlags(flags uint16)
}
