package utils

import (
	"io"
	"net"
	"sync"
	"time"
)

// SafeConn 包装 net.Conn，提供串行且完整的写操作。
// 读取与超时相关操作仍直接委托给底层连接。
type SafeConn struct {
	net.Conn
	mu sync.Mutex
}

// NewSafeConn 返回一个写入受串行保护的连接，确保缓冲区在无错误时完整写出。
// 如果传入的连接已经是 SafeConn，将原样返回。
func NewSafeConn(conn net.Conn) net.Conn {
	if conn == nil {
		return nil
	}
	if _, ok := conn.(*SafeConn); ok {
		return conn
	}
	return &SafeConn{Conn: conn}
}

// Write 在持有写锁期间尝试将数据全部发送，直到出错或全部写出。
func (s *SafeConn) Write(p []byte) (int, error) {
	if s == nil {
		return 0, io.ErrClosedPipe
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	total := 0
	for total < len(p) {
		n, err := s.Conn.Write(p[total:])
		if n > 0 {
			total += n
		}
		if err != nil {
			return total, err
		}
		if n == 0 {
			return total, io.ErrShortWrite
		}
	}
	return total, nil
}

// WithWrite 在持有写锁的前提下执行 fn，便于一次性执行多次写操作。
func (s *SafeConn) WithWrite(fn func(net.Conn) error) error {
	if s == nil {
		return io.ErrClosedPipe
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return fn(s.Conn)
}

// 其余方法直接透传到底层连接以保持原行为。
func (s *SafeConn) Read(b []byte) (int, error)         { return s.Conn.Read(b) }
func (s *SafeConn) Close() error                       { return s.Conn.Close() }
func (s *SafeConn) LocalAddr() net.Addr                { return s.Conn.LocalAddr() }
func (s *SafeConn) RemoteAddr() net.Addr               { return s.Conn.RemoteAddr() }
func (s *SafeConn) SetDeadline(t time.Time) error      { return s.Conn.SetDeadline(t) }
func (s *SafeConn) SetReadDeadline(t time.Time) error  { return s.Conn.SetReadDeadline(t) }
func (s *SafeConn) SetWriteDeadline(t time.Time) error { return s.Conn.SetWriteDeadline(t) }
