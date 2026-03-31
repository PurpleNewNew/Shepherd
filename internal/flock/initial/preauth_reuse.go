package initial

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"codeberg.org/agnoie/shepherd/pkg/utils"
)

const (
	preAuthReuseNonceSize = 32
	preAuthReuseMACSize   = sha256.Size

	preAuthReuseClientLabel = "shepherd/preauth/client"
	preAuthReuseServerLabel = "shepherd/preauth/server"
)

// PassivePreAuthOrProxy 会尝试在 conn 上完成被动预认证握手。
//
// 在 SO_REUSEPORT 场景中，同一个端口可能与真实服务共享。
// 如果传入流量并不是 Shepherd，本函数会用已缓冲的字节启动 ProxyStream，
// 并返回 ok=false、err=nil，让调用方继续接收后续连接。
//
// 注意：这里重新实现了 pkg/share/preauth.go 的最小必要逻辑，
// 因为通用的 PassivePreAuth 在失败时会直接关闭连接，
// 这会破坏对非 Shepherd 流量的透明代理。
func PassivePreAuthOrProxy(conn net.Conn, token string, reusePort string, timeout time.Duration) (ok bool, err error) {
	if conn == nil {
		return false, fmt.Errorf("preauth: nil connection")
	}
	if token == "" {
		return false, fmt.Errorf("preauth: empty token")
	}
	if strings.TrimSpace(reusePort) == "" {
		return false, fmt.Errorf("preauth: empty reuse port")
	}
	if timeout <= 0 {
		timeout = 2 * time.Second
	}

	reqLen := preAuthReuseNonceSize + preAuthReuseMACSize
	buf := make([]byte, reqLen)

	defer func() { _ = conn.SetReadDeadline(time.Time{}) }()
	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return false, err
	}

	n, err := io.ReadFull(conn, buf)
	if err != nil {
		_ = conn.SetReadDeadline(time.Time{})
		if timeoutErr, ok := err.(net.Error); ok && timeoutErr.Timeout() {
			go ProxyStream(conn, buf[:n], reusePort)
			return false, nil
		}
		return false, err
	}
	_ = conn.SetReadDeadline(time.Time{})

	clientNonce := buf[:preAuthReuseNonceSize]
	clientMAC := buf[preAuthReuseNonceSize:]
	expectedClientMAC := computeHMAC(token, []byte(preAuthReuseClientLabel), clientNonce)
	if !hmac.Equal(clientMAC, expectedClientMAC) {
		go ProxyStream(conn, buf, reusePort)
		return false, nil
	}

	serverNonce := make([]byte, preAuthReuseNonceSize)
	if _, err := io.ReadFull(rand.Reader, serverNonce); err != nil {
		return false, err
	}
	serverMAC := computeHMAC(token, []byte(preAuthReuseServerLabel), serverNonce, clientNonce)
	resp := make([]byte, preAuthReuseNonceSize+len(serverMAC))
	copy(resp, serverNonce)
	copy(resp[preAuthReuseNonceSize:], serverMAC)

	if err := conn.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
		return false, err
	}
	defer func() { _ = conn.SetWriteDeadline(time.Time{}) }()
	if err := utils.WriteFull(conn, resp); err != nil {
		return false, err
	}
	return true, nil
}

func computeHMAC(token string, parts ...[]byte) []byte {
	mac := hmac.New(sha256.New, []byte(token))
	for _, part := range parts {
		mac.Write(part)
	}
	return mac.Sum(nil)
}
