package protocol

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http/httputil"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"codeberg.org/agnoie/shepherd/pkg/utils"
	"codeberg.org/agnoie/shepherd/pkg/utils/runtimeerr"
)

const (
	errCodeHTTPStatusRead         = "PROTOCOL_HTTP_STATUS_READ"
	errCodeHTTPStatusUnexpected   = "PROTOCOL_HTTP_STATUS_UNEXPECTED"
	errCodeHTTPResponseNotChunked = "PROTOCOL_HTTP_RESPONSE_NOT_CHUNKED"
	errCodeHTTPRequestRead        = "PROTOCOL_HTTP_REQUEST_READ"
	errCodeHTTPRequestEmpty       = "PROTOCOL_HTTP_REQUEST_EMPTY"
	errCodeHTTPRequestHeaders     = "PROTOCOL_HTTP_HEADERS"
	errCodeHTTPRequestNotChunked  = "PROTOCOL_HTTP_REQUEST_NOT_CHUNKED"
	errCodeHTTPWriteHeaders       = "PROTOCOL_HTTP_WRITE_HEADERS"
)

type httpStreamConn struct {
	base   net.Conn
	reader io.Reader
	writer *chunkWriter

	closed    int32
	closeOnce sync.Once
}

func (c *httpStreamConn) Read(p []byte) (int, error) {
	if atomic.LoadInt32(&c.closed) == 1 {
		return 0, io.EOF
	}
	return c.reader.Read(p)
}

func (c *httpStreamConn) Write(p []byte) (int, error) {
	if atomic.LoadInt32(&c.closed) == 1 {
		return 0, io.ErrClosedPipe
	}
	return c.writer.Write(p)
}

func (c *httpStreamConn) Close() error {
	var err error
	c.closeOnce.Do(func() {
		atomic.StoreInt32(&c.closed, 1)
		err = c.writer.Close()
		if cerr := c.base.Close(); err == nil {
			err = cerr
		}
	})
	return err
}

func (c *httpStreamConn) LocalAddr() net.Addr  { return c.base.LocalAddr() }
func (c *httpStreamConn) RemoteAddr() net.Addr { return c.base.RemoteAddr() }

func (c *httpStreamConn) SetDeadline(t time.Time) error {
	return c.base.SetDeadline(t)
}

func (c *httpStreamConn) SetReadDeadline(t time.Time) error {
	return c.base.SetReadDeadline(t)
}

func (c *httpStreamConn) SetWriteDeadline(t time.Time) error {
	return c.base.SetWriteDeadline(t)
}

func (c *httpStreamConn) isHTTPStream() bool { return true }

func isHTTPStreamConn(conn net.Conn) bool {
	if conn == nil {
		return false
	}
	type streamDetector interface {
		isHTTPStream() bool
	}
	if detector, ok := conn.(streamDetector); ok {
		return detector.isHTTPStream()
	}
	return false
}

type chunkWriter struct {
	conn   net.Conn
	mu     sync.Mutex
	closed bool
}

func (w *chunkWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return 0, io.ErrClosedPipe
	}

	header := fmt.Sprintf("%x\r\n", len(p))
	if err := utils.WriteFull(w.conn, []byte(header)); err != nil {
		return 0, err
	}
	if err := utils.WriteFull(w.conn, p); err != nil {
		return 0, err
	}
	if err := utils.WriteFull(w.conn, []byte("\r\n")); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (w *chunkWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	_, err := w.conn.Write([]byte("0\r\n\r\n"))
	return err
}

func newHTTPClientStream(conn net.Conn, cfg HTTPConfig, hostHint string) (*httpStreamConn, error) {
	host := cfg.Host
	if host == "" {
		host = hostHint
	}
	if host == "" {
		if addr := conn.RemoteAddr(); addr != nil {
			host = addr.String()
		} else {
			host = "localhost"
		}
	}

	path := cfg.Path
	if path == "" {
		path = defaultHTTPPath
	}

	ua := cfg.UserAgent
	if ua == "" {
		ua = defaultHTTPUserAgent
	}

	headers := []string{
		fmt.Sprintf("POST %s HTTP/1.1", path),
		fmt.Sprintf("Host: %s", host),
		fmt.Sprintf("User-Agent: %s", ua),
		"Accept: application/octet-stream",
		"Connection: keep-alive",
		"Content-Type: application/octet-stream",
		"Transfer-Encoding: chunked",
		"X-Accel-Buffering: no",
	}
	request := strings.Join(headers, "\r\n") + "\r\n\r\n"
	if err := utils.WriteFull(conn, []byte(request)); err != nil {
		return nil, err
	}

	br := bufio.NewReader(conn)
	statusLine, err := br.ReadString('\n')
	if err != nil {
		return nil, runtimeerr.Wrap(err, errCodeHTTPStatusRead, runtimeerr.SeverityError, true, "读取 HTTP 响应状态行失败")
	}
	statusLine = strings.TrimSpace(statusLine)
	if !strings.HasPrefix(statusLine, "HTTP/1.1 200") {
		return nil, runtimeerr.New(errCodeHTTPStatusUnexpected, runtimeerr.SeverityError, false, "HTTP 响应状态异常: %s", statusLine)
	}
	respHeaders, chunked, err := readHTTPHeaders(br)
	if err != nil {
		return nil, err
	}
	if !chunked {
		return nil, runtimeerr.New(errCodeHTTPResponseNotChunked, runtimeerr.SeverityError, false, "上游响应未启用分块传输: %v", respHeaders)
	}

	stream := &httpStreamConn{
		base:   conn,
		reader: httputil.NewChunkedReader(br),
		writer: &chunkWriter{conn: conn},
	}
	return stream, nil
}

func newHTTPServerStream(conn net.Conn) (*httpStreamConn, error) {
	br := bufio.NewReader(conn)
	requestLine, err := br.ReadString('\n')
	if err != nil {
		return nil, runtimeerr.Wrap(err, errCodeHTTPRequestRead, runtimeerr.SeverityError, true, "读取 HTTP 请求行失败")
	}
	requestLine = strings.TrimSpace(requestLine)
	if requestLine == "" {
		return nil, runtimeerr.New(errCodeHTTPRequestEmpty, runtimeerr.SeverityWarn, false, "HTTP 请求行为空")
	}

	reqHeaders, chunked, err := readHTTPHeaders(br)
	if err != nil {
		return nil, err
	}
	if !chunked {
		return nil, runtimeerr.New(errCodeHTTPRequestNotChunked, runtimeerr.SeverityError, false, "入站请求未启用分块传输: %v", reqHeaders)
	}

	responseHeaders := []string{
		"HTTP/1.1 200 OK",
		"Content-Type: application/octet-stream",
		"Connection: keep-alive",
		"Transfer-Encoding: chunked",
		"X-Accel-Buffering: no",
	}
	if err := utils.WriteFull(conn, []byte(strings.Join(responseHeaders, "\r\n")+"\r\n\r\n")); err != nil {
		return nil, runtimeerr.Wrap(err, errCodeHTTPWriteHeaders, runtimeerr.SeverityError, true, "写入 HTTP 响应头失败")
	}

	stream := &httpStreamConn{
		base:   conn,
		reader: httputil.NewChunkedReader(br),
		writer: &chunkWriter{conn: conn},
	}
	return stream, nil
}

func readHTTPHeaders(br *bufio.Reader) (map[string]string, bool, error) {
	headers := make(map[string]string)
	chunked := false
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return nil, false, runtimeerr.Wrap(err, errCodeHTTPRequestHeaders, runtimeerr.SeverityError, true, "读取 HTTP 头部失败")
		}
		trimmed := strings.TrimRight(line, "\r\n")
		if trimmed == "" {
			break
		}
		colon := strings.Index(trimmed, ":")
		if colon <= 0 {
			continue
		}
		key := strings.ToLower(strings.TrimSpace(trimmed[:colon]))
		value := strings.TrimSpace(trimmed[colon+1:])
		headers[key] = value
		if key == "transfer-encoding" && strings.Contains(strings.ToLower(value), "chunked") {
			chunked = true
		}
	}
	return headers, chunked, nil
}
