package protocol

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"

	"codeberg.org/agnoie/shepherd/pkg/utils"
	"codeberg.org/agnoie/shepherd/pkg/utils/runtimeerr"
)

const (
	errCodeHTTPNegotiationConn = "PROTOCOL_HTTP_NEGOTIATE_CONN"
	errCodeHTTPChunkSize       = "PROTOCOL_HTTP_CHUNK_SIZE"
	errCodeHTTPChunkDelimiter  = "PROTOCOL_HTTP_CHUNK_DELIMITER"
	errCodeHTTPBodyLimit       = "PROTOCOL_HTTP_BODY_LIMIT"
)

type HTTPProto struct {
	param    *NegParam
	config   HTTPConfig
	hostHint string
}

type HTTPMessage struct {
	HTTPHeader []byte
	*RawMessage
	reader        *bufio.Reader
	config        HTTPConfig
	headers       map[string]string
	statusLine    string
	useChunked    bool
	contentLength int64
}

// 允许的 HTTP 包体上限（header + payload 一起计算）
const maxHTTPBodySize = maxDataLen + 16*1024

func (proto *HTTPProto) CNegotiate() error {
	if proto == nil || proto.param == nil || proto.param.Conn == nil {
		return runtimeerr.New(errCodeHTTPNegotiationConn, runtimeerr.SeverityError, false, "HTTP 协商失败：连接为空")
	}
	stream, err := newHTTPClientStream(proto.param.Conn, proto.config, proto.hostHint)
	if err != nil {
		return err
	}
	proto.param.Conn = stream
	return nil
}

func (proto *HTTPProto) SNegotiate() error {
	if proto == nil || proto.param == nil || proto.param.Conn == nil {
		return runtimeerr.New(errCodeHTTPNegotiationConn, runtimeerr.SeverityError, false, "HTTP 协商失败：连接为空")
	}
	stream, err := newHTTPServerStream(proto.param.Conn)
	if err != nil {
		return err
	}
	proto.param.Conn = stream
	return nil
}

// NewHTTPMessage 根据配置把原始消息封装成 HTTP 报文。
func NewHTTPMessage(rawMsg *RawMessage, cfg HTTPConfig) *HTTPMessage {
	return &HTTPMessage{
		RawMessage: rawMsg,
		config:     normalizeHTTPConfig(cfg),
	}
}

func (message *HTTPMessage) ConstructHeader() {
	if isHTTPStreamConn(message.RawMessage.Conn) {
		message.useChunked = false
		message.HTTPHeader = nil
		return
	}
	message.useChunked = message.shouldUseChunked()
	host := message.config.Host
	if host == "" {
		host = deriveHTTPHost(message.RawMessage.Conn)
	}
	path := message.config.Path
	ua := message.config.UserAgent
	headers := []string{
		fmt.Sprintf("POST %s HTTP/1.1", path),
		fmt.Sprintf("Host: %s", host),
		fmt.Sprintf("User-Agent: %s", ua),
		"Accept: application/octet-stream",
		"Connection: keep-alive",
	}
	if message.useChunked {
		headers = append(headers, "Transfer-Encoding: chunked")
	} else {
		headers = append(headers, fmt.Sprintf("Content-Length: %d", len(message.RawMessage.DataBuffer)))
	}
	message.HTTPHeader = []byte(strings.Join(headers, "\r\n") + "\r\n\r\n")
}

func (message *HTTPMessage) DeconstructHeader() {
	message.HTTPHeader = message.HTTPHeader[:0]
	message.headers = make(map[string]string)
	if buffered, ok := message.RawMessage.Conn.(*bufferedConn); ok {
		message.reader = buffered.reader
	} else {
		baseConn := message.RawMessage.Conn
		message.reader = bufio.NewReader(baseConn)
		message.RawMessage.Conn = &bufferedConn{Conn: baseConn, reader: message.reader}
	}

	lineNum := 0
	for {
		line, err := message.reader.ReadString('\n')
		if err != nil {
			message.HTTPHeader = append(message.HTTPHeader, []byte(line)...)
			return
		}
		message.HTTPHeader = append(message.HTTPHeader, []byte(line)...)
		trimmed := strings.TrimRight(line, "\r\n")
		if lineNum == 0 {
			message.statusLine = trimmed
		}
		lineNum++
		if trimmed == "" {
			break
		}
		if idx := strings.Index(trimmed, ":"); idx != -1 {
			key := strings.ToLower(strings.TrimSpace(trimmed[:idx]))
			value := strings.TrimSpace(trimmed[idx+1:])
			message.headers[key] = value
		}
	}
	message.useChunked = headerWantsChunked(message.headers)
	if cl, ok := message.headers["content-length"]; ok {
		if parsed, err := strconv.ParseInt(cl, 10, 64); err == nil {
			message.contentLength = parsed
		}
	}
}

func (message *HTTPMessage) DeconstructData() (*Header, interface{}, error) {
	if !message.useChunked {
		if message.contentLength > 0 && message.contentLength > maxHTTPBodySize {
			return nil, nil, runtimeerr.New(errCodeHTTPBodyLimit, runtimeerr.SeverityError, false, "HTTP body exceeds limit: %d > %d", message.contentLength, maxHTTPBodySize)
		}
		return message.RawMessage.DeconstructData()
	}
	body, err := message.decodeChunkedBody()
	if err != nil {
		return nil, nil, err
	}
	savedConn := message.RawMessage.Conn
	chunked := &chunkConn{Conn: savedConn, reader: bytes.NewReader(body)}
	message.RawMessage.Conn = chunked
	defer func() { message.RawMessage.Conn = savedConn }()
	return message.RawMessage.DeconstructData()
}

func (message *HTTPMessage) SendMessage() {
	if err := utils.WriteFull(message.RawMessage.Conn, message.HTTPHeader); err != nil {
		logger.Errorf("failed to write http header: %v", err)
		if message.RawMessage != nil && message.RawMessage.Conn != nil {
			_ = message.RawMessage.Conn.Close()
		}
		return
	}
	body := message.compiledPayload()
	if message.useChunked {
		if err := message.writeChunkedBody(body); err != nil {
			logger.Errorf("failed to write chunked body: %v", err)
			if message.RawMessage != nil && message.RawMessage.Conn != nil {
				_ = message.RawMessage.Conn.Close()
			}
		}
	} else {
		if err := utils.WriteFull(message.RawMessage.Conn, body); err != nil {
			logger.Errorf("failed to write http body: %v", err)
			if message.RawMessage != nil && message.RawMessage.Conn != nil {
				_ = message.RawMessage.Conn.Close()
			}
		}
	}
	message.HTTPHeader = nil
	message.RawMessage.DataBuffer = nil
	message.RawMessage.HeaderBuffer = nil
}

func (message *HTTPMessage) compiledPayload() []byte {
	if message.RawMessage == nil {
		return nil
	}
	body := append([]byte(nil), message.RawMessage.HeaderBuffer...)
	body = append(body, message.RawMessage.DataBuffer...)
	return body
}

func (message *HTTPMessage) shouldUseChunked() bool {
	if isHTTPStreamConn(message.RawMessage.Conn) {
		return false
	}
	return message.RawMessage != nil && message.RawMessage.Flags&FlagSupportChunked != 0
}

func (message *HTTPMessage) writeChunkedBody(body []byte) error {
	chunkSize := message.config.ChunkSize
	if chunkSize <= 0 {
		chunkSize = defaultHTTPChunkSize
	}
	remaining := body
	for len(remaining) > 0 {
		toSend := remaining
		if len(toSend) > chunkSize {
			toSend = remaining[:chunkSize]
		}
		chunkHeader := fmt.Sprintf("%x\r\n", len(toSend))
		if err := utils.WriteFull(message.RawMessage.Conn, []byte(chunkHeader)); err != nil {
			return err
		}
		if err := utils.WriteFull(message.RawMessage.Conn, toSend); err != nil {
			return err
		}
		if err := utils.WriteFull(message.RawMessage.Conn, []byte("\r\n")); err != nil {
			return err
		}
		remaining = remaining[len(toSend):]
	}
	return utils.WriteFull(message.RawMessage.Conn, []byte("0\r\n\r\n"))
}

func (message *HTTPMessage) decodeChunkedBody() ([]byte, error) {
	var result bytes.Buffer
	for {
		sizeLine, err := message.reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		sizeStr := strings.TrimSpace(sizeLine)
		size, err := strconv.ParseInt(sizeStr, 16, 64)
		if err != nil {
			return nil, runtimeerr.New(errCodeHTTPChunkSize, runtimeerr.SeverityError, false, "非法的分块长度: %s", sizeStr)
		}
		if size == 0 {
			if err := message.consumeChunkCRLF(); err != nil {
				return nil, err
			}
			break
		}
		if int64(result.Len())+size > maxHTTPBodySize {
			return nil, runtimeerr.New(errCodeHTTPBodyLimit, runtimeerr.SeverityError, false, "HTTP chunked body exceeds limit: %d + %d > %d", result.Len(), size, maxHTTPBodySize)
		}
		if _, err := io.CopyN(&result, message.reader, size); err != nil {
			return nil, err
		}
		if err := message.consumeChunkCRLF(); err != nil {
			return nil, err
		}
		if int64(result.Len()) > maxHTTPBodySize {
			return nil, runtimeerr.New(errCodeHTTPBodyLimit, runtimeerr.SeverityError, false, "HTTP chunked body exceeds limit: %d > %d", result.Len(), maxHTTPBodySize)
		}
	}
	return result.Bytes(), nil
}

func (message *HTTPMessage) consumeChunkCRLF() error {
	b := []byte{0, 0}
	if _, err := io.ReadFull(message.reader, b); err != nil {
		return err
	}
	if b[0] != '\r' || b[1] != '\n' {
		return runtimeerr.New(errCodeHTTPChunkDelimiter, runtimeerr.SeverityError, false, "HTTP 分块分隔符非法")
	}
	return nil
}

func headerWantsChunked(headers map[string]string) bool {
	if headers == nil {
		return false
	}
	if value, ok := headers["transfer-encoding"]; ok {
		return strings.Contains(strings.ToLower(value), "chunked")
	}
	return false
}

func deriveHTTPHost(conn net.Conn) string {
	if conn == nil {
		return "localhost"
	}
	addr := conn.RemoteAddr()
	if addr == nil {
		return "localhost"
	}
	if host, _, err := net.SplitHostPort(addr.String()); err == nil {
		return host
	}
	return addr.String()
}

type bufferedConn struct {
	net.Conn
	reader *bufio.Reader
}

func (c *bufferedConn) Read(p []byte) (int, error) {
	return c.reader.Read(p)
}

type chunkConn struct {
	net.Conn
	reader io.Reader
}

func (c *chunkConn) Read(p []byte) (int, error) {
	return c.reader.Read(p)
}
