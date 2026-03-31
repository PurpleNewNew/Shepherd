package protocol

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"codeberg.org/agnoie/shepherd/pkg/utils"
)

const websocketGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
const websocketPath = "/deadbeef"

type WSProto struct {
	param *NegParam
	*RawProto
}

const maxWSHeaderSize = 16 * 1024

func (proto *WSProto) CNegotiate() error {
	if proto == nil || proto.param == nil || proto.param.Conn == nil {
		return fmt.Errorf("websocket negotiate: nil connection")
	}
	conn := proto.param.Conn
	defer conn.SetReadDeadline(time.Time{})
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	reader := bufio.NewReader(conn)

	nonce := generateNonce()
	expectedAccept, err := getNonceAccept(nonce)
	if err != nil {
		return err
	}

	host := proto.param.Domain
	if host == "" {
		if addr := conn.RemoteAddr(); addr != nil {
			host = addr.String()
		} else {
			host = "localhost"
		}
	}

	wsHeaders := fmt.Sprintf(`GET %s HTTP/1.1
Host: %s
Upgrade: websocket
Connection: Upgrade
Sec-WebSocket-Key: %s
Origin: https://google.com
Sec-WebSocket-Version: 13

`, websocketPath, host, nonce)

	wsHeaders = strings.ReplaceAll(wsHeaders, "\n", "\r\n")
	if err := utils.WriteFull(conn, []byte(wsHeaders)); err != nil {
		return err
	}

	respHeader, err := readWSHeader(reader)
	if err != nil {
		return err
	}
	resp := string(respHeader)
	if !strings.Contains(resp, "Upgrade: websocket") ||
		!strings.Contains(resp, "Connection: Upgrade") ||
		!strings.Contains(resp, "Sec-WebSocket-Accept: "+string(expectedAccept)) {
		return errors.New("not websocket protocol")
	}

	proto.param.Conn = newWSConnWithReader(conn, reader, true)
	return nil
}

func (proto *WSProto) SNegotiate() error {
	if proto == nil || proto.param == nil || proto.param.Conn == nil {
		return fmt.Errorf("websocket negotiate: nil connection")
	}
	conn := proto.param.Conn
	defer conn.SetReadDeadline(time.Time{})
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	reader := bufio.NewReader(conn)
	reqHeader, err := readWSHeader(reader)
	if err != nil {
		return err
	}

	re := regexp.MustCompile(`Sec-WebSocket-Key: (.*)`)
	tkey := re.FindStringSubmatch(strings.ReplaceAll(string(reqHeader), "\r\n", "\n"))
	if len(tkey) < 2 {
		return errors.New("Sec-Websocket-Key is not in header")
	}

	key := tkey[1]
	expectedAccept, err := getNonceAccept([]byte(key))
	if err != nil {
		return err
	}

	respHeaders := fmt.Sprintf(`HTTP/1.1 101 Switching Protocols
Connection: Upgrade
Upgrade: websocket
Sec-WebSocket-Accept: %s

`, expectedAccept)

	respHeaders = strings.ReplaceAll(respHeaders, "\n", "\r\n")
	if err := utils.WriteFull(conn, []byte(respHeaders)); err != nil {
		return err
	}

	proto.param.Conn = newWSConnWithReader(conn, reader, false)
	return nil
}

type WSMessage struct {
	*RawMessage
}

func generateNonce() (nonce []byte) {
	key := make([]byte, 16)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		panic(err)
	}
	nonce = make([]byte, 24)
	base64.StdEncoding.Encode(nonce, key)
	return
}

func getNonceAccept(nonce []byte) (expected []byte, err error) {
	h := sha1.New()
	if _, err = h.Write(nonce); err != nil {
		return
	}
	if _, err = h.Write([]byte(websocketGUID)); err != nil {
		return
	}
	expected = make([]byte, 28)
	base64.StdEncoding.Encode(expected, h.Sum(nil))
	return
}

func readWSHeader(reader *bufio.Reader) ([]byte, error) {
	if reader == nil {
		return nil, fmt.Errorf("websocket negotiate: nil reader")
	}
	result := bytes.Buffer{}
	for result.Len() < maxWSHeaderSize {
		b, err := reader.ReadByte()
		if err != nil {
			if timeoutErr, ok := err.(net.Error); ok && timeoutErr.Timeout() {
				return nil, err
			}
			if err == io.EOF && bytes.HasSuffix(result.Bytes(), []byte("\r\n\r\n")) {
				break
			}
			if err == io.EOF {
				return nil, io.ErrUnexpectedEOF
			}
			return nil, err
		}
		result.WriteByte(b)
		if bytes.HasSuffix(result.Bytes(), []byte("\r\n\r\n")) {
			return result.Bytes(), nil
		}
	}
	if result.Len() >= maxWSHeaderSize {
		return nil, fmt.Errorf("websocket negotiate: header too large")
	}
	return result.Bytes(), nil
}

func newWSConn(base net.Conn, isClient bool) net.Conn {
	return newWSConnWithReader(base, nil, isClient)
}

func newWSConnWithReader(base net.Conn, reader *bufio.Reader, isClient bool) net.Conn {
	if reader == nil {
		reader = bufio.NewReader(base)
	}
	return &wsConn{
		base:     base,
		reader:   reader,
		isClient: isClient,
	}
}

type wsConn struct {
	base     net.Conn
	reader   *bufio.Reader
	isClient bool

	readBuf bytes.Buffer

	readMu  sync.Mutex
	writeMu sync.Mutex

	fragmenting bool
	fragBuf     bytes.Buffer

	closed int32
}

func (c *wsConn) Read(p []byte) (int, error) {
	if atomic.LoadInt32(&c.closed) == 1 {
		return 0, io.EOF
	}
	c.readMu.Lock()
	defer c.readMu.Unlock()

	for c.readBuf.Len() == 0 {
		payload, err := c.readDataMessage()
		if err != nil {
			return 0, err
		}
		if len(payload) == 0 {
			continue
		}
		c.readBuf.Write(payload)
	}
	return c.readBuf.Read(p)
}

func (c *wsConn) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if atomic.LoadInt32(&c.closed) == 1 {
		return 0, io.ErrClosedPipe
	}
	if err := c.writeFrame(0x2, p, true); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (c *wsConn) Close() error {
	if atomic.LoadInt32(&c.closed) == 0 {
		_ = c.writeClose(nil)
		atomic.StoreInt32(&c.closed, 1)
	}
	return c.base.Close()
}

func (c *wsConn) LocalAddr() net.Addr  { return c.base.LocalAddr() }
func (c *wsConn) RemoteAddr() net.Addr { return c.base.RemoteAddr() }

func (c *wsConn) SetDeadline(t time.Time) error {
	return c.base.SetDeadline(t)
}

func (c *wsConn) SetReadDeadline(t time.Time) error {
	return c.base.SetReadDeadline(t)
}

func (c *wsConn) SetWriteDeadline(t time.Time) error {
	return c.base.SetWriteDeadline(t)
}

func (c *wsConn) readDataMessage() ([]byte, error) {
	for {
		fin, opcode, payload, err := c.readFrame()
		if err != nil {
			return nil, err
		}
		switch opcode {
		case 0x0: // continuation 连续帧
			if !c.fragmenting {
				return nil, fmt.Errorf("unexpected continuation frame")
			}
			c.fragBuf.Write(payload)
			if fin {
				data := c.fragBuf.Bytes()
				result := make([]byte, len(data))
				copy(result, data)
				c.fragBuf.Reset()
				c.fragmenting = false
				return result, nil
			}
		case 0x1, 0x2: // text 或 binary 数据帧
			if !fin {
				c.fragmenting = true
				c.fragBuf.Reset()
				c.fragBuf.Write(payload)
				continue
			}
			return payload, nil
		case 0x8: // close 关闭帧
			if atomic.LoadInt32(&c.closed) == 0 {
				_ = c.writeControlFrame(0x8, payload)
				atomic.StoreInt32(&c.closed, 1)
			}
			_ = c.base.Close()
			return nil, io.EOF
		case 0x9: // ping 探活帧
			_ = c.writeControlFrame(0xA, payload)
			continue
		case 0xA: // pong 响应帧
			continue
		default:
			return nil, fmt.Errorf("unsupported websocket opcode %d", opcode)
		}
	}
}

func (c *wsConn) readFrame() (bool, byte, []byte, error) {
	header1, err := c.reader.ReadByte()
	if err != nil {
		return false, 0, nil, err
	}
	fin := (header1 & 0x80) != 0
	if header1&0x70 != 0 {
		return false, 0, nil, fmt.Errorf("rsv bits must be zero")
	}
	opcode := header1 & 0x0F

	header2, err := c.reader.ReadByte()
	if err != nil {
		return false, 0, nil, err
	}
	masked := (header2 & 0x80) != 0
	payloadLen := int64(header2 & 0x7F)

	switch payloadLen {
	case 126:
		var ext [2]byte
		if _, err := io.ReadFull(c.reader, ext[:]); err != nil {
			return false, 0, nil, err
		}
		payloadLen = int64(binary.BigEndian.Uint16(ext[:]))
	case 127:
		var ext [8]byte
		if _, err := io.ReadFull(c.reader, ext[:]); err != nil {
			return false, 0, nil, err
		}
		payloadLen = int64(binary.BigEndian.Uint64(ext[:]))
	}

	if payloadLen < 0 || payloadLen > math.MaxInt32 {
		return false, 0, nil, fmt.Errorf("frame payload too large: %d", payloadLen)
	}

	var maskKey [4]byte
	if masked {
		if _, err := io.ReadFull(c.reader, maskKey[:]); err != nil {
			return false, 0, nil, err
		}
	}

	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(c.reader, payload); err != nil {
		return false, 0, nil, err
	}

	if masked {
		for i := range payload {
			payload[i] ^= maskKey[i%4]
		}
	}

	if opcode >= 0x8 {
		if !fin {
			return false, 0, nil, fmt.Errorf("fragmented control frame")
		}
		if payloadLen > 125 {
			return false, 0, nil, fmt.Errorf("control frame too large")
		}
	}

	return fin, opcode, payload, nil
}

func (c *wsConn) writeFrame(opcode byte, payload []byte, fin bool) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if atomic.LoadInt32(&c.closed) == 1 {
		return io.ErrClosedPipe
	}

	first := opcode & 0x0F
	if fin {
		first |= 0x80
	}

	header := []byte{first, 0}
	length := len(payload)

	switch {
	case length <= 125:
		header[1] = byte(length)
	case length <= math.MaxUint16:
		header[1] = 126
		tmp := make([]byte, 2)
		binary.BigEndian.PutUint16(tmp, uint16(length))
		header = append(header, tmp...)
	default:
		header[1] = 127
		tmp := make([]byte, 8)
		binary.BigEndian.PutUint64(tmp, uint64(length))
		header = append(header, tmp...)
	}

	if c.isClient {
		header[1] |= 0x80
		var maskKey [4]byte
		if _, err := rand.Read(maskKey[:]); err != nil {
			return err
		}
		header = append(header, maskKey[:]...)
		masked := make([]byte, length)
		for i := range payload {
			masked[i] = payload[i] ^ maskKey[i%4]
		}
		payload = masked
	}

	if err := utils.WriteFull(c.base, header); err != nil {
		return err
	}
	if len(payload) > 0 {
		if err := utils.WriteFull(c.base, payload); err != nil {
			return err
		}
	}
	return nil
}

func (c *wsConn) writeControlFrame(opcode byte, payload []byte) error {
	if len(payload) > 125 {
		payload = payload[:125]
	}
	return c.writeFrame(opcode, payload, true)
}

func (c *wsConn) writeClose(payload []byte) error {
	if len(payload) == 0 {
		payload = make([]byte, 2)
		binary.BigEndian.PutUint16(payload, 1000)
	} else if len(payload) == 1 {
		payload = append([]byte{0}, payload...)
	}
	return c.writeControlFrame(0x8, payload)
}
