package dataplane

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"golang.org/x/time/rate"
	"io"
	"math"
	"net"
	"strings"
	"sync"
	"time"
)

// TCP 多路复用服务器：替代 HTTP /transfer，复用 TokenStore 与 Admin.OpenStream。

const (
	frameTypeOpen  = 1
	frameTypeData  = 2
	frameTypeClose = 3
)

// frame 为连接上的单个帧。
type frame struct {
	ft       byte
	streamID uint32
	payload  []byte
}

// 每个 stream 的状态（单条连接上可以承载多个 stream）。
type streamState struct {
	open   openFrame
	meta   TokenMeta
	dir    string
	token  string
	up     io.ReadWriteCloser
	in     chan frame
	cancel context.CancelFunc
	done   chan struct{}
}

// openFrame 描述客户端发起的单个逻辑流。
type openFrame struct {
	StreamID  uint32
	Token     string
	Direction string
	Path      string
	Offset    int64
	SizeHint  int64
	Hash      string
}

// tcpServer 是简化版多路复用器，当前每连接可承载多个流，但不做高级流控。
type tcpServer struct {
	cfg    Config
	tokens *TokenStore
	admin  AdminBridge
}

func estimateUploadCloseWait(meta TokenMeta, written int64) time.Duration {
	// 小文件或快速传输时的基线等待时间。
	wait := 120 * time.Second
	if written < 0 {
		written = 0
	}

	// 在 DTN 抖动场景下（sleep 窗口、repair、重路由），close 的传播
	// 往往比局域网假设慢得多。这里基于载荷大小按一个保守的最低吞吐率
	// 缩放 close-wait 预算，避免大传输被过早中止。
	const (
		fallbackFloorBps = int64(32 * 1024) // 32 KiB/s conservative DTN floor
		margin           = 45 * time.Second
		maxWait          = 10 * time.Minute
	)
	floorBps := fallbackFloorBps
	if meta.MaxRate > 0 {
		// MaxRate 虽然是上限，但当它非常低时，也等价于在预测更慢的排空速度。
		if capBps := int64(math.Ceil(meta.MaxRate)); capBps > 0 && capBps < floorBps {
			floorBps = capBps
		}
	}

	estimateFromBytes := func(n int64) time.Duration {
		if n <= 0 || floorBps <= 0 {
			return 0
		}
		return time.Duration((n*int64(time.Second))/floorBps) + margin
	}
	if est := estimateFromBytes(written); est > wait {
		wait = est
	}
	if meta.SizeHint > 0 {
		if est := estimateFromBytes(meta.SizeHint); est > wait {
			wait = est
		}
	}
	if wait > maxWait {
		wait = maxWait
	}
	if wait < 5*time.Second {
		wait = 5 * time.Second
	}
	return wait
}

func NewTCPServer(cfg Config, tokens *TokenStore) *tcpServer {
	return &tcpServer{cfg: cfg, tokens: tokens, admin: cfg.Admin}
}

// ListenAndServe 监听并处理 TCP 链接（可选 TLS）。
func (s *tcpServer) ListenAndServe(ctx context.Context) error {
	if s == nil {
		return fmt.Errorf("tcp server nil")
	}
	if s.tokens == nil {
		return fmt.Errorf("token store nil")
	}
	ln, err := s.listen()
	if err != nil {
		return err
	}
	defer ln.Close()
	var wg sync.WaitGroup
	acceptErr := make(chan error, 1)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				if ctx.Err() != nil {
					acceptErr <- nil
					return
				}
				acceptErr <- err
				return
			}
			wg.Add(1)
			go func(c net.Conn) {
				defer wg.Done()
				s.handleConn(ctx, c)
			}(conn)
		}
	}()

	select {
	case <-ctx.Done():
		_ = ln.Close()
		wg.Wait()
		return ctx.Err()
	case err := <-acceptErr:
		_ = ln.Close()
		wg.Wait()
		return err
	}
}

func (s *tcpServer) listen() (net.Listener, error) {
	if !s.cfg.EnableTLS {
		return net.Listen("tcp", s.cfg.Listen)
	}
	return s.listenTLS()
}

func (s *tcpServer) listenTLS() (net.Listener, error) {
	cert, pool, err := loadCertificate(s.cfg.TLSCert, s.cfg.TLSKey, s.cfg.TLSClientCA)
	if err != nil {
		return nil, err
	}
	cfg := &tls.Config{Certificates: []tls.Certificate{cert}}
	if pool != nil {
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
		cfg.ClientCAs = pool
	}
	return tls.Listen("tcp", s.cfg.Listen, cfg)
}

func (s *tcpServer) handleConn(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	br := bufio.NewReader(conn)
	writer := &sync.Mutex{}

	streams := make(map[uint32]*streamState)

	closeAll := func() {
		for _, st := range streams {
			st.cancel()
			<-st.done
		}
	}
	defer closeAll()

	for {
		if ctx.Err() != nil {
			return
		}
		ft, sid, payload, err := readFrame(br)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				// 静默丢弃。
			}
			return
		}

		if ft == frameTypeOpen {
			of, err := parseOpenFrame(payload)
			if err != nil {
				sendClose(conn, sid, 1, err.Error())
				continue
			}
			if of.StreamID == 0 {
				of.StreamID = sid
			}
			meta, ok := s.consumeToken(of.Token)
			if !ok {
				sendClose(conn, of.StreamID, 1, "invalid token")
				continue
			}
			if meta.Direction != "" && !strings.EqualFold(meta.Direction, of.Direction) {
				sendClose(conn, of.StreamID, 1, "direction mismatch")
				continue
			}
			up, err := s.admin.OpenStream(ctx, meta.Target, "", streamMeta(of, meta))
			if err != nil {
				sendClose(conn, of.StreamID, 1, err.Error())
				continue
			}
			cctx, cancel := context.WithCancel(ctx)
			st := &streamState{
				open:   of,
				meta:   meta,
				token:  of.Token,
				dir:    strings.ToLower(of.Direction),
				up:     up,
				in:     make(chan frame, 32),
				cancel: cancel,
				done:   make(chan struct{}),
			}
			streams[of.StreamID] = st
			go s.runStream(cctx, conn, writer, st)
			continue
		}

		// 将 data/close 分发到对应的活跃 stream。
		st, ok := streams[sid]
		if !ok {
			continue
		}
		select {
		case st.in <- frame{ft: ft, streamID: sid, payload: payload}:
		case <-st.done:
		}
	}
}

// consumeToken 校验并消费一次性 token。
func (s *tcpServer) consumeToken(tok string) (TokenMeta, bool) {
	if tok == "" {
		return TokenMeta{}, false
	}
	meta, ok := s.tokens.Consume(tok)
	return meta, ok
}

// runStream 在独立 goroutine 中处理单个逻辑流。
func (s *tcpServer) runStream(ctx context.Context, conn net.Conn, writer *sync.Mutex, st *streamState) {
	defer close(st.done)
	defer st.up.Close()

	switch st.dir {
	case "upload":
		s.runUpload(ctx, conn, writer, st)
	case "download":
		s.runDownload(ctx, conn, writer, st)
	default:
		sendClose(conn, st.open.StreamID, 1, "unknown direction")
	}
}

func (s *tcpServer) runUpload(ctx context.Context, conn net.Conn, writer *sync.Mutex, st *streamState) {
	limiter := newRateLimiter(st.meta.MaxRate)
	var written int64
	maxSize := st.meta.MaxSize
	success := false
	defer func() {
		if !success && st.meta.Retries > 0 {
			st.meta.Retries--
			s.tokens.Requeue(st.token, st.meta)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case fr := <-st.in:
			switch fr.ft {
			case frameTypeData:
				if maxSize > 0 && written+int64(len(fr.payload)) > maxSize {
					sendCloseLocked(writer, conn, st.open.StreamID, 1, "payload exceeds max size")
					return
				}
				if limiter != nil && !limiter.AllowN(time.Now(), len(fr.payload)) {
					// 背压：等待 token 可用。
					if err := limiter.WaitN(ctx, len(fr.payload)); err != nil {
						sendCloseLocked(writer, conn, st.open.StreamID, 1, err.Error())
						return
					}
				}
				if _, err := st.up.Write(fr.payload); err != nil {
					sendCloseLocked(writer, conn, st.open.StreamID, 1, err.Error())
					return
				}
				written += int64(len(fr.payload))
			case frameTypeClose:
				// 先向上游发出 EOF，让远端完成收尾（如 hash/size 校验），
				// 然后等待远端 close 或 error，再向客户端确认。
				if wc, ok := st.up.(interface{ CloseWrite() error }); ok && wc != nil {
					_ = wc.CloseWrite()
				} else {
					_ = st.up.Close()
				}

				readDone := make(chan error, 1)
				go func() {
					buf := make([]byte, 1)
					for {
						_, err := st.up.Read(buf)
						if err != nil {
							readDone <- err
							return
						}
					}
				}()

				// 一旦 token 已被消费且 stream 已建立，后续完成过程就不应再受 token 过期约束。
				// 否则一些本来合法的大传输，可能仅仅因为 close 传播时间超过剩余 token TTL
				// 就被错误中止。
				wait := estimateUploadCloseWait(st.meta, written)
				select {
				case err := <-readDone:
					if err == nil || errors.Is(err, io.EOF) {
						sendCloseLocked(writer, conn, st.open.StreamID, 0, "OK")
						success = true
						return
					}
					sendCloseLocked(writer, conn, st.open.StreamID, 1, err.Error())
					return
				case <-ctx.Done():
					_ = st.up.Close()
					sendCloseLocked(writer, conn, st.open.StreamID, 1, "canceled")
					return
				case <-time.After(wait):
					_ = st.up.Close()
					sendCloseLocked(writer, conn, st.open.StreamID, 1, "upstream close timeout")
					return
				}
			default:
				sendCloseLocked(writer, conn, st.open.StreamID, 1, "unexpected frame")
				return
			}
		}
	}
}

func (s *tcpServer) runDownload(ctx context.Context, conn net.Conn, writer *sync.Mutex, st *streamState) {
	bufSize := 32 * 1024
	minSize, maxSize := 8*1024, 128*1024
	limiter := newRateLimiter(st.meta.MaxRate)
	var sent int64
	maxAllowed := st.meta.MaxSize
	success := false
	defer func() {
		if !success && st.meta.Retries > 0 {
			st.meta.Retries--
			s.tokens.Requeue(st.token, st.meta)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			sendCloseLocked(writer, conn, st.open.StreamID, 1, "canceled")
			return
		case fr := <-st.in:
			if fr.ft == frameTypeClose {
				sendCloseLocked(writer, conn, st.open.StreamID, 0, "canceled")
				return
			}
		default:
		}

		buf := make([]byte, bufSize)
		n, err := st.up.Read(buf)
		if n > 0 {
			if maxAllowed > 0 && sent+int64(n) > maxAllowed {
				sendCloseLocked(writer, conn, st.open.StreamID, 1, "payload exceeds max size")
				return
			}
			if limiter != nil && !limiter.AllowN(time.Now(), n) {
				if err := limiter.WaitN(ctx, n); err != nil {
					sendCloseLocked(writer, conn, st.open.StreamID, 1, err.Error())
					return
				}
			}
			start := time.Now()
			writer.Lock()
			werr := writeFrame(conn, frameTypeData, st.open.StreamID, buf[:n])
			writer.Unlock()
			if werr != nil {
				sendCloseLocked(writer, conn, st.open.StreamID, 1, werr.Error())
				return
			}
			sent += int64(n)
			elapsed := time.Since(start)
			switch {
			case elapsed > 200*time.Millisecond && bufSize > minSize:
				bufSize /= 2
				if bufSize < minSize {
					bufSize = minSize
				}
			case elapsed < 50*time.Millisecond && bufSize < maxSize:
				bufSize *= 2
				if bufSize > maxSize {
					bufSize = maxSize
				}
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				sendCloseLocked(writer, conn, st.open.StreamID, 0, "OK")
				success = true
			} else {
				sendCloseLocked(writer, conn, st.open.StreamID, 1, err.Error())
			}
			return
		}
	}
}

func streamMeta(of openFrame, tok TokenMeta) map[string]string {
	kind := "file"
	switch strings.ToLower(strings.TrimSpace(of.Direction)) {
	case "upload":
		kind = "file-put"
	case "download":
		kind = "file-get"
	}
	meta := map[string]string{
		"kind": kind,
		"path": of.Path,
	}
	if of.Offset > 0 {
		meta["offset"] = fmt.Sprintf("%d", of.Offset)
	}
	if of.SizeHint > 0 {
		meta["size"] = fmt.Sprintf("%d", of.SizeHint)
	}
	if of.Hash != "" {
		meta["hash"] = of.Hash
	}
	if tok.MaxSize > 0 {
		meta["max_size"] = fmt.Sprintf("%d", tok.MaxSize)
	}
	return meta
}

// newRateLimiter 会在 rateBps > 0 时返回一个限速器。
func newRateLimiter(rateBps float64) *rate.Limiter {
	if rateBps <= 0 {
		return nil
	}
	// burst：允许最多 128KB 的突发，以吸收抖动。
	burst := int(128 * 1024)
	lim := rate.NewLimiter(rate.Limit(rateBps), burst)
	return lim
}

// --- framing辅助函数 ---

func readFrame(r *bufio.Reader) (ft byte, streamID uint32, payload []byte, err error) {
	var lenBuf [4]byte
	if _, err = io.ReadFull(r, lenBuf[:]); err != nil {
		return
	}
	total := binary.BigEndian.Uint32(lenBuf[:])
	if total < 6 {
		err = fmt.Errorf("frame too short")
		return
	}
	hdr := make([]byte, 6)
	if _, err = io.ReadFull(r, hdr); err != nil {
		return
	}
	ft = hdr[0]
	streamID = binary.BigEndian.Uint32(hdr[1:5])
	payloadLen := int(total) - 6
	if payloadLen > 0 {
		payload = make([]byte, payloadLen)
		if _, err = io.ReadFull(r, payload); err != nil {
			return
		}
	}
	return
}

func writeFrame(w io.Writer, ft byte, streamID uint32, payload []byte) error {
	plen := 0
	if payload != nil {
		plen = len(payload)
	}
	total := 6 + plen
	buf := make([]byte, 4+total)
	binary.BigEndian.PutUint32(buf[0:4], uint32(total))
	buf[4] = ft
	binary.BigEndian.PutUint32(buf[5:9], streamID)
	// flags byte 保留
	buf[9] = 0
	if plen > 0 {
		copy(buf[10:], payload)
	}
	_, err := w.Write(buf)
	return err
}

func sendClose(w io.Writer, streamID uint32, code uint16, reason string) {
	payload := make([]byte, 2+len(reason))
	binary.BigEndian.PutUint16(payload[0:2], code)
	copy(payload[2:], []byte(reason))
	_ = writeFrame(w, frameTypeClose, streamID, payload)
}

func sendCloseLocked(mu *sync.Mutex, w io.Writer, streamID uint32, code uint16, reason string) {
	mu.Lock()
	sendClose(w, streamID, code, reason)
	mu.Unlock()
}

func parseOpenFrame(b []byte) (openFrame, error) {
	of := openFrame{}
	rd := newFieldReader(b)
	tok, err := rd.readString()
	if err != nil {
		return of, err
	}
	dir, err := rd.readString()
	if err != nil {
		return of, err
	}
	path, err := rd.readString()
	if err != nil {
		return of, err
	}
	off, err := rd.readInt64()
	if err != nil {
		return of, err
	}
	size, err := rd.readInt64()
	if err != nil {
		return of, err
	}
	hash, err := rd.readString()
	if err != nil {
		return of, err
	}
	of.Token = tok
	of.Direction = dir
	of.Path = path
	of.Offset = off
	of.SizeHint = size
	of.Hash = hash
	return of, nil
}

// fieldReader 以 [u16 len][bytes] 方式解析字符串，紧跟 int64。
type fieldReader struct {
	data []byte
	off  int
}

func newFieldReader(b []byte) *fieldReader { return &fieldReader{data: b} }

func (r *fieldReader) readString() (string, error) {
	if r.off+2 > len(r.data) {
		return "", io.ErrUnexpectedEOF
	}
	l := int(binary.BigEndian.Uint16(r.data[r.off : r.off+2]))
	r.off += 2
	if r.off+l > len(r.data) {
		return "", io.ErrUnexpectedEOF
	}
	s := string(r.data[r.off : r.off+l])
	r.off += l
	return s, nil
}

func (r *fieldReader) readInt64() (int64, error) {
	if r.off+8 > len(r.data) {
		return 0, io.ErrUnexpectedEOF
	}
	v := int64(binary.BigEndian.Uint64(r.data[r.off : r.off+8]))
	r.off += 8
	return v, nil
}
