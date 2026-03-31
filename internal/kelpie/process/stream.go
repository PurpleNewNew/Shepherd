package process

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"codeberg.org/agnoie/shepherd/internal/kelpie/dtn"
	"codeberg.org/agnoie/shepherd/internal/kelpie/printer"
	"codeberg.org/agnoie/shepherd/internal/kelpie/stream"
	"codeberg.org/agnoie/shepherd/pkg/share/streamopts"
	"codeberg.org/agnoie/shepherd/pkg/streamid"
	"codeberg.org/agnoie/shepherd/protocol"
)

func parseStreamOptions(opt string) map[string]string {
	return streamopts.Parse(opt)
}

func normalizeKind(kind string) string {
	kind = strings.TrimSpace(strings.ToLower(kind))
	if kind == "" {
		return "unknown"
	}
	return kind
}

type streamStats struct {
	mu    sync.Mutex
	kinds map[string]*streamKindStats
}

type streamKindStats struct {
	opened     uint64
	closed     uint64
	lastReason string
	lastClosed time.Time
}

type StreamStat struct {
	Kind       string
	Opened     uint64
	Closed     uint64
	Active     uint64
	LastReason string
	LastClosed time.Time
}

func (admin *Admin) initStreamEngine() {
	if admin == nil {
		return
	}
	send := func(target string, payload []byte) error {
		if admin == nil {
			return fmt.Errorf("admin unavailable")
		}
		// STREAM_* 属于对时延敏感的控制/数据面流量；当 DTN 队列承压时
		// （例如 rescue 风暴），只要丢一个 STREAM_OPEN / STREAM_CLOSE，
		// 整个传输都可能被卡住。因此控制帧应优先使用更高的 DTN 优先级。
		prio := dtn.PriorityNormal
		env := string(payload)
		if strings.HasPrefix(env, "proto:") && len(env) >= len("proto:0000:") {
			if v, err := strconv.ParseUint(env[5:9], 16, 16); err == nil {
				switch uint16(v) {
				case protocol.STREAM_OPEN, protocol.STREAM_CLOSE, protocol.STREAM_ACK:
					prio = dtn.PriorityHigh
				}
			}
		}
		_, err := admin.EnqueueDiagnostic(target, env, prio, 0)
		return err
	}
	logFn := func(format string, args ...interface{}) {
		printer.Warning(format, args...)
	}
	// DTN-Stream 运行在 DTN 调度器之上（sleep 窗口、repair、focus hold 等）。
	// 这里应采用更适合 DTN 的重传超时预算；如果使用类似局域网 TCP 的 RTO
	// （例如 1 到 3 秒），在链路抖动下会产生伪丢包，并拖垮 dataplane 等上层系统。
	cfg := stream.DefaultConfig()
	// 更大的 chunk 能显著减少文件传输时的 DTN bundle 数量，
	// 让 dataplane 的“大文件”场景在 DTN 调度下也能在有限超时内完成。
	cfg.ChunkBytes = 8 * 1024
	cfg.MinWindow = 8
	cfg.InitialWindow = 12
	cfg.BaseRTO = 10 * time.Second
	cfg.MinRTO = 3 * time.Second
	cfg.MaxRTO = 2 * time.Minute
	cfg.IdleTimeout = 2 * time.Minute
	cfg.RetransLimit = 12
	admin.streamEngine = stream.New(cfg, send, logFn)
}

func (admin *Admin) recordStreamOpen(kind string) {
	if admin == nil {
		return
	}
	kind = normalizeKind(kind)
	admin.streamStats.mu.Lock()
	if admin.streamStats.kinds == nil {
		admin.streamStats.kinds = make(map[string]*streamKindStats)
	}
	stat := admin.streamStats.kinds[kind]
	if stat == nil {
		stat = &streamKindStats{}
		admin.streamStats.kinds[kind] = stat
	}
	stat.opened++
	admin.streamStats.mu.Unlock()
}

func (admin *Admin) recordStreamClose(kind, reason string) {
	if admin == nil {
		return
	}
	kind = normalizeKind(kind)
	admin.streamStats.mu.Lock()
	if admin.streamStats.kinds == nil {
		admin.streamStats.kinds = make(map[string]*streamKindStats)
	}
	stat := admin.streamStats.kinds[kind]
	if stat == nil {
		stat = &streamKindStats{}
		admin.streamStats.kinds[kind] = stat
	}
	stat.closed++
	if reason != "" {
		stat.lastReason = reason
	}
	stat.lastClosed = time.Now()
	admin.streamStats.mu.Unlock()
}

func (admin *Admin) rememberStreamReason(streamID uint32, reason string) {
	if admin == nil || streamID == 0 {
		return
	}
	admin.streamReasonMu.Lock()
	if admin.streamCloseReasons == nil {
		admin.streamCloseReasons = make(map[uint32]string)
	}
	if reason == "" {
		delete(admin.streamCloseReasons, streamID)
	} else {
		admin.streamCloseReasons[streamID] = reason
	}
	admin.streamReasonMu.Unlock()
}

// StreamCloseReason 返回并清除某个 stream ID 对应的关闭原因记录。
func (admin *Admin) StreamCloseReason(streamID uint32) string {
	if admin == nil || streamID == 0 {
		return ""
	}
	admin.streamReasonMu.Lock()
	reason := admin.streamCloseReasons[streamID]
	delete(admin.streamCloseReasons, streamID)
	admin.streamReasonMu.Unlock()
	return reason
}

func (admin *Admin) ensurePortProxyManager() *portProxyManager {
	if admin == nil {
		return nil
	}
	if admin.portProxies == nil {
		admin.portProxies = newPortProxyManager(admin.context, func(ctx context.Context, target, sessionID string, meta map[string]string) (io.ReadWriteCloser, error) {
			return admin.OpenStream(ctx, target, sessionID, meta)
		})
	}
	return admin.portProxies
}

func (admin *Admin) handleStreamOpen(header *protocol.Header, msg *protocol.StreamOpen) {
	if admin == nil || admin.streamEngine == nil || msg == nil {
		return
	}
	opts := parseStreamOptions(msg.Options)
	kind := strings.ToLower(opts["kind"])
	target := ""
	if header != nil {
		target = strings.TrimSpace(header.Sender)
	}
	if target == "" {
		printer.Fail("\r\n[*] Stream open missing sender, stream=%d\r\n", msg.StreamID)
		return
	}
	sessionStream := admin.streamEngine.Accept(msg.StreamID, stream.Options{Target: target, Meta: opts})
	if sessionStream == nil {
		return
	}
	switch kind {
	case "backward-conn":
		lport := strings.TrimSpace(opts["lport"])
		if lport == "" {
			sessionStream.Close()
			return
		}
		admin.recordStreamOpen(kind)
		go func() {
			defer sessionStream.Close()
			conn, err := net.Dial("tcp", net.JoinHostPort("127.0.0.1", lport))
			if err != nil {
				printer.Fail("\r\n[*] backward-conn dial %s failed: %v\r\n", lport, err)
				return
			}
			defer conn.Close()
			done := make(chan struct{}, 2)
			go func() { io.Copy(sessionStream, conn); done <- struct{}{} }()
			go func() { io.Copy(conn, sessionStream); done <- struct{}{} }()
			<-done
		}()
	default:
		admin.recordStreamOpen(kind)
		sessionStream.Close()
	}
}

func (admin *Admin) handleStreamData(_ *protocol.Header, msg *protocol.StreamData) {
	if admin == nil || admin.streamEngine == nil || msg == nil {
		return
	}
	admin.streamEngine.HandleData(msg)
}

func (admin *Admin) handleStreamAck(_ *protocol.Header, msg *protocol.StreamAck) {
	if admin == nil || admin.streamEngine == nil || msg == nil {
		return
	}
	admin.streamEngine.HandleAck(msg)
}

func (admin *Admin) handleStreamClose(hdr *protocol.Header, msg *protocol.StreamClose) {
	if admin == nil || admin.streamEngine == nil || msg == nil {
		return
	}
	meta := admin.streamEngine.SessionMeta(msg.StreamID)
	kind := ""
	if meta != nil {
		kind = meta["kind"]
	}
	target := ""
	if hdr != nil {
		target = strings.TrimSpace(hdr.Sender)
	}
	admin.recordStreamClose(kind, msg.Reason)
	admin.rememberStreamReason(msg.StreamID, msg.Reason)
	if kind == "" {
		kind = "unknown"
	}
	if msg.Reason != "" {
		printer.Warning("\r\n[*] Stream(%s) closed: %s\r\n", kind, msg.Reason)
	} else {
		printer.Warning("\r\n[*] Stream(%s) closed.\r\n", kind)
	}
	// 针对 file-put/file-get，记录完成的 loot（仅成功时）。
	if (kind == "file-put" || kind == "file-get") && admin.lootStore != nil {
		path := ""
		if meta != nil {
			path = strings.TrimSpace(meta["path"])
		}
		rec := LootRecord{
			TargetUUID: target,
			Category:   LootCategoryFile,
			Name:       path,
			OriginPath: path,
			Tags:       []string{kind, "completed"},
		}
		if parsed := parseStreamReason(msg.Reason); parsed != nil {
			if parsed.size > 0 {
				rec.Size = uint64(parsed.size)
			}
			if parsed.hash != "" {
				rec.Hash = parsed.hash
			}
			if parsed.mime != "" {
				rec.Mime = parsed.mime
			}
		}
		_, _ = admin.SubmitLoot(rec, nil)
	}
	admin.streamEngine.HandleClose(msg)
}

type streamCloseMeta struct {
	size int64
	hash string
	mime string
}

func parseStreamReason(reason string) *streamCloseMeta {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return nil
	}
	parts := strings.Fields(reason)
	if len(parts) == 0 {
		return nil
	}
	meta := &streamCloseMeta{}
	for _, p := range parts[1:] {
		kv := strings.SplitN(p, "=", 2)
		if len(kv) != 2 {
			continue
		}
		switch kv[0] {
		case "size":
			if sz, err := strconv.ParseInt(kv[1], 10, 64); err == nil {
				meta.size = sz
			}
		case "hash":
			meta.hash = kv[1]
		case "mime":
			meta.mime = kv[1]
		}
	}
	if meta.size == 0 && meta.hash == "" && meta.mime == "" {
		return nil
	}
	return meta
}

func (admin *Admin) OpenStream(ctx context.Context, target, sessionID string, meta map[string]string) (io.ReadWriteCloser, error) {
	if admin == nil || admin.streamEngine == nil {
		return nil, fmt.Errorf("stream engine unavailable")
	}
	opts := map[string]string{}
	for k, v := range meta {
		opts[k] = v
	}
	if sessionID != "" {
		opts["session"] = sessionID
	}
	streamHandle, err := admin.streamEngine.Open(ctx, target, stream.Options{Target: target, Meta: opts})
	if err != nil {
		return nil, err
	}
	admin.recordStreamOpen(opts["kind"])
	return streamHandle, nil
}

// ShellSessionID 返回节点当前或期望使用的 shell 会话标识。
func (admin *Admin) ShellSessionID(uuid string) string {
	if admin == nil || admin.mgr == nil || admin.mgr.ShellManager == nil {
		return uuid
	}
	sessionID := admin.mgr.ShellManager.SessionID(uuid)
	if sessionID == "" {
		return uuid
	}
	return sessionID
}

// StreamDiagnostics 暴露当前 stream engine 的诊断信息。
func (admin *Admin) StreamDiagnostics() []stream.SessionDiag {
	if admin == nil || admin.streamEngine == nil {
		return nil
	}
	return admin.streamEngine.Diagnostics()
}

func (admin *Admin) CloseStream(sessionID uint32, reason string) error {
	if admin == nil || admin.streamEngine == nil {
		return fmt.Errorf("stream engine unavailable")
	}
	if sessionID == 0 {
		return fmt.Errorf("stream id required")
	}
	if ok := admin.streamEngine.Close(sessionID, reason); !ok {
		return fmt.Errorf("stream %d not found", sessionID)
	}
	admin.rememberStreamReason(sessionID, reason)
	return nil
}

var (
	ErrStreamPingMissingTarget = errors.New("missing target uuid")
	ErrStreamPingInvalidCount  = errors.New("ping count must be > 0")
	ErrStreamPingInvalidSize   = errors.New("payload size must be >= 0")
)

func (admin *Admin) StreamPing(targetUUID string, count, payloadSize int) error {
	if admin == nil {
		return fmt.Errorf("admin unavailable")
	}
	targetUUID = strings.TrimSpace(targetUUID)
	if targetUUID == "" {
		return ErrStreamPingMissingTarget
	}
	if count <= 0 {
		return ErrStreamPingInvalidCount
	}
	if payloadSize < 0 {
		return ErrStreamPingInvalidSize
	}
	streamID := streamid.Next()
	open := &protocol.StreamOpen{StreamID: streamID, Options: "mode=ping"}
	openBytes, _ := protocol.EncodePayload(open)
	openEnvelope := fmt.Sprintf("proto:%04x:%s", uint16(protocol.STREAM_OPEN), hex.EncodeToString(openBytes))
	if _, err := admin.EnqueueDiagnostic(targetUUID, openEnvelope, dtn.PriorityNormal, 0); err != nil {
		return fmt.Errorf("enqueue open failed: %w", err)
	}
	for i := 1; i <= count; i++ {
		data := &protocol.StreamData{StreamID: streamID, Seq: uint32(i)}
		if payloadSize > 0 {
			data.Payload = make([]byte, payloadSize)
		}
		payload, _ := protocol.EncodePayload(data)
		env := fmt.Sprintf("proto:%04x:%s", uint16(protocol.STREAM_DATA), hex.EncodeToString(payload))
		if _, err := admin.EnqueueDiagnostic(targetUUID, env, dtn.PriorityNormal, 0); err != nil {
			return fmt.Errorf("enqueue data #%d failed: %w", i, err)
		}
	}
	close := &protocol.StreamClose{StreamID: streamID, Code: 0, Reason: "ok"}
	closeBytes, _ := protocol.EncodePayload(close)
	closeEnv := fmt.Sprintf("proto:%04x:%s", uint16(protocol.STREAM_CLOSE), hex.EncodeToString(closeBytes))
	if _, err := admin.EnqueueDiagnostic(targetUUID, closeEnv, dtn.PriorityNormal, 0); err != nil {
		return fmt.Errorf("enqueue close failed: %w", err)
	}
	return nil
}

func (admin *Admin) StreamStats() []StreamStat {
	if admin == nil {
		return nil
	}
	admin.streamStats.mu.Lock()
	defer admin.streamStats.mu.Unlock()
	snapshot := make([]StreamStat, 0, len(admin.streamStats.kinds))
	for kind, stat := range admin.streamStats.kinds {
		active := uint64(0)
		if stat.opened > stat.closed {
			active = stat.opened - stat.closed
		}
		snapshot = append(snapshot, StreamStat{
			Kind:       kind,
			Opened:     stat.opened,
			Closed:     stat.closed,
			Active:     active,
			LastReason: stat.lastReason,
			LastClosed: stat.lastClosed,
		})
	}
	sort.Slice(snapshot, func(i, j int) bool {
		return snapshot[i].Kind < snapshot[j].Kind
	})
	return snapshot
}
