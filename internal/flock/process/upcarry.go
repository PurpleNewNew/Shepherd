package process

import (
	"net"
	"time"

	"codeberg.org/agnoie/shepherd/pkg/utils"
	"codeberg.org/agnoie/shepherd/protocol"
)

const (
	// upCarryQueueCapacity 限制每个 agent 可缓存的 admin 定向消息总量。
	// 这里应保持相对较小：该队列只用于在短暂上游中断期间缓存
	// 小型控制面消息（如 DTN_ACK / RUNTIMELOG）。
	upCarryQueueCapacity = 256
	// upCarryMaxPayloadBytes 用于阻止缓存过大的 stream/file 载荷。
	upCarryMaxPayloadBytes = 256 * 1024
	// upCarryDefaultTTL 限制父链路抖动时（sleep/failover/reconnect），
	// 上游消息在本地最多保留多久。
	upCarryDefaultTTL = 10 * time.Minute
)

type upCarryItem struct {
	header      *protocol.Header
	payload     interface{}
	passThrough bool
	enqueuedAt  time.Time
	expireAt    time.Time
	holdUntil   time.Time
	attempts    int
}

func upCarryTTLForMessageType(msgType uint16) time.Duration {
	// 这里只缓存小而关键的控制面消息。大流量或交互式流
	// （如 STREAM_*）不应该走这里。
	switch msgType {
	case uint16(protocol.DTN_ACK):
		return upCarryDefaultTTL
	case uint16(protocol.RUNTIMELOG):
		return upCarryDefaultTTL
	// 拓扑与自愈控制面消息（小消息）。
	case uint16(protocol.NODEOFFLINE), uint16(protocol.NODEREONLINE):
		return upCarryDefaultTTL
	case uint16(protocol.SUPPLINKRESP), uint16(protocol.SUPPLINKHEARTBEAT):
		return upCarryDefaultTTL
	case uint16(protocol.RESCUE_RESPONSE):
		return upCarryDefaultTTL
	case uint16(protocol.SLEEP_UPDATE_ACK):
		return upCarryDefaultTTL
	default:
		return 0
	}
}

func (agent *Agent) maybeEnqueueUpCarry(header *protocol.Header, message interface{}) bool {
	if agent == nil || header == nil {
		return false
	}
	if header.Accepter != protocol.ADMIN_UUID {
		return false
	}
	payload, ok := message.([]byte)
	if !ok || len(payload) == 0 {
		return false
	}
	if len(payload) > upCarryMaxPayloadBytes {
		return false
	}
	ttl := upCarryTTLForMessageType(header.MessageType)
	if ttl <= 0 {
		return false
	}
	now := time.Now()
	item := &upCarryItem{
		header:      cloneHeader(header),
		payload:     append([]byte(nil), payload...),
		passThrough: true,
		enqueuedAt:  now,
		expireAt:    now.Add(ttl),
	}
	agent.requeueUpCarryItem(item)
	return true
}

func (agent *Agent) maybeEnqueueUpCarryLocal(header *protocol.Header, payload interface{}) bool {
	if agent == nil || header == nil || payload == nil {
		return false
	}
	if header.Accepter != protocol.ADMIN_UUID {
		return false
	}
	ttl := upCarryTTLForMessageType(header.MessageType)
	if ttl <= 0 {
		return false
	}
	// 对本地构造载荷做一个尽力而为的大小保护。
	if buf, err := protocol.EncodePayload(payload); err == nil && len(buf) > upCarryMaxPayloadBytes {
		return false
	}
	now := time.Now()
	item := &upCarryItem{
		header:      cloneHeader(header),
		payload:     payload,
		passThrough: false,
		enqueuedAt:  now,
		expireAt:    now.Add(ttl),
	}
	agent.requeueUpCarryItem(item)
	return true
}

func (agent *Agent) requeueUpCarryItem(item *upCarryItem) {
	if agent == nil || item == nil || item.header == nil {
		return
	}
	agent.upCarryMu.Lock()
	queue := agent.upCarryQueue
	if len(queue) >= upCarryQueueCapacity {
		queue = queue[1:]
	}
	agent.upCarryQueue = append(queue, item)
	agent.upCarryMu.Unlock()
}

func (agent *Agent) flushUpCarryQueue() {
	agent.flushUpCarryQueueMode(false)
}

func (agent *Agent) flushUpCarryQueueForce() {
	agent.flushUpCarryQueueMode(true)
}

func (agent *Agent) flushUpCarryQueueMode(force bool) {
	if agent == nil {
		return
	}
	now := time.Now()

	agent.upCarryMu.Lock()
	if len(agent.upCarryQueue) == 0 {
		agent.upCarryMu.Unlock()
		return
	}
	ready := make([]*upCarryItem, 0, len(agent.upCarryQueue))
	pending := agent.upCarryQueue[:0]
	for _, item := range agent.upCarryQueue {
		if item == nil || item.header == nil || item.payload == nil {
			continue
		}
		if item.passThrough {
			buf, ok := item.payload.([]byte)
			if !ok || len(buf) == 0 {
				continue
			}
		}
		if !item.passThrough {
			// 本地载荷：这里无需再做额外校验。
		}
		if item.header == nil {
			continue
		}
		if !item.expireAt.IsZero() && now.After(item.expireAt) {
			continue
		}
		if !force && !item.holdUntil.IsZero() && now.Before(item.holdUntil) {
			pending = append(pending, item)
			continue
		}
		ready = append(ready, item)
	}
	if len(pending) == 0 {
		agent.upCarryQueue = nil
	} else {
		agent.upCarryQueue = pending
	}
	agent.upCarryMu.Unlock()

	for _, item := range ready {
		if item == nil || item.header == nil || item.payload == nil {
			continue
		}
		if err := agent.sendUpCarryItem(item.header, item.payload, item.passThrough); err != nil {
			item.attempts++
			item.holdUntil = time.Now().Add(carryBackoff(item.attempts))
			agent.requeueUpCarryItem(item)
		}
	}
}

func (agent *Agent) sendUpCarryItem(header *protocol.Header, payload interface{}, passThrough bool) error {
	conn, secret, uuid := agent.connectionTriple()
	return agent.sendUpCarryItemOnConn(conn, secret, uuid, header, payload, passThrough)
}

func (agent *Agent) sendUpCarryItemOnConn(conn net.Conn, secret, uuid string, header *protocol.Header, payload interface{}, passThrough bool) error {
	if agent == nil || header == nil || payload == nil {
		return ErrInvalidDownstreamMessage
	}
	if conn == nil || secret == "" || uuid == "" {
		return ErrNoUpstreamSession
	}
	up := protocol.NewUpMsg(conn, secret, uuid)
	version, flags := agent.protocolMeta()
	protocol.SetMessageMeta(up, version, flags)
	protocol.ConstructMessage(up, header, payload, passThrough)

	// 尽力改进错误上报：如果能直接访问底层缓冲区，就直接写入，
	// 这样在临时失败时还可以重试（默认的 SendMessage() 路径会记录日志并关闭，
	// 但不会返回错误）。
	switch m := up.(type) {
	case *protocol.RawMessage:
		final := append([]byte(nil), m.HeaderBuffer...)
		final = append(final, m.DataBuffer...)
		m.HeaderBuffer = nil
		m.DataBuffer = nil
		if err := utils.WriteFull(m.Conn, final); err != nil {
			if m.Conn != nil {
				_ = m.Conn.Close()
			}
			return err
		}
		return nil
	case *protocol.WSMessage:
		rm := m.RawMessage
		final := append([]byte(nil), rm.HeaderBuffer...)
		final = append(final, rm.DataBuffer...)
		rm.HeaderBuffer = nil
		rm.DataBuffer = nil
		if err := utils.WriteFull(rm.Conn, final); err != nil {
			if rm.Conn != nil {
				_ = rm.Conn.Close()
			}
			return err
		}
		return nil
	default:
		up.SendMessage()
		return nil
	}
}
