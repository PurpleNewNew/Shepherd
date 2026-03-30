package process

import (
	"net"
	"time"

	"codeberg.org/agnoie/shepherd/pkg/utils"
	"codeberg.org/agnoie/shepherd/protocol"
)

const (
	// upCarryQueueCapacity bounds total buffered admin-bound messages per agent.
	// Keep this relatively small: this queue is only meant for small control-plane
	// messages (DTN_ACK / RUNTIMELOG) during short upstream outages.
	upCarryQueueCapacity = 256
	// upCarryMaxPayloadBytes prevents buffering large stream/file payloads.
	upCarryMaxPayloadBytes = 256 * 1024
	// upCarryDefaultTTL bounds how long we keep an upstream message when the parent
	// link is flapping (sleep/failover/reconnect churn).
	upCarryDefaultTTL = 10 * time.Minute
)

type upCarryItem struct {
	header      *protocol.Header
	payload     interface{}
	passThrough bool
	enqueuedAt time.Time
	expireAt   time.Time
	holdUntil  time.Time
	attempts   int
}

func upCarryTTLForMessageType(msgType uint16) time.Duration {
	// Buffer only small, high-value control-plane messages. Large/interactive
	// flows (STREAM_*) should not be buffered here.
	switch msgType {
	case uint16(protocol.DTN_ACK):
		return upCarryDefaultTTL
	case uint16(protocol.RUNTIMELOG):
		return upCarryDefaultTTL
	// Topology/self-heal control-plane (small).
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
		enqueuedAt: now,
		expireAt:   now.Add(ttl),
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
	// Best-effort size guard for locally constructed payloads.
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
			// Local payload: nothing further to validate here.
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

	// Best-effort error reporting: when we can access the underlying buffers, write
	// directly so we can retry on transient failures (the default SendMessage() path
	// logs+closes but doesn't return an error).
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
