package process

import (
	"errors"
	"time"

	"codeberg.org/agnoie/shepherd/protocol"
)

const (
	carryQueueCapacityPerTarget = 64
	carryDefaultTTL             = 30 * time.Minute
	// carrySuppLinkReqTTL 限制那些因临时拓扑或连接状态
	// （例如子节点尚未注册）而无法转发给子节点的 supplemental-link 请求
	// 在本地最多保留多久。把它设为非零，可以避免自愈期间因竞态导致的丢弃。
	carrySuppLinkReqTTL = 10 * time.Minute
	carryRetryBase      = 2 * time.Second
	carryRetryMax       = 1 * time.Minute
	carrySweepInterval  = 5 * time.Second
)

type carryItem struct {
	header     *protocol.Header
	payload    []byte
	targetUUID string
	preferSupp bool
	enqueuedAt time.Time
	expireAt   time.Time
	holdUntil  time.Time
	attempts   int
}

func cloneHeader(src *protocol.Header) *protocol.Header {
	if src == nil {
		return nil
	}
	cp := *src
	return &cp
}

func (agent *Agent) startCarryForward() {
	if agent == nil {
		return
	}
	go agent.carryLoop()
}

func (agent *Agent) carryLoop() {
	if agent == nil {
		return
	}
	ticker := time.NewTicker(carrySweepInterval)
	defer ticker.Stop()
	ctx := agent.context()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			agent.flushCarryAll()
			agent.flushUpCarryQueue()
		}
	}
}

func (agent *Agent) flushCarryAll() {
	if agent == nil {
		return
	}
	agent.carryMu.Lock()
	targets := make([]string, 0, len(agent.carryQueue))
	for target, queue := range agent.carryQueue {
		if len(queue) > 0 {
			targets = append(targets, target)
		}
	}
	agent.carryMu.Unlock()

	for _, target := range targets {
		agent.flushCarryQueue(target)
	}
}

func (agent *Agent) flushCarryQueue(target string) {
	agent.flushCarryQueueMode(target, false)
}

// flushCarryQueueForce 会忽略 holdUntil，强制刷新某个目标待发送的
// DTN carry-forward 项。
// 它主要用于子节点连接刚恢复可用时，因为 holdUntil 的目的本来就是
// 避免反复敲打离线链路。
func (agent *Agent) flushCarryQueueForce(target string) {
	agent.flushCarryQueueMode(target, true)
}

func (agent *Agent) flushCarryQueueMode(target string, force bool) {
	if agent == nil || target == "" {
		return
	}
	now := time.Now()
	var ready []*carryItem

	agent.carryMu.Lock()
	queue := agent.carryQueue[target]
	if len(queue) == 0 {
		delete(agent.carryQueue, target)
		agent.carryMu.Unlock()
		return
	}
	pending := queue[:0]
	for _, item := range queue {
		if item == nil {
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
		delete(agent.carryQueue, target)
	} else {
		agent.carryQueue[target] = pending
	}
	agent.carryMu.Unlock()

	for _, item := range ready {
		if item == nil {
			continue
		}
		msg := &ChildrenMess{
			cHeader:    cloneHeader(item.header),
			cMessage:   append([]byte(nil), item.payload...),
			targetUUID: item.targetUUID,
			preferSupp: item.preferSupp,
		}
		if err := agent.sendChildMessage(msg); err != nil {
			if !agent.shouldCarryRetry(err, msg) {
				continue
			}
			item.attempts++
			item.holdUntil = time.Now().Add(carryBackoff(item.attempts))
			agent.requeueCarryItem(item)
		}
	}
}

func (agent *Agent) shouldCarryRetry(err error, msg *ChildrenMess) bool {
	if err == nil || msg == nil || msg.cHeader == nil {
		return false
	}
	if !errors.Is(err, ErrNoRouteToChild) && !errors.Is(err, ErrNoUpstreamSession) {
		return false
	}
	switch msg.cHeader.MessageType {
	case uint16(protocol.DTN_DATA):
		return true
	case uint16(protocol.SUPPLINKREQ):
		return true
	default:
		return false
	}
}

func (agent *Agent) enqueueCarry(msg *ChildrenMess, reason error) {
	if agent == nil || msg == nil || msg.cHeader == nil || msg.targetUUID == "" {
		return
	}
	ttl := carryTTLForMessageType(msg.cHeader.MessageType)
	if ttl <= 0 {
		return
	}
	now := time.Now()
	item := &carryItem{
		header:     cloneHeader(msg.cHeader),
		payload:    append([]byte(nil), msg.cMessage...),
		targetUUID: msg.targetUUID,
		preferSupp: msg.preferSupp,
		enqueuedAt: now,
		expireAt:   now.Add(ttl),
	}
	agent.requeueCarryItem(item)
}

func carryTTLForMessageType(msgType uint16) time.Duration {
	switch msgType {
	case uint16(protocol.DTN_DATA):
		return carryDefaultTTL
	case uint16(protocol.SUPPLINKREQ):
		return carrySuppLinkReqTTL
	default:
		return 0
	}
}

func (agent *Agent) requeueCarryItem(item *carryItem) {
	if agent == nil || item == nil || item.targetUUID == "" {
		return
	}
	agent.carryMu.Lock()
	if agent.carryQueue == nil {
		agent.carryQueue = make(map[string][]*carryItem)
	}
	queue := agent.carryQueue[item.targetUUID]
	if len(queue) >= carryQueueCapacityPerTarget {
		queue = queue[1:]
	}
	agent.carryQueue[item.targetUUID] = append(queue, item)
	agent.carryMu.Unlock()
}

func carryBackoff(attempts int) time.Duration {
	if attempts <= 0 {
		return carryRetryBase
	}
	delay := carryRetryBase * time.Duration(1<<min(attempts, 6))
	if delay > carryRetryMax {
		return carryRetryMax
	}
	return delay
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
