package process

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"codeberg.org/agnoie/shepherd/pkg/bus"
	"codeberg.org/agnoie/shepherd/pkg/utils"
	"codeberg.org/agnoie/shepherd/protocol"
)

var (
	ErrInvalidDownstreamMessage = errors.New("invalid downstream message")
	ErrNoRouteToChild           = errors.New("no route to child")
	ErrNoUpstreamSession        = errors.New("no active upstream session")
)

func (agent *Agent) sendMyInfo() {
	conn, secret, uuid := agent.connectionTriple()
	if conn == nil {
		return
	}
	sMessage := protocol.NewUpMsg(conn, secret, uuid)
	header := &protocol.Header{
		Sender:      agent.UUID,
		Accepter:    protocol.ADMIN_UUID,
		MessageType: protocol.MYINFO,
		RouteLen:    uint32(len([]byte(protocol.TEMP_ROUTE))),
		Route:       protocol.TEMP_ROUTE,
	}

	hostname, username := utils.GetSystemInfo()
	agent.updateSystemInfo(hostname, username)

	myInfoMess := &protocol.MyInfo{
		UUIDLen:     uint16(len(agent.UUID)),
		UUID:        agent.UUID,
		UsernameLen: uint64(len(username)),
		Username:    username,
		HostnameLen: uint64(len(hostname)),
		Hostname:    hostname,
		MemoLen:     uint64(len(agent.Memo)),
		Memo:        agent.Memo,
	}

	protocol.ConstructMessage(sMessage, header, myInfoMess, false)
	sMessage.SendMessage()
	agent.sendConnInfo()
}

func (agent *Agent) sendConnInfo() {
	conn, secret, uuid := agent.connectionTriple()
	if conn == nil {
		return
	}
	sess := agent.currentSession()
	if sess == nil {
		return
	}
	info := agent.buildConnInfoPayload()
	if info == nil {
		return
	}
	sMessage := protocol.NewUpMsg(conn, secret, uuid)
	protocol.SetMessageMeta(sMessage, sess.ProtocolVersion(), sess.ProtocolFlags())
	header := &protocol.Header{
		Sender:      agent.UUID,
		Accepter:    protocol.ADMIN_UUID,
		MessageType: protocol.NODECONNINFO,
		RouteLen:    uint32(len([]byte(protocol.TEMP_ROUTE))),
		Route:       protocol.TEMP_ROUTE,
	}
	protocol.ConstructMessage(sMessage, header, info, false)
	sMessage.SendMessage()
}

func (agent *Agent) buildConnInfoPayload() *protocol.NodeConnInfo {
	if agent == nil {
		return nil
	}
	listenPort := agent.configuredListenPort()
	fallbackPort := agent.repairPort
	dialAddr := agent.repairDialAddress()
	transport := ""
	if agent.options != nil {
		transport = agent.options.Upstream
	}
	// 若没有任何数据就不发送
	if listenPort == 0 && fallbackPort == 0 && dialAddr == "" {
		return nil
	}
	info := &protocol.NodeConnInfo{
		UUID:            agent.UUID,
		DialAddress:     dialAddr,
		ListenPort:      uint16(clampPort(listenPort)),
		FallbackPort:    uint16(clampPort(fallbackPort)),
		Transport:       transport,
		LastSuccessUnix: time.Now().Unix(),
	}
	if agent.options != nil && agent.options.TlsEnable {
		info.TlsEnabled = 1
	}
	return info
}

func clampPort(port int) int {
	if port < 0 {
		return 0
	}
	if port > 65535 {
		return 65535
	}
	return port
}

func (agent *Agent) configuredListenPort() int {
	if agent == nil || agent.options == nil {
		return 0
	}
	listen := agent.options.Listen
	if listen == "" {
		return 0
	}
	_, portStr, err := net.SplitHostPort(listen)
	if err != nil {
		return 0
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return 0
	}
	return port
}

func (agent *Agent) repairDialAddress() string {
	if agent == nil {
		return ""
	}
	if agent.repairBind != "" && !isUnspecifiedIP(agent.repairBind) {
		return agent.repairBind
	}
	ip := agent.localSessionIP()
	if ip != "" {
		return ip
	}
	return ""
}

func (agent *Agent) sendChildMessage(msg *ChildrenMess) error {
	if agent == nil || msg == nil || msg.cHeader == nil {
		return ErrInvalidDownstreamMessage
	}
	conn, ok, fromSupp := agent.nextHopConn(msg.targetUUID, msg.preferSupp)
	if !ok || conn == nil {
		return fmt.Errorf("%w: %s (preferSupp=%v)", ErrNoRouteToChild, msg.targetUUID, msg.preferSupp)
	}
	sess := agent.currentSession()
	if sess == nil {
		return ErrNoUpstreamSession
	}
	if fromSupp {
		noteSupplementalActivity(msg.targetUUID)
	}
	sMessage := protocol.NewDownMsg(conn, sess.Secret(), sess.UUID())
	protocol.SetMessageMeta(sMessage, sess.ProtocolVersion(), sess.ProtocolFlags())
	protocol.ConstructMessage(sMessage, msg.cHeader, msg.cMessage, true)
	sMessage.SendMessage()
	return nil
}

// forwardToParent 将一个中转消息路由到当前父跳点。
// 如果调用方显式要求 supplemental 父路径，则优先使用它；
// 否则回退到上游会话加 up-carry 缓冲的方式。
func (agent *Agent) forwardToParent(header *protocol.Header, payload []byte, parentUUID string, preferSupp bool) bool {
	if agent == nil || header == nil || len(payload) == 0 || strings.TrimSpace(parentUUID) == "" {
		return false
	}
	if preferSupp {
		err := agent.sendChildMessage(&ChildrenMess{
			cHeader:    header,
			cMessage:   payload,
			targetUUID: parentUUID,
			preferSupp: true,
		})
		if err == nil {
			return true
		}
	}
	if err := agent.sendUpCarryItem(header, payload, true); err == nil {
		return true
	}
	return agent.maybeEnqueueUpCarry(header, payload)
}

func (agent *Agent) handleDataFromUpstream() {
	ctx := agent.context()

	conn, secret, uuid := agent.connectionTriple()
	if conn == nil {
		return
	}
	agent.watchConn(ctx, conn)
	rMessage := protocol.NewUpMsg(conn, secret, uuid)

	for {
		if ctx.Err() != nil {
			return
		}

		header, message, err := protocol.DestructMessage(rMessage)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			// 如果其他 goroutine 已经替换了当前活跃的上游会话
			// （repair/rescue/failover），就不要再走完整的 offline 处理流程。
			// offline 路径可能会阻塞在 sleep/reconnect 定时器上，反而把新建立的
			// 会话卡住；这里直接让读循环重新绑定到新的连接即可。
			if newConn, newSecret, newUUID := agent.connectionTriple(); newConn != nil && newConn != conn {
				conn, secret, uuid = newConn, newSecret, newUUID
				agent.watchConn(ctx, conn)
				rMessage = protocol.NewUpMsg(conn, secret, uuid)
				go agent.sendMyInfo()
				continue
			}
			upstreamOffline(agent)
			conn, secret, uuid = agent.connectionTriple()
			if conn == nil {
				return
			}
			agent.watchConn(ctx, conn)
			rMessage = protocol.NewUpMsg(conn, secret, uuid)
			go agent.sendMyInfo()
			continue
		}

		if header != nil {
			logger.Infof(
				"downstream message received: type=%d sender=%s accepter=%s route=%s len=%d",
				header.MessageType,
				header.Sender,
				header.Accepter,
				routeDisplay(header.Route),
				header.DataLen,
			)
		}
		agent.noteActivity()

		if header.Accepter == agent.UUID {
			if header.RouteLen == 0 {
				// Sender=ADMIN_UUID 用于 Kelpie 发出的端到端控制消息，
				// 这类消息可能跨越多跳，不应覆盖本地记录的上游父关系。
				// 只有在当前还没有父节点时（根节点或 bootstrap 阶段），
				// 才允许把 ADMIN_UUID 视作父节点。
				if header.Sender != protocol.ADMIN_UUID || agent.ParentUUID() == "" {
					agent.setParentUUID(header.Sender)
				}
			}
			logger.Infof("dispatching local message type=%d from=%s route=%s", header.MessageType, header.Sender, routeDisplay(header.Route))
			agent.dispatchLocalMessage(header, message, header.Sender, conn)
			continue
		}

		// 当 Accepter 不是本节点 UUID 时，按 Route 判定下一跳。
		nextUUID, preferSupp := nextHopFromRoute(agent.UUID, header)
		if nextUUID == agent.UUID {
			logger.Infof("dispatching local message (via route) type=%d from=%s", header.MessageType, header.Sender)
			agent.dispatchLocalMessage(header, message, header.Sender, conn)
			continue
		}
		if shouldDispatchTerminalRoute(header, nextUUID) {
			logger.Infof("dispatching local message (terminal route) type=%d from=%s", header.MessageType, header.Sender)
			agent.dispatchLocalMessage(header, message, header.Sender, conn)
			continue
		}

		// 需要转发到子节点
		if nextUUID == "" {
			logger.Warnf("drop downstream message: empty child route (sender=%s type=%d)", header.Sender, header.MessageType)
			continue
		}
		msgBytes, ok := message.([]byte)
		if !ok {
			logger.Warnf("drop downstream message: payload not []byte (type=%T) sender=%s msgType=%d", message, header.Sender, header.MessageType)
			continue
		}
		parentUUID := agent.ParentUUID()
		if parentUUID != "" && nextUUID == parentUUID {
			if !agent.forwardToParent(header, msgBytes, parentUUID, preferSupp) {
				logger.Warnf("failed to forward downstream message type=%d back to parent=%s via %s route=%s",
					header.MessageType, parentUUID, preferSuppString(preferSupp), routeDisplay(header.Route))
				continue
			}
			agent.noteActivity()
			continue
		}
		logger.Infof("forwarding message type=%d to child=%s via %s route=%s", header.MessageType, nextUUID, preferSuppString(preferSupp), routeDisplay(header.Route))
		agent.enqueueChildMessage(&ChildrenMess{
			cHeader:    header,
			cMessage:   msgBytes,
			targetUUID: nextUUID,
			preferSupp: preferSupp,
		})
		agent.noteActivity()
	}
}

func (agent *Agent) nextHopConn(uuid string, preferSupp bool) (net.Conn, bool, bool) {
	if agent.mgr == nil {
		return nil, false, false
	}
	if preferSupp && agent.mgr.SupplementalManager != nil {
		if conn, ok := agent.mgr.SupplementalManager.GetConn(uuid); ok && conn != nil {
			return conn, true, true
		}
	}
	if agent.mgr.ChildrenManager != nil {
		if conn, ok := agent.mgr.ChildrenManager.GetConn(uuid); ok && conn != nil {
			return conn, true, false
		}
	}
	if !preferSupp && agent.mgr.SupplementalManager != nil {
		if conn, ok := agent.mgr.SupplementalManager.GetConn(uuid); ok && conn != nil {
			return conn, true, true
		}
	}
	return nil, false, false
}

func routeDisplay(route string) string {
	if strings.TrimSpace(route) == "" {
		return "-"
	}
	return route
}

// TEMP_UUID 且 route 为空，表示路由遍历已经到达最后一跳。
// 此时消息必须本地交付，而不能直接丢弃。
func shouldDispatchTerminalRoute(header *protocol.Header, nextUUID string) bool {
	if header == nil || nextUUID != "" {
		return false
	}
	if header.Accepter != protocol.TEMP_UUID {
		return false
	}
	return strings.TrimSpace(header.Route) == "" || header.RouteLen == 0
}

func preferSuppString(preferSupp bool) string {
	if preferSupp {
		return "supplemental"
	}
	return "primary"
}

func (agent *Agent) waitingChild() {
	ctx := agent.context()
	for {
		select {
		case <-ctx.Done():
			return
		case childInfo, ok := <-agent.mgr.ChildrenManager.ChildComeChan:
			if !ok {
				return
			}
			if childInfo == nil {
				continue
			}
			if old := agent.currentDispatcher(childInfo.UUID); old != nil {
				agent.removeDispatcher(childInfo.UUID, old)
				logger.Infof("[diag][child_link] stage=replace child=%s old_dispatcher=%p conn=%s", childInfo.UUID, old, connEndpoints(childInfo.Conn))
			} else {
				logger.Infof("[diag][child_link] stage=add child=%s conn=%s", childInfo.UUID, connEndpoints(childInfo.Conn))
			}
			dispatcher := agent.dispatcherFor(childInfo.UUID)
			agent.addNeighbor(childInfo.UUID)
			go agent.handleDataFromDownstream(childInfo.Conn, childInfo.UUID, dispatcher)
			go agent.flushCarryQueueForce(childInfo.UUID)
		}
	}
}

func (agent *Agent) waitingSupplemental() {
	if agent.mgr == nil || agent.mgr.SupplementalManager == nil {
		return
	}
	connChan := agent.mgr.SupplementalManager.ConnReadyChan
	if connChan == nil {
		return
	}
	ctx := agent.context()
	for {
		select {
		case <-ctx.Done():
			return
		case link, ok := <-connChan:
			if !ok {
				return
			}
			if link == nil || link.Conn == nil || link.PeerUUID == "" {
				continue
			}
			if old := agent.currentDispatcher(link.PeerUUID); old != nil {
				agent.removeDispatcher(link.PeerUUID, old)
			}
			dispatcher := agent.dispatcherFor(link.PeerUUID)
			agent.addNeighbor(link.PeerUUID)
			go agent.handleDataFromSupplemental(link.Conn, link.LinkUUID, link.PeerUUID, dispatcher)
			go agent.flushCarryQueueForce(link.PeerUUID)
		}
	}
}

func (agent *Agent) dispatchLocalMessage(header *protocol.Header, message interface{}, origin string, originConn net.Conn) {
	if agent == nil || agent.mgr == nil {
		return
	}
	agent.ensureRouter()
	if agent.router == nil || !agent.routerReady {
		return
	}
	ctx := withOrigin(context.Background(), origin)
	ctx = withOriginConn(ctx, originConn)
	if err := agent.router.Dispatch(ctx, header, message); err != nil {
		switch {
		case errors.Is(err, bus.ErrNoHandler):
			logger.Warnf("unknown message type: %d", header.MessageType)
		case errors.Is(err, bus.ErrBackpressure):
			logger.Warnf("dropped message %d due to backpressure", header.MessageType)
		default:
			logger.Errorf("message dispatch error: %v", err)
		}
	}
}

func (agent *Agent) handleDataFromDownstream(conn net.Conn, uuid string, dispatcher *childDispatcher) {
	ctx := agent.context()
	if dispatcher != nil {
		defer agent.removeDispatcher(uuid, dispatcher)
	}
	sess := agent.currentSession()
	if sess == nil {
		return
	}
	rMessage := protocol.NewDownMsg(conn, sess.Secret(), sess.UUID())
	protocol.SetMessageMeta(rMessage, sess.ProtocolVersion(), sess.ProtocolFlags())
	agent.watchConn(ctx, conn)

	for {
		if ctx.Err() != nil {
			return
		}
		header, message, err := protocol.DestructMessage(rMessage)
		if err != nil {
			downStreamOffline(agent, uuid, dispatcher, conn)
			return
		}

		if header.MessageType == uint16(protocol.GOSSIP_UPDATE) && header.Accepter == agent.UUID {
			agent.handleGossipPayload(message, uuid)
			continue
		}

		// 发往 Admin 的消息会沿上游回传；如果条件允许，
		// 优先按透传字节转发，避免中间跳点重复解密和加密。
		if header != nil && header.Accepter == protocol.ADMIN_UUID {
			if msgBytes, ok := message.([]byte); ok {
				if err := agent.sendUpCarryItem(header, msgBytes, true); err != nil {
					// 上游暂时不可用，或写入失败（链路抖动）。这里缓存较小的
					// admin 定向透传消息，避免 DTN_ACK/RuntimeLog 被直接丢掉。
					if agent.maybeEnqueueUpCarry(header, msgBytes) {
						agent.noteActivity()
						continue
					}
					logger.Warnf("drop admin-bound pass-through message type=%d from=%s: %v", header.MessageType, uuid, err)
					continue
				}
				agent.noteActivity()
				continue
			}

			// 少见的兜底路径：如果收到的是一个已经解码好的、发往 ADMIN_UUID 的消息，
			// 就按本地载荷转发（会重新编组并加密）。
			if err := agent.sendUpCarryItem(header, message, false); err != nil {
				if agent.maybeEnqueueUpCarryLocal(header, message) {
					agent.noteActivity()
					continue
				}
				logger.Warnf("drop admin-bound local message type=%d from=%s: %v", header.MessageType, uuid, err)
				continue
			}
			agent.noteActivity()
			continue
		}

		// 本地交付。
		if header.Accepter == agent.UUID {
			agent.dispatchLocalMessage(header, message, uuid, conn)
			agent.noteActivity()
			continue
		}

		// 对非 admin 流量按 Route 转发。
		nextUUID, preferSupp := nextHopFromRoute(agent.UUID, header)
		if nextUUID == agent.UUID {
			agent.dispatchLocalMessage(header, message, uuid, conn)
			agent.noteActivity()
			continue
		}
		if shouldDispatchTerminalRoute(header, nextUUID) {
			agent.dispatchLocalMessage(header, message, uuid, conn)
			agent.noteActivity()
			continue
		}
		if nextUUID == "" {
			logger.Warnf("drop upstream message: empty child route (sender=%s type=%d)", header.Sender, header.MessageType)
			continue
		}
		msgBytes, ok := message.([]byte)
		if !ok {
			logger.Warnf("drop upstream message: payload not []byte (type=%T) sender=%s msgType=%d", message, header.Sender, header.MessageType)
			continue
		}
		parentUUID := agent.ParentUUID()
		if parentUUID != "" && nextUUID == parentUUID {
			if !agent.forwardToParent(header, msgBytes, parentUUID, preferSupp) {
				logger.Warnf("failed to forward message type=%d to parent=%s via %s route=%s",
					header.MessageType, parentUUID, preferSuppString(preferSupp), routeDisplay(header.Route))
				continue
			}
			agent.noteActivity()
			continue
		}
		agent.enqueueChildMessage(&ChildrenMess{
			cHeader:    header,
			cMessage:   msgBytes,
			targetUUID: nextUUID,
			preferSupp: preferSupp,
		})
		agent.noteActivity()
	}
}

func (agent *Agent) handleDataFromSupplemental(conn net.Conn, linkUUID, peerUUID string, dispatcher *childDispatcher) {
	ctx := agent.context()
	if dispatcher != nil {
		defer agent.removeDispatcher(peerUUID, dispatcher)
	}
	sess := agent.currentSession()
	if sess == nil {
		return
	}
	rMessage := protocol.NewDownMsg(conn, sess.Secret(), sess.UUID())
	protocol.SetMessageMeta(rMessage, sess.ProtocolVersion(), sess.ProtocolFlags())
	agent.watchConn(ctx, conn)
	promoted := false
	defer func() {
		if !promoted {
			CleanupSupplementalLink(agent.mgr, linkUUID)
			agent.removeNeighbor(peerUUID)
		}
	}()

	for {
		if ctx.Err() != nil {
			return
		}
		header, message, err := protocol.DestructMessage(rMessage)
		if err != nil {
			return
		}

		if header.MessageType == uint16(protocol.SUPPLINKPROMOTE) {
			if promote, ok := message.(*protocol.SuppLinkPromote); ok {
				if agent.handleSuppPromotionMessage(linkUUID, conn, promote) {
					promoted = true
					return
				}
			}
			continue
		}

		noteSupplementalActivity(peerUUID)

		// 发往 ADMIN_UUID 的消息走上游，不使用基于 route 的转发。
		// 当它们经 supplemental 链路到达时，应直接向上游透传。
		if header != nil && header.Accepter == protocol.ADMIN_UUID {
			if err := agent.sendUpCarryItem(header, message, true); err != nil {
				// 上游暂时不可用或写入失败时，缓存较小的透传消息
				// （如 DTN_ACK/RuntimeLog），而不是直接丢弃。
				if agent.maybeEnqueueUpCarry(header, message) {
					agent.noteActivity()
					continue
				}
				return
			}
			agent.noteActivity()
			continue
		}

		if header.Accepter == agent.UUID {
			agent.dispatchLocalMessage(header, message, peerUUID, conn)
			continue
		}

		nextUUID, preferSupp := nextHopFromRoute(agent.UUID, header)
		if nextUUID == agent.UUID {
			agent.dispatchLocalMessage(header, message, peerUUID, conn)
			continue
		}
		if shouldDispatchTerminalRoute(header, nextUUID) {
			agent.dispatchLocalMessage(header, message, peerUUID, conn)
			continue
		}

		if nextUUID == "" {
			logger.Warnf("drop supplemental message: empty child route (peer=%s type=%d)", peerUUID, header.MessageType)
			continue
		}
		msgBytes, ok := message.([]byte)
		if !ok {
			continue
		}
		parentUUID := agent.ParentUUID()
		if parentUUID != "" && nextUUID == parentUUID {
			if !agent.forwardToParent(header, msgBytes, parentUUID, preferSupp) {
				logger.Warnf("failed to forward supplemental message type=%d to parent=%s via %s route=%s",
					header.MessageType, parentUUID, preferSuppString(preferSupp), routeDisplay(header.Route))
				continue
			}
			agent.noteActivity()
			continue
		}
		agent.enqueueChildMessage(&ChildrenMess{
			cHeader:    header,
			cMessage:   msgBytes,
			targetUUID: nextUUID,
			preferSupp: preferSupp,
		})
		agent.noteActivity()
	}
}

func (agent *Agent) noteActivity() {
	if agent == nil {
		return
	}
	agent.sleepMu.Lock()
	agent.lastActivity = time.Now()
	agent.sleepMu.Unlock()
}

// holdAwakeFor 会延长一个短暂的宽限窗口，在此期间即使节点看起来空闲，
// 睡眠管理器也不能拆掉上游会话。
//
// 这用于 rescue/reparent、sleep 更新等控制面“关键区”，
// 因为如果立刻关闭上游会话，后续命令或 ACK 很容易被卡住。
func (agent *Agent) holdAwakeFor(d time.Duration) {
	if agent == nil || d <= 0 {
		return
	}
	now := time.Now()
	until := now.Add(d)
	agent.sleepMu.Lock()
	agent.lastActivity = now
	if until.After(agent.sleepGraceUntil) {
		agent.sleepGraceUntil = until
	}
	agent.sleepMu.Unlock()
}
