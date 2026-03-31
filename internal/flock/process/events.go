package process

import (
	"context"
	"net"
	"time"

	"codeberg.org/agnoie/shepherd/internal/flock/manager"
	"codeberg.org/agnoie/shepherd/pkg/config/defaults"
	"codeberg.org/agnoie/shepherd/pkg/utils"
	"codeberg.org/agnoie/shepherd/protocol"
)

func broadcastOfflineMess(ctx context.Context, mgr *manager.Manager) {
	if mgr == nil {
		return
	}
	sess := mgr.ActiveSession()
	if sess == nil {
		return
	}
	secret := sess.Secret()
	uuid := sess.UUID()

	children := mgr.ChildrenManager.AllChildren()
	for _, childUUID := range children {
		conn, ok := mgr.ChildrenManager.GetConn(childUUID)
		if !ok || conn == nil {
			continue
		}
		if ctx != nil && ctx.Err() != nil {
			return
		}
		sMessage := protocol.NewDownMsg(conn, secret, uuid)
		protocol.SetMessageMeta(sMessage, sess.ProtocolFlags())

		header := &protocol.Header{
			Sender:      uuid,
			Accepter:    childUUID,
			MessageType: protocol.UPSTREAMOFFLINE,
			RouteLen:    uint32(len([]byte(protocol.TEMP_ROUTE))),
			Route:       protocol.TEMP_ROUTE,
		}

		offlineMess := &protocol.UpstreamOffline{
			OK: 1,
		}

		protocol.ConstructMessage(sMessage, header, offlineMess, false)
		// 施加一个较短的写超时，避免被卡死的子节点长期阻塞。
		_ = conn.SetWriteDeadline(time.Now().Add(defaults.BroadcastWriteDeadline))
		sMessage.SendMessage()
		_ = conn.SetWriteDeadline(time.Time{})
	}
}

func broadcastReonlineMess(ctx context.Context, mgr *manager.Manager) {
	if mgr == nil {
		return
	}
	sess := mgr.ActiveSession()
	if sess == nil {
		return
	}
	secret := sess.Secret()
	uuid := sess.UUID()

	children := mgr.ChildrenManager.AllChildren()
	for _, childUUID := range children {
		conn, ok := mgr.ChildrenManager.GetConn(childUUID)
		if !ok || conn == nil {
			continue
		}
		if ctx != nil && ctx.Err() != nil {
			return
		}
		sMessage := protocol.NewDownMsg(conn, secret, uuid)
		protocol.SetMessageMeta(sMessage, sess.ProtocolFlags())

		header := &protocol.Header{
			Sender:      uuid,
			Accepter:    childUUID,
			MessageType: protocol.UPSTREAMREONLINE,
			RouteLen:    uint32(len([]byte(protocol.TEMP_ROUTE))),
			Route:       protocol.TEMP_ROUTE,
		}

		reOnlineMess := &protocol.UpstreamReonline{
			OK: 1,
		}

		protocol.ConstructMessage(sMessage, header, reOnlineMess, false)
		_ = conn.SetWriteDeadline(time.Now().Add(defaults.BroadcastWriteDeadline))
		sMessage.SendMessage()
		_ = conn.SetWriteDeadline(time.Time{})
	}
}

func downStreamOffline(agent *Agent, uuid string, expected *childDispatcher, expectedConn net.Conn) {
	if agent == nil {
		return
	}
	// 子节点重连可能与旧 reader 的收尾过程并发竞争：如果同一 uuid 已经装上了新的 dispatcher，
	// 这次 offline 回调就属于旧连接，不应清掉新的子节点路由。
	if expected != nil {
		current := agent.currentDispatcher(uuid)
		if current != expected {
			logger.Infof("[diag][child_offline] stage=skip_stale_dispatcher child=%s expected=%p current=%p conn=%s", uuid, expected, current, connEndpoints(expectedConn))
			return
		}
	}

	mgr := agent.mgr
	if mgr == nil {
		return
	}
	if expectedConn != nil && mgr.ChildrenManager != nil {
		if currentConn, ok := mgr.ChildrenManager.GetConn(uuid); ok && currentConn != nil && currentConn != expectedConn {
			if !sameBaseConn(currentConn, expectedConn) {
				logger.Infof("[diag][child_offline] stage=skip_stale_conn child=%s expected=%s current=%s", uuid, connEndpoints(expectedConn), connEndpoints(currentConn))
				return
			}
		}
	}

	logger.Infof("[diag][child_offline] stage=remove child=%s conn=%s", uuid, connEndpoints(expectedConn))
	mgr.ChildrenManager.RemoveChild(uuid)

	agent.removeNeighbor(uuid)

	sMessage, sess, ok := agent.newUpMsg()
	if !ok {
		return
	}
	senderUUID := ""
	if sess != nil {
		senderUUID = sess.UUID()
	}

	header := &protocol.Header{
		Sender:      senderUUID,
		Accepter:    protocol.ADMIN_UUID,
		MessageType: protocol.NODEOFFLINE,
		RouteLen:    uint32(len([]byte(protocol.TEMP_ROUTE))),
		Route:       protocol.TEMP_ROUTE,
	}

	offlineMess := &protocol.NodeOffline{
		UUIDLen: uint16(len(uuid)),
		UUID:    uuid,
	}

	protocol.ConstructMessage(sMessage, header, offlineMess, false)
	sMessage.SendMessage()
}

func connEndpoints(conn net.Conn) string {
	if conn == nil {
		return "-"
	}
	local := "?"
	remote := "?"
	if addr := conn.LocalAddr(); addr != nil {
		local = addr.String()
	}
	if addr := conn.RemoteAddr(); addr != nil {
		remote = addr.String()
	}
	return local + "->" + remote
}

func sameBaseConn(a, b net.Conn) bool {
	ua := unwrapSafeConn(a)
	ub := unwrapSafeConn(b)
	if ua == nil || ub == nil {
		return false
	}
	return ua == ub
}

func unwrapSafeConn(conn net.Conn) net.Conn {
	if conn == nil {
		return nil
	}
	if safe, ok := conn.(*utils.SafeConn); ok && safe != nil {
		return safe.Conn
	}
	return conn
}

func tellAdminReonline(mgr *manager.Manager) {
	if mgr == nil {
		return
	}
	sess := mgr.ActiveSession()
	if sess == nil {
		return
	}
	conn := sess.Conn()
	if conn == nil {
		return
	}
	activeUUID := sess.UUID()
	sMessage := protocol.NewUpMsg(conn, sess.Secret(), activeUUID)

	reheader := &protocol.Header{
		Sender:      activeUUID,
		Accepter:    protocol.ADMIN_UUID,
		MessageType: protocol.NODEREONLINE,
		RouteLen:    uint32(len([]byte(protocol.TEMP_ROUTE))),
		Route:       protocol.TEMP_ROUTE,
	}

	for _, childUUID := range mgr.ChildrenManager.AllChildren() {
		childConn, ok := mgr.ChildrenManager.GetConn(childUUID)
		if !ok || childConn == nil {
			continue
		}

		reMess := &protocol.NodeReonline{
			ParentUUIDLen: uint16(len(activeUUID)),
			ParentUUID:    activeUUID,
			UUIDLen:       uint16(len(childUUID)),
			UUID:          childUUID,
			IPLen:         uint16(len(childConn.RemoteAddr().String())),
			IP:            childConn.RemoteAddr().String(),
		}

		protocol.ConstructMessage(sMessage, reheader, reMess, false)
		sMessage.SendMessage()
	}
}

func DispatchOfflineMess(agent *Agent) {
	ctx := agent.context()
	for {
		select {
		case <-ctx.Done():
			return
		case message := <-agent.mgr.OfflineManager.OfflineMessChan:
			switch message.(type) {
			case *protocol.UpstreamOffline:
				broadcastOfflineMess(ctx, agent.mgr)
			case *protocol.UpstreamReonline:
				agent.sendMyInfo()
				tellAdminReonline(agent.mgr)
				broadcastReonlineMess(ctx, agent.mgr)
			}
		}
	}
}
