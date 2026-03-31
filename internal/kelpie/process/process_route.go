package process

import (
	"context"
	"fmt"
	"strings"

	"codeberg.org/agnoie/shepherd/internal/kelpie/topology"
	"codeberg.org/agnoie/shepherd/pkg/config/defaults"
	"codeberg.org/agnoie/shepherd/pkg/session"
	"codeberg.org/agnoie/shepherd/protocol"
)

func (admin *Admin) currentSession() session.Session {
	if admin == nil {
		return nil
	}
	if admin.sessions != nil {
		if sess := admin.sessions.primary(); sess != nil {
			return sess
		}
	}
	if admin.store != nil {
		if sess := admin.store.ActiveSession(); sess != nil {
			return sess
		}
	}
	// 最后回退到缓存的会话指针；在 failover/reconnect 期间可能短暂滞后。
	if admin.session != nil {
		return admin.session
	}
	return nil
}

func (admin *Admin) sessionForUUID(uuid string) session.Session {
	if admin == nil {
		return nil
	}
	uuid = strings.TrimSpace(uuid)
	if uuid == "" {
		return admin.currentSession()
	}
	if sess := admin.sessionForComponent(uuid); sess != nil && sess.Conn() != nil {
		return sess
	}
	var (
		route string
		ok    bool
	)
	if fn := admin.routeOverride; fn != nil {
		route, ok = fn(uuid)
	} else {
		route, ok = admin.fetchRoute(uuid)
	}
	if ok && strings.TrimSpace(route) != "" {
		return admin.sessionForRoute(route)
	}
	return admin.currentSession()
}

func (admin *Admin) sessionForRoute(route string) session.Session {
	if admin == nil {
		return nil
	}
	route = strings.TrimSpace(route)
	if route == "" {
		return admin.currentSession()
	}
	firstHop := dtnRouteFirstHop(route)
	if firstHop != "" {
		if sess := admin.sessionForComponent(firstHop); sess != nil && sess.Conn() != nil {
			return sess
		}
	}
	sess := admin.currentSession()
	if sess == nil || sess.Conn() == nil {
		return nil
	}
	if firstHop == "" {
		return sess
	}
	sessUUID := strings.TrimSpace(sess.UUID())
	if sessUUID == firstHop || sessUUID == protocol.ADMIN_UUID || sessUUID == protocol.TEMP_UUID {
		return sess
	}
	return nil
}

func (admin *Admin) newDownstreamMessageForRoute(targetUUID, route string) (protocol.Message, error) {
	var sess session.Session
	if strings.TrimSpace(route) != "" {
		sess = admin.sessionForRoute(route)
	} else {
		sess = admin.sessionForUUID(targetUUID)
	}
	if sess == nil {
		return nil, fmt.Errorf("session unavailable for %s", targetUUID)
	}
	conn := sess.Conn()
	if conn == nil {
		return nil, fmt.Errorf("connection unavailable for %s", targetUUID)
	}
	msg := protocol.NewDownMsg(conn, sess.Secret(), sess.UUID())
	protocol.SetMessageMeta(msg, sess.ProtocolVersion(), sess.ProtocolFlags())
	return msg, nil
}

func (admin *Admin) fetchRoute(uuid string) (string, bool) {
	if uuid == "" {
		return "", false
	}
	if uuid == protocol.ADMIN_UUID {
		return "", true
	}
	if !admin.nodeExists(uuid) {
		return "", false
	}
	task := &topology.TopoTask{
		Mode: topology.GETROUTE,
		UUID: uuid,
	}
	res, err := admin.topoRequest(task)
	if err != nil || res == nil {
		return "", false
	}
	return res.Route, true
}

// TopologySnapshot 返回适合 UI 展示的当前节点和边快照。
func (admin *Admin) TopologySnapshot(entry, network string) topology.UISnapshot {
	if admin == nil || admin.topology == nil {
		return topology.UISnapshot{}
	}
	return admin.topology.UISnapshot(entry, network)
}

func (admin *Admin) topoRequest(task *topology.TopoTask) (*topology.Result, error) {
	if admin == nil || admin.topoService == nil {
		return nil, fmt.Errorf("topology service unavailable")
	}
	ctx, cancel := context.WithTimeout(context.Background(), defaults.TopologyRequestTimeout)
	defer cancel()
	return admin.topoService.Request(ctx, task)
}
