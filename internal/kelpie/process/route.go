package process

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"codeberg.org/agnoie/shepherd/internal/kelpie/topology"
	"codeberg.org/agnoie/shepherd/pkg/config/defaults"
	"codeberg.org/agnoie/shepherd/pkg/session"
	"codeberg.org/agnoie/shepherd/protocol"
)

var ErrTargetUnreachable = errors.New("target unreachable")

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
	firstHop := routeFirstHop(route)
	if firstHop != "" {
		if sess := admin.sessionForComponent(firstHop); sess != nil && sess.Conn() != nil {
			return sess
		}
		if admin.bindAdminEntrySession(firstHop) {
			if sess := admin.sessionForComponent(firstHop); sess != nil && sess.Conn() != nil {
				return sess
			}
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

func (admin *Admin) adminEntrySession() session.Session {
	if admin == nil {
		return nil
	}
	if admin.store != nil {
		if sess := admin.store.SessionFor(protocol.ADMIN_UUID); sess != nil && sess.Conn() != nil {
			return sess
		}
		if sess := admin.store.SessionFor(protocol.TEMP_UUID); sess != nil && sess.Conn() != nil {
			return sess
		}
	}
	if admin.session != nil {
		switch strings.TrimSpace(admin.session.UUID()) {
		case protocol.ADMIN_UUID, protocol.TEMP_UUID:
			if admin.session.Conn() != nil {
				return admin.session
			}
		}
	}
	return nil
}

func (admin *Admin) bindAdminEntrySessions() {
	if admin == nil || admin.topology == nil {
		return
	}
	snapshot := admin.topology.UISnapshot("", "")
	for _, node := range snapshot.Nodes {
		if node.ParentUUID == protocol.ADMIN_UUID {
			admin.bindAdminEntrySession(node.UUID)
		}
	}
}

func (admin *Admin) bindAdminEntrySession(uuid string) bool {
	if admin == nil || admin.sessions == nil || !admin.isAdminEntryFirstHop(uuid) {
		return false
	}
	sess := admin.adminEntrySession()
	if sess == nil || sess.Conn() == nil {
		return false
	}
	admin.sessions.set(uuid, newRouteAliasSession(sess))
	return true
}

func (admin *Admin) isAdminEntryFirstHop(uuid string) bool {
	if admin == nil || admin.topology == nil {
		return false
	}
	uuid = strings.TrimSpace(uuid)
	if uuid == "" || uuid == protocol.ADMIN_UUID || uuid == protocol.TEMP_UUID {
		return false
	}
	snapshot := admin.topology.UISnapshot("", "")
	for _, node := range snapshot.Nodes {
		if node.UUID == uuid {
			return node.ParentUUID == protocol.ADMIN_UUID
		}
	}
	return false
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
	protocol.SetMessageMeta(msg, sess.ProtocolFlags())
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

func (admin *Admin) targetStatus(uuid string) (topology.NodeStatus, bool) {
	if admin == nil || admin.topology == nil || strings.TrimSpace(uuid) == "" {
		return topology.NodeStatusOffline, false
	}
	return admin.topology.NodeStatus(strings.TrimSpace(uuid))
}

func (admin *Admin) validateRouteForControl(targetUUID, route string) error {
	targetUUID = strings.TrimSpace(targetUUID)
	route = strings.TrimSpace(route)
	if targetUUID == "" {
		return fmt.Errorf("%w: missing target uuid", ErrTargetUnreachable)
	}
	if route == "" {
		return fmt.Errorf("%w: route unavailable for %s", ErrTargetUnreachable, targetUUID)
	}
	if admin != nil && admin.topology != nil {
		for hop, status := range admin.topology.PathStatuses(route) {
			switch status {
			case topology.NodeStatusQuarantined, topology.NodeStatusRetired:
				return fmt.Errorf("%w: route contains %s node %s", ErrTargetUnreachable, status, shortID(hop))
			}
		}
	}
	firstHop := routeFirstHop(route)
	if firstHop == "" {
		return fmt.Errorf("%w: first hop unavailable for %s", ErrTargetUnreachable, targetUUID)
	}
	sess := admin.sessionForRoute(route)
	if sess == nil || sess.Conn() == nil {
		return fmt.Errorf("%w: first hop %s has no live session", ErrTargetUnreachable, shortID(firstHop))
	}
	return nil
}

func (admin *Admin) routeForControl(targetUUID string) (string, error) {
	targetUUID = strings.TrimSpace(targetUUID)
	if admin == nil {
		return "", fmt.Errorf("admin unavailable")
	}
	if targetUUID == "" {
		return "", fmt.Errorf("%w: missing target uuid", ErrTargetUnreachable)
	}
	if strings.EqualFold(targetUUID, protocol.ADMIN_UUID) {
		return "", nil
	}
	status, ok := admin.targetStatus(targetUUID)
	if !ok {
		return "", fmt.Errorf("%w: node %s not found", ErrTargetUnreachable, shortID(targetUUID))
	}
	if status != topology.NodeStatusOnline {
		return "", fmt.Errorf("%w: node %s is %s", ErrTargetUnreachable, shortID(targetUUID), status)
	}
	route, ok := admin.fetchRoute(targetUUID)
	if !ok || strings.TrimSpace(route) == "" {
		return "", fmt.Errorf("%w: route unavailable for %s", ErrTargetUnreachable, shortID(targetUUID))
	}
	if err := admin.validateRouteForControl(targetUUID, route); err != nil {
		return "", err
	}
	return route, nil
}

// CheckTargetReady verifies that interactive/control traffic can be sent now.
func (admin *Admin) CheckTargetReady(targetUUID string) error {
	if admin != nil && admin.topology == nil {
		return nil
	}
	_, err := admin.routeForControl(targetUUID)
	return err
}

func (admin *Admin) fetchDTNRoute(uuid string, attempts int) (string, bool) {
	route, ok := admin.fetchRoute(uuid)
	if !ok || route == "" || !routeUsesSupplemental(route) || attempts <= 0 {
		return route, ok
	}
	if admin == nil || admin.topology == nil {
		return route, ok
	}
	primaryRoute, primaryOK := admin.topology.PrimaryRoute(uuid)
	if !primaryOK || strings.TrimSpace(primaryRoute) == "" {
		return route, ok
	}
	if sess := admin.sessionForRoute(primaryRoute); sess == nil || sess.Conn() == nil {
		return route, ok
	}
	return primaryRoute, true
}

func routeFirstHop(route string) string {
	route = strings.TrimSpace(route)
	if route == "" {
		return ""
	}
	parts := strings.Split(route, ":")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		hop, _ := stripRouteSegment(part)
		return hop
	}
	return ""
}

func routeUsesSupplemental(route string) bool {
	if route == "" {
		return false
	}
	parts := strings.Split(route, ":")
	for _, part := range parts {
		_, supplemental := stripRouteSegment(strings.TrimSpace(part))
		if supplemental {
			return true
		}
	}
	return false
}

func routeIncludesUUID(route, uuid string) bool {
	if route == "" || uuid == "" {
		return false
	}
	parts := strings.Split(route, ":")
	for _, part := range parts {
		next, _ := stripRouteSegment(part)
		if next == uuid {
			return true
		}
	}
	return false
}

func stripRouteSegment(segment string) (string, bool) {
	const suffix = "#supp"
	if strings.HasSuffix(segment, suffix) {
		return strings.TrimSuffix(segment, suffix), true
	}
	return segment, false
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
