package process

import (
	"context"
	"net"
	"strings"
	"time"

	"codeberg.org/agnoie/shepherd/internal/flock/manager"
	"codeberg.org/agnoie/shepherd/protocol"
)

func (agent *Agent) handleSuppPromotionMessage(linkUUID string, conn net.Conn, promote *protocol.SuppLinkPromote) bool {
	if promote == nil {
		return false
	}
	switch promote.Role {
	case protocol.SuppPromoteRoleChild:
		agent.sendSuppPromotionAck(conn, promote)
		agent.promoteSupplementalAsChild(linkUUID, promote.ParentUUID, conn)
		agent.clearPendingFailover(linkUUID)
		return true
	case protocol.SuppPromoteRoleParent:
		agent.promoteSupplementalAsParent(linkUUID, promote.ChildUUID, conn)
		agent.clearPendingFailover(linkUUID)
		return true
	default:
		return false
	}
}

func (agent *Agent) sendSuppPromotionAck(conn net.Conn, promote *protocol.SuppLinkPromote) {
	if conn == nil || promote == nil {
		return
	}
	sess := agent.currentSession()
	if sess == nil {
		return
	}
	header := &protocol.Header{
		Sender:      agent.UUID,
		Accepter:    promote.ParentUUID,
		MessageType: uint16(protocol.SUPPLINKPROMOTE),
		RouteLen:    0,
		Route:       "",
	}
	ack := &protocol.SuppLinkPromote{
		LinkUUIDLen:   promote.LinkUUIDLen,
		LinkUUID:      promote.LinkUUID,
		ParentUUIDLen: promote.ParentUUIDLen,
		ParentUUID:    promote.ParentUUID,
		ChildUUIDLen:  promote.ChildUUIDLen,
		ChildUUID:     promote.ChildUUID,
		Role:          protocol.SuppPromoteRoleParent,
	}
	sMessage := protocol.NewDownMsg(conn, sess.Secret(), sess.UUID())
	protocol.SetMessageMeta(sMessage, sess.ProtocolVersion(), sess.ProtocolFlags())
	protocol.ConstructMessage(sMessage, header, ack, false)
	sMessage.SendMessage()
}

func (agent *Agent) promoteSupplementalAsChild(linkUUID, parentUUID string, conn net.Conn) {
	if agent == nil {
		return
	}
	link := DetachSupplementalLink(agent.mgr, linkUUID)
	if link != nil && link.Conn != nil {
		conn = link.Conn
	}
	if conn == nil {
		return
	}
	if parentUUID == "" && link != nil {
		parentUUID = link.PeerUUID
	}
	agent.queueFailoverConn(conn, parentUUID, linkUUID)

	// Trigger adoption promptly: if we stay on the old upstream session, the agent
	// may keep sending (and ACKing) through a parent that is currently blind/offline.
	// Closing the old upstream forces handleDataFromUpstream() into upstreamOffline(),
	// which will adopt the queued failover candidate.
	if sess := agent.currentSession(); sess != nil {
		if up := sess.Conn(); up != nil && up != conn {
			_ = up.Close()
		}
	}
}

func (agent *Agent) promoteSupplementalAsParent(linkUUID, childUUID string, conn net.Conn) {
	if agent == nil || agent.mgr == nil || agent.mgr.ChildrenManager == nil {
		return
	}
	link := DetachSupplementalLink(agent.mgr, linkUUID)
	if link != nil && link.Conn != nil {
		conn = link.Conn
		if childUUID == "" {
			childUUID = link.PeerUUID
		}
	}
	if conn == nil || childUUID == "" {
		return
	}
	agent.mgr.ChildrenManager.AddChild(childUUID, conn)
	agent.mgr.ChildrenManager.NotifyChild(&manager.ChildInfo{UUID: childUUID, Conn: conn})
}

func (agent *Agent) clearPendingFailover(linkUUID string) {
	if agent == nil {
		return
	}
	agent.failoverMu.Lock()
	delete(agent.pendingFailovers, linkUUID)
	if len(agent.pendingFailovers) == 0 {
		agent.failoverDeadline = time.Time{}
	}
	agent.failoverMu.Unlock()
}

func (agent *Agent) queueFailoverConn(conn net.Conn, parentUUID, linkUUID string) {
	if agent == nil || conn == nil || agent.failoverConnChan == nil {
		return
	}
	candidate := &failoverCandidate{conn: conn, parentUUID: parentUUID, linkUUID: linkUUID}
	select {
	case agent.failoverConnChan <- candidate:
	default:
		logger.Warnf("dropping failover candidate for %s: pending adoption", linkUUID)
	}
}

func (agent *Agent) tryAdoptFailoverParent(ctx context.Context) bool {
	if agent == nil || agent.failoverConnChan == nil {
		return false
	}
	if ctx == nil {
		ctx = agent.context()
	}

	// If a failover candidate is already queued, adopt it immediately.
	select {
	case candidate := <-agent.failoverConnChan:
		return agent.adoptFailoverCandidate(candidate)
	default:
	}

	// Do not block normal reconnection unless a failover plan is pending.
	agent.failoverMu.Lock()
	hasPending := len(agent.pendingFailovers) > 0
	agent.failoverMu.Unlock()
	if !hasPending {
		return false
	}

	initialDeadline := time.Now().Add(failoverInitialWait)
	ticker := time.NewTicker(failoverPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return false
		case candidate := <-agent.failoverConnChan:
			if agent.adoptFailoverCandidate(candidate) {
				return true
			}
		case <-ticker.C:
			if agent.shouldContinueWaiting(initialDeadline) {
				continue
			}
			return false
		}
	}
}

func (agent *Agent) adoptFailoverCandidate(candidate *failoverCandidate) bool {
	if agent == nil || candidate == nil || candidate.conn == nil {
		return false
	}
	agent.setActiveConnection(candidate.conn)
	if candidate.parentUUID != "" {
		agent.setParentUUID(candidate.parentUUID)
	}
	agent.resetFailoverState()
	return true
}

func (agent *Agent) shouldContinueWaiting(initialDeadline time.Time) bool {
	if time.Now().Before(initialDeadline) {
		return true
	}
	agent.failoverMu.Lock()
	defer agent.failoverMu.Unlock()
	if len(agent.pendingFailovers) == 0 {
		return false
	}
	if agent.failoverDeadline.IsZero() {
		return true
	}
	return time.Now().Before(agent.failoverDeadline)
}

func (agent *Agent) extendFailoverDeadline(duration time.Duration) {
	if agent == nil || duration <= 0 {
		return
	}
	agent.failoverMu.Lock()
	defer agent.failoverMu.Unlock()
	deadline := time.Now().Add(duration)
	if deadline.After(agent.failoverDeadline) {
		agent.failoverDeadline = deadline
	}
}

func (agent *Agent) resetFailoverState() {
	agent.failoverMu.Lock()
	agent.pendingFailovers = make(map[string]*protocol.SuppFailoverCommand)
	agent.failoverDeadline = time.Time{}
	agent.failoverMu.Unlock()
}

func (agent *Agent) handleSuppFailoverCommand(cmd *protocol.SuppFailoverCommand) {
	if agent == nil || cmd == nil || cmd.LinkUUID == "" {
		return
	}
	agent.failoverMu.Lock()
	agent.pendingFailovers[cmd.LinkUUID] = cmd
	agent.failoverMu.Unlock()
	agent.extendFailoverDeadline(failoverCommandWait)
	if cmd.Flags&protocol.SuppFailoverFlagInitiator != 0 && cmd.Role == protocol.SuppFailoverRoleParent {
		agent.triggerSuppPromotion(cmd)
	}
}

func (agent *Agent) triggerSuppPromotion(cmd *protocol.SuppFailoverCommand) {
	if agent == nil || cmd == nil || agent.mgr == nil || agent.mgr.SupplementalManager == nil {
		return
	}
	peer := cmd.ChildUUID
	if peer == "" {
		return
	}
	conn, ok := agent.mgr.SupplementalManager.GetConn(peer)
	if !ok || conn == nil {
		return
	}
	agent.sendSuppPromotionMessage(conn, cmd, protocol.SuppPromoteRoleChild)
}

func (agent *Agent) sendSuppPromotionMessage(conn net.Conn, cmd *protocol.SuppFailoverCommand, role uint16) {
	if conn == nil || cmd == nil {
		return
	}
	sess := agent.currentSession()
	if sess == nil {
		return
	}
	target := cmd.ChildUUID
	if role == protocol.SuppPromoteRoleParent {
		target = cmd.ParentUUID
	}
	header := &protocol.Header{
		Sender:      agent.UUID,
		Accepter:    target,
		MessageType: uint16(protocol.SUPPLINKPROMOTE),
		RouteLen:    0,
		Route:       "",
	}
	promote := &protocol.SuppLinkPromote{
		LinkUUIDLen:   cmd.LinkUUIDLen,
		LinkUUID:      cmd.LinkUUID,
		ParentUUIDLen: cmd.ParentUUIDLen,
		ParentUUID:    cmd.ParentUUID,
		ChildUUIDLen:  cmd.ChildUUIDLen,
		ChildUUID:     cmd.ChildUUID,
		Role:          role,
	}
	sMessage := protocol.NewDownMsg(conn, sess.Secret(), sess.UUID())
	protocol.ConstructMessage(sMessage, header, promote, false)
	sMessage.SendMessage()
}

func changeRoute(header *protocol.Header) (string, bool) {
	route := header.Route
	if route == "" {
		header.Route = ""
		header.RouteLen = 0
		return "", false
	}
	routes := strings.Split(route, ":")
	if len(routes) == 0 {
		header.Route = ""
		header.RouteLen = 0
		return "", false
	}
	nextUUID, isSupp := parseRouteSegment(routes[0])
	if len(routes) == 1 {
		header.Route = ""
		header.RouteLen = 0
		return nextUUID, isSupp
	}
	remaining := strings.Join(routes[1:], ":")
	header.Route = remaining
	header.RouteLen = uint32(len(remaining))
	return nextUUID, isSupp
}

// nextHopFromRoute pops Route segments until it finds a next hop that is not
// the current node, or until the route becomes empty. This prevents accidental
// self-truncation of multi-hop routes like "SELF:child:...".
func nextHopFromRoute(self string, header *protocol.Header) (string, bool) {
	nextUUID, preferSupp := changeRoute(header)
	for nextUUID == self && header.RouteLen != 0 {
		nextUUID, preferSupp = changeRoute(header)
	}
	return nextUUID, preferSupp
}

func parseRouteSegment(segment string) (string, bool) {
	const suffix = "#supp"
	if strings.HasSuffix(segment, suffix) {
		return strings.TrimSuffix(segment, suffix), true
	}
	return segment, false
}
