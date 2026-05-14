package topology

import (
	"strings"

	"codeberg.org/agnoie/shepherd/protocol"
)

// NodeStatus describes the controller's view of a node lifecycle.
type NodeStatus string

const (
	NodeStatusOnline      NodeStatus = "online"
	NodeStatusOffline     NodeStatus = "offline"
	NodeStatusUnreachable NodeStatus = "unreachable"
	NodeStatusQuarantined NodeStatus = "quarantined"
	NodeStatusRetired     NodeStatus = "retired"
)

const (
	nodeStatusPersistUnknown = iota
	nodeStatusPersistOnline
	nodeStatusPersistOffline
	nodeStatusPersistUnreachable
	nodeStatusPersistQuarantined
	nodeStatusPersistRetired
)

func normalizeNodeStatus(status NodeStatus, alive bool) NodeStatus {
	switch NodeStatus(strings.ToLower(strings.TrimSpace(string(status)))) {
	case NodeStatusOnline:
		return NodeStatusOnline
	case NodeStatusOffline:
		return NodeStatusOffline
	case NodeStatusUnreachable:
		return NodeStatusUnreachable
	case NodeStatusQuarantined:
		return NodeStatusQuarantined
	case NodeStatusRetired:
		return NodeStatusRetired
	default:
		if alive {
			return NodeStatusOnline
		}
		return NodeStatusOffline
	}
}

func statusFromPersisted(code int, alive bool) NodeStatus {
	switch code {
	case nodeStatusPersistOnline:
		return NodeStatusOnline
	case nodeStatusPersistOffline:
		return NodeStatusOffline
	case nodeStatusPersistUnreachable:
		return NodeStatusUnreachable
	case nodeStatusPersistQuarantined:
		return NodeStatusQuarantined
	case nodeStatusPersistRetired:
		return NodeStatusRetired
	default:
		return normalizeNodeStatus("", alive)
	}
}

func statusToPersisted(status NodeStatus, alive bool) int {
	switch normalizeNodeStatus(status, alive) {
	case NodeStatusOnline:
		return nodeStatusPersistOnline
	case NodeStatusOffline:
		return nodeStatusPersistOffline
	case NodeStatusUnreachable:
		return nodeStatusPersistUnreachable
	case NodeStatusQuarantined:
		return nodeStatusPersistQuarantined
	case NodeStatusRetired:
		return nodeStatusPersistRetired
	default:
		return nodeStatusPersistUnknown
	}
}

func statusAlive(status NodeStatus) bool {
	return normalizeNodeStatus(status, false) == NodeStatusOnline
}

func statusRouteExcluded(status NodeStatus) bool {
	switch normalizeNodeStatus(status, false) {
	case NodeStatusQuarantined, NodeStatusRetired:
		return true
	default:
		return false
	}
}

func statusControlAllowed(status NodeStatus) bool {
	return normalizeNodeStatus(status, false) == NodeStatusOnline
}

func (n *node) lifecycleStatus() NodeStatus {
	if n == nil {
		return NodeStatusOffline
	}
	return normalizeNodeStatus(n.status, n.isAlive)
}

func (n *node) setLifecycleStatus(status NodeStatus) bool {
	if n == nil {
		return false
	}
	status = normalizeNodeStatus(status, n.isAlive)
	changed := n.lifecycleStatus() != status || n.isAlive != statusAlive(status)
	n.status = status
	n.isAlive = statusAlive(status)
	return changed
}

func (topology *Topology) nodeStatusUnlocked(uuid string) (NodeStatus, bool) {
	uuid = strings.TrimSpace(uuid)
	if topology == nil || uuid == "" {
		return NodeStatusOffline, false
	}
	if uuid == protocol.ADMIN_UUID {
		return NodeStatusOnline, true
	}
	n := topology.nodeByUUID(uuid)
	if n == nil || n.uuid == "" {
		return NodeStatusOffline, false
	}
	return n.lifecycleStatus(), true
}

// NodeStatus returns the current lifecycle status for a node.
func (topology *Topology) NodeStatus(uuid string) (NodeStatus, bool) {
	if topology == nil || strings.TrimSpace(uuid) == "" {
		return NodeStatusOffline, false
	}
	topology.mu.RLock()
	defer topology.mu.RUnlock()
	return topology.nodeStatusUnlocked(strings.TrimSpace(uuid))
}

// PathStatuses returns lifecycle statuses for each node in a route display string.
func (topology *Topology) PathStatuses(route string) map[string]NodeStatus {
	out := make(map[string]NodeStatus)
	if topology == nil || strings.TrimSpace(route) == "" {
		return out
	}
	topology.mu.RLock()
	defer topology.mu.RUnlock()
	for _, part := range strings.Split(route, ":") {
		hop := strings.TrimSpace(strings.TrimSuffix(part, "#supp"))
		if hop == "" {
			continue
		}
		if status, ok := topology.nodeStatusUnlocked(hop); ok {
			out[hop] = status
		}
	}
	return out
}

// ControlAllowed reports whether a target can accept interactive control traffic.
func (topology *Topology) ControlAllowed(uuid string) bool {
	status, ok := topology.NodeStatus(uuid)
	return ok && statusControlAllowed(status)
}
