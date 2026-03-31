package topology

import (
	"sort"
	"strings"
	"time"

	"codeberg.org/agnoie/shepherd/protocol"
)

// UISnapshot 为 UI 提供节点与边的只读视图。
type UISnapshot struct {
	Nodes       []UINodeSnapshot
	Edges       []UIEdgeSnapshot
	LastUpdated time.Time
}

// UINodeSnapshot 描述一个适合渲染展示的节点快照。
type UINodeSnapshot struct {
	UUID        string
	ParentUUID  string
	Anchor      string
	Alias       string
	Network     string
	Memo        string
	IsAlive     bool
	Depth       int
	Hostname    string
	Username    string
	LastSeen    time.Time
	SleepSecond int
	WorkSecond  int
	NextWake    time.Time
}

// UIEdgeSnapshot 描述两个节点之间的一条关系边。
type UIEdgeSnapshot struct {
	ParentUUID   string
	ChildUUID    string
	Supplemental bool
}

// UISnapshot 会在提供 entry/network 条件时返回经过过滤的拓扑视图。
func (topology *Topology) UISnapshot(filterEntry, network string) UISnapshot {
	snapshot := UISnapshot{}
	if topology == nil {
		return snapshot
	}
	topology.mu.RLock()
	defer topology.mu.RUnlock()
	snapshot.LastUpdated = topology.lastUpdateTime
	filterEntry = strings.TrimSpace(filterEntry)
	network = strings.TrimSpace(network)

	for _, node := range topology.nodes {
		if node == nil || node.uuid == "" || node.uuid == protocol.ADMIN_UUID {
			continue
		}
		if filterEntry != "" && !topology.matchesEntry(node.uuid, filterEntry) && node.uuid != filterEntry {
			continue
		}
		if network != "" && !topology.matchesNetwork(node.uuid, network) {
			continue
		}
		anchor := topology.networkAnchor(node.uuid)
		alias := node.currentHostname
		if alias == "" {
			alias = anchor
		}
		if alias == "" && len(node.uuid) >= 8 {
			alias = node.uuid[:8]
		}
		if alias == "" {
			alias = node.uuid
		}
		snapshot.Nodes = append(snapshot.Nodes, UINodeSnapshot{
			UUID:        node.uuid,
			ParentUUID:  topology.parentOfUnlocked(node.uuid),
			Anchor:      anchor,
			Alias:       alias,
			Network:     topology.networkForUnlocked(node.uuid),
			Memo:        node.memo,
			IsAlive:     node.isAlive,
			Depth:       topology.depthOf(node.uuid),
			Hostname:    node.currentHostname,
			Username:    node.currentUser,
			LastSeen:    node.lastSeen,
			SleepSecond: node.sleepSeconds,
			WorkSecond:  node.workSeconds,
			NextWake:    node.nextWake,
		})
	}

	sort.Slice(snapshot.Nodes, func(i, j int) bool {
		return snapshot.Nodes[i].UUID < snapshot.Nodes[j].UUID
	})

	for _, node := range snapshot.Nodes {
		if node.ParentUUID == "" {
			continue
		}
		snapshot.Edges = append(snapshot.Edges, UIEdgeSnapshot{
			ParentUUID:   node.ParentUUID,
			ChildUUID:    node.UUID,
			Supplemental: false,
		})
	}

	seen := make(map[string]struct{})
	for from, neighbors := range topology.edges {
		for _, to := range neighbors {
			if from == "" || to == "" || from == to {
				continue
			}
			if !topology.isSupplementalEdge(from, to) {
				continue
			}
			key := normalizedEdgeKey(from, to)
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			if filterEntry != "" {
				if !topology.matchesEntry(from, filterEntry) && !topology.matchesEntry(to, filterEntry) {
					continue
				}
			}
			if network != "" {
				if !topology.matchesNetwork(from, network) && !topology.matchesNetwork(to, network) {
					continue
				}
			}
			snapshot.Edges = append(snapshot.Edges, UIEdgeSnapshot{
				ParentUUID:   from,
				ChildUUID:    to,
				Supplemental: true,
			})
		}
	}

	return snapshot
}

func normalizedEdgeKey(a, b string) string {
	if a <= b {
		return a + "|" + b
	}
	return b + "|" + a
}
