package topology

import (
	"sort"
	"strings"

	"codeberg.org/agnoie/shepherd/protocol"
)

func (topology *Topology) getSuppCandidates(task *TopoTask) {
	result := &topoResult{}
	if topology == nil || task == nil || task.UUID == "" {
		topology.ResultChan <- result
		return
	}
	network := strings.TrimSpace(task.NetworkID)
	if network != "" && !topology.matchesNetwork(task.UUID, network) {
		topology.ResultChan <- result
		return
	}
	limit := task.Count
	if limit <= 0 {
		limit = 4
	}
	targetInfo := topology.routeInfoUnlocked(task.UUID)
	if targetInfo == nil || len(targetInfo.Path) == 0 {
		topology.ResultChan <- result
		return
	}
	targetNetwork := topology.networkForUnlocked(task.UUID)
	neighborSet := topology.directNeighbors(task.UUID)
	candidates := make([]*SuppCandidate, 0)
	for _, node := range topology.nodes {
		if node == nil || node.uuid == "" {
			continue
		}
		if node.uuid == task.UUID || node.uuid == protocol.ADMIN_UUID {
			continue
		}
		if network != "" && !topology.matchesNetwork(node.uuid, network) {
			continue
		}
		if _, ok := neighborSet[node.uuid]; ok {
			continue
		}
		if topology.hasEdgeUnsafe(task.UUID, node.uuid) {
			continue
		}
		if targetNetwork != "" {
			peerNetwork := topology.networkForUnlocked(node.uuid)
			if peerNetwork != "" && !strings.EqualFold(peerNetwork, targetNetwork) {
				continue
			}
		}
		peerInfo := topology.routeInfoUnlocked(node.uuid)
		if peerInfo == nil || len(peerInfo.Path) == 0 {
			continue
		}
		overlap := countPathOverlap(targetInfo.Path, peerInfo.Path)
		redundancy := 0.0
		if length := len(peerInfo.Path); length > 0 {
			redundancy = 1 - float64(overlap)/float64(length)
			if redundancy < 0 {
				redundancy = 0
			}
		}
		sleepBudget := topology.pathSleepBudget(node.uuid)
		candidate := &SuppCandidate{
			UUID:        node.uuid,
			Path:        clonePath(peerInfo.Path),
			Display:     peerInfo.Display,
			Depth:       peerInfo.Depth,
			Overlap:     overlap,
			SleepBudget: sleepBudget,
			Redundancy:  redundancy,
			WorkSeconds: node.workSeconds,
		}
		candidates = append(candidates, candidate)
	}
	if len(candidates) > 1 {
		sort.Slice(candidates, func(i, j int) bool {
			if candidates[i].SleepBudget != candidates[j].SleepBudget {
				return candidates[i].SleepBudget < candidates[j].SleepBudget
			}
			if candidates[i].Overlap != candidates[j].Overlap {
				return candidates[i].Overlap < candidates[j].Overlap
			}
			if candidates[i].Depth != candidates[j].Depth {
				return candidates[i].Depth < candidates[j].Depth
			}
			return candidates[i].UUID < candidates[j].UUID
		})
	}
	if len(candidates) > limit {
		candidates = candidates[:limit]
	}
	result.Candidates = candidates
	topology.ResultChan <- result
}

func countPathOverlap(a, b []string) int {
	if len(a) == 0 || len(b) == 0 {
		return 0
	}
	set := make(map[string]struct{}, len(a))
	for _, node := range a {
		set[node] = struct{}{}
	}
	count := 0
	for _, node := range b {
		if _, ok := set[node]; ok {
			count++
		}
	}
	return count
}

func (topology *Topology) directNeighbors(uuid string) map[string]struct{} {
	neighbors := make(map[string]struct{})
	if topology == nil || uuid == "" {
		return neighbors
	}
	if list, ok := topology.edges[uuid]; ok {
		for _, neighbor := range list {
			neighbors[neighbor] = struct{}{}
		}
	}
	return neighbors
}

func (topology *Topology) hasEdgeUnsafe(a, b string) bool {
	if topology == nil || a == "" || b == "" {
		return false
	}
	if a == b {
		return true
	}
	if neighbors, ok := topology.edges[a]; ok {
		for _, neighbor := range neighbors {
			if neighbor == b {
				return true
			}
		}
	}
	return false
}
