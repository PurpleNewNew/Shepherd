package topology

import (
	"context"
	"strings"
	"time"
)

func (topology *Topology) setEdgeType(from, to string, edgeType EdgeType) {
	if _, exists := topology.edgeTypes[from]; !exists {
		topology.edgeTypes[from] = make(map[string]EdgeType)
	}
	topology.edgeTypes[from][to] = edgeType
}

func (topology *Topology) getEdgeType(from, to string) EdgeType {
	if neighbors, exists := topology.edgeTypes[from]; exists {
		if tp, ok := neighbors[to]; ok {
			return tp
		}
	}
	return TreeEdge
}

func (topology *Topology) isSupplementalEdge(from, to string) bool {
	return topology.getEdgeType(from, to) == SupplementalEdge
}

func (topology *Topology) IsSupplementalEdge(from, to string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), topoRequestTimeout)
	defer cancel()
	task := &TopoTask{
		Mode:         CHECKSUPPEDGE,
		UUID:         from,
		NeighborUUID: to,
	}
	result, err := topology.Request(ctx, task)
	if err != nil || result == nil {
		return false
	}
	return result.IsSupplemental
}

func (topology *Topology) ensureEdgeWeight(from, to string, weight uint32) {
	if _, exists := topology.edgeWeights[from]; !exists {
		topology.edgeWeights[from] = make(map[string]uint32)
	}
	if _, ok := topology.edgeWeights[from][to]; !ok {
		topology.edgeWeights[from][to] = weight
	}
}

func (topology *Topology) addEdge(task *TopoTask) {
	// 添加从task.UUID到task.NeighborUUID的边
	if neighbors, exists := topology.edges[task.UUID]; exists {
		// 检查边是否已存在
		found := false
		for _, neighbor := range neighbors {
			if neighbor == task.NeighborUUID {
				found = true
				break
			}
		}
		if !found {
			topology.edges[task.UUID] = append(topology.edges[task.UUID], task.NeighborUUID)
		}
	} else {
		topology.edges[task.UUID] = []string{task.NeighborUUID}
	}

	// 添加从task.NeighborUUID到task.UUID的反向边（使其双向）
	if neighbors, exists := topology.edges[task.NeighborUUID]; exists {
		// 检查反向边是否已存在
		found := false
		for _, neighbor := range neighbors {
			if neighbor == task.UUID {
				found = true
				break
			}
		}
		if !found {
			topology.edges[task.NeighborUUID] = append(topology.edges[task.NeighborUUID], task.UUID)
		}
	} else {
		topology.edges[task.NeighborUUID] = []string{task.UUID}
	}

	edgeType := task.EdgeType
	if edgeType != SupplementalEdge {
		edgeType = TreeEdge
	}

	topology.setEdgeType(task.UUID, task.NeighborUUID, edgeType)
	topology.setEdgeType(task.NeighborUUID, task.UUID, edgeType)

	defaultWeight := uint32(1)
	if edgeType == SupplementalEdge {
		defaultWeight = 100
	}
	topology.ensureEdgeWeight(task.UUID, task.NeighborUUID, defaultWeight)
	topology.ensureEdgeWeight(task.NeighborUUID, task.UUID, defaultWeight)
	if weights, ok := topology.edgeWeights[task.UUID]; ok {
		if weight, exists := weights[task.NeighborUUID]; exists {
			topology.persistEdge(task.UUID, task.NeighborUUID, edgeType, weight)
			topology.persistEdge(task.NeighborUUID, task.UUID, edgeType, weight)
		}
	}

	// 刷新最后更新时间
	topology.lastUpdateTime = time.Now()

	topology.ResultChan <- &topoResult{}
}

func (topology *Topology) setEdgeWeight(task *TopoTask) {
	if task.Weight == 0 {
		task.Weight = 1
	}

	applyWeight := func(from, to string, weight uint32, force bool) bool {
		if _, exists := topology.edgeWeights[from]; !exists {
			topology.edgeWeights[from] = make(map[string]uint32)
		}
		if !force {
			if existing, ok := topology.edgeWeights[from][to]; ok && existing == weight {
				return false
			}
		}
		topology.edgeWeights[from][to] = weight
		return true
	}

	changed := applyWeight(task.UUID, task.NeighborUUID, task.Weight, task.Force)
	if applyWeight(task.NeighborUUID, task.UUID, task.Weight, task.Force) {
		changed = true
	}

	if changed {
		topology.lastUpdateTime = time.Now()
		if weights, ok := topology.edgeWeights[task.UUID]; ok {
			if weight, exists := weights[task.NeighborUUID]; exists {
				edgeType := topology.getEdgeType(task.UUID, task.NeighborUUID)
				topology.persistEdge(task.UUID, task.NeighborUUID, edgeType, weight)
			}
		}
		if weights, ok := topology.edgeWeights[task.NeighborUUID]; ok {
			if weight, exists := weights[task.UUID]; exists {
				edgeType := topology.getEdgeType(task.NeighborUUID, task.UUID)
				topology.persistEdge(task.NeighborUUID, task.UUID, edgeType, weight)
			}
		}
	}

	topology.ResultChan <- &topoResult{}
}

func (topology *Topology) removeEdge(task *TopoTask) {
	// 从task.UUID移除到task.NeighborUUID的边
	if neighbors, exists := topology.edges[task.UUID]; exists {
		for i, neighbor := range neighbors {
			if neighbor == task.NeighborUUID {
				topology.edges[task.UUID] = append(neighbors[:i], neighbors[i+1:]...)
				break
			}
		}
	}

	// 从task.NeighborUUID移除到task.UUID的反向边
	if neighbors, exists := topology.edges[task.NeighborUUID]; exists {
		for i, neighbor := range neighbors {
			if neighbor == task.UUID {
				topology.edges[task.NeighborUUID] = append(neighbors[:i], neighbors[i+1:]...)
				break
			}
		}
	}

	// 移除边权重
	if weights, exists := topology.edgeWeights[task.UUID]; exists {
		delete(weights, task.NeighborUUID)
		if len(weights) == 0 {
			delete(topology.edgeWeights, task.UUID)
		}
	}
	if weights, exists := topology.edgeWeights[task.NeighborUUID]; exists {
		delete(weights, task.UUID)
		if len(weights) == 0 {
			delete(topology.edgeWeights, task.NeighborUUID)
		}
	}

	if types, exists := topology.edgeTypes[task.UUID]; exists {
		delete(types, task.NeighborUUID)
		if len(types) == 0 {
			delete(topology.edgeTypes, task.UUID)
		}
	}
	if types, exists := topology.edgeTypes[task.NeighborUUID]; exists {
		delete(types, task.UUID)
		if len(types) == 0 {
			delete(topology.edgeTypes, task.NeighborUUID)
		}
	}

	topology.deleteEdge(task.UUID, task.NeighborUUID)
	topology.deleteEdge(task.NeighborUUID, task.UUID)

	// 刷新最后更新时间
	topology.lastUpdateTime = time.Now()

	topology.ResultChan <- &topoResult{}
}

func (topology *Topology) getNeighbors(task *TopoTask) {
	if task == nil || task.UUID == "" {
		topology.ResultChan <- &topoResult{Neighbors: []string{}}
		return
	}
	network := strings.TrimSpace(task.NetworkID)
	if network != "" && !topology.matchesNetwork(task.UUID, network) {
		topology.ResultChan <- &topoResult{Neighbors: []string{}}
		return
	}
	neighbors, exists := topology.edges[task.UUID]
	if !exists {
		topology.ResultChan <- &topoResult{Neighbors: []string{}}
		return
	}
	var filtered []string
	for _, neighbor := range neighbors {
		if network != "" && !topology.matchesNetwork(neighbor, network) {
			continue
		}
		filtered = append(filtered, neighbor)
	}
	if filtered == nil {
		filtered = []string{}
	}
	topology.ResultChan <- &topoResult{Neighbors: filtered}
}

func (topology *Topology) checkSupplementalEdge(task *TopoTask) {
	isSupp := topology.isSupplementalEdge(task.UUID, task.NeighborUUID)
	topology.ResultChan <- &topoResult{IsSupplemental: isSupp}
}
