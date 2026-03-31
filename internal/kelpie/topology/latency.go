package topology

import (
	"container/heap"
	"math"
	"time"

	"codeberg.org/agnoie/shepherd/protocol"
)

const (
	defaultLatencyWeight           uint32 = 1000
	latencySampleExpiry                   = 5 * time.Minute
	latencySmoothingPreviousWeight        = 3
)

func clampLatencyMs(ms int64) uint32 {
	if ms <= 0 {
		return 0
	}
	if ms > int64(math.MaxUint32) {
		return math.MaxUint32
	}
	return uint32(ms)
}

func (topology *Topology) nodeLatencyValue(uuid string) uint32 {
	if uuid == protocol.ADMIN_UUID {
		return 1
	}
	if latency, ok := topology.nodeLatency[uuid]; ok && latency > 0 {
		return latency
	}
	return 0
}

func (topology *Topology) estimateEdgeLatency(a, b string) uint32 {
	latA := topology.nodeLatencyValue(a)
	latB := topology.nodeLatencyValue(b)

	switch {
	case latA == 0 && latB == 0:
		return 0
	case latA == 0:
		return latB
	case latB == 0:
		return latA
	default:
		value := (uint64(latA) + uint64(latB)) / 2
		if value == 0 {
			value = 1
		}
		if value > uint64(math.MaxUint32) {
			return math.MaxUint32
		}
		return uint32(value)
	}
}

func (topology *Topology) setLatencySample(from, to string, value uint32, ts time.Time) {
	if from == "" || to == "" || value == 0 {
		return
	}
	if topology.edgeLatencies == nil {
		topology.edgeLatencies = make(map[string]map[string]latencySample)
	}
	if _, ok := topology.edgeLatencies[from]; !ok {
		topology.edgeLatencies[from] = make(map[string]latencySample)
	}
	prev := topology.edgeLatencies[from][to]
	if prev.value > 0 {
		value = uint32((uint64(prev.value)*latencySmoothingPreviousWeight + uint64(value)) / (latencySmoothingPreviousWeight + 1))
		if value == 0 {
			value = 1
		}
	}
	topology.edgeLatencies[from][to] = latencySample{
		value:   value,
		updated: ts,
	}
}

func (topology *Topology) refreshEdgeLatency(uuid string) {
	if uuid == "" {
		return
	}
	now := time.Now()
	neighbors := append([]string(nil), topology.edges[uuid]...)
	parent := topology.getParent(uuid)
	if parent != "" {
		shouldAppend := true
		for _, existing := range neighbors {
			if existing == parent {
				shouldAppend = false
				break
			}
		}
		if shouldAppend {
			neighbors = append(neighbors, parent)
		}
	}
	for _, neighbor := range neighbors {
		value := topology.estimateEdgeLatency(uuid, neighbor)
		if value == 0 {
			continue
		}
		topology.setLatencySample(uuid, neighbor, value, now)
		topology.setLatencySample(neighbor, uuid, value, now)
	}
}

func (topology *Topology) applyNodeLatency(uuid string, latencyMs int64) bool {
	if uuid == "" {
		return false
	}
	value := clampLatencyMs(latencyMs)
	if value == 0 {
		return false
	}
	prev := topology.nodeLatency[uuid]
	if prev == value {
		return false
	}
	topology.nodeLatency[uuid] = value
	topology.refreshEdgeLatency(uuid)
	topology.lastUpdateTime = time.Now()
	return true
}

func (topology *Topology) baseEdgeLatencyMs(from, to string) uint32 {
	// 边样本
	if topology.edgeLatencies != nil {
		if m, ok := topology.edgeLatencies[from]; ok {
			if s, ok := m[to]; ok {
				if s.value > 0 && time.Since(s.updated) <= latencySampleExpiry {
					return s.value
				}
			}
		}
	}
	// 节点估计
	if v := topology.estimateEdgeLatency(from, to); v > 0 {
		return v
	}
	// 回退默认
	return defaultLatencyWeight
}

// expectedWaitAtNodeMs 基于到达时刻 arrival 估计在节点 uuid 处由于 sleep/next_wake 带来的等待（毫秒）。
func (topology *Topology) expectedWaitAtNodeMs(uuid string, arrival time.Time) uint32 {
	if uuid == "" || uuid == protocol.ADMIN_UUID {
		return 0
	}
	id := topology.id2IDNum(uuid)
	if id < 0 {
		return 0
	}
	n := topology.nodes[id]
	if n == nil || n.sleepSeconds <= 0 {
		return 0
	}
	// 有明确 nextWake：仅在未来时需要等待
	if !n.nextWake.IsZero() {
		if n.nextWake.After(arrival) {
			d := n.nextWake.Sub(arrival)
			ms := d.Milliseconds()
			if ms <= 0 {
				return 0
			}
			if ms > int64(math.MaxUint32) {
				return math.MaxUint32
			}
			return uint32(ms)
		}
		return 0
	}
	// 无 nextWake：使用 duty-cycle 模型估计平均等待
	waitMs := dutyCycleWaitMilliseconds(n.sleepSeconds, n.workSeconds)
	if waitMs <= 0 {
		return 0
	}
	if waitMs > int64(math.MaxUint32) {
		return math.MaxUint32
	}
	return uint32(waitMs)
}

// expectedWaitAtNodeMsWithMode 与 expectedWaitAtNodeMs 一样返回等待时间，
// 但会额外返回一个布尔值，用来表示该估计是基于显式的 nextWake 时间戳
// （true），还是基于 duty-cycle 平均值（false）。
//
// RecommendSendDelay 会用到这个信息：如果没有显式 nextWake，
// 持续对一个恒定的平均等待值做“对齐”会发散，并把发送时间越推越迟。
func (topology *Topology) expectedWaitAtNodeMsWithMode(uuid string, arrival time.Time) (uint32, bool) {
	if uuid == "" || uuid == protocol.ADMIN_UUID {
		return 0, false
	}
	id := topology.id2IDNum(uuid)
	if id < 0 {
		return 0, false
	}
	n := topology.nodes[id]
	if n == nil || n.sleepSeconds <= 0 {
		return 0, false
	}
	// 有明确 nextWake：仅在未来时需要等待
	if !n.nextWake.IsZero() {
		if n.nextWake.After(arrival) {
			d := n.nextWake.Sub(arrival)
			ms := d.Milliseconds()
			if ms <= 0 {
				return 0, true
			}
			if ms > int64(math.MaxUint32) {
				return math.MaxUint32, true
			}
			return uint32(ms), true
		}
		return 0, true
	}
	// 无 nextWake：使用 duty-cycle 模型估计平均等待
	waitMs := dutyCycleWaitMilliseconds(n.sleepSeconds, n.workSeconds)
	if waitMs <= 0 {
		return 0, false
	}
	if waitMs > int64(math.MaxUint32) {
		return math.MaxUint32, false
	}
	return uint32(waitMs), false
}

// ExpectedWaitMsAt 导出包装，用于在给定到达时刻估计节点处的等待时间（毫秒）。
func (topology *Topology) ExpectedWaitMsAt(uuid string, arrival time.Time) uint32 {
	if topology == nil || uuid == "" {
		return 0
	}
	topology.mu.RLock()
	defer topology.mu.RUnlock()
	return topology.expectedWaitAtNodeMs(uuid, arrival)
}

// RecommendSendDelay 估算“从 base 时刻起最早无（或尽量少）中途等待”的发送延迟（毫秒）。
// 通过沿既定路径迭代对齐各 hop 的唤醒窗口（基于 next_wake/sleep），
// 在每次发现某一 hop 到达早于其 next_wake 时，整体推迟出发时刻以对齐，重复至收敛或达到迭代上限。
// 注意：这是启发式近似，优先对齐首跳与少量后续 hop，不能保证全局最优。
func (topology *Topology) RecommendSendDelay(target string, base time.Time) time.Duration {
	if topology == nil || target == "" {
		return 0
	}
	topology.mu.RLock()
	defer topology.mu.RUnlock()
	info := topology.routeInfoUnlocked(target)
	if info == nil || len(info.Path) < 2 {
		return 0
	}
	path := info.Path
	// 迭代对齐，避免无限循环
	const maxIter = 4
	delayMs := int64(0)
	for iter := 0; iter < maxIter; iter++ {
		adjusted := false
		arrival := base.Add(time.Duration(delayMs) * time.Millisecond)
		for i := 1; i < len(path); i++ {
			prev := path[i-1]
			next := path[i]
			// Sleep 会影响下一跳接收消息的能力（agent 在睡眠时会关闭自己的上游会话）。
			// 为避免反复触发“no route”的 carry-forward 抖动，
			// 应把投递时间对齐到“下一跳”节点的唤醒窗口。
			lat := int64(topology.baseEdgeLatencyMs(prev, next))
			if lat <= 0 {
				lat = int64(defaultLatencyWeight)
			}
			arrival = arrival.Add(time.Duration(lat) * time.Millisecond)
			waitMs, exact := topology.expectedWaitAtNodeMsWithMode(next, arrival)
			if wait := int64(waitMs); wait > 0 {
				delayMs += wait
				arrival = arrival.Add(time.Duration(wait) * time.Millisecond)
				// 只有显式的 next_wake 才能通过平移发送时间来“对齐”。
				// duty-cycle 平均值是一个常量，继续迭代只会导致发散。
				if exact {
					adjusted = true
				}
			}
		}
		if !adjusted {
			break
		}
	}
	if delayMs < 0 {
		delayMs = 0
	}
	return time.Duration(delayMs) * time.Millisecond
}

type arriveItem struct {
	uuid  string
	tms   int64 // 自基准时刻起的毫秒到达时间
	index int
}
type arrivePQ []*arriveItem

func (pq arrivePQ) Len() int            { return len(pq) }
func (pq arrivePQ) Less(i, j int) bool  { return pq[i].tms < pq[j].tms }
func (pq arrivePQ) Swap(i, j int)       { pq[i], pq[j] = pq[j], pq[i]; pq[i].index = i; pq[j].index = j }
func (pq *arrivePQ) Push(x interface{}) { *pq = append(*pq, x.(*arriveItem)) }
func (pq *arrivePQ) Pop() interface{} {
	old := *pq
	n := len(old)
	it := old[n-1]
	old[n-1] = nil
	it.index = -1
	*pq = old[:n-1]
	return it
}

// earliestArrivalFrom 基于时间依赖权重计算从 source 出发到所有节点的最早到达时间与前驱。
// 返回：prev 映射（用于还原路径）、dist 映射（毫秒）。
func (topology *Topology) earliestArrivalFrom(base time.Time, source string) (map[string]string, map[string]int64) {
	prev := make(map[string]string)
	dist := make(map[string]int64)
	// 初始化：所有已知节点设置为 +Inf
	for _, node := range topology.nodes {
		if node == nil || node.uuid == "" {
			continue
		}
		dist[node.uuid] = math.MaxInt64
	}
	if _, ok := dist[source]; !ok {
		dist[source] = 0
	} else {
		dist[source] = 0
	}
	pq := make(arrivePQ, 0, len(dist))
	heap.Push(&pq, &arriveItem{uuid: source, tms: 0})

	for pq.Len() > 0 {
		it := heap.Pop(&pq).(*arriveItem)
		u := it.uuid
		t := it.tms
		if t > dist[u] {
			continue
		}
		// 松弛所有邻居
		neighbors := topology.edges[u]
		if len(neighbors) == 0 {
			// 若当前只恢复了父关系信息，则将父节点也纳入邻居候选。
			if p := topology.getParent(u); p != "" {
				neighbors = append(neighbors, p)
			}
		}
		for _, v := range neighbors {
			if v == "" || v == u {
				continue
			}
			wait := int64(topology.expectedWaitAtNodeMs(u, base.Add(time.Duration(t)*time.Millisecond)))
			lat := int64(topology.baseEdgeLatencyMs(u, v))
			if lat <= 0 {
				lat = int64(defaultLatencyWeight)
			}
			nt := t + wait + lat
			if cur, ok := dist[v]; !ok || nt < cur {
				dist[v] = nt
				prev[v] = u
				heap.Push(&pq, &arriveItem{uuid: v, tms: nt})
			}
		}
	}
	return prev, dist
}

func dutyCycleWaitMilliseconds(sleepSeconds, workSeconds int) int64 {
	waitSeconds := dutyCycleWaitSeconds(sleepSeconds, workSeconds)
	if waitSeconds <= 0 {
		return 0
	}
	ms := waitSeconds * 1000
	if ms <= 0 {
		return 0
	}
	return int64(ms)
}

func dutyCycleWaitSeconds(sleepSeconds, workSeconds int) float64 {
	if sleepSeconds <= 0 {
		return 0
	}
	work := workSeconds
	if work <= 0 {
		work = defaultWorkSeconds
	}
	cycle := float64(sleepSeconds + work)
	if cycle <= 0 {
		return 0
	}
	sleep := float64(sleepSeconds)
	return (sleep * sleep) / (2 * cycle)
}
