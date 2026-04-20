package topology

import (
	"sort"
	"strings"

	"codeberg.org/agnoie/shepherd/protocol"
)

func (topology *Topology) ensureRelationStorageLocked() {
	if topology.parentByChild == nil {
		topology.parentByChild = make(map[string]string)
	}
	if topology.childrenByParent == nil {
		topology.childrenByParent = make(map[string]map[string]struct{})
	}
}

func (topology *Topology) parentOfUnlocked(uuid string) string {
	if topology == nil || uuid == "" || uuid == protocol.ADMIN_UUID {
		return ""
	}
	if topology.parentByChild == nil {
		return ""
	}
	return strings.TrimSpace(topology.parentByChild[uuid])
}

func (topology *Topology) childrenOfUnlocked(parentUUID string) []string {
	if topology == nil || parentUUID == "" || topology.childrenByParent == nil {
		return nil
	}
	childrenSet := topology.childrenByParent[parentUUID]
	if len(childrenSet) == 0 {
		return nil
	}
	children := make([]string, 0, len(childrenSet))
	for childUUID := range childrenSet {
		if childUUID == "" {
			continue
		}
		children = append(children, childUUID)
	}
	sort.Strings(children)
	return children
}

func (topology *Topology) setParentRelationLocked(childUUID, parentUUID string) {
	if topology == nil {
		return
	}
	childUUID = strings.TrimSpace(childUUID)
	parentUUID = strings.TrimSpace(parentUUID)
	if childUUID == "" || childUUID == protocol.ADMIN_UUID {
		return
	}

	topology.ensureRelationStorageLocked()

	oldParent := strings.TrimSpace(topology.parentByChild[childUUID])
	if oldParent == parentUUID {
		return
	}
	if oldParent != "" {
		if childrenSet := topology.childrenByParent[oldParent]; childrenSet != nil {
			delete(childrenSet, childUUID)
			if len(childrenSet) == 0 {
				delete(topology.childrenByParent, oldParent)
			}
		}
	}
	if parentUUID == "" {
		delete(topology.parentByChild, childUUID)
		return
	}

	topology.parentByChild[childUUID] = parentUUID
	childrenSet := topology.childrenByParent[parentUUID]
	if childrenSet == nil {
		childrenSet = make(map[string]struct{})
		topology.childrenByParent[parentUUID] = childrenSet
	}
	childrenSet[childUUID] = struct{}{}
}

func (topology *Topology) removeParentRelationLocked(childUUID string) {
	topology.setParentRelationLocked(childUUID, "")
}

// breakParentCycleTowardsLocked 从 proposedParent 出发向上遍历父链，
// 若遇到 childUUID 自身或走入已访问过的节点（即父链已成环），则断掉
// 引入环的那条反向边，使 childUUID 能以 proposedParent 作父挂入而不
// 形成循环。返回被断开的边数（0 表示原本就无环）。
//
// 调用方应在 topology.mu 锁内使用。
func (topology *Topology) breakParentCycleTowardsLocked(childUUID, proposedParent string) int {
	childUUID = strings.TrimSpace(childUUID)
	proposedParent = strings.TrimSpace(proposedParent)
	if childUUID == "" || proposedParent == "" || proposedParent == protocol.ADMIN_UUID {
		return 0
	}
	if childUUID == proposedParent {
		// "声明自己做自己的父"是无法通过破环修复的退化输入，
		// 直接放手交给调用方的 fallback 路径处理，避免做无意义的
		// 空转迭代。
		return 0
	}
	broken := 0
	// 反复迭代——即便单次 removeParent 之后剩余链路仍有环，
	// 也能收敛到无环状态，最坏情况受节点数线性 bound。
	for {
		current := proposedParent
		// visited 按 parent 方向有序 push：visited[i].parent == visited[i+1]
		// （或等于首次走出 visited 的那个 current）。用于定位"环闭回 child
		// 的上一跳"。
		visited := make([]string, 0, 8)
		inVisited := make(map[string]struct{}, 8)
		// 要断的那条边是 victim.parent = ???，被删后环打破。
		victim := ""
		for current != "" && current != protocol.ADMIN_UUID {
			if current == childUUID {
				// 环在 "visited 末尾节点 → current(== childUUID)" 这条边上闭合。
				// 删除该末尾节点的 parent 指针即可打破环，同时保留 childUUID 的
				// 旧父子关系不受干扰（留给上层根据新声明覆盖）。
				if len(visited) == 0 {
					// 理论不可达：入口处已拒绝 childUUID == proposedParent。
					return broken
				}
				victim = visited[len(visited)-1]
				break
			}
			if _, seen := inVisited[current]; seen {
				// 不含 childUUID 的纯环。断当前节点的 parent 指针即可。
				victim = current
				break
			}
			visited = append(visited, current)
			inVisited[current] = struct{}{}
			current = topology.parentOfUnlocked(current)
		}
		if victim == "" {
			return broken
		}
		topology.removeParentRelationLocked(victim)
		broken++
		// 防御：每次迭代必删掉一条现存父指针，若未删到预期的边，
		// 父指针总数下降一，整体迭代受 parentByChild 大小 bound。
		if broken > len(topology.parentByChild)+len(visited)+2 {
			return broken
		}
	}
}

func (topology *Topology) deleteNodeRelationsLocked(uuid string) {
	if topology == nil {
		return
	}
	uuid = strings.TrimSpace(uuid)
	if uuid == "" {
		return
	}
	topology.removeParentRelationLocked(uuid)
	children := topology.childrenOfUnlocked(uuid)
	delete(topology.childrenByParent, uuid)
	for _, childUUID := range children {
		if strings.TrimSpace(topology.parentByChild[childUUID]) == uuid {
			delete(topology.parentByChild, childUUID)
		}
	}
}
