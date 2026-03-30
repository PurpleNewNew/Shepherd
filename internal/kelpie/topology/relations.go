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
