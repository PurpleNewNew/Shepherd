package topology

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"codeberg.org/agnoie/shepherd/protocol"
)

func (topology *Topology) ensureNetworkStorageLocked() {
	if topology.targetNetworks == nil {
		topology.targetNetworks = make(map[string]string)
	}
	if topology.networks == nil {
		topology.networks = make(map[string]*Network)
	}
}

func (topology *Topology) ensureNetworkLocked(id string) *Network {
	if topology.networks == nil {
		topology.networks = make(map[string]*Network)
	}
	network, ok := topology.networks[id]
	if !ok {
		network = &Network{
			ID:      id,
			Targets: make(map[string]struct{}),
			Created: time.Now(),
		}
		topology.networks[id] = network
	}
	return network
}

func (topology *Topology) removeTargetFromNetworkLocked(networkID, target string) {
	network, ok := topology.networks[networkID]
	if !ok || network == nil {
		return
	}
	delete(network.Targets, target)
	if len(network.Targets) == 0 {
		delete(topology.networks, networkID)
	}
}

func (topology *Topology) assignNodeNetwork(target, network string) {
	if topology == nil || target == "" {
		return
	}
	network = strings.TrimSpace(network)
	if network == "" {
		network = target
	}
	topology.networkMu.Lock()
	defer topology.networkMu.Unlock()
	topology.ensureNetworkStorageLocked()
	old := topology.targetNetworks[target]
	if old == network && old != "" {
		return
	}
	topology.targetNetworks[target] = network
	topology.ensureNetworkLocked(network).Targets[target] = struct{}{}
	if old != "" && old != network {
		topology.removeTargetFromNetworkLocked(old, target)
	}
	topology.persistNetwork(target, network)
}

func (topology *Topology) clearNodeNetwork(target string) {
	if topology == nil || target == "" {
		return
	}
	topology.networkMu.Lock()
	defer topology.networkMu.Unlock()
	if topology.targetNetworks == nil {
		return
	}
	network := topology.targetNetworks[target]
	if network == "" {
		return
	}
	delete(topology.targetNetworks, target)
	topology.removeTargetFromNetworkLocked(network, target)
	topology.deleteNetwork(target)
}

func (topology *Topology) restoreNetwork(target, network string) {
	if topology == nil || target == "" {
		return
	}
	network = strings.TrimSpace(network)
	if network == "" {
		network = target
	}
	topology.networkMu.Lock()
	defer topology.networkMu.Unlock()
	topology.ensureNetworkStorageLocked()
	old := topology.targetNetworks[target]
	topology.targetNetworks[target] = network
	topology.ensureNetworkLocked(network).Targets[target] = struct{}{}
	if old != "" && old != network {
		topology.removeTargetFromNetworkLocked(old, target)
	}
}

func (topology *Topology) selectNetworkID(target, parent, override string) string {
	override = strings.TrimSpace(override)
	if override != "" {
		return override
	}
	if parent == "" || parent == protocol.ADMIN_UUID {
		return target
	}
	if netID := topology.networkForUnlocked(parent); netID != "" {
		return netID
	}
	return target
}

func (topology *Topology) networkForUnlocked(uuid string) string {
	if topology == nil || uuid == "" || uuid == protocol.ADMIN_UUID {
		return ""
	}
	current := uuid
	visited := make(map[string]struct{}, 8)
	for current != "" && current != protocol.ADMIN_UUID {
		if _, ok := visited[current]; ok {
			// 父链出现环（损坏/竞态）时直接中断，避免无限递归导致栈溢出。
			return ""
		}
		visited[current] = struct{}{}

		topology.networkMu.RLock()
		netID := topology.targetNetworks[current]
		topology.networkMu.RUnlock()
		if netID != "" {
			return netID
		}
		current = topology.parentOf(current)
	}
	return ""
}

func (topology *Topology) NetworkFor(uuid string) string {
	if topology == nil || uuid == "" || uuid == protocol.ADMIN_UUID {
		return ""
	}
	topology.mu.RLock()
	defer topology.mu.RUnlock()
	return topology.networkForUnlocked(uuid)
}

func (topology *Topology) NetworkIDs() []string {
	if topology == nil {
		return nil
	}
	topology.mu.RLock()
	defer topology.mu.RUnlock()
	topology.networkMu.RLock()
	defer topology.networkMu.RUnlock()
	if len(topology.networks) == 0 {
		return nil
	}
	ids := make([]string, 0, len(topology.networks))
	for id := range topology.networks {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func (topology *Topology) NetworkEntries(networkID string) []string {
	if topology == nil || strings.TrimSpace(networkID) == "" {
		return nil
	}
	topology.mu.RLock()
	defer topology.mu.RUnlock()
	topology.networkMu.RLock()
	defer topology.networkMu.RUnlock()
	network, ok := topology.networks[networkID]
	if !ok || network == nil || len(network.Targets) == 0 {
		return nil
	}
	targets := make([]string, 0, len(network.Targets))
	for target := range network.Targets {
		targets = append(targets, target)
	}
	sort.Strings(targets)
	return targets
}

func (topology *Topology) matchesNetwork(uuid, network string) bool {
	if network == "" || uuid == "" || uuid == protocol.ADMIN_UUID {
		return true
	}
	netID := topology.networkForUnlocked(uuid)
	if netID == "" {
		return false
	}
	return strings.EqualFold(netID, network)
}

func (topology *Topology) matchesEntry(uuid, entry string) bool {
	if entry == "" || uuid == "" {
		return true
	}
	if strings.EqualFold(uuid, entry) {
		return true
	}
	return strings.EqualFold(topology.networkForUnlocked(uuid), entry)
}

func (topology *Topology) RootTargets() []string {
	if topology == nil {
		return nil
	}
	topology.mu.RLock()
	defer topology.mu.RUnlock()
	if topology.nodes == nil {
		return nil
	}
	var roots []string
	for _, n := range topology.nodes {
		if n == nil || n.uuid == "" {
			continue
		}
		if topology.parentOfUnlocked(n.uuid) == protocol.ADMIN_UUID {
			roots = append(roots, n.uuid)
		}
	}
	return roots
}

func (topology *Topology) networkAnchor(uuid string) string {
	netID := topology.networkForUnlocked(uuid)
	if netID != "" {
		return netID
	}
	return uuid
}

func (topology *Topology) SetNetwork(target, network string) error {
	if topology == nil {
		return ErrNilTopology
	}
	target = strings.TrimSpace(target)
	if target == "" {
		return fmt.Errorf("target uuid required")
	}
	network = strings.TrimSpace(network)
	topology.assignNodeNetwork(target, network)
	return nil
}

func (topology *Topology) parentOf(uuid string) string {
	return topology.parentOfUnlocked(uuid)
}
