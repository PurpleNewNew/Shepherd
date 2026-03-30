package topology

import (
	"time"

	"codeberg.org/agnoie/shepherd/internal/kelpie/printer"
)

// Persistence 定义将拓扑状态持久化到外部存储所需的操作集合。
type Persistence interface {
	UpsertNode(NodeSnapshot) error
	DeleteNode(uuid string) error
	UpsertEdge(EdgeSnapshot) error
	DeleteEdge(from, to string) error
	UpsertNetwork(targetUUID, networkID string) error
	DeleteNetwork(targetUUID string) error
	Load() (*Snapshot, error)
	UpsertSupplementalLink(SupplementalLinkSnapshot) error
	DeleteSupplementalLink(linkUUID string) error
	UpsertPlannerMetrics(PlannerMetricsSnapshot) error
}

// NodeSnapshot 描述节点持久化时需要记录的状态。
type NodeSnapshot struct {
	UUID           string
	Parent         string
	Network        string
	IP             string
	Port           int
	ListenPort     int
	DialAddress    string
	FallbackPort   int
	Transport      string
	TLSEnabled     bool
	LastSuccess    time.Time
	RepairFailures int
	RepairUpdated  time.Time
	Hostname       string
	Username       string
	Memo           string
	IsAlive        bool
	LastSeen       time.Time
}

// EdgeSnapshot 描述两节点之间的边信息。
type EdgeSnapshot struct {
	From   string
	To     string
	Type   EdgeType
	Weight uint32
}

// SupplementalLinkSnapshot 表示一条补链记录的持久化形式。
type SupplementalLinkSnapshot struct {
	LinkUUID       string
	Listener       string
	Dialer         string
	Status         int
	FailureReason  string
	LastTransition time.Time
}

// PlannerMetricsSnapshot 存储补链调度器的统计指标。
type PlannerMetricsSnapshot struct {
	Dispatched     uint64
	Success        uint64
	Failures       uint64
	Dropped        uint64
	Recycled       uint64
	QueueHigh      int
	LastFailure    string
	RepairAttempts uint64
	RepairSuccess  uint64
	RepairFailures uint64
}

// Snapshot 汇总拓扑持久化所需的各类组件。
type Snapshot struct {
	Nodes             []NodeSnapshot
	Edges             []EdgeSnapshot
	Networks          map[string]string
	SupplementalLinks []SupplementalLinkSnapshot
	PlannerMetrics    PlannerMetricsSnapshot
}

// SetPersistence 为拓扑配置持久化后端。
func (topology *Topology) SetPersistence(p Persistence) {
	if topology == nil {
		return
	}
	topology.mu.Lock()
	topology.persist = p
	topology.mu.Unlock()
}

func (topology *Topology) persistNode(uuid string) {
	if topology == nil || topology.persist == nil || uuid == "" {
		return
	}
	node := topology.nodeByUUID(uuid)
	if node == nil {
		return
	}
	record := NodeSnapshot{
		UUID:           node.uuid,
		Parent:         topology.parentOfUnlocked(node.uuid),
		Network:        topology.networkForUnlocked(node.uuid),
		IP:             node.currentIP,
		Port:           node.listenPort,
		ListenPort:     node.listenPort,
		DialAddress:    node.dialAddress,
		FallbackPort:   node.fallbackPort,
		Transport:      node.transport,
		TLSEnabled:     node.tlsEnabled,
		LastSuccess:    node.lastSuccess,
		RepairFailures: node.repairFailures,
		RepairUpdated:  node.repairUpdated,
		Hostname:       node.currentHostname,
		Username:       node.currentUser,
		Memo:           node.memo,
		IsAlive:        node.isAlive,
		LastSeen:       node.lastSeen,
	}
	if err := topology.persist.UpsertNode(record); err != nil {
		printer.Warning("\r\n[*] Persist node %s failed: %v\r\n", uuid, err)
	}
}

func (topology *Topology) deleteNode(uuid string) {
	if topology == nil || topology.persist == nil || uuid == "" {
		return
	}
	if err := topology.persist.DeleteNode(uuid); err != nil {
		printer.Warning("\r\n[*] Persist delete node %s failed: %v\r\n", uuid, err)
	}
}

func (topology *Topology) persistEdge(from, to string, edgeType EdgeType, weight uint32) {
	if topology == nil || topology.persist == nil || from == "" || to == "" {
		return
	}
	record := EdgeSnapshot{
		From:   from,
		To:     to,
		Type:   edgeType,
		Weight: weight,
	}
	if err := topology.persist.UpsertEdge(record); err != nil {
		printer.Warning("\r\n[*] Persist edge %s->%s failed: %v\r\n", from, to, err)
	}
}

func (topology *Topology) deleteEdge(from, to string) {
	if topology == nil || topology.persist == nil || from == "" || to == "" {
		return
	}
	if err := topology.persist.DeleteEdge(from, to); err != nil {
		printer.Warning("\r\n[*] Persist delete edge %s->%s failed: %v\r\n", from, to, err)
	}
}

func (topology *Topology) persistNetwork(entry, network string) {
	if topology == nil || topology.persist == nil || entry == "" {
		return
	}
	if network == "" {
		network = entry
	}
	if err := topology.persist.UpsertNetwork(entry, network); err != nil {
		printer.Warning("\r\n[*] Persist network %s=%s failed: %v\r\n", entry, network, err)
	}
}

func (topology *Topology) deleteNetwork(entry string) {
	if topology == nil || topology.persist == nil || entry == "" {
		return
	}
	if err := topology.persist.DeleteNetwork(entry); err != nil {
		printer.Warning("\r\n[*] Persist delete network %s failed: %v\r\n", entry, err)
	}
}

// PersistSupplementalLink 通过已配置的后端保存补链状态。
func (topology *Topology) PersistSupplementalLink(snapshot SupplementalLinkSnapshot) {
	if topology == nil || topology.persist == nil || snapshot.LinkUUID == "" {
		return
	}
	if err := topology.persist.UpsertSupplementalLink(snapshot); err != nil {
		printer.Warning("\r\n[*] Persist supplemental link %s failed: %v\r\n", snapshot.LinkUUID, err)
	}
}

// DeleteSupplementalLink 从持久化后端移除补链状态。
func (topology *Topology) DeleteSupplementalLink(linkUUID string) {
	if topology == nil || topology.persist == nil || linkUUID == "" {
		return
	}
	if err := topology.persist.DeleteSupplementalLink(linkUUID); err != nil {
		printer.Warning("\r\n[*] Persist delete supplemental link %s failed: %v\r\n", linkUUID, err)
	}
}

// PersistPlannerMetrics 将调度器指标写入持久化后端。
func (topology *Topology) PersistPlannerMetrics(snapshot PlannerMetricsSnapshot) {
	if topology == nil || topology.persist == nil {
		return
	}
	if err := topology.persist.UpsertPlannerMetrics(snapshot); err != nil {
		printer.Warning("\r\n[*] Persist planner metrics failed: %v\r\n", err)
	}
}

func (topology *Topology) nodeByUUID(uuid string) *node {
	if topology == nil || uuid == "" {
		return nil
	}
	id := topology.id2IDNum(uuid)
	if id < 0 {
		return nil
	}
	return topology.nodes[id]
}
