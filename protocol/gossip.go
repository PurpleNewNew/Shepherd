package protocol

import (
	"time"
)

// GossipMessageType 定义了gossip消息的类型
type GossipMessageType uint16

const (
	// GossipUpdateType 表示一个gossip更新消息
	GossipUpdateType GossipMessageType = iota
	// GossipHeartbeatType 表示一个心跳消息
	GossipHeartbeatType
	// GossipRouteUpdateType 表示一个路由表更新
	GossipRouteUpdateType
)

// GossipUpdate 表示一个gossip更新消息
type GossipUpdate struct {
	TTL           int32  `json:"ttl"`
	NodeDataLen   uint64 `json:"-"`
	NodeData      []byte `json:"node_data"`
	SenderUUIDLen uint16 `json:"-"`
	SenderUUID    string `json:"sender_uuid"`
	Timestamp     int64  `json:"timestamp"`
}

// GossipHeartbeat 表示一个心跳消息
type GossipHeartbeat struct {
	SenderUUIDLen uint16 `json:"-"`
	SenderUUID    string `json:"sender_uuid"`
	Status        int32  `json:"status"`
	Load          int32  `json:"load"`
	Timestamp     int64  `json:"timestamp"`
}

// GossipRouteUpdate 表示一个路由表更新消息
type GossipRouteUpdate struct {
	SenderUUIDLen  uint16 `json:"-"`
	SenderUUID     string `json:"sender_uuid"`
	TargetUUIDLen  uint16 `json:"-"`
	TargetUUID     string `json:"target_uuid"`
	NextHopUUIDLen uint16 `json:"-"`
	NextHopUUID    string `json:"next_hop_uuid"`
	HopCount       int32  `json:"hop_count"`
	Weight         int32  `json:"weight"`
	IsReachable    bool   `json:"is_reachable"`
	Timestamp      int64  `json:"timestamp"`
}

// NodeInfo 表示用于gossip交换的节点信息
type NodeInfo struct {
	UUID       string   `json:"uuid"`
	IP         string   `json:"ip"`
	Port       int      `json:"port"`
	Username   string   `json:"username"`
	Hostname   string   `json:"hostname"`
	Memo       string   `json:"memo"`
	IsAdmin    bool     `json:"is_admin"`
	Status     uint8    `json:"status"`       // 0=离线, 1=在线
	Health     uint32   `json:"health_score"` // 0-100，越大表示越差
	LastSeen   int64    `json:"last_seen"`    // Unix时间戳
	LatencyMs  int64    `json:"latency_ms"`   // 最近一次测量的往返延迟（毫秒）
	FailRate   float32  `json:"failure_rate"` // 0-1 范围的失败率估计
	QueueDepth uint32   `json:"queue_depth"`  // Gossip 内部队列堆积
	Neighbors  []string `json:"neighbors"`    // 邻居UUID列表
	IsFirst    bool     `json:"is_first"`     // 这是否是第一个节点
	// 短连接/DTN 相关（可选字段，缺省表示未知）：
	SleepSeconds int32 `json:"sleep_seconds,omitempty"` // 预期睡眠周期（秒），0=长连接
	WorkSeconds  int32 `json:"work_seconds,omitempty"`  // 每次唤醒窗口长度（秒）
	NextWake     int64 `json:"next_wake,omitempty"`     // 预计下次唤醒的Unix时间（秒），未知=0
}

// GossipConfig 表示gossip协议配置
type GossipConfig struct {
	GossipInterval    time.Duration // Gossip传播间隔
	HeartbeatInterval time.Duration // 心跳间隔
	MaxTTL            uint16        // 最大TTL值
	Fanout            uint8         // 每次传播的邻居数量
}

type GossipRequest struct {
	RequestUUIDLen   uint16 `json:"-"`
	RequestUUID      string `json:"request_uuid"`
	RequesterUUIDLen uint16 `json:"-"`
	RequesterUUID    string `json:"requester_uuid"`
	TargetUUIDLen    uint16 `json:"-"`
	TargetUUID       string `json:"target_uuid"`
	Timestamp        int64  `json:"timestamp"`
}

// GossipResponse 表示一个gossip响应消息
type GossipResponse struct {
	RequestUUIDLen   uint16 `json:"-"`
	RequestUUID      string `json:"request_uuid"`
	RequesterUUIDLen uint16 `json:"-"`
	RequesterUUID    string `json:"requester_uuid"`
	ResponderUUIDLen uint16 `json:"-"`
	ResponderUUID    string `json:"responder_uuid"`
	TargetUUIDLen    uint16 `json:"-"`
	TargetUUID       string `json:"target_uuid"`
	Reachable        bool   `json:"reachable"`
	Latency          int64  `json:"latency"`
	NodeDataLen      uint64 `json:"-"`
	NodeData         []byte `json:"node_data"`
	Timestamp        int64  `json:"timestamp"`
}

// GossipDiscover 表示一个节点发现消息
type GossipDiscover struct {
	SenderUUIDLen uint16 `json:"-"`
	SenderUUID    string `json:"sender_uuid"`
	SenderIPLen   uint16 `json:"-"`
	SenderIP      string `json:"sender_ip"`
	SenderPort    uint16 `json:"sender_port"`
	Timestamp     int64  `json:"timestamp"`
}

// GossipNodeJoin 表示一个节点加入消息
type GossipNodeJoin struct {
	NodeUUIDLen   uint16 `json:"-"`
	NodeUUID      string `json:"node_uuid"`
	NodeIPLen     uint16 `json:"-"`
	NodeIP        string `json:"node_ip"`
	NodePort      uint16 `json:"node_port"`
	ParentUUIDLen uint16 `json:"-"`
	ParentUUID    string `json:"parent_uuid"`
	Timestamp     int64  `json:"timestamp"`
}

// GossipNodeLeave 表示一个节点离开消息
type GossipNodeLeave struct {
	NodeUUIDLen uint16 `json:"-"`
	NodeUUID    string `json:"node_uuid"`
	Reason      int32  `json:"reason"` // 离开原因：0=正常离开，1=超时，2=错误
	Timestamp   int64  `json:"timestamp"`
}
