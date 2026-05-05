package service

import "time"

// ConnectionPhase 描述客户端连接状态机的阶段。
type ConnectionPhase string

const (
	PhaseDisconnected ConnectionPhase = "disconnected"
	// PhaseAwaitingTrust 表示握手拿到了证书，但指纹未被用户信任，需要前端确认后再重试。
	PhaseAwaitingTrust ConnectionPhase = "awaiting-trust"
	PhaseConnecting    ConnectionPhase = "connecting"
	PhaseConnected     ConnectionPhase = "connected"
)

// ConnectRequest 是前端发起连接时的参数。
type ConnectRequest struct {
	Endpoint   string `json:"endpoint"`
	Token      string `json:"token"`
	UseTLS     bool   `json:"useTLS"`
	ServerName string `json:"serverName,omitempty"`
	// Remember 为 true 时持久化到 recent connections；token 字段不会落盘以防泄露。
	Remember bool `json:"remember"`
	// Label 可选的连接别名，用于 UI 列表显示。
	Label string `json:"label,omitempty"`
	// ForceAccept 在 PhaseAwaitingTrust 下由 UI 传回，表示用户已信任该指纹。
	ForceAccept bool `json:"forceAccept"`
}

// ConnectResult 反映一次 Connect 调用的结果。
type ConnectResult struct {
	Phase ConnectionPhase `json:"phase"`
	// PendingFingerprint 在 PhaseAwaitingTrust 时非空，前端应展示该指纹并等待用户决定。
	PendingFingerprint string `json:"pendingFingerprint,omitempty"`
	// MismatchExpected 在本地指纹与服务端不匹配时非空，前端应严格提示"可能是中间人"。
	MismatchExpected string `json:"mismatchExpected,omitempty"`
	// Endpoint 为实际使用的端点（便于前端做回显）。
	Endpoint string `json:"endpoint,omitempty"`
	// Message 附加信息（如错误摘要），仅用于 UI 展示。
	Message string `json:"message,omitempty"`
	// Status 汇总当前连接状态。
	Status *ConnectionStatus `json:"status,omitempty"`
}

// ConnectionStatus 供前端轮询/刷新，描述连接当前态。
type ConnectionStatus struct {
	Phase       ConnectionPhase `json:"phase"`
	Endpoint    string          `json:"endpoint,omitempty"`
	UseTLS      bool            `json:"useTLS"`
	Fingerprint string          `json:"fingerprint,omitempty"`
	ConnectedAt time.Time       `json:"connectedAt,omitempty"`
	Error       string          `json:"error,omitempty"`
}

// RecentConnectionDTO 是持久化的连接记录在前端的投影。
type RecentConnectionDTO struct {
	ID          string    `json:"id"`
	Label       string    `json:"label"`
	Endpoint    string    `json:"endpoint"`
	UseTLS      bool      `json:"useTLS"`
	ServerName  string    `json:"serverName,omitempty"`
	Fingerprint string    `json:"fingerprint,omitempty"`
	LastUsedAt  time.Time `json:"lastUsedAt"`
}

// NodeSummary 拓扑节点的 UI 投影。
type NodeSummary struct {
	UUID          string   `json:"uuid"`
	Alias         string   `json:"alias,omitempty"`
	ParentUUID    string   `json:"parentUuid,omitempty"`
	Status        string   `json:"status,omitempty"`
	Network       string   `json:"network,omitempty"`
	Sleep         string   `json:"sleep,omitempty"`
	Memo          string   `json:"memo,omitempty"`
	Depth         int32    `json:"depth"`
	Tags          []string `json:"tags,omitempty"`
	ActiveStreams uint32   `json:"activeStreams"`
	WorkProfile   string   `json:"workProfile,omitempty"`
}

// EdgeSummary 拓扑边的 UI 投影。
type EdgeSummary struct {
	ParentUUID   string `json:"parentUuid"`
	ChildUUID    string `json:"childUuid"`
	Supplemental bool   `json:"supplemental"`
}

// Snapshot 是 UI 初始化需要的一整份拓扑。
type Snapshot struct {
	Nodes         []NodeSummary     `json:"nodes"`
	Edges         []EdgeSummary     `json:"edges"`
	Streams       []StreamDiagDTO   `json:"streams"`
	Sessions      []SessionSummary  `json:"sessions"`
	SleepProfiles []SleepProfileDTO `json:"sleepProfiles,omitempty"`
	FetchedAt     time.Time         `json:"fetchedAt"`
}

// StreamDiagDTO 用于节点详情的"活跃流"小视图。
type StreamDiagDTO struct {
	StreamID     uint32 `json:"streamId"`
	TargetUUID   string `json:"targetUuid"`
	Kind         string `json:"kind"`
	Outbound     bool   `json:"outbound"`
	Pending      uint32 `json:"pending"`
	InFlight     uint32 `json:"inFlight"`
	Window       uint32 `json:"window"`
	Seq          uint32 `json:"seq"`
	Ack          uint32 `json:"ack"`
	Rto          string `json:"rto"`
	LastActivity string `json:"lastActivity"`
	SessionID    string `json:"sessionId"`
}

// SessionSummary 会话的 UI 投影。
type SessionSummary struct {
	TargetUUID   string   `json:"targetUuid"`
	Status       string   `json:"status"`
	Active       bool     `json:"active"`
	Connected    bool     `json:"connected"`
	RemoteAddr   string   `json:"remoteAddr,omitempty"`
	Upstream     string   `json:"upstream,omitempty"`
	Downstream   string   `json:"downstream,omitempty"`
	NetworkID    string   `json:"networkId,omitempty"`
	LastSeen     string   `json:"lastSeen,omitempty"`
	LastError    string   `json:"lastError,omitempty"`
	SleepSeconds *int32   `json:"sleepSeconds,omitempty"`
	WorkSeconds  *int32   `json:"workSeconds,omitempty"`
	Jitter       *float64 `json:"jitter,omitempty"`
}

// SleepProfileDTO 节点 sleep 快照。
type SleepProfileDTO struct {
	TargetUUID   string   `json:"targetUuid"`
	SleepSeconds *int32   `json:"sleepSeconds,omitempty"`
	WorkSeconds  *int32   `json:"workSeconds,omitempty"`
	Jitter       *float64 `json:"jitter,omitempty"`
	Profile      string   `json:"profile,omitempty"`
	LastUpdated  string   `json:"lastUpdated,omitempty"`
	NextWakeAt   string   `json:"nextWakeAt,omitempty"`
	Status       string   `json:"status,omitempty"`
}

// MetricsDTO 聚合指标（DTN/router/reconnect + 补链）。
type MetricsDTO struct {
	DTN          DTNMetricsDTO        `json:"dtn"`
	Reconnect    ReconnectMetricsDTO  `json:"reconnect"`
	Supplemental SupplementalSnapshot `json:"supplemental"`
	CapturedAt   time.Time            `json:"capturedAt"`
}

// DTNMetricsDTO 描述 DTN 队列指标。
type DTNMetricsDTO struct {
	Enqueued  uint64 `json:"enqueued"`
	Delivered uint64 `json:"delivered"`
	Failed    uint64 `json:"failed"`
	Retried   uint64 `json:"retried"`
	Global    struct {
		Total         int32  `json:"total"`
		Ready         int32  `json:"ready"`
		Held          int32  `json:"held"`
		Capacity      int32  `json:"capacity"`
		HighWatermark int32  `json:"highWatermark"`
		AverageWait   string `json:"averageWait"`
		DroppedTotal  uint64 `json:"droppedTotal"`
		ExpiredTotal  uint64 `json:"expiredTotal"`
	} `json:"global"`
}

// ReconnectMetricsDTO 描述上游重连统计。
type ReconnectMetricsDTO struct {
	Attempts  uint64 `json:"attempts"`
	Success   uint64 `json:"success"`
	Failures  uint64 `json:"failures"`
	LastError string `json:"lastError,omitempty"`
}

// SupplementalSnapshot 补链调度器综合状态。
type SupplementalSnapshot struct {
	Enabled        bool   `json:"enabled"`
	QueueLength    int32  `json:"queueLength"`
	PendingActions int32  `json:"pendingActions"`
	ActiveLinks    int32  `json:"activeLinks"`
	Dispatched     uint64 `json:"dispatched"`
	Success        uint64 `json:"success"`
	Failures       uint64 `json:"failures"`
	Dropped        uint64 `json:"dropped"`
	Recycled       uint64 `json:"recycled"`
	QueueHigh      uint64 `json:"queueHigh"`
	LastFailure    string `json:"lastFailure,omitempty"`
}

// NodeDetail 节点详情面板的数据。
type NodeDetail struct {
	Node           NodeSummary        `json:"node"`
	Sessions       []SessionSummary   `json:"sessions"`
	Streams        []StreamDiagDTO    `json:"streams"`
	PivotListeners []PivotListenerDTO `json:"pivotListeners,omitempty"`
	Sleep          *SleepProfileDTO   `json:"sleep,omitempty"`
	FetchedAt      time.Time          `json:"fetchedAt"`
}

// PivotListenerDTO 节点上的 pivot 监听器。
type PivotListenerDTO struct {
	ListenerID string `json:"listenerId"`
	Protocol   string `json:"protocol"`
	Bind       string `json:"bind"`
	Status     string `json:"status"`
	Mode       string `json:"mode"`
}

// EnqueueDTNRequest 答辩控制台发一条 DTN payload 的参数。
type EnqueueDTNRequest struct {
	Target  string `json:"target"`
	Payload string `json:"payload"`
	// Priority ∈ {"low","normal","high"}，默认 normal。
	Priority   string `json:"priority"`
	TTLSeconds int64  `json:"ttlSeconds"`
}

// EnqueueDTNResult 返回 DTN 入队结果。
type EnqueueDTNResult struct {
	BundleID   string    `json:"bundleId"`
	EnqueuedAt time.Time `json:"enqueuedAt"`
}

// UpdateSleepRequest 调整节点 sleep 配置。
// 为 nil 的字段不会被下发。
type UpdateSleepRequest struct {
	Target       string   `json:"target"`
	SleepSeconds *int32   `json:"sleepSeconds,omitempty"`
	WorkSeconds  *int32   `json:"workSeconds,omitempty"`
	Jitter       *float64 `json:"jitter,omitempty"`
}

// PruneOfflineResult 返回清理结果。
type PruneOfflineResult struct {
	Removed int32 `json:"removed"`
}

type StreamHandleDTO struct {
	HandleID   string            `json:"handleId"`
	TargetUUID string            `json:"targetUuid"`
	SessionID  string            `json:"sessionId"`
	Kind       string            `json:"kind"`
	StreamID   uint32            `json:"streamId,omitempty"`
	Options    map[string]string `json:"options,omitempty"`
	Status     string            `json:"status"`
}

type StartShellRequest struct {
	Target          string `json:"target"`
	Mode            string `json:"mode,omitempty"`
	ResumeSessionID string `json:"resumeSessionId,omitempty"`
}

type StartSocksProxyRequest struct {
	Target   string `json:"target"`
	Auth     string `json:"auth,omitempty"`
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
}

type StartForwardProxyRequest struct {
	Target     string `json:"target"`
	LocalBind  string `json:"localBind"`
	RemoteAddr string `json:"remoteAddr"`
}

type StartForwardProxyResult struct {
	Handle     StreamHandleDTO `json:"handle"`
	ProxyID    string          `json:"proxyId"`
	Bind       string          `json:"bind"`
	RemoteAddr string          `json:"remoteAddr"`
}

type StopForwardProxyRequest struct {
	Target  string `json:"target"`
	ProxyID string `json:"proxyId"`
}

type StopForwardProxyResult struct {
	Stopped int32 `json:"stopped"`
}

type StreamDataRequest struct {
	HandleID string `json:"handleId"`
	Data     string `json:"data"`
}

type StreamResizeRequest struct {
	HandleID string `json:"handleId"`
	Rows     uint32 `json:"rows"`
	Cols     uint32 `json:"cols"`
}

type CloseInteractiveStreamRequest struct {
	HandleID string `json:"handleId"`
	Reason   string `json:"reason,omitempty"`
}

type CloseStreamByIDRequest struct {
	StreamID uint32 `json:"streamId"`
	Reason   string `json:"reason,omitempty"`
}

type StreamPingRequest struct {
	Target      string `json:"target"`
	Count       int32  `json:"count"`
	PayloadSize int32  `json:"payloadSize"`
}

type StreamEventDTO struct {
	HandleID   string `json:"handleId"`
	TargetUUID string `json:"targetUuid,omitempty"`
	SessionID  string `json:"sessionId,omitempty"`
	Kind       string `json:"kind,omitempty"`
	StreamID   uint32 `json:"streamId,omitempty"`
	Type       string `json:"type"`
	Data       string `json:"data,omitempty"`
	Error      string `json:"error,omitempty"`
}

type RemoteFileEntryDTO struct {
	Name       string `json:"name"`
	Path       string `json:"path"`
	IsDir      bool   `json:"isDir"`
	IsDrive    bool   `json:"isDrive,omitempty"`
	IsSymlink  bool   `json:"isSymlink,omitempty"`
	Size       uint64 `json:"size,omitempty"`
	Mode       string `json:"mode,omitempty"`
	ModifiedAt string `json:"modifiedAt,omitempty"`
	Hidden     bool   `json:"hidden,omitempty"`
}

type RemoteFileListingDTO struct {
	RequestedPath string               `json:"requestedPath,omitempty"`
	ResolvedPath  string               `json:"resolvedPath,omitempty"`
	DisplayPath   string               `json:"displayPath,omitempty"`
	RootPath      string               `json:"rootPath,omitempty"`
	ParentPath    string               `json:"parentPath,omitempty"`
	CanGoUp       bool                 `json:"canGoUp"`
	VirtualRoot   bool                 `json:"virtualRoot"`
	Entries       []RemoteFileEntryDTO `json:"entries,omitempty"`
}

type ListRemoteFilesRequest struct {
	Target string `json:"target"`
	Path   string `json:"path,omitempty"`
}

type CollectRemoteFileRequest struct {
	Target     string   `json:"target"`
	RemotePath string   `json:"remotePath"`
	Tags       []string `json:"tags,omitempty"`
}

type LootItemDTO struct {
	LootID     string            `json:"lootId"`
	TargetUUID string            `json:"targetUuid,omitempty"`
	Operator   string            `json:"operator,omitempty"`
	Category   string            `json:"category,omitempty"`
	Name       string            `json:"name,omitempty"`
	StorageRef string            `json:"storageRef,omitempty"`
	OriginPath string            `json:"originPath,omitempty"`
	Hash       string            `json:"hash,omitempty"`
	Size       uint64            `json:"size,omitempty"`
	Mime       string            `json:"mime,omitempty"`
	Metadata   map[string]string `json:"metadata,omitempty"`
	Tags       []string          `json:"tags,omitempty"`
	CreatedAt  string            `json:"createdAt,omitempty"`
}

type CollectRemoteFileResult struct {
	Item LootItemDTO `json:"item"`
}

// SupplementalEventDTO 补链事件的简单投影。
type SupplementalEventDTO struct {
	Seq       uint64 `json:"seq"`
	Kind      string `json:"kind"`
	Action    string `json:"action"`
	Source    string `json:"source,omitempty"`
	Target    string `json:"target,omitempty"`
	Detail    string `json:"detail,omitempty"`
	Timestamp string `json:"timestamp"`
}
