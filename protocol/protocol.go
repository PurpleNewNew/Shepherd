package protocol

import (
	"net"
	"strings"
	"sync"

	"codeberg.org/agnoie/shepherd/pkg/crypto"
)

// Transports 保存默认传输方式以及按组件覆盖的传输选择。
type Transports struct {
	defaultPair transportPair
	components  sync.Map
}

type transportPair struct {
	upstream   string
	downstream string
}

var (
	defaultTransports = NewTransports("raw", "raw")
)

func DefaultTransports() *Transports { return defaultTransports }

// SetDefaultTransports 设置进程级默认传输方式。
// 若条件允许，更推荐显式传入一个 Transports 实例。
func SetDefaultTransports(upstream, downstream string) {
	defaultTransports = NewTransports(upstream, downstream)
}

func NewTransports(upstream, downstream string) *Transports {
	return &Transports{defaultPair: transportPair{
		upstream:   normalizeTransport(upstream),
		downstream: normalizeTransport(downstream),
	}}
}

func (t *Transports) Upstream() string   { return t.defaultPair.upstream }
func (t *Transports) Downstream() string { return t.defaultPair.downstream }

func (t *Transports) SetComponentTransports(uuid, upstream, downstream string) {
	if t == nil || uuid == "" {
		return
	}
	pair := transportPair{
		upstream:   normalizeTransport(upstream),
		downstream: normalizeTransport(downstream),
	}
	t.components.Store(uuid, pair)
}

func (t *Transports) ClearComponentTransports(uuid string) {
	if t == nil || uuid == "" {
		return
	}
	t.components.Delete(uuid)
}

func (t *Transports) resolveUpstreamTransport(uuid string) string {
	if t == nil {
		return "raw"
	}
	if uuid != "" {
		if value, ok := t.components.Load(uuid); ok {
			pair := value.(transportPair)
			if pair.upstream != "" {
				return pair.upstream
			}
		}
	}
	return t.defaultPair.upstream
}

func (t *Transports) resolveDownstreamTransport(uuid string) string {
	if t == nil {
		return "raw"
	}
	if uuid != "" {
		if value, ok := t.components.Load(uuid); ok {
			pair := value.(transportPair)
			if pair.downstream != "" {
				return pair.downstream
			}
		}
	}
	return t.defaultPair.downstream
}

const (
	FlagSupportChunked uint16 = 0x0002
)

const DefaultProtocolFlags uint16 = FlagSupportChunked

const (
	// 活跃的控制、拓扑与状态消息（仍在路由器中使用）
	HI = iota
	UUID
	CHILDUUIDREQ
	CHILDUUIDRES
	MYINFO
	MYMEMO
	NODECONNINFO
	LISTENREQ
	LISTENRES
	LISTENSTOP
	CONNECTSTART
	CONNECTDONE
	NODEOFFLINE
	NODEREONLINE
	UPSTREAMOFFLINE
	UPSTREAMREONLINE
	SHUTDOWN
	HEARTBEAT
	RUNTIMELOG
	// Gossip 协议消息
	GOSSIP_UPDATE
	GOSSIP_REQUEST
	GOSSIP_RESPONSE
	GOSSIP_DISCOVER
	GOSSIP_HEARTBEAT
	GOSSIP_ROUTE_UPDATE
	GOSSIP_NODE_JOIN
	GOSSIP_NODE_LEAVE
	SUPPLINKREQ
	SUPPLINKRESP
	SUPPLINKTEARDOWN
	SUPPLINKHEARTBEAT
	SUPPFAILOVER
	SUPPLINKPROMOTE
	RESCUE_REQUEST
	RESCUE_RESPONSE
	// DTN data plane（延迟容忍数据面）
	DTN_DATA
	DTN_ACK
	DTN_PULL
	// 通过 DTN 传输的 Stream 控制与数据消息。
	STREAM_OPEN
	STREAM_DATA
	STREAM_ACK
	STREAM_CLOSE
	SLEEP_UPDATE
	SLEEP_UPDATE_ACK
)

const ADMIN_UUID = "IAMADMINXD"
const TEMP_UUID = "IAMNEWHERE"
const TEMP_ROUTE = "THEREISNOROUTE"

type Proto interface {
	CNegotiate() error
	SNegotiate() error
}

type NegParam struct {
	Domain string
	Conn   net.Conn
}

type Message interface {
	ConstructHeader()
	ConstructData(*Header, interface{}, bool)
	ConstructSuffix()
	DeconstructHeader()
	DeconstructData() (*Header, interface{}, error)
	DeconstructSuffix()
	SendMessage()
}

func ConstructMessage(message Message, header *Header, mess interface{}, isPass bool) {
	message.ConstructData(header, mess, isPass)
	message.ConstructHeader()
	message.ConstructSuffix()
}

func DestructMessage(message Message) (*Header, interface{}, error) {
	message.DeconstructHeader()
	header, mess, err := message.DeconstructData()
	message.DeconstructSuffix()
	return header, mess, err
}

type Header struct {
	Flags       uint16
	Sender      string // sender 与 accepter 均固定为 10 字节
	Accepter    string
	MessageType uint16
	RouteLen    uint32
	Route       string
	DataLen     uint64
}

type HIMess struct {
	GreetingLen uint16
	Greeting    string
	UUIDLen     uint16
	UUID        string
	IsAdmin     uint16
	IsReconnect uint16
	ProtoFlags  uint16
}

type UUIDMess struct {
	UUIDLen    uint16
	UUID       string
	ProtoFlags uint16
}

type ProtocolMeta struct {
	Flags uint16
}

func SharedProtocolFlags(localFlags, peerFlags uint16) uint16 {
	return localFlags & peerFlags
}

func ResolveProtocolMeta(localFlags, peerFlags uint16) ProtocolMeta {
	return ProtocolMeta{Flags: SharedProtocolFlags(localFlags, peerFlags)}
}

type ChildUUIDReq struct {
	ParentUUIDLen uint16
	ParentUUID    string
	IPLen         uint16
	IP            string
	RequestIDLen  uint16
	RequestID     string
}

type ChildUUIDRes struct {
	UUIDLen      uint16
	UUID         string
	RequestIDLen uint16
	RequestID    string
}

type MyInfo struct {
	UUIDLen     uint16
	UUID        string
	UsernameLen uint64
	Username    string
	HostnameLen uint64
	Hostname    string
	MemoLen     uint64
	Memo        string
}

type MyMemo struct {
	MemoLen uint64
	Memo    string
}

// DTNData 承载一个 DTN bundle 的载荷。
type DTNData struct {
	BundleIDLen uint16
	BundleID    string
	PayloadLen  uint64
	Payload     []byte
}

// DTNAck 用于确认一个 DTN bundle 已完成投递。
type DTNAck struct {
	BundleIDLen uint16
	BundleID    string
	OK          uint16
	ErrorLen    uint16
	Error       string
}

// DTNPull 请求 admin 为发送者推送最多 Limit 条 DTN_DATA 消息。
type DTNPull struct {
	Limit uint16
}

// --- Stream 消息（DTN-Stream） ---
// StreamOpen 用基础选项（key=value;...）协商创建一个新的 stream。
type StreamOpen struct {
	StreamID   uint32
	OptionsLen uint16
	Options    string
}

// StreamData 承载一个分帧后的载荷，并附带基础的累计 ack/window 提示。
type StreamData struct {
	StreamID   uint32
	Seq        uint32
	Ack        uint32
	Window     uint16
	PayloadLen uint32
	Payload    []byte
}

// StreamAck 提供 ack，以及可选的仅 credit 更新（不含数据）。
type StreamAck struct {
	StreamID uint32
	Ack      uint32
	Credit   uint16
	SACKLen  uint16
	SACK     []byte
}

// StreamClose 以应用层结果码结束一个 stream。
type StreamClose struct {
	StreamID  uint32
	Code      uint16
	ReasonLen uint16
	Reason    string
}

const (
	SleepUpdateFlagSleepSeconds uint16 = 1 << iota
	SleepUpdateFlagWorkSeconds
	SleepUpdateFlagJitter
)

// SleepUpdate 请求 agent 调整其 sleep/work/jitter 配置。
// 只有对应 flag bit 被置位的字段才会生效。
type SleepUpdate struct {
	Flags          uint16
	SleepSeconds   int32
	WorkSeconds    int32
	JitterPermille uint16
}

// SleepUpdateAck 汇报配置是否已成功应用。
type SleepUpdateAck struct {
	OK       uint16
	ErrorLen uint16
	Error    string
}

type NodeConnInfo struct {
	UUIDLen         uint16
	UUID            string
	DialAddrLen     uint16
	DialAddress     string
	ListenPort      uint16
	FallbackPort    uint16
	TransportLen    uint16
	Transport       string
	TlsEnabled      uint16
	LastSuccessUnix int64
}

const (
	ShellModePipe uint16 = iota
	ShellModePTY
)

type ListenReq struct {
	Method        uint16
	AddrLen       uint64
	Addr          string
	ListenerIDLen uint16
	ListenerID    string
}

type ListenRes struct {
	OK            uint16
	ListenerIDLen uint16
	ListenerID    string
}

type ListenStop struct {
	ListenerIDLen uint16
	ListenerID    string
}

type SSHCommand struct {
	CommandLen uint64
	Command    string
}

type ConnectStart struct {
	AddrLen uint16
	Addr    string
}

type ConnectDone struct {
	OK uint16
}

type NodeOffline struct {
	UUIDLen uint16
	UUID    string
}

type NodeReonline struct {
	ParentUUIDLen uint16
	ParentUUID    string
	UUIDLen       uint16
	UUID          string
	IPLen         uint16
	IP            string
}

type UpstreamOffline struct {
	OK uint16
}

type UpstreamReonline struct {
	OK uint16
}

type Shutdown struct {
	OK uint16
}

type HeartbeatMsg struct {
	Ping uint16
}

type RuntimeLog struct {
	UUIDLen     uint16
	UUID        string
	SeverityLen uint16
	Severity    string
	CodeLen     uint16
	Code        string
	MessageLen  uint64
	Message     string
	Retryable   uint16
	CauseLen    uint64
	Cause       string
}

const (
	SuppLinkRoleInitiator uint16 = iota
	SuppLinkRoleResponder
)

const (
	SuppLinkActionListen uint16 = 1
	SuppLinkActionDial   uint16 = 2
)

type SuppLinkRequest struct {
	RequestUUIDLen   uint16
	RequestUUID      string
	LinkUUIDLen      uint16
	LinkUUID         string
	InitiatorUUIDLen uint16
	InitiatorUUID    string
	TargetUUIDLen    uint16
	TargetUUID       string
	PeerIPLen        uint16
	PeerIP           string
	PeerPort         uint16
	Action           uint16
	Role             uint16
	Flags            uint16
	Timeout          uint32
}

type SuppLinkResponse struct {
	RequestUUIDLen uint16
	RequestUUID    string
	LinkUUIDLen    uint16
	LinkUUID       string
	AgentUUIDLen   uint16
	AgentUUID      string
	PeerUUIDLen    uint16
	PeerUUID       string
	Role           uint16
	Status         uint16
	MessageLen     uint16
	Message        string
	ListenIPLen    uint16
	ListenIP       string
	ListenPort     uint16
}

type SuppLinkTeardown struct {
	LinkUUIDLen uint16
	LinkUUID    string
	ReasonLen   uint16
	Reason      string
}

type SuppLinkHeartbeat struct {
	LinkUUIDLen uint16
	LinkUUID    string
	PeerUUIDLen uint16
	PeerUUID    string
	Status      uint16
	Timestamp   int64
}

type SuppFailoverCommand struct {
	LinkUUIDLen   uint16
	LinkUUID      string
	ParentUUIDLen uint16
	ParentUUID    string
	ChildUUIDLen  uint16
	ChildUUID     string
	Role          uint16
	Flags         uint16
}

type SuppLinkPromote struct {
	LinkUUIDLen   uint16
	LinkUUID      string
	ParentUUIDLen uint16
	ParentUUID    string
	ChildUUIDLen  uint16
	ChildUUID     string
	Role          uint16
}

const (
	RescueFlagRequireTLS uint16 = 1 << iota
)

type RescueRequest struct {
	TargetUUIDLen uint16
	TargetUUID    string
	DialAddrLen   uint16
	DialAddr      string
	DialPort      uint16
	TransportLen  uint16
	Transport     string
	SecretLen     uint16
	Secret        string
	Flags         uint16
}

type RescueResponse struct {
	TargetUUIDLen  uint16
	TargetUUID     string
	RescuerUUIDLen uint16
	RescuerUUID    string
	Status         uint16
	MessageLen     uint16
	Message        string
	ParentUUIDLen  uint16
	ParentUUID     string
	ChildUUIDLen   uint16
	ChildUUID      string
}

const (
	SuppFailoverRoleParent uint16 = 1
	SuppFailoverRoleChild  uint16 = 2
)

const SuppFailoverFlagInitiator uint16 = 1

const (
	SuppPromoteRoleParent uint16 = 1
	SuppPromoteRoleChild  uint16 = 2
)

type MessageComponent struct {
	UUID       string
	Conn       net.Conn
	Secret     string
	Flags      uint16
	Upstream   string
	Downstream string
}

func NewUpProto(param *NegParam) Proto {
	return defaultTransports.NewUpProto(param)
}

func (t *Transports) NewUpProto(param *NegParam) Proto {
	switch t.Upstream() {
	case "raw":
		tProto := new(RawProto)
		return tProto
	case "http":
		cfg := upstreamHTTPConfig()
		var domain string
		if param != nil {
			domain = param.Domain
		}
		proto := &HTTPProto{
			param:    param,
			config:   cfg,
			hostHint: domain,
		}
		return proto
	case "ws":
		return &WSProto{param: param, RawProto: new(RawProto)}
	}
	return nil
}

func NewDownProto(param *NegParam) Proto {
	return defaultTransports.NewDownProto(param)
}

func (t *Transports) NewDownProto(param *NegParam) Proto {
	switch t.Downstream() {
	case "raw":
		tProto := new(RawProto)
		return tProto
	case "http":
		cfg := downstreamHTTPConfig()
		var domain string
		if param != nil {
			domain = param.Domain
		}
		proto := &HTTPProto{
			param:    param,
			config:   cfg,
			hostHint: domain,
		}
		return proto
	case "ws":
		return &WSProto{param: param, RawProto: new(RawProto)}
	}
	return nil
}

func NewUpMsg(conn net.Conn, secret string, uuid string) Message {
	return defaultTransports.NewUpMsg(conn, secret, uuid)
}

func (t *Transports) NewUpMsg(conn net.Conn, secret string, uuid string) Message {
	transport := t.resolveUpstreamTransport(uuid)
	return newUpMsgWithTransport(conn, secret, uuid, transport)
}

func newUpMsgWithTransport(conn net.Conn, secret, uuid, transport string) Message {
	switch normalizeTransport(transport) {
	case "raw":
		tMessage := new(RawMessage)
		tMessage.Conn = conn
		tMessage.UUID = uuid
		tMessage.CryptoSecret = crypto.KeyPadding([]byte(secret))
		tMessage.Flags = DefaultProtocolFlags
		return tMessage
	case "ws":
		tMessage := new(WSMessage)
		tMessage.RawMessage = new(RawMessage)
		tMessage.RawMessage.Conn = conn
		tMessage.RawMessage.UUID = uuid
		tMessage.RawMessage.CryptoSecret = crypto.KeyPadding([]byte(secret))
		tMessage.RawMessage.Flags = DefaultProtocolFlags
		return tMessage
	case "http":
		raw := new(RawMessage)
		raw.Conn = conn
		raw.UUID = uuid
		raw.CryptoSecret = crypto.KeyPadding([]byte(secret))
		raw.Flags = DefaultProtocolFlags
		if isHTTPStreamConn(conn) {
			return raw
		}
		return NewHTTPMessage(raw, upstreamHTTPConfig())
	}
	return nil
}

func NewDownMsg(conn net.Conn, secret string, uuid string) Message {
	return defaultTransports.NewDownMsg(conn, secret, uuid)
}

func (t *Transports) NewDownMsg(conn net.Conn, secret string, uuid string) Message {
	transport := t.resolveDownstreamTransport(uuid)
	return newDownMsgWithTransport(conn, secret, uuid, transport)
}

func newDownMsgWithTransport(conn net.Conn, secret, uuid, transport string) Message {
	switch normalizeTransport(transport) {
	case "raw":
		tMessage := new(RawMessage)
		tMessage.Conn = conn
		tMessage.UUID = uuid
		tMessage.CryptoSecret = crypto.KeyPadding([]byte(secret))
		tMessage.Flags = DefaultProtocolFlags
		return tMessage
	case "ws":
		tMessage := new(WSMessage)
		tMessage.RawMessage = new(RawMessage)
		tMessage.RawMessage.Conn = conn
		tMessage.RawMessage.UUID = uuid
		tMessage.RawMessage.CryptoSecret = crypto.KeyPadding([]byte(secret))
		tMessage.RawMessage.Flags = DefaultProtocolFlags
		return tMessage
	case "http":
		raw := new(RawMessage)
		raw.Conn = conn
		raw.UUID = uuid
		raw.CryptoSecret = crypto.KeyPadding([]byte(secret))
		raw.Flags = DefaultProtocolFlags
		if isHTTPStreamConn(conn) {
			return raw
		}
		return NewHTTPMessage(raw, downstreamHTTPConfig())
	}
	return nil
}

func NewUpMsgWithTransport(conn net.Conn, secret, uuid, transport string) Message {
	return newUpMsgWithTransport(conn, secret, uuid, transport)
}

func NewDownMsgWithTransport(conn net.Conn, secret, uuid, transport string) Message {
	return newDownMsgWithTransport(conn, secret, uuid, transport)
}

func normalizeTransport(value string) string {
	switch strings.ToLower(value) {
	case "ws":
		return "ws"
	case "http":
		return "http"
	default:
		return "raw"
	}
}

func SetMessageMeta(message Message, flags uint16) {
	if flags == 0 {
		flags = DefaultProtocolFlags
	}
	switch msg := message.(type) {
	case *RawMessage:
		if msg != nil {
			msg.Flags = flags
		}
	case *WSMessage:
		if msg != nil && msg.RawMessage != nil {
			msg.RawMessage.Flags = flags
		}
	case *HTTPMessage:
		if msg != nil && msg.RawMessage != nil {
			msg.RawMessage.Flags = flags
		}
	}
}
