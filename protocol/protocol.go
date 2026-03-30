package protocol

import (
	"net"
	"strings"
	"sync"

	"codeberg.org/agnoie/shepherd/pkg/crypto"
)

// Transports holds default and per-component transport selections.
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

// SetDefaultTransports sets the process-wide default transports.
// Prefer passing a Transports instance explicitly where possible.
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
	// CurrentProtocolVersion 表示当前主协议版本号
	CurrentProtocolVersion uint16 = 1
)

const (
	FlagSupportChunked uint16 = 0x0002
)

const DefaultProtocolFlags uint16 = FlagSupportChunked

const (
	// Active control / topology / status messages（仍在路由器中使用）
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
	// Unified Stream (V2): DTN-Stream control/data
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
	Version     uint16
	Flags       uint16
	Sender      string // sender 与 accepter 均固定为 10 字节
	Accepter    string
	MessageType uint16
	RouteLen    uint32
	Route       string
	DataLen     uint64
}

type HIMess struct {
	GreetingLen  uint16
	Greeting     string
	UUIDLen      uint16
	UUID         string
	IsAdmin      uint16
	IsReconnect  uint16
	ProtoVersion uint16
	ProtoFlags   uint16
}

type UUIDMess struct {
	UUIDLen      uint16
	UUID         string
	ProtoVersion uint16
	ProtoFlags   uint16
}

type Negotiation struct {
	LocalVersion uint16
	LocalFlags   uint16
	PeerVersion  uint16
	PeerFlags    uint16
	Version      uint16
	Flags        uint16
}

func Negotiate(localVersion, localFlags, peerVersion, peerFlags uint16) Negotiation {
	result := Negotiation{
		LocalVersion: localVersion,
		LocalFlags:   localFlags,
		PeerVersion:  peerVersion,
		PeerFlags:    peerFlags,
	}
	if localVersion == 0 || peerVersion == 0 {
		return result
	}
	version := localVersion
	if peerVersion < version {
		version = peerVersion
	}
	result.Version = version
	result.Flags = localFlags & peerFlags
	return result
}

func (n Negotiation) IsV1() bool {
	return n.Version >= 1
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

// DTNData carries a DTN bundle payload (experimental).
type DTNData struct {
	BundleIDLen uint16
	BundleID    string
	PayloadLen  uint64
	Payload     []byte
}

// DTNAck acknowledges a DTN bundle delivery (experimental).
type DTNAck struct {
	BundleIDLen uint16
	BundleID    string
	OK          uint16
	ErrorLen    uint16
	Error       string
}

// DTNPull requests admin to push up to Limit DTN_DATA messages for the sender.
type DTNPull struct {
	Limit uint16
}

// --- V2 Stream messages (DTN-Stream) ---
// StreamOpen negotiates a new stream with basic options (key=value;...)
type StreamOpen struct {
	StreamID   uint32
	OptionsLen uint16
	Options    string
}

// StreamData carries a framed payload with basic cumulative ack/window hints.
type StreamData struct {
	StreamID   uint32
	Seq        uint32
	Ack        uint32
	Window     uint16
	PayloadLen uint32
	Payload    []byte
}

// StreamAck provides ack and optional credit-only updates (without data).
type StreamAck struct {
	StreamID uint32
	Ack      uint32
	Credit   uint16
	SACKLen  uint16
	SACK     []byte
}

// StreamClose terminates a stream with an application/result code.
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

// SleepUpdate requests an agent to adjust its sleep/work/jitter configuration.
// Fields are applied only if the relevant flag bit is set.
type SleepUpdate struct {
	Flags          uint16
	SleepSeconds   int32
	WorkSeconds    int32
	JitterPermille uint16
}

// SleepUpdateAck reports whether the configuration was applied.
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
	Version    uint16
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
		tMessage.Version = CurrentProtocolVersion
		tMessage.Flags = DefaultProtocolFlags
		return tMessage
	case "ws":
		tMessage := new(WSMessage)
		tMessage.RawMessage = new(RawMessage)
		tMessage.RawMessage.Conn = conn
		tMessage.RawMessage.UUID = uuid
		tMessage.RawMessage.CryptoSecret = crypto.KeyPadding([]byte(secret))
		tMessage.RawMessage.Version = CurrentProtocolVersion
		tMessage.RawMessage.Flags = DefaultProtocolFlags
		return tMessage
	case "http":
		raw := new(RawMessage)
		raw.Conn = conn
		raw.UUID = uuid
		raw.CryptoSecret = crypto.KeyPadding([]byte(secret))
		raw.Version = CurrentProtocolVersion
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
		tMessage.Version = CurrentProtocolVersion
		tMessage.Flags = DefaultProtocolFlags
		return tMessage
	case "ws":
		tMessage := new(WSMessage)
		tMessage.RawMessage = new(RawMessage)
		tMessage.RawMessage.Conn = conn
		tMessage.RawMessage.UUID = uuid
		tMessage.RawMessage.CryptoSecret = crypto.KeyPadding([]byte(secret))
		tMessage.RawMessage.Version = CurrentProtocolVersion
		tMessage.RawMessage.Flags = DefaultProtocolFlags
		return tMessage
	case "http":
		raw := new(RawMessage)
		raw.Conn = conn
		raw.UUID = uuid
		raw.CryptoSecret = crypto.KeyPadding([]byte(secret))
		raw.Version = CurrentProtocolVersion
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

func SetMessageMeta(message Message, version, flags uint16) {
	if version == 0 {
		version = CurrentProtocolVersion
	}
	switch msg := message.(type) {
	case *RawMessage:
		if msg != nil {
			msg.Version = version
			msg.Flags = flags
		}
	case *WSMessage:
		if msg != nil && msg.RawMessage != nil {
			msg.RawMessage.Version = version
			msg.RawMessage.Flags = flags
		}
	case *HTTPMessage:
		if msg != nil && msg.RawMessage != nil {
			msg.RawMessage.Version = version
			msg.RawMessage.Flags = flags
		}
	}
}
