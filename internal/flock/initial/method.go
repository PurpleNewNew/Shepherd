package initial

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"codeberg.org/agnoie/shepherd/pkg/config/defaults"
	"codeberg.org/agnoie/shepherd/pkg/share"
	"codeberg.org/agnoie/shepherd/pkg/share/handshake"
	"codeberg.org/agnoie/shepherd/pkg/share/transport"
	"codeberg.org/agnoie/shepherd/pkg/utils"
	"codeberg.org/agnoie/shepherd/pkg/utils/runtimeerr"
	"codeberg.org/agnoie/shepherd/protocol"

	reuseport "github.com/libp2p/go-reuseport"
)

const CHAIN_NAME = "SHEPHERD"
const mfaEnvVar = "SHEPHERD_MFA_PIN"

var START_FORWARDING string
var STOP_FORWARDING string

func achieveUUID(conn net.Conn, secret, transport string) (string, error) {
	rMessage := protocol.NewUpMsgWithTransport(conn, secret, protocol.TEMP_UUID, transport)
	fHeader, fMessage, err := protocol.DestructMessage(rMessage)

	if err != nil {
		conn.Close()
		return "", runtimeerr.Wrap(err, "AGENT_FETCH_UUID", runtimeerr.SeverityError, true, "fail to achieve uuid")
	}

	if fHeader.MessageType == protocol.UUID {
		mmess := fMessage.(*protocol.UUIDMess)
		return mmess.UUID, nil
	}

	conn.Close()
	return "", runtimeerr.New("AGENT_FETCH_UUID", runtimeerr.SeverityWarn, false, "unexpected message type %d when expecting uuid", fHeader.MessageType)
}

func NormalActive(ctx context.Context, userOptions *Options, proxy share.Proxy) (net.Conn, string, *protocol.Negotiation, error) {
	var sMessage, rMessage protocol.Message

	baseSecret := userOptions.Secret
	localVersion := protocol.CurrentProtocolVersion
	localFlags := protocol.DefaultProtocolFlags
	if strings.ToLower(userOptions.Upstream) != "http" {
		localFlags &^= protocol.FlagSupportChunked
	}
	handshakeSecret := handshake.HandshakeSecret(baseSecret, userOptions.TlsEnable)

	hiMess := handshake.NewHIMess(handshake.RoleAgent, localVersion, localFlags, false)

	header := &protocol.Header{
		Version:     localVersion,
		Flags:       localFlags,
		Sender:      protocol.TEMP_UUID,
		Accepter:    protocol.ADMIN_UUID,
		MessageType: protocol.HI,
		RouteLen:    uint32(len([]byte(protocol.TEMP_ROUTE))),
		Route:       protocol.TEMP_ROUTE,
	}

	trace := handshake.NewTranscript()
	trace.Record(handshake.CodeStart, nil)

	var (
		conn net.Conn
		err  error
	)

	dialer := &net.Dialer{Timeout: defaults.HandshakeDialTimeout}
	if proxy == nil {
		if ctx == nil {
			ctx = context.Background()
		}
		conn, err = dialer.DialContext(ctx, "tcp", userOptions.Connect)
	} else {
		// Proxy 拨号本身不感知 context；这里保留该行为，但用 deadline 限制下游读取。
		conn, err = proxy.Dial()
	}

	if err != nil {
		trace.Record(handshake.CodeDial, err)
		return nil, "", nil, trace.Annotate(runtimeerr.Wrap(err, "AGENT_DIAL_FAILED", runtimeerr.SeverityError, true, "connect %s failed", userOptions.Connect))
	}
	trace.Record(handshake.CodeDial, nil)

	if userOptions.TlsEnable {
		var tlsConfig *tls.Config
		tlsConfig, err = transport.NewClientTLSConfig(userOptions.Domain, userOptions.PreAuthToken)
		if err != nil {
			trace.Record(handshake.CodeTLS, err)
			conn.Close()
			return nil, "", nil, trace.Annotate(runtimeerr.Wrap(err, "AGENT_TLS_CONFIG", runtimeerr.SeverityError, false, "prepare tls client"))
		}
		conn = transport.WrapTLSClientConn(conn, tlsConfig)
		trace.Record(handshake.CodeTLS, nil)
	}

	param := new(protocol.NegParam)
	param.Conn = conn
	param.Domain = userOptions.Domain
	proto := protocol.NewUpProto(param)
	if err := proto.CNegotiate(); err != nil {
		trace.Record(handshake.CodeNegotiate, err)
		conn.Close()
		return nil, "", nil, trace.Annotate(runtimeerr.Wrap(err, "AGENT_NEGOTIATE", runtimeerr.SeverityError, true, "upstream negotiate failed"))
	}
	trace.Record(handshake.CodeNegotiate, nil)
	conn = param.Conn

	if err := share.ActivePreAuth(conn, userOptions.PreAuthToken); err != nil {
		trace.Record(handshake.CodePreAuth, err)
		conn.Close()
		return nil, "", nil, trace.Annotate(runtimeerr.Wrap(err, "AGENT_PREAUTH", runtimeerr.SeverityError, true, "pre-auth failed"))
	}
	trace.Record(handshake.CodePreAuth, nil)

	if userOptions.MFAPin != "" {
		provided := strings.TrimSpace(os.Getenv(mfaEnvVar))
		if provided == "" {
			err := fmt.Errorf("mfa pin missing (set %s)", mfaEnvVar)
			trace.Record(handshake.CodeMFA, err)
			conn.Close()
			return nil, "", nil, trace.Annotate(runtimeerr.Wrap(err, "AGENT_MFA", runtimeerr.SeverityError, false, "mfa pin not provided"))
		}
		if provided != userOptions.MFAPin {
			err := fmt.Errorf("mfa pin mismatch")
			trace.Record(handshake.CodeMFA, err)
			conn.Close()
			return nil, "", nil, trace.Annotate(runtimeerr.New("AGENT_MFA", runtimeerr.SeverityWarn, false, "mfa pin mismatch"))
		}
		trace.Record(handshake.CodeMFA, nil)
	}

	sMessage = protocol.NewUpMsgWithTransport(conn, handshakeSecret, protocol.TEMP_UUID, userOptions.Upstream)
	protocol.SetMessageMeta(sMessage, localVersion, localFlags)
	protocol.ConstructMessage(sMessage, header, hiMess, false)
	_ = conn.SetWriteDeadline(time.Now().Add(defaults.HandshakeReadTimeout))
	sMessage.SendMessage()
	_ = conn.SetWriteDeadline(time.Time{})

	// 设置一个较短的握手 deadline，避免无限阻塞。
	_ = conn.SetReadDeadline(time.Now().Add(defaults.HandshakeReadTimeout))
	rMessage = protocol.NewUpMsgWithTransport(conn, handshakeSecret, protocol.TEMP_UUID, userOptions.Upstream)
	fHeader, fMessage, err := protocol.DestructMessage(rMessage)
	_ = conn.SetReadDeadline(time.Time{})
	if err != nil {
		trace.Record(handshake.CodeExchange, err)
		addr := conn.RemoteAddr().String()
		conn.Close()
		return nil, "", nil, trace.Annotate(runtimeerr.Wrap(err, "AGENT_HANDSHAKE", runtimeerr.SeverityError, true, "fail to connect admin %s", addr))
	}
	trace.Record(handshake.CodeExchange, nil)

	if fHeader.MessageType != protocol.HI {
		err := fmt.Errorf("unexpected message type %d", fHeader.MessageType)
		trace.Record(handshake.CodeExchange, err)
		conn.Close()
		return nil, "", nil, trace.Annotate(runtimeerr.New("AGENT_ILLEGAL_ADMIN", runtimeerr.SeverityWarn, false, err.Error()))
	}

	mmess := fMessage.(*protocol.HIMess)
	if !handshake.ValidGreeting(handshake.RoleAdmin, mmess.Greeting) || mmess.IsAdmin != 1 {
		err := fmt.Errorf("admin greeting mismatch")
		trace.Record(handshake.CodeExchange, err)
		conn.Close()
		return nil, "", nil, trace.Annotate(runtimeerr.New("AGENT_ILLEGAL_ADMIN", runtimeerr.SeverityWarn, false, err.Error()))
	}
	negotiation := protocol.Negotiate(localVersion, localFlags, mmess.ProtoVersion, mmess.ProtoFlags)
	if !negotiation.IsV1() {
		err := fmt.Errorf("admin protocol version %d unsupported", mmess.ProtoVersion)
		trace.Record(handshake.CodeNegotiate, err)
		conn.Close()
		return nil, "", nil, trace.Annotate(runtimeerr.New("AGENT_PROTOCOL_VERSION", runtimeerr.SeverityError, false, err.Error()))
	}
	if strings.ToLower(userOptions.Upstream) == "http" && negotiation.Flags&protocol.FlagSupportChunked == 0 {
		err := fmt.Errorf("admin does not support HTTP chunked transfer")
		trace.Record(handshake.CodeNegotiate, err)
		conn.Close()
		return nil, "", nil, trace.Annotate(runtimeerr.New("AGENT_HTTP_CHUNKED", runtimeerr.SeverityError, false, err.Error()))
	}
	userOptions.Secret = handshake.SessionSecret(baseSecret, userOptions.TlsEnable)

	uuid, err := achieveUUID(conn, userOptions.Secret, userOptions.Upstream)
	if err != nil {
		trace.Record(handshake.CodeComplete, err)
		return nil, "", nil, trace.Annotate(err)
	}
	trace.Record(handshake.CodeComplete, nil)
	return conn, uuid, &negotiation, nil
}

func NormalPassive(userOptions *Options) (net.Conn, string, *protocol.Negotiation, error) {
	listenAddr, _, err := utils.CheckIPPort(userOptions.Listen)
	if err != nil {
		return nil, "", nil, runtimeerr.Wrap(err, "AGENT_LISTEN_ADDR", runtimeerr.SeverityError, false, "invalid listen address %s", userOptions.Listen)
	}

	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, "", nil, runtimeerr.Wrap(err, "AGENT_LISTEN_FAILED", runtimeerr.SeverityError, true, "listen on %s failed", listenAddr)
	}

	defer listener.Close()

	baseSecret := userOptions.Secret
	localVersion := protocol.CurrentProtocolVersion
	localFlags := protocol.DefaultProtocolFlags
	if strings.ToLower(userOptions.Upstream) != "http" {
		localFlags &^= protocol.FlagSupportChunked
	}
	handshakeSecret := handshake.HandshakeSecret(baseSecret, userOptions.TlsEnable)

	greetAdmin := handshake.RandomGreeting(handshake.RoleAdmin)
	hiTemplate := &protocol.HIMess{
		GreetingLen:  uint16(len(greetAdmin)),
		Greeting:     greetAdmin,
		UUIDLen:      uint16(len(protocol.TEMP_UUID)),
		UUID:         protocol.TEMP_UUID,
		IsAdmin:      0,
		IsReconnect:  0,
		ProtoVersion: localVersion,
		ProtoFlags:   localFlags,
	}

	header := &protocol.Header{
		Version:     localVersion,
		Flags:       localFlags,
		Sender:      protocol.TEMP_UUID,
		Accepter:    protocol.ADMIN_UUID,
		MessageType: protocol.HI,
		RouteLen:    uint32(len([]byte(protocol.TEMP_ROUTE))),
		Route:       protocol.TEMP_ROUTE,
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			logger.Warnf("accept connection failed: %v", err)
			continue
		}

		if userOptions.TlsEnable {
			var tlsConfig *tls.Config
			tlsConfig, err = transport.NewServerTLSConfig(userOptions.PreAuthToken, userOptions.Domain)
			if err != nil {
				conn.Close()
				return nil, "", nil, runtimeerr.Wrap(err, "AGENT_TLS_CONFIG", runtimeerr.SeverityError, false, "prepare tls server")
			}
			conn = transport.WrapTLSServerConn(conn, tlsConfig)
		}

		param := &protocol.NegParam{Conn: conn}
		proto := protocol.NewUpProto(param)
		if err := proto.SNegotiate(); err != nil {
			conn.Close()
			return nil, "", nil, runtimeerr.Wrap(err, "AGENT_NEGOTIATE", runtimeerr.SeverityError, true, "downstream negotiate failed")
		}
		conn = param.Conn

		if err := share.PassivePreAuth(conn, userOptions.PreAuthToken); err != nil {
			conn.Close()
			return nil, "", nil, runtimeerr.Wrap(err, "AGENT_PREAUTH", runtimeerr.SeverityError, true, "pre-auth failed")
		}

		rMessage := protocol.NewUpMsgWithTransport(conn, handshakeSecret, protocol.TEMP_UUID, userOptions.Upstream)
		fHeader, fMessage, err := protocol.DestructMessage(rMessage)
		if err != nil {
			logger.Warnf("failed to initialize connection from %s: %v", conn.RemoteAddr().String(), err)
			conn.Close()
			continue
		}
		if fHeader.MessageType != protocol.HI {
			conn.Close()
			logger.Warnf("incoming connection seems illegal")
			continue
		}

		mmess, ok := fMessage.(*protocol.HIMess)
		if !ok || !handshake.ValidGreeting(handshake.RoleAgent, mmess.Greeting) || mmess.IsAdmin != 1 {
			conn.Close()
			logger.Warnf("incoming connection seems illegal")
			continue
		}

		negotiation := protocol.Negotiate(localVersion, localFlags, mmess.ProtoVersion, mmess.ProtoFlags)
		if !negotiation.IsV1() {
			conn.Close()
			return nil, "", nil, runtimeerr.New("AGENT_PROTOCOL_VERSION", runtimeerr.SeverityError, false, "incoming connection uses unsupported protocol version %d", mmess.ProtoVersion)
		}
		if strings.ToLower(userOptions.Upstream) == "http" && negotiation.Flags&protocol.FlagSupportChunked == 0 {
			conn.Close()
			logger.Warnf("incoming connection does not support HTTP chunked transfer")
			continue
		}

		sMessage := protocol.NewUpMsgWithTransport(conn, handshakeSecret, protocol.TEMP_UUID, userOptions.Upstream)
		protocol.SetMessageMeta(sMessage, negotiation.Version, negotiation.Flags)
		header.Version = negotiation.Version
		header.Flags = negotiation.Flags
		hiResp := *hiTemplate
		hiResp.ProtoVersion = negotiation.Version
		hiResp.ProtoFlags = negotiation.Flags
		protocol.ConstructMessage(sMessage, header, &hiResp, false)
		sMessage.SendMessage()
		userOptions.Secret = handshake.SessionSecret(baseSecret, userOptions.TlsEnable)

		uuid, err := achieveUUID(conn, userOptions.Secret, userOptions.Upstream)
		if err != nil {
			conn.Close()
			return nil, "", nil, err
		}

		return conn, uuid, &negotiation, nil
	}
}

// IPTables 复用端口流程
func IPTableReusePassive(userOptions *Options) (net.Conn, string, *protocol.Negotiation, error) {
	// 先缓存复用所需的密钥，避免 TLS 模式清空 Secret
	setReuseSecret(userOptions)
	if err := SetPortReuseRules(userOptions.Listen, userOptions.ReusePort); err != nil {
		return nil, "", nil, err
	}
	go waitForExit(userOptions.Listen, userOptions.ReusePort)

	conn, uuid, negotiation, err := NormalPassive(userOptions)
	if err != nil {
		_ = DeletePortReuseRules(userOptions.Listen, userOptions.ReusePort)
		return nil, "", nil, err
	}
	return conn, uuid, negotiation, err
}

func waitForExit(localPort, reusedPort string) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM) // 捕获 Ctrl+C 和 kill 信号
	for {
		<-sigs
		DeletePortReuseRules(localPort, reusedPort)
		signal.Stop(sigs)
		return
	}
}

func setReuseSecret(userOptions *Options) {
	firstSecret := utils.GetStringMd5(userOptions.Secret)
	secondSecret := utils.GetStringMd5(firstSecret)
	finalSecret := firstSecret[:24] + secondSecret[:24]
	START_FORWARDING = finalSecret[16:32]
	STOP_FORWARDING = finalSecret[32:]
}

func DeletePortReuseRules(localPort string, reusedPort string) error {
	var cmds []string

	cmds = append(cmds, fmt.Sprintf("iptables -t nat -D PREROUTING -p tcp --dport %s --syn -m recent --rcheck --seconds 3600 --name %s --rsource -j %s", reusedPort, strings.ToLower(CHAIN_NAME), CHAIN_NAME))
	cmds = append(cmds, fmt.Sprintf("iptables -D INPUT -p tcp -m string --string %s --algo bm -m recent --name %s --remove -j ACCEPT", STOP_FORWARDING, strings.ToLower(CHAIN_NAME)))
	cmds = append(cmds, fmt.Sprintf("iptables -D INPUT -p tcp -m string --string %s --algo bm -m recent --set --name %s --rsource -j ACCEPT", START_FORWARDING, strings.ToLower(CHAIN_NAME)))
	cmds = append(cmds, fmt.Sprintf("iptables -t nat -F %s", CHAIN_NAME))
	cmds = append(cmds, fmt.Sprintf("iptables -t nat -X %s", CHAIN_NAME))

	for _, each := range cmds {
		cmd := strings.Split(each, " ")
		if err := exec.Command(cmd[0], cmd[1:]...).Run(); err != nil {
			// 删除流程尽量继续，记录首个错误。
			return err
		}
	}

	return nil
}

func SetPortReuseRules(localPort string, reusedPort string) error {
	var cmds []string

	cmds = append(cmds, fmt.Sprintf("iptables -t nat -N %s", CHAIN_NAME))                                                                                                                                      // 新建自定义链
	cmds = append(cmds, fmt.Sprintf("iptables -t nat -A %s -p tcp -j REDIRECT --to-port %s", CHAIN_NAME, localPort))                                                                                           // 将自定义链重定向到本地监听端口
	cmds = append(cmds, fmt.Sprintf("iptables -A INPUT -p tcp -m string --string %s --algo bm -m recent --set --name %s --rsource -j ACCEPT", START_FORWARDING, strings.ToLower(CHAIN_NAME)))                  // 将带指定字符串的源地址加入特定列表
	cmds = append(cmds, fmt.Sprintf("iptables -A INPUT -p tcp -m string --string %s --algo bm -m recent --name %s --remove -j ACCEPT", STOP_FORWARDING, strings.ToLower(CHAIN_NAME)))                          // 将带指定字符串的源地址从特定列表移除
	cmds = append(cmds, fmt.Sprintf("iptables -t nat -A PREROUTING -p tcp --dport %s --syn -m recent --rcheck --seconds 3600 --name %s --rsource -j %s", reusedPort, strings.ToLower(CHAIN_NAME), CHAIN_NAME)) // 校验命中列表的源地址并跳转到自定义链

	for _, each := range cmds {
		cmd := strings.Split(each, " ")
		if err := exec.Command(cmd[0], cmd[1:]...).Run(); err != nil {
			return err
		}
	}

	return nil
}

// SO_REUSEPORT 复用端口流程
func SoReusePassive(userOptions *Options) (net.Conn, string, *protocol.Negotiation, error) {
	listenAddr := fmt.Sprintf("%s:%s", userOptions.ReuseHost, userOptions.ReusePort)

	listener, err := reuseport.Listen("tcp", listenAddr)
	if err != nil {
		return nil, "", nil, runtimeerr.Wrap(err, "AGENT_LISTEN_FAILED", runtimeerr.SeverityError, true, "listen on %s failed", listenAddr)
	}

	defer func() {
		listener.Close()
	}()

	var sMessage, rMessage protocol.Message

	baseSecret := userOptions.Secret
	localVersion := protocol.CurrentProtocolVersion
	localFlags := protocol.DefaultProtocolFlags
	if strings.ToLower(userOptions.Upstream) != "http" {
		localFlags &^= protocol.FlagSupportChunked
	}
	handshakeSecret := handshake.HandshakeSecret(baseSecret, userOptions.TlsEnable)

	greetAdmin := handshake.RandomGreeting(handshake.RoleAdmin)
	hiMess := &protocol.HIMess{
		GreetingLen:  uint16(len(greetAdmin)),
		Greeting:     greetAdmin,
		UUIDLen:      uint16(len(protocol.TEMP_UUID)),
		UUID:         protocol.TEMP_UUID,
		IsAdmin:      0,
		IsReconnect:  0,
		ProtoVersion: localVersion,
		ProtoFlags:   localFlags,
	}

	header := &protocol.Header{
		Sender:      protocol.TEMP_UUID,
		Accepter:    protocol.ADMIN_UUID,
		MessageType: protocol.HI,
		RouteLen:    uint32(len([]byte(protocol.TEMP_ROUTE))),
		Route:       protocol.TEMP_ROUTE,
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			logger.Warnf("accept connection failed: %v", err)
			continue
		}

		if userOptions.TlsEnable {
			var tlsConfig *tls.Config
			tlsConfig, err = transport.NewServerTLSConfig(userOptions.PreAuthToken, userOptions.Domain)
			if err != nil {
				conn.Close()
				return nil, "", nil, runtimeerr.Wrap(err, "AGENT_TLS_CONFIG", runtimeerr.SeverityError, false, "prepare tls server")
			}
			conn = transport.WrapTLSServerConn(conn, tlsConfig)
		}

		param := new(protocol.NegParam)
		param.Conn = conn
		proto := protocol.NewUpProto(param)
		if err := proto.SNegotiate(); err != nil {
			conn.Close()
			return nil, "", nil, runtimeerr.Wrap(err, "AGENT_NEGOTIATE", runtimeerr.SeverityError, true, "downstream negotiate failed")
		}
		conn = param.Conn

		ok, err := PassivePreAuthOrProxy(conn, userOptions.PreAuthToken, userOptions.ReusePort, 2*time.Second)
		if err != nil {
			conn.Close()
			continue
		}
		if !ok {
			// 非 Shepherd 流量已经被代理转发。
			continue
		}

		rMessage = protocol.NewUpMsgWithTransport(conn, handshakeSecret, protocol.TEMP_UUID, userOptions.Upstream)
		fHeader, fMessage, err := protocol.DestructMessage(rMessage)

		if err != nil {
			logger.Warnf("failed to initialize connection from %s: %v", conn.RemoteAddr().String(), err)
			conn.Close()
			continue
		}

		if fHeader.MessageType == protocol.HI {
			mmess := fMessage.(*protocol.HIMess)
			if handshake.ValidGreeting(handshake.RoleAgent, mmess.Greeting) && mmess.IsAdmin == 1 {
				negotiation := protocol.Negotiate(localVersion, localFlags, mmess.ProtoVersion, mmess.ProtoFlags)
				if strings.ToLower(userOptions.Upstream) == "http" && negotiation.Flags&protocol.FlagSupportChunked == 0 {
					conn.Close()
					logger.Warnf("incoming connection does not support HTTP chunked transfer")
					continue
				}
				sMessage = protocol.NewUpMsgWithTransport(conn, handshakeSecret, protocol.TEMP_UUID, userOptions.Upstream)
				hiResp := *hiMess
				hiResp.ProtoVersion = negotiation.Version
				hiResp.ProtoFlags = negotiation.Flags
				protocol.ConstructMessage(sMessage, header, &hiResp, false)
				sMessage.SendMessage()
				userOptions.Secret = handshake.SessionSecret(baseSecret, userOptions.TlsEnable)
				uuid, err := achieveUUID(conn, userOptions.Secret, userOptions.Upstream)
				if err != nil {
					return nil, "", nil, err
				}
				return conn, uuid, &negotiation, nil
			}
		}

		conn.Close()
		logger.Warnf("incoming connection seems illegal")
	}
}

// 非 Shepherd 流量转交至真实端口
func ProxyStream(conn net.Conn, message []byte, report string) {
	reuseAddr := fmt.Sprintf("127.0.0.1:%s", report)

	reuseConn, err := net.Dial("tcp", reuseAddr)

	if err != nil {
		logger.Errorf("proxy stream dial %s failed: %v", reuseAddr, err)
		return
	}
	// 回写已读取的数据
	if err := utils.WriteFull(reuseConn, message); err != nil {
		logger.Errorf("failed to replay buffered data to reuse connection: %v", err)
		reuseConn.Close()
		conn.Close()
		return
	}

	go CopyTraffic(conn, reuseConn)
	CopyTraffic(reuseConn, conn)
}

func CopyTraffic(input, output net.Conn) {
	defer input.Close()

	buf := make([]byte, 10240)

	for {
		count, err := input.Read(buf)
		if err != nil {
			if err == io.EOF && count > 0 {
				if err := utils.WriteFull(output, buf[:count]); err != nil {
					logger.Warnf("failed to flush tail segment: %v", err)
				}
			}
			break
		}
		if count > 0 {
			if err := utils.WriteFull(output, buf[:count]); err != nil {
				logger.Errorf("failed to proxy traffic: %v", err)
				output.Close()
				break
			}
		}
	}
}
