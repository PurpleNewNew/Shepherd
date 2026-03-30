package process

import (
	"errors"
	"fmt"
	"time"

	"codeberg.org/agnoie/shepherd/internal/flock/manager"
	"codeberg.org/agnoie/shepherd/pkg/share"
	"codeberg.org/agnoie/shepherd/pkg/utils"
	"codeberg.org/agnoie/shepherd/protocol"

	"golang.org/x/crypto/ssh"
)

type SSHTunnel struct {
	Method      int
	Addr        string
	Port        string
	Username    string
	Password    string
	Certificate []byte
}

func newSSHTunnel(method int, addr, port, username, password string, certificate []byte) *SSHTunnel {
	sshTunnel := new(SSHTunnel)
	sshTunnel.Method = method
	sshTunnel.Addr = addr
	sshTunnel.Port = port
	sshTunnel.Username = username
	sshTunnel.Password = password
	sshTunnel.Certificate = certificate
	return sshTunnel
}

// NewSSHTunnelForStream 暴露给 stream 入口的便捷构造
func NewSSHTunnelForStream(method int, addr, port, username, password string, certificate []byte) *SSHTunnel {
	return newSSHTunnel(method, addr, port, username, password, certificate)
}

// Start 便捷启动（用于 stream 模式），不会显式回送 SSHTunnelRes，由调用者负责关闭流。
func (sshTunnel *SSHTunnel) Start(mgr *manager.Manager) {
	sshTunnel.start(mgr)
}

func (sshTunnel *SSHTunnel) start(mgr *manager.Manager) {
	var authPayload ssh.AuthMethod
	var err error
	var sUMessage, sLMessage, rMessage protocol.Message

	sess := mgr.ActiveSession()
	sUMessage, sess, ok := newAgentUpMsg(sess)
	if !ok {
		return
	}
	senderUUID := ""
	secret := ""
	if sess != nil {
		senderUUID = sess.UUID()
		secret = sess.Secret()
	}
	if senderUUID == "" && mgr != nil {
		senderUUID = mgr.ActiveUUID()
	}
	if secret == "" && mgr != nil {
		secret = mgr.ActiveSecret()
	}

	defer func() {
		_ = err
	}()

	switch sshTunnel.Method {
	case UPMETHOD:
		authPayload = ssh.Password(sshTunnel.Password)
	case CERMETHOD:
		var key ssh.Signer
		key, err = ssh.ParsePrivateKey(sshTunnel.Certificate)
		if err != nil {
			return
		}
		authPayload = ssh.PublicKeys(key)
	}

	sshDial, err := ssh.Dial("tcp", sshTunnel.Addr, &ssh.ClientConfig{
		User:            sshTunnel.Username,
		Auth:            []ssh.AuthMethod{authPayload},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         10 * time.Second,
	})
	if err != nil {
		return
	}

	conn, err := sshDial.Dial("tcp", fmt.Sprintf("127.0.0.1:%s", sshTunnel.Port))
	if err != nil {
		return
	}

	if mgr == nil {
		return
	}
	token := mgr.PreAuthToken()
	if err = share.ActivePreAuth(conn, token); err != nil {
		return
	}

	sLMessage = protocol.NewDownMsg(conn, secret, protocol.ADMIN_UUID)

	hiHeader := &protocol.Header{
		Sender:      protocol.ADMIN_UUID, // 假装是管理员
		Accepter:    protocol.TEMP_UUID,
		MessageType: protocol.HI,
		RouteLen:    uint32(len([]byte(protocol.TEMP_ROUTE))),
		Route:       protocol.TEMP_ROUTE,
	}

	version := protocol.CurrentProtocolVersion
	flags := protocol.DefaultProtocolFlags
	if sess != nil {
		if v := sess.ProtocolVersion(); v != 0 {
			version = v
		}
		flags = sess.ProtocolFlags()
	}
	protocol.SetMessageMeta(sLMessage, version, flags)

	// 假装是管理员
	hiMess := &protocol.HIMess{
		GreetingLen:  uint16(len("Shhh...")),
		Greeting:     "Shhh...",
		UUIDLen:      uint16(len(protocol.ADMIN_UUID)),
		UUID:         protocol.ADMIN_UUID,
		IsAdmin:      1,
		IsReconnect:  0,
		ProtoVersion: version,
		ProtoFlags:   flags,
	}

	protocol.ConstructMessage(sLMessage, hiHeader, hiMess, false)
	sLMessage.SendMessage()

	rMessage = protocol.NewDownMsg(conn, secret, protocol.ADMIN_UUID)
	fHeader, fMessage, err := protocol.DestructMessage(rMessage)
	if err != nil {
		conn.Close()
		return
	}

	if fHeader.MessageType == protocol.HI {
		mmess := fMessage.(*protocol.HIMess)
		if mmess.Greeting == "Keep slient" && mmess.IsAdmin == 0 {
			if sess != nil {
				if v := sess.ProtocolVersion(); v != 0 {
					version = v
				}
				flags = sess.ProtocolFlags()
			}
			negotiation := protocol.Negotiate(version, flags, mmess.ProtoVersion, mmess.ProtoFlags)
			if !negotiation.IsV1() {
				conn.Close()
				return
			}
			childIP := conn.RemoteAddr().String()

			parentUUID := senderUUID
			if sess != nil && sess.UUID() != "" {
				parentUUID = sess.UUID()
			}

			cUUIDReqHeader := &protocol.Header{
				Sender:      parentUUID,
				Accepter:    protocol.ADMIN_UUID,
				MessageType: protocol.CHILDUUIDREQ,
				RouteLen:    uint32(len([]byte(protocol.TEMP_ROUTE))),
				Route:       protocol.TEMP_ROUTE,
			}

			requestID := utils.GenerateUUID()
			waitCh := mgr.ListenManager.RegisterChildWaiter(requestID)
			defer mgr.ListenManager.CancelChildWaiter(requestID)

			cUUIDMess := &protocol.ChildUUIDReq{
				ParentUUIDLen: uint16(len(parentUUID)),
				ParentUUID:    parentUUID,
				IPLen:         uint16(len(childIP)),
				IP:            childIP,
				RequestIDLen:  uint16(len(requestID)),
				RequestID:     requestID,
			}

			if sess != nil {
				protocol.SetMessageMeta(sUMessage, sess.ProtocolVersion(), sess.ProtocolFlags())
			}
			protocol.ConstructMessage(sUMessage, cUUIDReqHeader, cUUIDMess, false)
			sUMessage.SendMessage()

			childUUID := <-waitCh

			uuidHeader := &protocol.Header{
				Sender:      protocol.ADMIN_UUID,
				Accepter:    protocol.TEMP_UUID,
				MessageType: protocol.UUID,
				RouteLen:    uint32(len([]byte(protocol.TEMP_ROUTE))),
				Route:       protocol.TEMP_ROUTE,
			}

			uuidMess := &protocol.UUIDMess{
				UUIDLen:      uint16(len(childUUID)),
				UUID:         childUUID,
				ProtoVersion: negotiation.Version,
				ProtoFlags:   negotiation.Flags,
			}

			protocol.SetMessageMeta(sLMessage, negotiation.Version, negotiation.Flags)
			protocol.ConstructMessage(sLMessage, uuidHeader, uuidMess, false)
			sLMessage.SendMessage()

			mgr.ChildrenManager.AddChild(childUUID, conn)
			mgr.ChildrenManager.NotifyChild(&manager.ChildInfo{UUID: childUUID, Conn: conn})

			return
		}
	}

	conn.Close()
	err = errors.New("node seems illegal")
}
