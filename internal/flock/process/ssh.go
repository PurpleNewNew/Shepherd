package process

import (
	"context"
	"io"
	"time"

	"codeberg.org/agnoie/shepherd/internal/flock/manager"
	"codeberg.org/agnoie/shepherd/pkg/utils"
	"codeberg.org/agnoie/shepherd/protocol"

	"golang.org/x/crypto/ssh"
)

const (
	UPMETHOD = iota
	CERMETHOD
)

type SSH struct {
	stdin       io.Writer
	stdout      io.Reader
	sshHost     *ssh.Session
	Method      int
	Addr        string
	Username    string
	Password    string
	Certificate []byte
	mgr         *manager.Manager
	// SSH 输出绑定到 stream 时使用的状态。
	streamID uint32
	txSeq    uint32
}

func newSSH(mgr *manager.Manager) *SSH {
	ssh := new(SSH)
	ssh.mgr = mgr
	return ssh
}

// SSHReqMsg 是本地 SSH 请求结构，不经过协议层。
type SSHReqMsg struct {
	Method      uint16
	Addr        string
	Username    string
	Password    string
	Certificate []byte
}

// SSHReqWithStream 允许附带 streamID 来承载 SSH 输出。
type SSHReqWithStream struct {
	Req      *SSHReqMsg
	StreamID uint32
}

// StartSSH 启动ssh
func (mySSH *SSH) start() {
	var authPayload ssh.AuthMethod
	var err error

	defer func() {
		if mySSH.streamID != 0 {
			code := uint16(0)
			reason := "ok"
			if err != nil {
				code = 1
				reason = err.Error()
			}
			sendStreamClose(mySSH.mgr, mySSH.streamID, code, reason)
		}
	}()

	switch mySSH.Method {
	case UPMETHOD:
		authPayload = ssh.Password(mySSH.Password)
	case CERMETHOD:
		var key ssh.Signer
		key, err = ssh.ParsePrivateKey(mySSH.Certificate)
		if err != nil {
			return
		}
		authPayload = ssh.PublicKeys(key)
	}

	sshDial, err := ssh.Dial("tcp", mySSH.Addr, &ssh.ClientConfig{
		User:            mySSH.Username,
		Auth:            []ssh.AuthMethod{authPayload},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         10 * time.Second,
	})
	if err != nil {
		return
	}

	mySSH.sshHost, err = sshDial.NewSession()
	if err != nil {
		return
	}

	mySSH.stdout, err = mySSH.sshHost.StdoutPipe()
	if err != nil {
		return
	}

	mySSH.stdin, err = mySSH.sshHost.StdinPipe()
	if err != nil {
		return
	}

	mySSH.sshHost.Stderr = mySSH.sshHost.Stdout

	modes := ssh.TerminalModes{
		ssh.ECHO:          1,
		ssh.TTY_OP_ISPEED: 14400,
		ssh.TTY_OP_OSPEED: 14400,
	}

	var term string

	switch utils.CheckSystem() {
	case 0x01:
		term = ""
	case 0x02:
		term = "linux"
	case 0x03:
		term = "xterm"
	}

	err = mySSH.sshHost.RequestPty(term, 25, 80, modes)
	if err != nil {
		return
	}

	err = mySSH.sshHost.Shell()
	if err != nil {
		return
	}

	buffer := make([]byte, 4096)
	for {
		length, err := mySSH.stdout.Read(buffer)

		if err != nil {
			// 通知会话结束
			if mySSH.streamID != 0 {
				sendStreamClose(mySSH.mgr, mySSH.streamID, 0, "ok")
			}
			return
		}

		chunk := buffer[:length]
		if mySSH.streamID != 0 {
			mySSH.txSeq++
			sendStreamData(mySSH.mgr, mySSH.streamID, mySSH.txSeq, chunk)
			continue
		}
	}
}

// WriteCommand 写入命令
func (mySSH *SSH) input(command string) {
	if mySSH.stdin == nil {
		return
	}
	if _, err := mySSH.stdin.Write([]byte(command)); err != nil {
		agentWarnRuntime(mySSH.mgr, "AGENT_SSH_WRITE_STDIN", true, err, "failed to write ssh command to stdin")
	}
}

func DispatchSSHMess(ctx context.Context, mgr *manager.Manager) {
	var mySSH *SSH

	for {
		var message interface{}
		select {
		case <-ctx.Done():
			return
		case message = <-mgr.SSHManager.SSHMessChan:
		}

		switch mess := message.(type) {
		case *SSHReqWithStream:
			if mess.Req == nil {
				continue
			}
			mySSH = newSSH(mgr)
			mySSH.Addr = mess.Req.Addr
			mySSH.Method = int(mess.Req.Method)
			mySSH.Username = mess.Req.Username
			mySSH.Password = mess.Req.Password
			mySSH.Certificate = mess.Req.Certificate
			mySSH.streamID = mess.StreamID
			go mySSH.start()
		case *SSHReqMsg:
			mySSH = newSSH(mgr)
			mySSH.Addr = mess.Addr
			mySSH.Method = int(mess.Method)
			mySSH.Username = mess.Username
			mySSH.Password = mess.Password
			mySSH.Certificate = mess.Certificate
			go mySSH.start()
		case *protocol.SSHCommand:
			mySSH.input(mess.Command)
		}
	}
}

func sendStreamData(mgr *manager.Manager, streamID uint32, seq uint32, payload []byte) {
	if mgr == nil || streamID == 0 || len(payload) == 0 {
		return
	}
	up, sess, ok := newAgentUpMsgForManager(mgr)
	if !ok || sess == nil {
		return
	}
	header := &protocol.Header{
		Sender:      sess.UUID(),
		Accepter:    protocol.ADMIN_UUID,
		MessageType: uint16(protocol.STREAM_DATA),
		RouteLen:    uint32(len(protocol.TEMP_ROUTE)),
		Route:       protocol.TEMP_ROUTE,
	}
	data := &protocol.StreamData{StreamID: streamID, Seq: seq, Ack: 0, Window: 0, Payload: append([]byte(nil), payload...)}
	protocol.ConstructMessage(up, header, data, false)
	up.SendMessage()
}

func sendStreamClose(mgr *manager.Manager, streamID uint32, code uint16, reason string) {
	if mgr == nil || streamID == 0 {
		return
	}
	up, sess, ok := newAgentUpMsgForManager(mgr)
	if !ok || sess == nil {
		return
	}
	header := &protocol.Header{
		Sender:      sess.UUID(),
		Accepter:    protocol.ADMIN_UUID,
		MessageType: uint16(protocol.STREAM_CLOSE),
		RouteLen:    uint32(len(protocol.TEMP_ROUTE)),
		Route:       protocol.TEMP_ROUTE,
	}
	closeMsg := &protocol.StreamClose{StreamID: streamID, Code: code, Reason: reason}
	protocol.ConstructMessage(up, header, closeMsg, false)
	up.SendMessage()
}
