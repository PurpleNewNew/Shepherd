package process

import (
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	socksVersion5       = 0x05
	socksAuthNone       = 0x00
	socksAuthUserPass   = 0x02
	socksCmdConnect     = 0x01
	socksAtypIPv4       = 0x01
	socksAtypDomainName = 0x03
	socksAtypIPv6       = 0x04
	socksStatusOK       = 0x00
	socksStatusFail     = 0x01
	socksStatusNoAccept = 0xFF
)

var errNeedMoreSocksData = errors.New("need more socks data")

type socksStage uint8

const (
	socksStageGreeting socksStage = iota
	socksStageAuth
	socksStageRequest
	socksStageRelay
)

type socksStream struct {
	mu sync.Mutex

	stage      socksStage
	authMethod byte
	username   string
	password   string
	buffer     []byte
	conn       net.Conn
}

func (agent *Agent) socksOnOpen(streamID uint32, opts map[string]string) {
	if agent == nil {
		return
	}
	authMethod, username, password, err := parseSocksAuthOptions(opts)
	if err != nil {
		agent.rejectStreamOpen(streamID, err.Error())
		return
	}
	st := &socksStream{
		stage:      socksStageGreeting,
		authMethod: authMethod,
		username:   username,
		password:   password,
	}
	agent.fwdMu.Lock()
	if agent.socksByID == nil {
		agent.socksByID = make(map[uint32]*socksStream)
	}
	agent.socksByID[streamID] = st
	agent.fwdMu.Unlock()
}

func (agent *Agent) socksOnData(streamID uint32, data []byte) {
	if agent == nil || len(data) == 0 {
		return
	}
	agent.fwdMu.Lock()
	st := agent.socksByID[streamID]
	agent.fwdMu.Unlock()
	if st == nil {
		return
	}

	st.mu.Lock()
	if st.stage == socksStageRelay && st.conn != nil {
		conn := st.conn
		st.mu.Unlock()
		if _, err := conn.Write(data); err != nil {
			agent.socksClose(streamID, 1, fmt.Sprintf("socks relay write failed: %v", err))
		}
		return
	}
	st.buffer = append(st.buffer, data...)
	for {
		progressed := false
		switch st.stage {
		case socksStageGreeting:
			method, consumed, err := parseSocksGreeting(st.buffer, st.authMethod)
			if errors.Is(err, errNeedMoreSocksData) {
				st.mu.Unlock()
				return
			}
			if err != nil {
				st.mu.Unlock()
				agent.socksClose(streamID, 1, err.Error())
				return
			}
			st.buffer = st.buffer[consumed:]
			agent.sendStreamData(streamID, []byte{socksVersion5, method})
			if method == socksStatusNoAccept {
				st.mu.Unlock()
				agent.socksClose(streamID, 1, "socks auth mismatch")
				return
			}
			if method == socksAuthUserPass {
				st.stage = socksStageAuth
			} else {
				st.stage = socksStageRequest
			}
			progressed = true

		case socksStageAuth:
			ok, consumed, err := parseSocksUserPass(st.buffer, st.username, st.password)
			if errors.Is(err, errNeedMoreSocksData) {
				st.mu.Unlock()
				return
			}
			if err != nil {
				st.mu.Unlock()
				agent.socksClose(streamID, 1, err.Error())
				return
			}
			st.buffer = st.buffer[consumed:]
			status := byte(0x00)
			if !ok {
				status = 0x01
			}
			agent.sendStreamData(streamID, []byte{0x01, status})
			if !ok {
				st.mu.Unlock()
				agent.socksClose(streamID, 1, "socks auth failed")
				return
			}
			st.stage = socksStageRequest
			progressed = true

		case socksStageRequest:
			host, port, consumed, rep, err := parseSocksConnectRequest(st.buffer)
			if errors.Is(err, errNeedMoreSocksData) {
				st.mu.Unlock()
				return
			}
			if err != nil {
				agent.sendStreamData(streamID, socksReply(rep, nil))
				st.mu.Unlock()
				agent.socksClose(streamID, 1, err.Error())
				return
			}
			st.buffer = st.buffer[consumed:]
			target := net.JoinHostPort(host, strconv.Itoa(int(port)))
			conn, dialErr := net.DialTimeout("tcp", target, 10*time.Second)
			if dialErr != nil {
				agent.sendStreamData(streamID, socksReply(0x05, nil))
				st.mu.Unlock()
				agent.socksClose(streamID, 1, fmt.Sprintf("socks dial failed: %v", dialErr))
				return
			}
			st.conn = conn
			st.stage = socksStageRelay
			leftover := append([]byte(nil), st.buffer...)
			st.buffer = nil
			agent.sendStreamData(streamID, socksReply(socksStatusOK, conn.LocalAddr()))
			st.mu.Unlock()
			go agent.pumpSocksToStream(streamID, conn)
			if len(leftover) > 0 {
				if _, err := conn.Write(leftover); err != nil {
					agent.socksClose(streamID, 1, fmt.Sprintf("socks relay write failed: %v", err))
				}
			}
			return

		case socksStageRelay:
			conn := st.conn
			payload := append([]byte(nil), st.buffer...)
			st.buffer = nil
			st.mu.Unlock()
			if len(payload) > 0 && conn != nil {
				if _, err := conn.Write(payload); err != nil {
					agent.socksClose(streamID, 1, fmt.Sprintf("socks relay write failed: %v", err))
				}
			}
			return
		}
		if !progressed {
			break
		}
	}
	st.mu.Unlock()
}

func (agent *Agent) pumpSocksToStream(streamID uint32, conn net.Conn) {
	if agent == nil || conn == nil {
		return
	}
	buf := make([]byte, 32*1024)
	for {
		n, err := conn.Read(buf)
		if n > 0 {
			agent.sendStreamData(streamID, append([]byte(nil), buf[:n]...))
		}
		if err != nil {
			code := uint16(0)
			reason := "socks closed"
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				code = 1
				reason = err.Error()
			}
			agent.socksClose(streamID, code, reason)
			return
		}
	}
}

func (agent *Agent) socksOnClose(streamID uint32) {
	agent.socksClose(streamID, 0, "socks closed")
}

func (agent *Agent) socksClose(streamID uint32, code uint16, reason string) {
	if agent == nil {
		return
	}
	var (
		conn    net.Conn
		removed bool
	)
	agent.fwdMu.Lock()
	if agent.socksByID != nil {
		if st := agent.socksByID[streamID]; st != nil {
			conn = st.conn
			delete(agent.socksByID, streamID)
			removed = true
		}
	}
	agent.fwdMu.Unlock()

	if conn != nil {
		_ = conn.Close()
	}
	if removed {
		agent.sendStreamClose(streamID, code, reason)
	}
}

func parseSocksAuthOptions(opts map[string]string) (byte, string, string, error) {
	mode := strings.ToLower(strings.TrimSpace(opts["auth"]))
	switch mode {
	case "", "none":
		return socksAuthNone, "", "", nil
	case "userpass":
		username := strings.TrimSpace(opts["username"])
		password := opts["password"]
		if username == "" {
			return 0, "", "", fmt.Errorf("socks username required for userpass auth")
		}
		if password == "" {
			return 0, "", "", fmt.Errorf("socks password required for userpass auth")
		}
		return socksAuthUserPass, username, password, nil
	default:
		return 0, "", "", fmt.Errorf("unsupported socks auth mode: %s", mode)
	}
}

func parseSocksGreeting(buf []byte, expectedAuth byte) (byte, int, error) {
	if len(buf) < 2 {
		return 0, 0, errNeedMoreSocksData
	}
	if buf[0] != socksVersion5 {
		return 0, 0, fmt.Errorf("unsupported socks version: %d", buf[0])
	}
	nMethods := int(buf[1])
	if len(buf) < 2+nMethods {
		return 0, 0, errNeedMoreSocksData
	}
	methods := buf[2 : 2+nMethods]
	for _, method := range methods {
		if method == expectedAuth {
			return expectedAuth, 2 + nMethods, nil
		}
	}
	return socksStatusNoAccept, 2 + nMethods, nil
}

func parseSocksUserPass(buf []byte, username, password string) (bool, int, error) {
	if len(buf) < 2 {
		return false, 0, errNeedMoreSocksData
	}
	if buf[0] != 0x01 {
		return false, 0, fmt.Errorf("unsupported socks auth version: %d", buf[0])
	}
	uLen := int(buf[1])
	if len(buf) < 2+uLen+1 {
		return false, 0, errNeedMoreSocksData
	}
	pLenOffset := 2 + uLen
	pLen := int(buf[pLenOffset])
	total := pLenOffset + 1 + pLen
	if len(buf) < total {
		return false, 0, errNeedMoreSocksData
	}
	u := string(buf[2 : 2+uLen])
	p := string(buf[pLenOffset+1 : total])
	return u == username && p == password, total, nil
}

func parseSocksConnectRequest(buf []byte) (string, uint16, int, byte, error) {
	if len(buf) < 4 {
		return "", 0, 0, socksStatusFail, errNeedMoreSocksData
	}
	if buf[0] != socksVersion5 {
		return "", 0, 0, socksStatusFail, fmt.Errorf("unsupported socks request version: %d", buf[0])
	}
	if buf[1] != socksCmdConnect {
		return "", 0, 0, 0x07, fmt.Errorf("unsupported socks cmd: %d", buf[1])
	}
	atyp := buf[3]
	index := 4
	var host string
	switch atyp {
	case socksAtypIPv4:
		if len(buf) < index+4+2 {
			return "", 0, 0, socksStatusFail, errNeedMoreSocksData
		}
		host = net.IP(buf[index : index+4]).String()
		index += 4
	case socksAtypDomainName:
		if len(buf) < index+1 {
			return "", 0, 0, socksStatusFail, errNeedMoreSocksData
		}
		n := int(buf[index])
		index++
		if len(buf) < index+n+2 {
			return "", 0, 0, socksStatusFail, errNeedMoreSocksData
		}
		host = string(buf[index : index+n])
		index += n
	case socksAtypIPv6:
		if len(buf) < index+16+2 {
			return "", 0, 0, socksStatusFail, errNeedMoreSocksData
		}
		host = net.IP(buf[index : index+16]).String()
		index += 16
	default:
		return "", 0, 0, 0x08, fmt.Errorf("unsupported socks atyp: %d", atyp)
	}
	port := uint16(buf[index])<<8 | uint16(buf[index+1])
	index += 2
	if strings.TrimSpace(host) == "" {
		return "", 0, 0, socksStatusFail, fmt.Errorf("socks destination host missing")
	}
	return host, port, index, socksStatusOK, nil
}

func socksReply(status byte, bound net.Addr) []byte {
	host := net.IPv4zero
	port := uint16(0)
	if tcpAddr, ok := bound.(*net.TCPAddr); ok && tcpAddr != nil {
		if ip4 := tcpAddr.IP.To4(); ip4 != nil {
			host = ip4
		}
		if tcpAddr.Port > 0 && tcpAddr.Port <= 65535 {
			port = uint16(tcpAddr.Port)
		}
	}
	return []byte{
		socksVersion5,
		status,
		0x00,
		socksAtypIPv4,
		host[0], host[1], host[2], host[3],
		byte(port >> 8), byte(port),
	}
}
