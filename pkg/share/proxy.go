package share

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"

	"codeberg.org/agnoie/shepherd/pkg/config/defaults"
	"codeberg.org/agnoie/shepherd/pkg/utils"
)

type Proxy interface {
	Dial() (net.Conn, error)
}

type Socks5Proxy struct {
	PeerAddr  string
	ProxyAddr string
	UserName  string
	Password  string
}

func NewSocks5Proxy(peerAddr, proxyAddr, username, password string) *Socks5Proxy {
	proxy := new(Socks5Proxy)
	proxy.PeerAddr = peerAddr
	proxy.ProxyAddr = proxyAddr
	proxy.UserName = username
	proxy.Password = password
	return proxy
}

func (proxy *Socks5Proxy) Dial() (net.Conn, error) {
	var NOT_SUPPORT = errors.New("unknown protocol")
	var WRONG_AUTH = errors.New("wrong auth method")
	var SERVER_ERROR = errors.New("proxy server error")
	var TOO_LONG = errors.New("user/pass too long(max 255)")
	var AUTH_FAIL = errors.New("wrong username/password")
	var HOST_TOO_LONG = errors.New("host too long(max 255)")

	proxyConn, err := net.Dial("tcp", proxy.ProxyAddr)
	if err != nil {
		return proxyConn, err
	}
	// Apply a short overall deadline to proxy negotiation to avoid hangs
	_ = proxyConn.SetDeadline(time.Now().Add(defaults.ProxyHandshakeDeadline))

	host, portS, err := net.SplitHostPort(proxy.PeerAddr)
	if err != nil {
		return proxyConn, err
	}
	portUint64, err := strconv.ParseUint(portS, 10, 16)
	if err != nil {
		return proxyConn, err
	}

	port := uint16(portUint64)
	portB := make([]byte, 2)
	binary.BigEndian.PutUint16(portB, port)
	// 无需认证
	if proxy.UserName == "" && proxy.Password == "" {
		if err := utils.WriteFull(proxyConn, []byte{0x05, 0x01, 0x00}); err != nil {
			proxyConn.Close()
			return nil, err
		}
	} else {
		// 使用用户名与密码认证
		if err := utils.WriteFull(proxyConn, []byte{0x05, 0x01, 0x02}); err != nil {
			proxyConn.Close()
			return nil, err
		}
	}

	authWayBuf := make([]byte, 2)

	_, err = io.ReadFull(proxyConn, authWayBuf)
	if err != nil {
		return proxyConn, err
	}

	if authWayBuf[0] == 0x05 {
		switch authWayBuf[1] {
		case 0x00:
		case 0x02:
			userLen := len(proxy.UserName)
			passLen := len(proxy.Password)
			if userLen > 255 || passLen > 255 {
				return proxyConn, TOO_LONG
			}

			buff := make([]byte, 0, 3+userLen+passLen)
			buff = append(buff, 0x01, byte(userLen))
			buff = append(buff, []byte(proxy.UserName)...)
			buff = append(buff, byte(passLen))
			buff = append(buff, []byte(proxy.Password)...)
			if err := utils.WriteFull(proxyConn, buff); err != nil {
				proxyConn.Close()
				return nil, err
			}

			responseBuf := make([]byte, 2)
			_, err = io.ReadFull(proxyConn, responseBuf)
			if err != nil {
				proxyConn.Close()
				return nil, err
			}

			if responseBuf[0] == 0x01 {
				if responseBuf[1] == 0x00 {
					break
				} else {
					return proxyConn, AUTH_FAIL
				}
			} else {
				return proxyConn, NOT_SUPPORT
			}
		case 0xff:
			return proxyConn, WRONG_AUTH
		default:
			return proxyConn, NOT_SUPPORT
		}

		var (
			buff      []byte
			addrBytes []byte
			atyp      byte
		)

		if ip := net.ParseIP(host); ip != nil {
			if v4 := ip.To4(); v4 != nil {
				atyp = 0x01
				addrBytes = v4
			} else {
				atyp = 0x04
				addrBytes = ip.To16()
			}
			buff = make([]byte, 0, 4+len(addrBytes)+2)
			buff = append(buff, 0x05, 0x01, 0x00, atyp)
			buff = append(buff, addrBytes...)
		} else {
			hostBytes := []byte(host)
			if len(hostBytes) == 0 || len(hostBytes) > 255 {
				return proxyConn, HOST_TOO_LONG
			}
			atyp = 0x03
			buff = make([]byte, 0, 5+len(hostBytes)+2)
			buff = append(buff, 0x05, 0x01, 0x00, atyp, byte(len(hostBytes)))
			buff = append(buff, hostBytes...)
		}

		buff = append(buff, portB...)
		if err := utils.WriteFull(proxyConn, buff); err != nil {
			proxyConn.Close()
			return nil, err
		}

		respBuf := make([]byte, 4)
		_, err = io.ReadFull(proxyConn, respBuf)
		if err != nil {
			proxyConn.Close()
			return nil, err
		}
		if respBuf[0] == 0x05 {
			if respBuf[1] != 0x00 {
				return proxyConn, SERVER_ERROR
			}
			switch respBuf[3] {
			case 0x01:
				resultBuf := make([]byte, 6)
				_, err = io.ReadFull(proxyConn, resultBuf)
			case 0x03:
				lenBuf := make([]byte, 1)
				if _, err = io.ReadFull(proxyConn, lenBuf); err != nil {
					return proxyConn, err
				}
				hostLen := int(lenBuf[0])
				if hostLen > 0 {
					hostBuf := make([]byte, hostLen)
					if _, err = io.ReadFull(proxyConn, hostBuf); err != nil {
						return proxyConn, err
					}
				}
				portBuf := make([]byte, 2)
				_, err = io.ReadFull(proxyConn, portBuf)
			case 0x04:
				resultBuf := make([]byte, 18)
				_, err = io.ReadFull(proxyConn, resultBuf)
			default:
				return proxyConn, NOT_SUPPORT
			}
			if err != nil {
				return proxyConn, err
			}

			// Clear deadline after successful negotiation
			_ = proxyConn.SetDeadline(time.Time{})
			return proxyConn, nil
		} else {
			proxyConn.Close()
			return nil, NOT_SUPPORT
		}
	} else {
		proxyConn.Close()
		return nil, NOT_SUPPORT
	}
}

type HTTPProxy struct {
	PeerAddr  string
	ProxyAddr string
}

func NewHTTPProxy(peerAddr, proxyAddr string) *HTTPProxy {
	proxy := new(HTTPProxy)
	proxy.PeerAddr = peerAddr
	proxy.ProxyAddr = proxyAddr
	return proxy
}

func (proxy *HTTPProxy) Dial() (net.Conn, error) {
	var SERVER_ERROR = errors.New("proxy server error")
	var RESPONSE_TOO_LARGE = errors.New("http connect response is too large > 40KB")

	proxyConn, err := net.Dial("tcp", proxy.ProxyAddr)
	if err != nil {
		return proxyConn, SERVER_ERROR
	}
	// Bound CONNECT handshake by a deadline
	_ = proxyConn.SetDeadline(time.Now().Add(defaults.ProxyHandshakeDeadline))

	var http_proxy_payload_template = "CONNECT %s HTTP/1.1\r\n" +
		"Content-Length: 0\r\n\r\n"
	var payload = fmt.Sprintf(http_proxy_payload_template, proxy.PeerAddr)
	var buf = []byte(payload)

	if err := utils.WriteFull(proxyConn, buf); err != nil {
		proxyConn.Close()
		return nil, err
	}

	var done = "\r\n\r\n"
	var success = "HTTP/1.1 200"
	var begin = 0
	var resultBuf = make([]byte, 40960)

	for {
		count, err := proxyConn.Read(resultBuf[begin:])
		if err != nil {
			return proxyConn, SERVER_ERROR
		}

		begin += count
		if begin >= 40960 {
			return proxyConn, RESPONSE_TOO_LARGE
		}

		if string(resultBuf[begin-4:begin]) == done {
			if string(resultBuf[:len(success)]) == success {
				_ = proxyConn.SetDeadline(time.Time{})
				return proxyConn, nil
			}
			return proxyConn, SERVER_ERROR
		}
	}
}
