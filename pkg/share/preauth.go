package share

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"codeberg.org/agnoie/shepherd/pkg/utils"
)

const (
	preAuthTimeout     = 10 * time.Second
	preAuthPepper      = "shepherd/preauth/v2"
	preAuthNonceSize   = 32
	preAuthTokenSize   = 32
	preAuthClientLabel = "shepherd/preauth/client"
	preAuthServerLabel = "shepherd/preauth/server"
)

// GeneratePreAuthToken 根据密钥生成固定长度的预认证口令。
func GeneratePreAuthToken(secret string) string {
	if secret == "" {
		panic("preauth: secret must not be empty")
	}
	digest := sha256.Sum256([]byte(preAuthPepper + secret))
	token := hex.EncodeToString(digest[:])
	if len(token) <= preAuthTokenSize {
		return token
	}
	return token[:preAuthTokenSize]
}

func ActivePreAuth(conn net.Conn, token string) error {
	if conn == nil {
		return errors.New("preauth: nil connection")
	}
	if len(token) == 0 {
		return errors.New("preauth: empty token")
	}

	clientNonce := make([]byte, preAuthNonceSize)
	if err := fillRandom(clientNonce); err != nil {
		conn.Close()
		return err
	}
	clientMAC := computeClientMAC(token, clientNonce)

	defer conn.SetReadDeadline(time.Time{})
	if err := conn.SetReadDeadline(time.Now().Add(preAuthTimeout)); err != nil {
		conn.Close()
		return err
	}

	payload := make([]byte, preAuthNonceSize+len(clientMAC))
	copy(payload, clientNonce)
	copy(payload[preAuthNonceSize:], clientMAC)
	if err := conn.SetWriteDeadline(time.Now().Add(preAuthTimeout)); err != nil {
		conn.Close()
		return err
	}
	defer conn.SetWriteDeadline(time.Time{})
	if err := utils.WriteFull(conn, payload); err != nil {
		conn.Close()
		return err
	}

	response := make([]byte, preAuthNonceSize+sha256.Size)
	if _, err := io.ReadFull(conn, response); err != nil {
		if timeoutErr, ok := err.(net.Error); ok && timeoutErr.Timeout() {
			conn.Close()
			return errors.New("preauth: connection timeout")
		}
		conn.Close()
		return err
	}

	serverNonce := response[:preAuthNonceSize]
	serverMAC := response[preAuthNonceSize:]
	expected := computeServerMAC(token, serverNonce, clientNonce)
	if !hmac.Equal(serverMAC, expected) {
		conn.Close()
		return errors.New("preauth: invalid server response")
	}

	return nil
}

func PassivePreAuth(conn net.Conn, token string) error {
	if conn == nil {
		return errors.New("preauth: nil connection")
	}
	if len(token) == 0 {
		return errors.New("preauth: empty token")
	}

	defer conn.SetReadDeadline(time.Time{})
	if err := conn.SetReadDeadline(time.Now().Add(preAuthTimeout)); err != nil {
		conn.Close()
		return err
	}

	request := make([]byte, preAuthNonceSize+sha256.Size)
	if _, err := io.ReadFull(conn, request); err != nil {
		if timeoutErr, ok := err.(net.Error); ok && timeoutErr.Timeout() {
			conn.Close()
			return errors.New("preauth: connection timeout")
		}
		conn.Close()
		return err
	}

	clientNonce := request[:preAuthNonceSize]
	clientMAC := request[preAuthNonceSize:]
	expected := computeClientMAC(token, clientNonce)
	if !hmac.Equal(clientMAC, expected) {
		conn.Close()
		return errors.New("preauth: invalid client token")
	}

	serverNonce := make([]byte, preAuthNonceSize)
	if err := fillRandom(serverNonce); err != nil {
		conn.Close()
		return err
	}
	serverMAC := computeServerMAC(token, serverNonce, clientNonce)

	response := make([]byte, preAuthNonceSize+len(serverMAC))
	copy(response, serverNonce)
	copy(response[preAuthNonceSize:], serverMAC)
	if err := conn.SetWriteDeadline(time.Now().Add(preAuthTimeout)); err != nil {
		conn.Close()
		return err
	}
	defer conn.SetWriteDeadline(time.Time{})
	if err := utils.WriteFull(conn, response); err != nil {
		conn.Close()
		return err
	}

	return nil
}

func fillRandom(buf []byte) error {
	if _, err := io.ReadFull(rand.Reader, buf); err != nil {
		return fmt.Errorf("preauth: entropy source failure: %w", err)
	}
	return nil
}

func computeClientMAC(token string, nonce []byte) []byte {
	return computeMAC(token, []byte(preAuthClientLabel), nonce)
}

func computeServerMAC(token string, serverNonce, clientNonce []byte) []byte {
	return computeMAC(token, []byte(preAuthServerLabel), serverNonce, clientNonce)
}

func computeMAC(token string, parts ...[]byte) []byte {
	mac := hmac.New(sha256.New, []byte(token))
	for _, part := range parts {
		mac.Write(part)
	}
	return mac.Sum(nil)
}
