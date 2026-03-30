package share

import (
	"crypto/sha256"
	"encoding/hex"
	"io"

	"golang.org/x/crypto/hkdf"
)

const (
	secretDeriveSalt = "shepherd/secret/v1"
	sessionInfoBase  = "shepherd/session"
)

// DeriveSessionSecret 根据基础密钥派生会话阶段使用的独立口令。
// 通过 HKDF 保证不同阶段互不影响，TLS 与否会作为额外信息参与派生。
func DeriveSessionSecret(base string, tlsEnabled bool) string {
	if base == "" {
		return ""
	}
	infoSuffix := "plain"
	if tlsEnabled {
		infoSuffix = "tls"
	}
	info := sessionInfoBase + "/" + infoSuffix
	kdf := hkdf.New(sha256.New, []byte(base), []byte(secretDeriveSalt), []byte(info))
	buf := make([]byte, 32)
	if _, err := io.ReadFull(kdf, buf); err != nil {
		panic("share: derive session secret failed: " + err.Error())
	}
	return hex.EncodeToString(buf)
}
