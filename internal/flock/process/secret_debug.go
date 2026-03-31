package process

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
)

func handshakeDebugEnabled() bool {
	return os.Getenv("SHEPHERD_DEBUG_HANDSHAKE") != ""
}

func secretFingerprint(secret string) string {
	if secret == "" {
		return "empty"
	}
	sum := sha256.Sum256([]byte(secret))
	// 8 字节转成 16 个十六进制字符，足以关联密钥而不至于泄露完整内容。
	return hex.EncodeToString(sum[:8])
}
