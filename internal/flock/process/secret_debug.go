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
	// 8 bytes -> 16 hex chars is enough to correlate secrets without leaking them.
	return hex.EncodeToString(sum[:8])
}
