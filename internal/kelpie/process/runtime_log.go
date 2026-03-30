package process

import (
	"fmt"
	"strings"

	"codeberg.org/agnoie/shepherd/internal/kelpie/printer"
	"codeberg.org/agnoie/shepherd/protocol"
)

func printRuntimeLog(entry string, log *protocol.RuntimeLog) {
	if log == nil {
		return
	}
	severity := strings.ToUpper(log.Severity)
	message := fmt.Sprintf("[%s][%s] %s (retryable=%v)", severity, log.Code, log.Message, log.Retryable != 0)
	if log.CauseLen > 0 && log.Cause != "" {
		message += ": " + log.Cause
	}
	prefix := ""
	if entry != "" {
		prefix = fmt.Sprintf("[entry %s] ", shortRuntimeUUID(entry))
	}
	if log.UUID != "" {
		printer.Warning("\r\n%s[agent %s] %s\r\n", prefix, shortRuntimeUUID(log.UUID), message)
	} else {
		printer.Warning("\r\n%s%s\r\n", prefix, message)
	}
}

func shortRuntimeUUID(id string) string {
	if len(id) <= 8 {
		return id
	}
	return id[:8]
}
