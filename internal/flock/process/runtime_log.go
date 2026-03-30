package process

import (
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"syscall"

	"codeberg.org/agnoie/shepherd/internal/flock/manager"
	"codeberg.org/agnoie/shepherd/pkg/utils/runtimeerr"
	"codeberg.org/agnoie/shepherd/protocol"
)

func formatAgentRuntimeMessage(r *runtimeerr.RuntimeError) string {
	if r == nil {
		return ""
	}
	severity := strings.ToUpper(string(r.Severity))
	msg := fmt.Sprintf("[%s][%s] %s (retryable=%v)", severity, r.Code, r.Message, r.Retryable)
	if r.Err != nil {
		msg += ": " + r.Err.Error()
	}
	return msg
}

func agentWarnRuntime(mgr *manager.Manager, code string, retryable bool, err error, format string, args ...interface{}) {
	if isBenignNetClose(err) {
		return
	}
	var rerr *runtimeerr.RuntimeError
	if err != nil {
		rerr = runtimeerr.Wrap(err, code, runtimeerr.SeverityWarn, retryable, format, args...)
	} else {
		rerr = runtimeerr.New(code, runtimeerr.SeverityWarn, retryable, format, args...)
	}
	logger.Warnf("%s", formatAgentRuntimeMessage(rerr))
	sendRuntimeLogToAdmin(mgr, rerr)
}

// isBenignNetClose filters common close-related errors that are expected during
// shutdown (EOF, use of closed network connection, broken pipe, reset by peer).
func isBenignNetClose(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return true
	}
	if errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNRESET) {
		return true
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "use of closed network connection") ||
		strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "connection reset by peer") {
		return true
	}
	return false
}

func sendRuntimeLogToAdmin(mgr *manager.Manager, rerr *runtimeerr.RuntimeError) {
	if mgr == nil || rerr == nil {
		return
	}
	msg, sess, ok := newAgentUpMsgForManager(mgr)
	if !ok {
		return
	}
	uuid := sess.UUID()
	header := &protocol.Header{
		Sender:      uuid,
		Accepter:    protocol.ADMIN_UUID,
		MessageType: protocol.RUNTIMELOG,
		RouteLen:    uint32(len([]byte(protocol.TEMP_ROUTE))),
		Route:       protocol.TEMP_ROUTE,
	}
	severity := strings.ToUpper(string(rerr.Severity))
	logPayload := &protocol.RuntimeLog{
		UUIDLen:     uint16(len(uuid)),
		UUID:        uuid,
		SeverityLen: uint16(len(severity)),
		Severity:    severity,
		CodeLen:     uint16(len(rerr.Code)),
		Code:        rerr.Code,
		MessageLen:  uint64(len(rerr.Message)),
		Message:     rerr.Message,
		Retryable:   boolToUint16(rerr.Retryable),
	}
	if rerr.Err != nil {
		cause := rerr.Err.Error()
		logPayload.CauseLen = uint64(len(cause))
		logPayload.Cause = cause
	}
	protocol.ConstructMessage(msg, header, logPayload, false)
	msg.SendMessage()
}

func boolToUint16(v bool) uint16 {
	if v {
		return 1
	}
	return 0
}

// WarnRuntime 对外暴露代理侧的运行时日志能力。
func WarnRuntime(mgr *manager.Manager, code string, retryable bool, err error, format string, args ...interface{}) {
	agentWarnRuntime(mgr, code, retryable, err, format, args...)
}
