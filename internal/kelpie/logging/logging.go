package logging

import (
	"fmt"
	"strings"

	"codeberg.org/agnoie/shepherd/internal/kelpie/printer"
	"codeberg.org/agnoie/shepherd/pkg/utils/runtimeerr"
)

// RuntimeMessage 将运行时错误转换为可打印的字符串。
func RuntimeMessage(r *runtimeerr.RuntimeError) string {
	if r == nil {
		return ""
	}
	severity := strings.ToUpper(string(r.Severity))
	message := fmt.Sprintf("[%s][%s] %s (retryable=%v)", severity, r.Code, r.Message, r.Retryable)
	if r.Err != nil {
		message += ": " + r.Err.Error()
	}
	return message
}

// Warn 以统一的运行时错误格式记录警告并交由 printer 输出。
func Warn(code string, retryable bool, err error, format string, args ...interface{}) *runtimeerr.RuntimeError {
	var rerr *runtimeerr.RuntimeError
	if err != nil {
		rerr = runtimeerr.Wrap(err, code, runtimeerr.SeverityWarn, retryable, format, args...)
	} else {
		rerr = runtimeerr.New(code, runtimeerr.SeverityWarn, retryable, format, args...)
	}
	printer.Warning("\r\n%s\r\n", RuntimeMessage(rerr))
	return rerr
}
