package runtimeerr

import (
	"errors"
	"fmt"
)

type Severity string

const (
	SeverityInfo  Severity = "info"
	SeverityWarn  Severity = "warn"
	SeverityError Severity = "error"
	SeverityFatal Severity = "fatal"
)

type RuntimeError struct {
	Severity  Severity
	Retryable bool
	Code      string
	Message   string
	Err       error
}

func (r *RuntimeError) Error() string {
	if r == nil {
		return ""
	}
	base := fmt.Sprintf("[%s][%s] %s", r.Severity, r.Code, r.Message)
	if r.Err != nil {
		return base + ": " + r.Err.Error()
	}
	return base
}

func (r *RuntimeError) Unwrap() error {
	if r == nil {
		return nil
	}
	return r.Err
}

func Wrap(err error, code string, severity Severity, retryable bool, format string, args ...interface{}) *RuntimeError {
	msg := fmt.Sprintf(format, args...)
	return &RuntimeError{
		Severity:  severity,
		Retryable: retryable,
		Code:      code,
		Message:   msg,
		Err:       err,
	}
}

func New(code string, severity Severity, retryable bool, format string, args ...interface{}) *RuntimeError {
	return Wrap(nil, code, severity, retryable, format, args...)
}

func FromError(err error) (*RuntimeError, bool) {
	var rerr *RuntimeError
	if errors.As(err, &rerr) {
		return rerr, true
	}
	return nil, false
}

func Format(err error) string {
	if err == nil {
		return ""
	}
	if rerr, ok := FromError(err); ok {
		return rerr.Error()
	}
	return err.Error()
}
