package logging

import (
	"fmt"
	"log"
	"sync"
	"time"
)

// Level 表示日志条目的严重程度。
// 使用字符串可以兼顾可读性并保持类型区分。
type Level string

const (
	LevelDebug Level = "DEBUG"
	LevelInfo  Level = "INFO"
	LevelWarn  Level = "WARN"
	LevelError Level = "ERROR"
)

// Sink 是日志事件的可插拔目标，实现需要保证并发安全。
type Sink interface {
	Log(level Level, subsystem, message string)
}

// stdSink 是默认实现，会将日志输出到内置 logger。
type stdSink struct{}

func (stdSink) Log(level Level, subsystem, message string) {
	timestamp := time.Now().Format(time.RFC3339)
	if subsystem != "" {
		log.Printf("[%s] %s %s %s", level, timestamp, subsystem, message)
	} else {
		log.Printf("[%s] %s %s", level, timestamp, message)
	}
}

var (
	sinkMu     sync.RWMutex
	activeSink Sink = stdSink{}
)

func setSink(s Sink) {
	sinkMu.Lock()
	defer sinkMu.Unlock()
	if s == nil {
		activeSink = stdSink{}
		return
	}
	activeSink = s
}

func currentSink() Sink {
	sinkMu.RLock()
	defer sinkMu.RUnlock()
	return activeSink
}

// Logger 输出带 subsystem 标识的结构化日志。
type Logger struct {
	subsystem string
}

// New 为指定 subsystem 创建 logger，允许 subsystem 为空。
func New(subsystem string) *Logger {
	return &Logger{subsystem: subsystem}
}

func (l *Logger) logf(level Level, format string, args ...interface{}) {
	sink := currentSink()
	msg := fmt.Sprintf(format, args...)
	sink.Log(level, l.subsystem, msg)
}

// Debugf 输出 debug 级日志。
func (l *Logger) Debugf(format string, args ...interface{}) {
	l.logf(LevelDebug, format, args...)
}

// Infof 输出 info 级日志。
func (l *Logger) Infof(format string, args ...interface{}) {
	l.logf(LevelInfo, format, args...)
}

// Warnf 输出 warn 级日志。
func (l *Logger) Warnf(format string, args ...interface{}) {
	l.logf(LevelWarn, format, args...)
}

// Errorf 输出 error 级日志。
func (l *Logger) Errorf(format string, args ...interface{}) {
	l.logf(LevelError, format, args...)
}
