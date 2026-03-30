package dataplane

import (
	"context"
	"io"
)

// Config 定义 dataplane 运行时参数（TCP 数据面与可选 SOCKS 代理）。
type Config struct {
	Listen       string
	EnableTLS    bool
	TLSCert      string
	TLSKey       string
	TLSClientCA  string
	MaxRateBps   float64
	MaxSize      int64
	TokenRetries int
	Admin        AdminBridge
	// Optional completion hook：在数据通路完成后回调，用于审计/统计。
	CompleteHook func(meta TokenMeta, bytes int64, err error)
}

// AdminBridge 抽象 process.Admin 需要的最小接口，便于测试。
type AdminBridge interface {
	OpenStream(ctx context.Context, target, sessionID string, meta map[string]string) (io.ReadWriteCloser, error)
}
