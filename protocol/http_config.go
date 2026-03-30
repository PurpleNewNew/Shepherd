package protocol

import "strings"

// HTTPConfig 描述传输层使用的 HTTP 封装偏好。
type HTTPConfig struct {
	Host      string
	Path      string
	UserAgent string
	ChunkSize int
}

const (
	defaultHTTPPath      = "/upload"
	defaultHTTPUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
	defaultHTTPChunkSize = 8192
)

var (
	httpUpConfig   HTTPConfig
	httpDownConfig HTTPConfig
)

func init() {
	ConfigureHTTP(HTTPConfig{}, HTTPConfig{})
}

// ConfigureHTTP 设置上下游流量使用的 HTTP 封装配置，零值会被合理默认值替换。
func ConfigureHTTP(up, down HTTPConfig) {
	httpUpConfig = normalizeHTTPConfig(up)
	httpDownConfig = normalizeHTTPConfig(down)
}

func normalizeHTTPConfig(cfg HTTPConfig) HTTPConfig {
	if cfg.Path == "" {
		cfg.Path = defaultHTTPPath
	} else if !strings.HasPrefix(cfg.Path, "/") {
		cfg.Path = "/" + cfg.Path
	}
	if cfg.UserAgent == "" {
		cfg.UserAgent = defaultHTTPUserAgent
	}
	if cfg.ChunkSize <= 0 {
		cfg.ChunkSize = defaultHTTPChunkSize
	}
	return cfg
}

func upstreamHTTPConfig() HTTPConfig {
	return httpUpConfig
}

func downstreamHTTPConfig() HTTPConfig {
	return httpDownConfig
}
