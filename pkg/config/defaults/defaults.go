package defaults

import "time"

// 管理端与代理端共享的核心超时和缓冲区默认值。
const (
	// TopologyRequestTimeout 是单次拓扑查询允许的最长耗时。
	TopologyRequestTimeout = 5 * time.Second

	// FileSliceSize 定义文件传输时的分片大小（30 KiB）。
	FileSliceSize = 30 * 1024
)
