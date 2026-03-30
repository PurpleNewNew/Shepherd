# Shepherd 现有问题梳理（不引入新功能）

## 1. gRPC / 认证面（已修复）
- 已修复：UI gRPC 新增 bootstrap 拦截器，`ui-grpc-token` 会被强制校验（见 `cmd/kelpie/main.go`）。
- 已缓解：握手问候改为随机词库并校验，固定指纹降低。
- 已修复：TLS 证书校验已实现 TOFU 模式，Stockman 从 Token 自动派生证书作为信任根，无需手动配置 CA。

## 2. Dataplane（TLS 已修复）
- 已切换为 TCP/TCPS 帧协议，HTTP 路径与落盘存储移除。
- 传输增加 per-stream `MaxRate/MaxSize` 限制，失败可按配置次数自动重试（令牌回收），抖动下不再一次性烧 token。
- 已修复：Stockman 客户端现已从 Token 派生证书并自动校验，MITM 风险已消除。
- 块大小 AIMD 自适应有限，尚无 RTT 反馈的带宽估计或多流公平调度。

## 3. STREAM / DTN 传输
- STREAM 拥塞控制升级为“慢启动 + AIMD”，较原固定窗口更能随 RTT 吞吐提升；但仍缺乏跨流公平与显式 RTT/丢包反馈指标。
- `applyDTNPayload` 仍无长度/速率限制，可被滥用刷日志或耗内存。

## 4. 拓扑 / 补链
- 调度队列 64 满即丢弃，仅打印 Warning，无回压/重试。

## 5. 可靠性 / 生命周期
- UI WatchEvents 出错仅日志，不自动重连。ProxyStreamHandle 队列无上限，慢流可能导致 Stockman OOM。
- repair 监听端口 40k–60k 暴露，仅靠 pre-auth，无 IP/频率保护；token 泄露可被第三方接管。
- SO_REUSE/IPTABLES 复用模式在握手前可透传任意流量到本地端口，存在短暂“任意转发”窗口。

## 6. 审计 / 日志
- 补链 drop、STREAM 超时、dataplane 令牌发放/消费等关键事件未进入审计，事故难以追溯。

## 7. PluginHost（已移除）
- 本分支已移除 Stockman 侧 Python 插件系统（PluginHost/PluginManager/IPC 等），相关安全边界问题不再适用。

## 8. 其他代码质量 / 实现缺陷
- Stockman dataplane 现走 TCP 帧，文件写入已校验返回值；TLS 校验已修复（TOFU 模式）。
- ProxyStreamHandle 队列无上限，WaitForData 无超时，消费端阻塞会拖垮线程/内存。
- STREAM ACK 始终 `Credit=0`，发送端窗口固定；长时延/高丢包时吞吐低且易触发固定超时。
- 补链队列溢出仅 Warning，未计入审计/指标，掉队情况难被运维察觉。
- repair 监听/握手错误路径只 Warn，不入审计，外部探测/爆破难追溯。
