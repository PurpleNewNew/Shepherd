# Mininet 网络仿真（骨架）

Mininet 适合在 Linux 环境下对带宽/时延/丢包/分区等网络条件做可重复的“仿真式复现”，并对上层系统（Shepherd）进行端到端评估。

本目录刻意保持为“骨架”：

- 给出一套可复现实验的推荐路线
- 提供一个最小拓扑脚本（可扩展）

## 环境要求

- Linux 主机或 Linux VM（推荐）
- 已安装 `mininet`
- root 权限（Mininet 与 `tc netem` 需要）
- `go`、`make`

## 推荐工作流（建议）

1. 在宿主机编译二进制：

```sh
make admin agent
```

2. 将二进制复制到 Mininet 各 host 可访问的路径（或通过共享目录挂载）。

3. 使用 `tc netem` 在链路上注入网络条件：

- 固定时延/抖动
- 丢包/重复
- 分区（up/down）

4. 用上层脚本驱动“可复现实验场景”：

- `experiments/trace_replay`（控制面 trace 事件：sleep、DTN 入队、故障注入等）
- Kelpie 的 gRPC UI 指标（Topology/Metrics/Supplemental 等）

## 拓扑脚本

`topo.py` 提供一个星型拓扑的最小示例：

- 一个 root host 连接到交换机
- N 个子 host 同样连接该交换机

你可以在此基础上扩展：

- 多跳树（交换机链/分层交换）
- 动态链路调度（基于 trace 将链路 down/up）

