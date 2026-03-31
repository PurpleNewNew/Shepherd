# 实验与复现（Experiments）

本仓库包含可运行的三组件实现：Kelpie（管理端/服务端）、Flock（代理端）与 Stockman（Qt6 GUI）。
根据开题报告 `docs/paper.md` 的研究交付物要求，本目录提供三类“可复现材料”的落点：

- **Trace 回放**：可重复的工作负载/接触模式，用于本机可复现实验与数据采集
- **仿真/仿真骨架**：Mininet（仿真/网络仿真）与 ns-3（离散事件模拟）的脚手架
- **形式化验证骨架**：见 `formal/`（Tamarin/ProVerif）

## 快速开始（本机 Trace 回放）

Trace 回放是最实用、跨平台且最容易获得实验数据的路径：在本机启动一个 mini-cluster，并由脚本驱动控制面事件。

1. 构建 Go 二进制

```sh
make admin agent
```

2. 运行 Trace 回放器（默认星型拓扑）

```sh
go run ./experiments/trace_replay --nodes 5 --duration 60s --topology star
```

或链式多跳拓扑：

```sh
go run ./experiments/trace_replay --nodes 5 --duration 60s --topology chain
```

运行输出写入 `experiments/out/`（已在 `.gitignore` 中忽略，不会提交到版本库）。

## 工程回归（推荐）

项目内置两类“工程回归”入口：

- 全量回归（跑全部 traces，star+chain，一次一遍）：`make regress`
- Soak 压测（跑高风险 traces，多次重复，失败自动打包留档）：`make soak`（可用 `SOAK_REPEAT=50` 加大强度）

## 子目录说明

- `experiments/trace_replay/`：可运行的回放器、trace 格式与示例 trace
- `experiments/analysis/`：将 `metrics.jsonl` 转为 CSV/并做后处理的脚本
- `experiments/mininet/`：Mininet 仿真骨架（更偏 Linux 环境）
- `experiments/ns3/`：ns-3 仿真骨架（占位与路线说明）
