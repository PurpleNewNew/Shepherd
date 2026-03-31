# Shepherd（毕业设计/研究原型）

Shepherd 是一个面向**受限网络**（高时延、间歇连接、拓扑频繁变化）的“延迟容忍远程运维/控制”研究原型，采用 **Gossip 维护拓扑 + 补链自愈 + DTN（store-carry-forward）消息队列 + 多路复用流（STREAM）** 的组合架构。

本仓库包含三个主要组件：

- **Kelpie**（管理端/服务端）：`cmd/kelpie`，核心逻辑在 `internal/kelpie/`
- **Flock**（代理端）：`cmd/flock`，核心逻辑在 `internal/flock/`
- **Stockman**（Qt6 GUI 客户端）：`clientui/`

> 说明：本项目用于课程/科研场景下的系统原型与实验复现，请仅在**已授权**的环境中使用。

## 构建与测试

本项目以 `make` 为入口：

```sh
make all        # 构建 kelpie/flock/Stockman（Stockman 依赖见 docs/stockmansetup.md）
make admin      # Go -> build/kelpie
make agent      # Go -> build/flock
make regress    # Trace 回放回归（跑全部 traces，star+chain）
make soak       # Soak 压测（跑“高风险 traces”，默认 repeat=10，失败会自动打包留档）
make check      # 一键工程回归：go test -race ./... + make regress（CI/本地都可用）
make test       # go test ./...
make clean      # rm -rf build/
```

仅构建 Go 端（推荐先跑实验时使用）：

```sh
make admin agent
```

Soak 压测常用参数（环境变量）：

```sh
SOAK_REPEAT=50 make soak
SOAK_PROFILE=extended make soak
SOAK_TOPOS=chain make soak
```

## 实验与复现（中期阶段）

中期阶段的“可复现实验”以 **Trace 回放**为主，能够在本机快速启动一个 mini-cluster，并产出可分析的数据与图表。

一键运行（会写入 `docs/data/*.csv` 与 `docs/figures/*.svg`）：

```sh
bash script/experiments.sh
```

产物包括：

- `docs/data/bootstrap_summary.csv`：拓扑 bootstrap/收敛时间统计（多拓扑、多节点规模、多重复）
- `docs/data/dtn_latency_samples.csv`：DTN 时延逐条样本（每条消息一行）
- `docs/data/dtn_latency_summary.csv`：DTN 时延按 run 汇总
- `docs/figures/bootstrap_convergence.svg`：拓扑收敛时间图
- `docs/figures/dtn_latency.svg`：DTN 时延对比图（含 duty-cycle 理论期望线）

更详细说明见：

- `experiments/README.md`（总览）
- `experiments/trace_replay/README.md`（Trace 格式、输出与指标记录）
- `docs/experiments.md`（中文实验报告，含场景/指标/结果/威胁）
- `docs/stockmansetup.md`（Stockman 的 macOS/Homebrew 依赖与版本固定说明）

## 形式化验证（骨架）

中期阶段已提供握手流程的形式化模型骨架与可复现脚本（Docker 方式推荐）：

- `formal/README.md`

## 文档

- `docs/paper.md`：开题报告（研究目标、技术路线、模型与计划）
- `docs/midterm.md`：中期答辩/中期检查报告（阶段成果、实验结果、下一步计划）
- `docs/experiments.md`：实验报告（复现步骤、场景、数据、图表与分析）
