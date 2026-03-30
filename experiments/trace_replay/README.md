# Trace 回放器（研究脚手架）

`experiments/trace_replay` 是一个“研究脚手架风格”的编排工具，用于在本机快速构建可重复的实验环境，并导出可分析的数据。

它会完成：

1. 启动本地 Kelpie（启用 gRPC UI）
2. 通过 **Controller Listener** 引导一个 root Flock 连接
3. 为后续节点创建 **Pivot Listener**，并按拓扑形状启动更多 Flock
4. 可选：按 trace（JSONL）注入控制面事件（睡眠、DTN 入队、故障注入等）
5. 周期性抓取 gRPC 指标快照并写入 `metrics.jsonl`
6. 可选：录制 UI 的事件流到 `events.jsonl`

它刻意不追求生产级编排能力，而追求“易改、易复跑、易拿数据”。

## 运行

```sh
make admin agent
go run ./experiments/trace_replay --nodes 6 --duration 120s --topology star --watch-events
```

常用参数：

- `--nodes`：总节点数（包含 root）
- `--topology`：`star` 或 `chain`
- `--trace`：可选 trace 文件（JSONL，一行一个事件）
- `--metrics-every`：指标快照周期（默认 1s）
- `--watch-events`：是否录制 gRPC WatchEvents

## 批量回归（跑全部 traces 并自动判定 PASS/FAIL）

回归脚本会逐个运行 `traces/*.jsonl`，并在 `metrics.jsonl` 中断言：

- `dtn_metrics.enqueued == trace 中 dtn_enqueue 事件数`
- `dtn_metrics.delivered == trace 中 dtn_enqueue 事件数`
- `dtn_metrics.failed == 0`

并对下述事件类型额外要求：每个事件都必须在 `metrics.jsonl` 中产出一条 `kind=trace_result` 且 `ok=true` 的记录（由回放器写入）：

- `kelpie_restart`
- `dataplane_roundtrip`
- `stream_proxy`

运行示例：

```sh
python3 experiments/trace_replay/run_regress.py --topologies star,chain
```

若希望在出现 FAIL 时自动打包留档（包含失败 run 目录 + summary.json）：

```sh
python3 experiments/trace_replay/run_regress.py --topologies star,chain --archive-failures experiments/out/regress/archives
```

## Soak 压测（重复跑关键 trace 找 flake）

Soak 默认只跑“高风险 traces”，并在失败时自动归档：

```sh
make soak
```

常用调整（环境变量）：

```sh
SOAK_REPEAT=50 make soak
SOAK_PROFILE=extended make soak
SOAK_TOPOS=chain make soak
```

## Trace 格式（JSONL）

每行一个 JSON 对象。例如：

```json
{"at_ms":0,"type":"sleep","node":"n1","sleep_seconds":15,"work_seconds":3,"jitter_percent":0}
{"at_ms":5000,"type":"dtn_enqueue","target":"n2","payload":"log:hello from trace","priority":"normal","ttl_seconds":600}
{"at_ms":10000,"type":"kill","node":"n1"}
{"at_ms":12000,"type":"metrics","note":"checkpoint"}
```

字段说明：

- `at_ms`（int）：相对运行开始的毫秒偏移
- `type`：`sleep` | `dtn_enqueue` | `kill` | `metrics` | `kelpie_restart` | `dataplane_roundtrip` | `stream_proxy`
- `node` / `target`：节点标签，`root` 或 `n1..n{N-1}`
- `sleep_seconds` / `work_seconds` / `jitter_percent`：`sleep` 事件参数
- `payload` / `priority` / `ttl_seconds`：`dtn_enqueue` 事件参数
- `path`：`dataplane_roundtrip` 的远端文件路径（相对于 agent 工作目录；会做 path traversal 拒绝）
- `expect`：`stream_proxy` 的期望回包内容（默认等于 `payload`，用于 echo 验证）
- `timeout_ms`：`kelpie_restart` / `dataplane_roundtrip` / `stream_proxy` 的动作超时时间（毫秒）
- `note`：`metrics` 事件的备注标签，会写入输出记录

运行时，回放器会把动作额外记录为 `kind=trace_action`（开始）以及 `kind=trace_result`（结束，含 ok/error）写入 `metrics.jsonl`，便于后处理脚本对齐时间轴与自动判定。

## 输出

回放器会在本次 run 目录下写入：

- `labels.json`：节点标签（`root`,`n1`...）到运行时 UUID 的映射
- `metrics.jsonl`：周期性指标快照（protobuf JSON）与 `trace_action`
- `events.jsonl`：UI 事件流（可选，`--watch-events`）
- `logs/kelpie*.{out,err}.log`、`logs/flock-*.{out,err}.log`：各进程日志（若发生 `kelpie_restart`，会出现 `kelpie-2.*` 等）

默认输出基目录为 `experiments/out/`（在 `.gitignore` 中忽略）。
