# Shepherd 中期实验报告（初版）

日期：2026-02-08  
作者：`<姓名>`（本科毕业设计）

---

## 1 实验目的

本实验报告围绕 Shepherd 的两个核心“可量化目标”展开：

1. **拓扑收敛性**：在不同拓扑形状与节点规模下，Gossip 驱动的拓扑 bootstrap/收敛是否能在可接受时间内完成。
2. **延迟容忍交付**：在 duty-cycling（目标节点周期睡眠）条件下，DTN store-carry-forward 能否保证消息最终交付；交付时延随 duty-cycle 参数变化的规律是否可解释、可复现。

实验以“本机 Trace 回放 + 统一指标采集 + 脚本化分析出图”为基本方法，重点强调**可复现性**：同一套脚本可重复运行并生成论文级 CSV 与 SVG 图表。

---

## 2 实验环境与工具链

本中期实验不依赖外部仿真器，默认在本机完成（后续可拓展到 Mininet/ns-3）。

### 2.1 必需环境

- **Go**：1.22（见 `go.mod`）
- **Make**：用于构建 `build/kelpie` 与 `build/flock`
- **Python 3**：仅使用标准库（用于 CSV 汇总与 SVG 出图，见 `experiments/analysis/`）
- **Bash**：运行一键脚本（`script/run_experiments_local.sh`）

### 2.2 可选环境（后续阶段）

- **Docker / Docker Compose**：用于形式化验证骨架的复现（见 `formal/README.md`）
- **Mininet / ns-3**：网络仿真/离散事件模拟（中期阶段仅提供骨架，见 `experiments/mininet/`、`experiments/ns3/`）

---

## 3 可复现实验方法：Trace 回放

### 3.1 Trace 回放器做了什么

`experiments/trace_replay` 以“研究脚手架”的方式在本机启动一个 mini-cluster，完成以下工作流（详见 `experiments/trace_replay/README.md`）：

1. 启动 Kelpie（管理端），开启 gRPC UI；
2. 引导 root Flock 连接；
3. 根据拓扑形状（`star`/`chain`）创建 listener 并启动更多 Flock；
4. 可选：按 trace（JSONL）注入控制面事件（睡眠、DTN 入队、故障等）；
5. 以固定周期抓取指标快照写入 `metrics.jsonl`；
6. 可选：录制 gRPC WatchEvents 事件流。

### 3.2 输出与数据格式

每次运行会在 `experiments/out/<run>/` 下生成（目录结构可由 `--out` 指定）：

- `config.json`：本次运行参数（拓扑、节点数、duration、metrics_every 等）
- `labels.json`：节点标签（`root`,`n1`…）与运行期 UUID 映射
- `metrics.jsonl`：JSONL 形式的指标快照与 trace_action 记录（实验分析的主数据源）
- `logs/*.log`：Kelpie/Flock 的 stdout/stderr

其中 `metrics.jsonl` 既包含周期性快照（`kind=metrics`），也包含 trace 注入动作（`kind=trace_action`），便于后处理脚本对齐时间轴。

---

## 4 指标定义与统计口径

### 4.1 拓扑收敛时间

在 `experiments/analysis/analyze_bootstrap.py` 中，收敛时间 `converged_ms` 采用如下判据提取：

- 指标快照中拓扑节点总数达到期望值；
- 节点全部处于 online 状态；
- 边数达到阈值（考虑 Admin→root 等实现细节，阈值以“至少 N 条边”为近似）。

### 4.2 DTN 交付时延

在 `experiments/analysis/analyze_dtn_latency.py` 中，DTN 交付时延按“入队时刻”与“交付计数增长时刻”的差值计算：

- `enqueue_ms`：trace_action 中 `dtn_enqueue` 的 `since_start_ms`
- `deliver_ms`：metrics 快照中 `dtn_metrics.delivered` 相比上一快照增量的时间戳（`since_start_ms`）
- `latency_ms = deliver_ms - enqueue_ms`

该口径的特点是：

- 优点：无需解析内部消息 ID，即可稳定提取“入队→交付”时延；
- 代价：`deliver_ms` 受指标采样周期（本实验为 500ms）影响，存在量化误差。

---

## 5 实验场景设计

本中期实验包含两组场景，均由一键脚本自动执行并产出论文插图级的 CSV/SVG。

### 5.1 场景 A：拓扑 bootstrap/收敛（Gossip）

脚本：`script/run_experiments_local.sh`（第 2/4 步）  
参数：

- topology：`star`、`chain`
- nodes：4、6、8（包含 root）
- duration：20s
- metrics_every：500ms
- repetitions：3

输出汇总：`docs/data/bootstrap_summary.csv`  
出图：`docs/figures/bootstrap_convergence.svg`

### 5.2 场景 B：duty-cycling 下 DTN 交付时延

脚本：`script/run_experiments_local.sh`（第 3/4 步）  
固定参数：

- topology：`chain`
- nodes：4（root → n1 → n2 → n3）
- target：n3
- duration：70s
- metrics_every：500ms
- repetitions：2

trace（JSONL）：

- `experiments/trace_replay/traces/dtn_sleep_effect_baseline.jsonl`（无睡眠）
- `experiments/trace_replay/traces/dtn_sleep_effect_sleep8_work2.jsonl`
- `experiments/trace_replay/traces/dtn_sleep_effect_sleep16_work2.jsonl`

输出汇总：

- `docs/data/dtn_latency_samples.csv`（逐条样本）
- `docs/data/dtn_latency_summary.csv`（按 run 汇总）
- `docs/figures/dtn_latency.svg`（对比图，含理论期望线）

---

## 6 复现步骤（端到端）

在仓库根目录执行：

```sh
bash script/run_experiments_local.sh
```

该脚本会：

1. 构建 `kelpie/flock` 与 `trace_replay`
2. 执行拓扑收敛实验（场景 A）
3. 执行 DTN 时延实验（场景 B）
4. 生成聚合 CSV 与 SVG 图表到 `docs/`

复现成功的判据：

- `docs/data/*.csv` 与 `docs/figures/*.svg` 被生成/更新；
- `docs/data/dtn_latency_summary.csv` 中每个 run 的 `delivered_n == enqueued_n`（本中期脚本设置为每次入队 3 条）。

---

## 7 实验结果与分析

本节所有表格数据均可由脚本生成的 CSV 直接核对。

### 7.1 拓扑收敛实验结果

表 1 给出收敛时间统计（秒）。每组 3 次重复，统计口径为 `converged_ms`。

| 拓扑 | 节点数 | 收敛时间均值 ± 标准差 | 最小-最大 |
| --- | --- | --- | --- |
| star  | 4 | 1.661 ± 0.116 | 1.593–1.795 |
| star  | 6 | 2.517 ± 0.003 | 2.514–2.520 |
| star  | 8 | 3.438 ± 0.005 | 3.433–3.441 |
| chain | 4 | 1.905 ± 0.004 | 1.901–1.908 |
| chain | 6 | 3.138 ± 0.002 | 3.136–3.139 |
| chain | 8 | 4.372 ± 0.006 | 4.368–4.378 |

图 1 展示了随节点数增长的收敛趋势：

![图 1 拓扑收敛时间（Trace 回放，均值±标准差）](figures/bootstrap_convergence.svg)

**讨论**：

- `star` 拓扑中节点与 root 直接接触，收敛更快；
- `chain` 拓扑需多跳建立与中继稳定，收敛时间随节点数增长更明显；
- `star,n=4` 的方差较大，提示本机调度抖动/端口争用等因素会放大在小规模实验中的噪声（后续可引入 Mininet 的固定时延链路做对照）。

### 7.2 duty-cycling 下 DTN 时延结果

#### 7.2.1 理论期望模型

对 duty-cycling 目标节点，采用开题阶段的期望等待模型：

\[
E[T_w] = \frac{T_{sleep}^2}{2(T_{sleep}+T_{work})}.
\]

该模型刻画“随机入队到下次唤醒”的平均等待时间，未显式包含多跳转发、队列排队与重传等额外开销，因此在实现系统中通常会出现测量值大于理论值的现象。

#### 7.2.2 实测统计

本实验每个场景 2 次重复，每次入队 3 条消息（共 6 条样本）。表 2 给出逐条样本聚合后的统计（秒）。

| 场景（sleep/work） | 理论 E[T_w] | 平均交付时延（均值 ± 标准差） | 中位数（p50） | 最小-最大 | 交付数 |
| --- | --- | --- | --- | --- | --- |
| 0/0   | 0.000 | 2.825 ± 1.251 | 2.491 | 1.394–4.589 | 6/6 |
| 8/2   | 3.200 | 5.401 ± 1.633 | 5.402 | 3.401–7.402 | 6/6 |
| 16/2  | 7.111 | 9.400 ± 4.320 | 11.401 | 3.400–13.400 | 6/6 |

图 2 使用 `docs/data/dtn_latency_summary.csv`（按 run 汇总的 mean_ms）生成柱状对比，并叠加理论期望线：

![图 2 DTN 交付时延对比（按 run 汇总的均值±标准差）](figures/dtn_latency.svg)

**讨论**：

1. **可靠性**：在本场景与参数下，DTN 最终交付成功率为 100%（所有 run 均满足 `delivered_n == enqueued_n`）。
2. **时延趋势**：随着 `T_sleep` 从 0 增大到 8、16，交付时延显著上升，且分布更分散。
3. **理论与实测差异**：测量均值大于理论 \(E[T_w]\) 的主要来源包括：多跳转发与排队开销、指标采样量化误差、睡眠相位与入队事件对齐导致的“窗口错过”等。
4. **误差棒口径**：图 2 的误差棒统计的是“每次 run 的 mean_ms”在两次重复中的标准差，反映 run 级稳定性；消息级的离散程度可通过表 2 的 min/max 与样本 CSV 进一步分析。

---

## 8 可复现性与威胁分析

### 8.1 可复现性保证

- 一键脚本固定了拓扑、节点数、运行时长、采样周期与重复次数；
- 分析与出图脚本不依赖第三方库，避免环境差异导致的不可复现；
- `docs/data/*.csv` 与 `docs/figures/*.svg` 作为“论文插图/表格来源”进行版本化管理。

### 8.2 威胁与局限性

- **规模局限**：中期实验节点规模较小（最高 8），尚不能代表更大规模网络的收敛行为；
- **网络真实性**：本机回放主要反映实现逻辑与调度抖动，尚未引入“可控链路参数”（带宽/丢包/时延）；需要 Mininet/ns-3 对照；
- **统计显著性**：重复次数较少（收敛实验 3 次、DTN 实验 2 次），更严格的结论需增加重复与置信区间分析；
- **指标口径误差**：DTN 交付时刻由 metrics 采样近似得到，存在 500ms 级量化误差；后续可引入更细粒度的交付事件日志以减少误差。

---

## 9 产物清单（用于论文/答辩引用）

复现脚本：

- `script/run_experiments_local.sh`

数据与图表（由脚本生成，可版本化）：

- `docs/data/bootstrap_summary.csv`
- `docs/data/dtn_latency_samples.csv`
- `docs/data/dtn_latency_summary.csv`
- `docs/figures/bootstrap_convergence.svg`
- `docs/figures/dtn_latency.svg`

Trace 与分析脚本：

- `experiments/trace_replay/traces/*.jsonl`
- `experiments/analysis/analyze_bootstrap.py`
- `experiments/analysis/analyze_dtn_latency.py`
- `experiments/analysis/plot_svg.py`

形式化验证骨架：

- `formal/README.md`

