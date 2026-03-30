# ns-3 离散事件模拟（骨架）

当你希望得到“论文友好、可重复、模型清晰”的评估结果时，ns-3 往往比直接跑真实网络更合适：它能精确控制链路参数、调度事件并复现大量场景。

本仓库不内置（vendor）ns-3；本目录给出两条可行路线，供你在后续阶段选择其一落地。

## 路线 A：TapBridge + 真二进制（System-in-the-Loop）

目标：让 ns-3 只负责“网络”，Kelpie/Flock 使用真实实现运行在 Linux namespace 中，从而兼顾模型可控与实现可信。

高层步骤：

1. 构建带 TapBridge 的 ns-3
2. 在 ns-3 中创建节点与链路（CSMA/WiFi 等），为每个节点挂接 `TapBridge`
3. 将每个 tap 绑定到一个 Linux namespace，在该 namespace 中运行一个 `flock` 进程
4. 用 ns-3 调度链路事件（down/up、delay/loss 动态变化）
5. 通过 Kelpie gRPC UI 抓取实验指标并画图（Topology/Metrics/Supplemental/DTN 等）

优点：

- 直接验证真实实现（更有说服力）

缺点：

- 环境搭建复杂度更高（更依赖 Linux）

## 路线 B：算法级模拟（只模拟控制面/队列策略）

目标：不运行真实网络与二进制，而是仅模拟 gossip、补链与 DTN 队列策略，在可控模型下做规模化评估。

建议输出指标：

- gossip 收敛时间 vs 节点数 vs duty-cycle 参数
- 补链修复成功率/修复时延 vs 拓扑深度
- DTN 队列丢弃率 vs hold/spray/focus 阈值

落地建议：

- 复用现有包的核心逻辑（队列/评分/路由），替换传输与时钟为 mock（确定性）
- 以 trace（接触、睡眠窗口）作为输入，统一输出 metrics 便于画图

## 下一步建议

优先确定路线 A 或 B（两者实现复杂度差异很大）。
若选择 B，通常最快的路径是实现“mock transport + 可控时钟”，复用现有 DTN/补链/流控代码做确定性仿真。

