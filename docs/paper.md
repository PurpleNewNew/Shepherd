# Shepherd：面向受限网络的 Gossip 化延迟容忍远控系统（开题报告）

> 本报告面向研究生阶段学位课题的开题评审，采用规范结构：研究背景与意义、国内外现状、拟解决的关键问题、研究内容及技术路线、研究方法、预期成果与风险分析。参考文献包含中英文期刊/会议论文，以体现调研深度。

---

## 1 研究背景与意义

工业控制网、涉密内网及高对抗环境对远程运维渠道提出了三项核心诉求：通信路径必须具备 **隐蔽性**（避免被审计系统捕捉）、**自愈性**（上游节点损坏后能迅速恢复拓扑）以及 **延迟容忍性**（允许链路长时间中断而不丢失关键信息）。传统树状 C2 拓扑虽然有利于集中控制，但一旦父节点离线即导致整支子树失联；若通过持续隧道或频繁心跳维持连接，又容易暴露流量特征。因此迫切需要新的架构，将 gossip 拓扑维护、补链修复、延迟容忍流层、睡眠预测与安全握手五大策略结合，形成“能隐身、能自愈、能缓发”的混合控制面。

Shepherd 项目基于 Go 实现 Flock 代理与 Kelpie 管理端，已具备 gossip、补链、DTN、睡眠调度、预认证握手等功能。本课题拟在此基础上开展系统性研究，使其不仅可部署，更能形成具有理论依据与实验数据支撑的研究成果。

---

## 2 国内外研究现状

### 2.1 Gossip 与自愈拓扑

Demers 等~[1] 提出的 epidemic 模型与 Jelasity 等~[2] 的 push-pull 框架为节点状态同步提供了概率保证，后续学者多用于资源发布与 membership 维护。国内张曦等（《计算机研究与发展》2017）通过 P2P 场景验证了 gossip 可显著降低集中式调度瓶颈。然而现有研究多关注纯扁平 overlay，对“树状主拓扑 + 补链冗余”的组合策略讨论较少，尤其缺乏在受限网络场景下的工程实现。

### 2.2 延迟容忍网络（DTN）

Vahdat 与 Becker~[3] 首次提出 store-carry-forward 路由，Fall~[4] 将 DTN 推广至更普适的受限网络。国内王翰林等（《通信学报》2015）从概率角度分析了延迟容忍路由的时延；Jacquet 等~[12] 则给出了移动 DTN 的空间-时间容量界限。现有成果对远控系统的结合尚不充分。

### 2.3 Duty Cycling 睡眠策略

Versatile MAC~[6] 与 X-MAC~[7] 在低功耗无线网络中引入随机唤醒与短前导机制；国内朱利军等（《电子与信息学报》2019）研究哈希相位随机化对碰撞概率的影响。这些成果为 Shepherd 的睡眠预测与离线判定提供了理论参考。

### 2.4 安全握手

Bellare 等~[8] 的 AKE 理论奠定了预共享口令协议的安全框架；国内李晓光（《密码学报》2021）研究了预共享组密钥的认证机制。Shepherd 的 `pkg/share/handshake` 需要在此基础上补足日志与形式化安全验证。

---

## 3 拟解决的关键问题

1. **Gossip 参数自适应**：根据节点规模、拓扑层数和实时带宽负载动态调整 fanout、TTL 与间隔，避免固定参数造成整体收敛过慢或带宽放大。
2. **补链优化模型**：将 `SupplementalPlanner` 与图论模型结合，使用最小割、Steiner Tree 或 ILP 约束，定义重连延迟、带宽消耗等可量化指标。
3. **DTN Inbox 的可靠性与安全性**：坚持 Flock 端“纯内存、不落盘”的 store-carry-forward 策略，通过内存加密与 HMAC 校验保证可靠性，在不牺牲隐蔽性的前提下吸收长时间离线的抖动。
4. **睡眠预测与拓扑协同**：建立 hash 相位与离线误判率模型，分析 duty cycling 参数对补链调度和 gossip 推送的影响。
5. **握手安全形式化**：构建 `pkg/share/handshake` 的状态机与日志，利用 Tamarin/ProVerif 验证认证和密钥保密性，并提供安全审计。
6. **实验评估体系**：设计覆盖 gossip、补链、DTN、睡眠、握手的可复现实验，用统一指标对比不同策略的性能。

---

## 4 研究内容与技术路线

### 4.1 Gossip 与补链理论化

参考 Demers 与 Jelasity 对 epidemic/push-pull 的分析~[1,2]，我们采用自适应 fanout 模型：
\[
f(n)=\min\left(F_{\max}, F_{\text{base}} + \lceil \log_2 n \rceil\right),
\]
其中 $n$ 为已知节点数，$F_{\text{base}}$ 为配置值，$F_{\max}$ 取 10 以避免带宽爆炸。TTL 则依据节点规模、睡眠配置与补链状态动态调整：
\[
\text{TTL} = \operatorname{clip}\left(T_{\text{base}} + \lceil \log_{10}(n+1)\rceil + \mathbf{1}_{\text{sleep}} + 2\mathbf{1}_{\text{failover}},\, T_{\max}\right),
\]
使得短连接节点和补链场景拥有更大的传播半径。补链方面，我们将设计代价函数 $\min_{e \in E_{\text{supp}}} \text{ReconnectCost}(e)$，结合最小割或 Steiner Tree 进行选择。同时结合 You 等的一跳冗余度控制策略~[18]，在 `SupplementalPlanner` 中加入 Redundancy 得分，优先挑选与主干树重叠少的路径，减少“虚线补边”。

### 4.2 DTN/STREAM 改进

* 定义带 HMAC 的 bundle 格式，在“只驻留内存”的前提下提供加密校验与崩溃自清理逻辑，明确禁止 Flock 将 DTN inbox 落盘；
* 结合 Jacquet 等~[12] 的空间-时间容量公式，推导 `WindowFrames`、`ChunkBytes` 的配置准则；
* 在 `internal/kelpie/stream` 引入拥塞检测与动态 RTO。
* 借鉴 Joshi 与 Radenkovic 提出的 LED Wristband 路由测试~[17]，在 Kelpie 监测 per-target 队列占用与 Held 比例，低负载采用 PRoPHET 式“Spray”，高负载进入“Focus”模式，仅允许少量 in-flight 并调整 `hold_until`，避免小 buffer 场景被高 fanout 冲垮。

### 4.3 Duty Cycling 协同

* 参考 Biondi 等~[16] 对占空比机会网络的建模，将期望等待 $E[T_w]=\frac{T_{sleep}^2}{2(T_{sleep}+T_{work})}$ 融入 `ExpectedWaitMsAt` 与 `PathSleepBudget`，使 ETTD 能区分“短唤醒长睡眠”与“微睡眠”节点；
* 抽象 `sleep_predictor` 的随机相位模型，推导离线误判率；
* 扩展 Kelpie 拓扑的离线判定，使其利用各节点的睡眠计划；
* 借鉴朱利军等对哈希相位的改进，在 Shepherd 中实现可配置方案。

说明：上述连接管理、离线判定与发送时机控制目前采用 sleep-aware 的启发式近似实现。`ExpectedWaitMsAt`、`RecommendSendDelay`、路径睡眠预算和补链离线探测阈值用于降低误判和窗口错过概率，但并不宣称构成严格的全局最优调度或最优离线判定。

### 4.4 安全握手

* 为 `pkg/share/handshake` 提供结构化返回值与安全日志；
* 编写 handshake 状态机，使用 Tamarin/ProVerif 验证认证与密钥保密性；
* 引入国内预共享口令研究中的复杂度约束与多因素策略。

### 4.5 实验评估

* Gossip 覆盖度与收敛时间；
* 补链 failover 延迟与成功率；
* DTN/STREAM 吞吐与稳定性；
* 睡眠策略的离线误判率；
* 握手安全的形式化验证报告。

---

## 5 研究方法

1. **理论推导 + 仿真验证**：结合数学模型与 ns-3/Mininet 仿真验证。
2. **模块化重构**：继续拆分 `internal/flock/process`，降低耦合度。
3. **Trace 重放**：利用真实或合成 trace 测试 DTN/STREAM 性能。
4. **形式化工具**：使用 Tamarin/ProVerif 对握手安全性进行证明。

---

## 6 预期成果

1. 完整的开题报告与理论推导文档；
2. 模块化、可测试的 Shepherd 代码架构；
3. 可复现的仿真/trace 回放脚本；
4. 握手安全日志与形式化验证结果；
5. 覆盖中英文资料的参考文献列表，支撑后续论文写作。

---

## 7 风险分析与对策

* **仿真平台搭建难**：提前规划脚本与容器环境，方便复现；
* **形式化验证耗时**：从简化模型入手，逐步扩展；
* **代码重构风险**：配合自动化测试与渐进式提交；
* **文献调研工作量大**：建立共享文献库并记录摘要。

---

## 8 阶段成果与代码改进

1. **自适应 Gossip 实现**：在 `internal/flock/process/gossip.go` 新增 `dynamicFanout()`、`dynamicTTL()` 等函数，实现基于 $\log n$ 的 fanout/TTL 调整，并在任务触发、发现流程中替换原固定参数，响应了文献~[1,2] 对负载均衡的建议。
2. **补链评分框架**：`SupplementalPlanner` 现在独立位于 `internal/kelpie/planner/`，其中 `metrics.go` 与 `events.go` 引入 candidate score、nodeQuality 与 duty-cycle 感知探测逻辑，将睡眠预算与路径重叠度归一化后按权重组合，实现了参考最小割/Spanner 文献~[5] 的成本驱动策略。
3. **睡眠信息上报**：`buildNodeInfo()` 现根据 `sleep_predictor` 将 `SleepSeconds` 与 `NextWake` 写入 gossip payload，便于 Kelpie 在 duty cycling 环境中做拓扑判定，对应 Versatile MAC/X-MAC 的随机唤醒思想~[6,7]。
4. **理论-实现联动文档**：本稿补充了 fanout/TTL 数学公式、补链代价函数与实验计划，形成中期汇报所需的学术化材料。
5. **DTN 队列观测与内存多级队列**：受 BPv7 与 MaxProp 对丢弃策略的启发~[3,15]，`internal/kelpie/dtn` 将 per-target 队列升级为多级优先级缓冲并统计水位、平均等待时延、按优先级丢包次数等指标；CLI 的 `dtn queue` 命令能实时展示 `ready/held` 队列深度与抛弃原因，实现 RFC 4838 所倡导的可观测内存存储，同时保持“只驻留内存、不落盘”的隐蔽性约束。
6. **DTN-Stream 自适应窗口/RTO**：依据 TCPCLv4 与 BSS/BP 流控建议~[12,14]，`internal/kelpie/stream` 引入 SRTT/RTTVAR 估计、AIMD 滑窗与超时退避，`stream diag` 命令可观察每条流的窗口、RTO、空闲时间，使 STREAM 能在高延迟/高丢包链路上维持稳定吞吐，同时不增加磁盘痕迹。
7. **Spray/Focus 策略切换**：结合音乐节 LED Wristband 的路由实测~[17]，Kelpie 在队列占用和 Held 占比升高时转入“Focus”模式，仅保留 1 个 in-flight 并对其余 bundle 设置延迟，低占用时回到“Spray”以加速扩散。
8. **duty-cycle ETTD**：参考 Biondi 等~[16] 的占空比模型，将 $\frac{T_{sleep}^2}{2(T_{sleep}+T_{work})}$ 注入 `ExpectedWaitMsAt` 与 `PathSleepBudget`，Kelpie 能区分“短醒长睡”路径，避免盲目拉长 TTL。
9. **补链冗余度指标**：应用 You 等的一跳冗余度控制方法~[18]，在 `SuppCandidate` 中记录 Redundancy 并参与排序，使自动补链优先创建与主树重叠最少的备份链路。
10. **Gossip 细粒度自适应**：`internal/flock/process/gossip.go` 中新增健康度/队列感知的 fanout/TTL 调节和 token bucket 速率限制，实现 Demers/Jelasity 提出的 push-pull 负载控制思想，即便节点数或补链压力上升也不会出现过度洪泛。
11. **补链成本图分析**：`SupplementalPlanner.inspectTopology()` 现在会构建代价图，统计深度/睡眠预算/补链缺口并输出“node cost vs. candidate gain”报告，为论文中的图优化和最小割讨论提供量化数据。
12. **Jacquet 式 STREAM 容量调优**：`internal/kelpie/stream` 在 ACK 中计算吞吐 EMA 与 SRTT，按 Jacquet 等的空间-时间容量估算 BDP，动态调节窗口帧数；同时 `Admin.DTNMetricsSnapshot` 暴露 ready/held/avg_wait/dropped 等队列指标，为监控和论文实验提供数据。
13. **睡眠-补链协同**：Flock 端根据子节点/补链压力动态压缩 sleep 周期与 jitter，Kelpie 在补链候选中新增 `WorkSeconds` 权重，从而让关键树干节点保持更长在线窗口并优先被选作父节点。
14. **握手安全日志与多因素**：`pkg/share/handshake` 引入 Transcript/Code 结构记录各阶段事件，Kelpie 的 `NormalActive` 在 dial/TLS/预认证/HI 交换时写入 trace 并在报错时输出；同时 CLI 新增 `--mfa-pin`（配合 `SHEPHERD_MFA_PIN` 环境变量）作为可选多因素要素，强化 Bellare 等提出的认证流程。

---

## 参考文献

[1] Demers, A. et al. “Epidemic Algorithms for Replicated Database Maintenance.” PODC, 1987.  
[2] Jelasity, M. et al. “Gossip-based Peer Sampling.” ACM TOCS, 2005.  
[3] Vahdat, A.; Becker, D. “Epidemic Routing for Partially Connected Ad Hoc Networks.” Duke TR-2000-06, 2000.  
[4] Fall, K. “A Delay-Tolerant Network Architecture for Challenged Internets.” SIGCOMM, 2003.  
[5] Karger, D. et al. “Consistent Hashing and Random Trees.” STOC, 1997.  
[6] Polastre, J. et al. “Versatile Low Power Media Access for Wireless Sensor Networks.” SenSys, 2004.  
[7] Buettner, M. et al. “X-MAC: A Short Preamble MAC Protocol for Duty-Cycled Wireless Sensor Networks.” SenSys, 2006.  
[8] Bellare, M. et al. “Authenticated Key Exchange Secure Against Dictionary Attacks.” EUROCRYPT, 2000.  
[9] 张曦, 李建强, “基于 Gossip 的 P2P 资源发布机制研究”, 计算机研究与发展, 2017.  
[10] 王翰林, 赵春江, “一种延迟容忍网络路由算法分析”, 通信学报, 2015.  
[11] 朱利军, 马婷婷, “哈希相位随机化在低功耗 MAC 中的应用”, 电子与信息学报, 2019.  
[12] Jacquet, P.; Mans, B.; Rodolakis, G. “On Space-Time Capacity Limits in Mobile and Delay Tolerant Networks.” arXiv:0912.3441, 2009.  
[13] 李晓光, “预共享口令认证协议的安全性研究”, 密码学报, 2021.
[14] Burleigh, S.; Fall, K.; Birrane, E. “RFC 9174: TCP Convergence Layer Protocol Version 4.” IETF, 2022.  
[15] Burgess, J.; Gallagher, B.; Jensen, D.; Levine, B. “MaxProp: Routing for Vehicle-Based Disruption-Tolerant Networks.” IEEE INFOCOM, 2006.  
[16] Biondi, E.; Boldrini, C.; Passarella, A.; Conti, M. “What You Lose When You Snooze: How Duty Cycling Impacts on the Contact Process in Opportunistic Networks.” 2017.  
[17] Joshi, N.; Radenkovic, M. “Opportunistic Delay Tolerant Routing for LED Wristbands in Music Events.” arXiv:2406.02694, 2024.  
[18] You, L.; Li, J.; We, C.; Dai, C. “A One-Hop Information Based Geographic Routing Protocol for Delay Tolerant MANETs.” arXiv:1602.08461, 2016.
