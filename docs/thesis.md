# Shepherd：面向受限网络的 Gossip 化延迟容忍远程运维系统设计与实现

> 本科毕业设计论文
> 学生：`<姓名>`  
> 学号：`<学号>`  
> 学院：`<学院>`  
> 专业：`<专业>`  
> 指导教师：`<教师姓名>`  
> 日期：2026 年 5 月

---

## 摘要

受限网络广泛存在于灾害应急、野外科研、低功耗物联网、临时组网和隔离运维等场景中。这类网络通常具有高时延、间歇连接、节点周期睡眠、链路质量波动和拓扑频繁变化等特征。传统远程运维系统大多假设管理端与被管理节点之间存在持续在线的链路，并依赖中心化会话、即时请求响应和稳定路由。当节点进入睡眠窗口、父链路断开或多跳拓扑发生变化时，这类系统容易出现控制面视图陈旧、子树失联、消息丢失和长流中断等问题。

针对上述问题，本文设计并实现了 Shepherd 原型系统。系统由管理端 Kelpie、代理端 Flock 和桌面客户端 Stockman 三部分构成。Kelpie 负责维护全局拓扑、调度补链、管理延迟容忍网络（Delay-Tolerant Networking, DTN）队列、提供可靠流式传输能力并向客户端暴露 gRPC 控制面；Flock 运行在受限网络节点上，负责接入、Gossip 信息传播、多跳转发、睡眠状态上报、链路自愈和本地 carry-forward；Stockman 作为 Wails v3 + Vue 3 实现的演示客户端，用于展示拓扑、节点详情、事件时间线和 DTN/Sleep 控制台。

本文的核心设计是将 Gossip 拓扑维护、补链自愈、DTN store-carry-forward 队列和 DTN 上的可靠 STREAM 传输层组合起来，使系统在节点 duty-cycling 和多跳链路下仍能维持控制面收敛与消息最终交付。实现上，Flock 使用自适应 fanout/TTL 的 Gossip 机制传播节点视图；Kelpie 维护带父子关系和补链边的拓扑图，并基于睡眠预算估计投递时机；DTN 管理器按目标节点维护优先级队列、TTL、HoldUntil、ACK 和重试；STREAM 层在 DTN 之上实现分片、ACK、RTO、AIMD 窗口和重传；补链调度器根据节点质量、路径重叠、睡眠预算、深度和冗余度选择候选节点。

为验证系统可行性，本文构建了 Trace 回放实验框架，在本机自动启动 Kelpie 与多个 Flock，注入睡眠、DTN 入队、故障和重启等事件，并周期性采集指标。实验结果表明，在 4、6、8 节点的 star 与 chain 拓扑下，Gossip 驱动的拓扑收敛时间随节点数增长而上升，chain 拓扑由于多跳路径更长而慢于 star 拓扑；在 chain 拓扑、目标节点周期睡眠的 DTN 实验中，baseline、sleep8/work2、sleep16/work2 三组场景均实现 6/6 最终交付，平均交付时延分别约为 2.825s、5.401s 和 9.400s，呈现与 duty-cycle 理论等待模型一致的上升趋势；17 节点 star/chain 规模回归也验证了连续两条 DTN memo bundle 均能最终交付。本文还给出了基于预共享秘密、Nonce 与 HMAC 的预认证握手机制，并提供 Tamarin/ProVerif 形式化验证骨架。

实验说明，Shepherd 能够在本机可复现环境中体现面向受限网络的拓扑收敛和延迟容忍交付能力。本文也如实讨论了当前原型的局限：实验规模仍较小，主要基于本机 trace replay；形式化模型仍是骨架级；补链策略和 Gossip 参数还需要更多消融实验和 Mininet/ns-3 验证。总体而言，Shepherd 为“受限网络下远程运维控制面如何保持可达、可观测和可最终交付”提供了一个完整、可运行、可复现实验验证的系统化答案。

**关键词**：受限网络；Gossip；延迟容忍网络；DTN；Duty Cycling；补链自愈；远程运维

---

## Abstract

Challenged networks are common in emergency response, field research, low-power IoT deployments, temporary ad hoc networks, and isolated maintenance environments. Such networks often exhibit high latency, intermittent connectivity, duty-cycled nodes, volatile link quality, and frequently changing topologies. Conventional remote operations systems usually assume continuously available links between a controller and managed nodes. They rely on centralized sessions, immediate request-response interactions, and stable routes. Once a node enters a sleep window, an upstream link fails, or a multi-hop topology changes, these systems can suffer from stale control-plane views, disconnected subtrees, message loss, and interrupted long-lived streams.

This thesis presents Shepherd, a prototype system for delay-tolerant remote operations in challenged networks. Shepherd consists of three components: Kelpie, Flock, and Stockman. Kelpie is the management server that maintains the global topology, schedules supplemental links, manages DTN queues, provides reliable stream transport, and exposes a gRPC control plane. Flock runs on network nodes and handles connection establishment, gossip propagation, multi-hop relay, sleep reporting, failover, repair, and local carry-forward. Stockman is a Wails v3 and Vue 3 desktop client used to visualize topology, inspect node details, observe UI events, and trigger demonstration actions such as DTN enqueue and sleep-profile updates.

The main design of Shepherd combines gossip-based topology maintenance, supplemental self-healing links, DTN store-carry-forward queues, and a reliable STREAM layer over DTN. Flock propagates node views with adaptive fanout and TTL. Kelpie maintains a topology graph with both tree edges and supplemental edges, estimates delivery opportunities using sleep budgets, and dispatches queued bundles accordingly. The DTN manager maintains per-target priority queues with TTL, HoldUntil, ACK tracking, and retry logic. The STREAM layer implements fragmentation, ACKs, RTO estimation, AIMD window adjustment, and retransmission over DTN. The supplemental planner selects candidates based on node quality, path overlap, sleep budget, depth, and redundancy.

To evaluate the system, this thesis builds a trace replay framework that automatically launches local Kelpie and Flock processes, injects sleep, DTN enqueue, failure, and restart events, and periodically records metrics. Results show that in 4-, 6-, and 8-node star and chain topologies, gossip-driven convergence time increases with node count, and chain topologies converge more slowly due to multi-hop structure. In a chain topology with a duty-cycled target, DTN achieves 6/6 final delivery in baseline, sleep8/work2, and sleep16/work2 scenarios, with average delivery latency of approximately 2.825s, 5.401s, and 9.400s respectively. The trend is consistent with the theoretical waiting-time model for duty cycling. A supplemental 17-node star/chain regression further verifies successful delivery of two consecutive DTN memo bundles. The thesis also presents a pre-authentication handshake based on a shared secret, nonces, and HMAC, together with a Tamarin/ProVerif verification skeleton.

The evaluation demonstrates that Shepherd can reproduce topology convergence and delay-tolerant delivery behavior in a local experimental environment. The thesis also discusses limitations: the current experiments are small-scale and mainly trace-based; the formal model is still a skeleton; and supplemental-link and gossip-parameter policies require further ablation and Mininet/ns-3 validation. Overall, Shepherd provides a complete, runnable, and reproducible prototype for studying how remote operations control planes can remain observable and eventually deliver messages under challenged-network conditions.

**Keywords**: Challenged Networks; Gossip; Delay-Tolerant Networking; DTN; Duty Cycling; Self-Healing Links; Remote Operations

---

## 目录

1. 绪论  
2. 相关技术与研究现状  
3. 需求分析与总体架构  
4. Gossip 拓扑维护与补链自愈设计  
5. DTN/STREAM 与 Duty-Cycling 协同机制  
6. 系统实现  
7. 实验设计与结果分析  
8. 安全机制与形式化验证  
9. 局限性与改进方向  
10. 总结  

---

## 第 1 章 绪论

### 1.1 研究背景

远程运维系统的基本任务是让操作者能够在远端观察节点状态、下发控制消息、获取诊断信息，并在必要时建立数据通道。普通数据中心、局域网或云环境中的远程运维通常默认三项条件成立：节点与管理端长期在线；网络路径相对稳定；请求发送后可以在较短时间内得到响应。因此，传统设计往往围绕会话连接、中心化控制、即时 RPC 或长连接隧道展开。

然而，在受限网络中，这些前提不再成立。受限网络也被称为 challenged networks，Delay-Tolerant Networking 领域的 RFC 4838 将其描述为可能具有长时延、频繁中断、链路容量受限、错误率高或路由不稳定等特征的网络环境 [5]。典型场景包括：

- 野外科研或应急通信中节点通过临时链路接入，链路间歇可用；
- 低功耗传感器或边缘节点为了节能采用 duty-cycling，周期性关闭无线或网络接口；
- 多跳临时组网中父节点失效会导致整支子树暂时失联；
- 隔离运维环境中网络路径受访问控制、代理、单向链路或临时隧道限制；
- 高时延链路中短时间超时并不意味着节点永久离线。

在这些场景下，远程运维系统需要从“持续在线控制”转向“延迟容忍控制”。系统不能简单地把所有未响应节点删除，也不能假设消息发送失败就意味着目标不可达。更合理的策略是维护一个随时间更新的拓扑视图，在链路断开时保留节点状态，在目标不可达时先缓存消息，等后续接触机会出现时再继续投递。

### 1.2 问题定义

本文研究的问题可以概括为：如何在受限网络中构建一个可观测、可自愈、可最终交付消息的远程运维控制面。

与普通远控或远程管理系统相比，本文关注的不是单条 TCP 连接如何保持不断，而是以下系统性问题：

1. **拓扑如何维护**：当节点多跳接入、链路频繁变化时，管理端如何获得足够新鲜的拓扑视图？
2. **失联如何处理**：当父链路断开或节点进入睡眠时，系统如何避免误删节点和整支子树？
3. **消息如何最终交付**：当目标当前不可达时，控制消息如何排队、等待、重试和确认？
4. **长流如何承载**：当单条消息不足以支撑文件、代理或交互式数据流时，如何在 DTN 之上实现可靠传输？
5. **运维如何观测**：操作者如何通过统一界面看到拓扑、节点状态、事件、DTN 队列和补链状态？
6. **握手如何证明可信**：共享密钥认证、预认证和会话建立过程如何给出可复现的安全论证材料？

本文的研究对象 Shepherd 不是一个面向生产落地的完整商业产品，而是一个毕业设计/研究原型。它的价值在于把受限网络远程运维中的关键矛盾拆解为可实现、可测试、可复现的系统机制，并通过实验数据验证这些机制的基本有效性。

### 1.3 研究目标

本文的主要目标如下：

1. 设计并实现三组件远程运维原型，包括 Kelpie 管理端、Flock 代理端和 Stockman 客户端。
2. 设计 Gossip 化拓扑维护机制，使节点视图可以在动态多跳网络中逐步收敛。
3. 设计补链自愈机制，在父链路失效或节点离线时保留拓扑结构并尝试建立冗余连接。
4. 设计 DTN store-carry-forward 队列，使目标暂时不可达时消息不会立即丢失。
5. 在 DTN 之上实现 STREAM 可靠流，支持分片、确认、重传和窗口控制。
6. 将节点 duty-cycling 信息纳入离线判定、发送时机和 ACK 超时估计。
7. 构建可复现实验框架，量化拓扑收敛时间和 duty-cycling 下 DTN 交付时延。
8. 给出握手机制的形式化验证骨架，形成安全章节的可审计材料。

### 1.4 主要贡献

本文的主要贡献包括：

1. **提出一体化的受限网络远程运维控制面设计**：将 Gossip、补链、DTN 和 STREAM 组合为一个完整原型，而不是分别讨论孤立算法。
2. **实现 sleep-aware 的 DTN 投递机制**：Kelpie 根据目标路径上的 sleep/work/next_wake 信息估计发送延迟、HoldUntil 和 ACK 超时，避免把短时睡眠误判为永久离线。
3. **实现补链自愈调度器**：通过节点质量、路径重叠、睡眠预算、深度、冗余度和工作窗口等因素对候选节点排序，使补链不只是简单随机重连。
4. **实现 DTN 上的可靠 STREAM**：在 DTN 消息承载之上加入分片、ACK、RTO 估计、AIMD 窗口和超时重传，让系统可以支撑更长的数据交换。
5. **构建 Trace 回放实验框架**：能够在本机自动启动 mini-cluster、注入事件、采集 metrics、生成 CSV 和 SVG 图表，增强毕业论文实验的可复现性。
6. **给出形式化验证骨架**：将预认证和握手流程抽象到 Tamarin/ProVerif 模型中，为后续安全证明扩展提供基础。

### 1.5 论文组织

全文组织如下：

第 2 章介绍相关技术与研究现状，包括 Gossip、DTN、duty cycling、补链自愈和形式化验证。第 3 章分析系统需求与总体架构。第 4 章详细介绍 Gossip 拓扑维护和补链调度。第 5 章介绍 DTN/STREAM 与 duty-cycling 的协同机制。第 6 章描述系统实现，包括代码结构、协议、存储、UI 和实验器。第 7 章给出实验设计、结果和分析。第 8 章讨论安全机制与形式化验证。第 9 章总结局限性与改进方向。第 10 章给出全文总结。

---

## 第 2 章 相关技术与研究现状

### 2.1 受限网络与 DTN

DTN 的基本思想是 store-carry-forward，即节点在无法立即转发数据时先存储数据，等未来出现接触机会时再继续转发。Fall 在 SIGCOMM 2003 中提出面向 challenged internets 的 DTN 架构，指出传统 Internet 协议栈中关于端到端持续路径和低时延反馈的假设在深空通信、传感器网络、移动自组网等场景中并不成立 [4]。RFC 4838 进一步从体系结构角度总结了 DTN 的设计目标和网络模型 [5]。

Bundle Protocol 是 DTN 领域的重要协议。RFC 9171 定义了 Bundle Protocol Version 7，强调 bundle 可以跨越不同区域、不同收敛层和间歇连接环境进行传递 [6]。RFC 9174 则定义了 TCP Convergence-Layer Protocol Version 4，为 Bundle Protocol 在 TCP 链路上的承载提供规范 [16]。Shepherd 没有直接实现 BPv7，而是借鉴了 DTN 的核心思想：管理端按目标维护 bundle 队列，代理端在链路恢复或主动 pull 时继续投递，系统用 ACK 和 TTL 保证控制面消息不会因短时不可达而立即丢失。

DTN 路由方面，Vahdat 与 Becker 提出的 Epidemic Routing 通过泛洪式复制在部分连接网络中提升交付概率 [7]；PRoPHET 使用历史接触概率进行转发选择，后来被写入 RFC 6693 [8]；Spray and Wait 通过限制副本数量在交付概率与资源消耗之间折中 [9]；MaxProp 则在车辆网络中结合优先级、历史交付概率和 ACK 清理缓冲区 [10]。这些工作说明：在间歇连接环境中，单纯“失败即丢弃”的即时通信模型是不充分的，系统必须显式管理队列、副本、优先级、重试和过期策略。

Shepherd 的 DTN 设计与上述研究的关系是：它不追求通用 DTN 路由最优，而是面向远程运维控制面做工程化裁剪。Kelpie 是管理中心，知道当前拓扑与目标路径，因此不需要完全分布式地在所有节点之间复制 bundle；但目标离线、父链路断开和睡眠窗口错过仍然会发生，因此必须实现 per-target 队列、HoldUntil、ACK、重试和 sleep-aware 发送时机。

### 2.2 Gossip 与成员关系维护

Gossip 协议常用于大规模分布式系统的状态传播、成员关系维护和故障检测。Demers 等人在 replicated database maintenance 中提出 epidemic algorithms，用随机传播方式同步副本状态 [1]。Jelasity 等人的 peer sampling 研究将 Gossip 用于构建非结构化 P2P 系统的随机邻居视图 [2]。SWIM 则将 infection-style 传播与故障检测结合，用于可扩展的弱一致成员关系维护 [3]。

Gossip 的优势在于去中心化、鲁棒、实现简单、对局部失败不敏感。它不要求所有节点同时连接管理端，也不要求每次状态变化都通过中心广播。缺点是收敛具有概率性，传播延迟与 fanout、TTL、周期、节点规模和链路质量相关，并且在资源受限环境中需要控制传播开销。

Shepherd 使用 Gossip 的原因是：受限网络中的代理节点不一定能长期保持到 Kelpie 的直接连接；Flock 节点之间的局部视图可以逐步帮助系统发现节点和路径；同时，Gossip payload 可以携带 sleepSeconds、nextWake、health、queueDepth 等轻量信息，为 Kelpie 的拓扑判定和补链调度提供依据。

### 2.3 Duty Cycling 与睡眠网络

Duty cycling 是低功耗网络中的常见策略。节点周期性进入睡眠状态以降低能耗，只在工作窗口内收发数据。X-MAC 等低功耗 MAC 协议通过短前导、低功耗监听等方式降低能耗并提高睡眠节点的通信效率 [11]。机会网络中的 duty cycling 研究也指出，节点睡眠会改变接触过程，从而影响延迟、交付概率和转发机会 [12]。

对于远程运维系统而言，duty cycling 带来的关键问题不是节能本身，而是“睡眠”和“故障”在观测上相似：节点不响应既可能是永久离线，也可能只是处于睡眠窗口。如果控制面把短时睡眠误判为故障，就会错误删除节点或触发过度补链；如果系统完全忽略睡眠，则消息可能反复在错误时间投递，错过短暂工作窗口。

Shepherd 将 sleepSeconds、workSeconds 和 nextWake 纳入拓扑模型，并使用如下期望等待模型：

\[
E[T_w] = \frac{T_{sleep}^2}{2(T_{sleep}+T_{work})}.
\]

该模型刻画随机时刻到达时，消息等待目标下一次工作窗口的平均时间。它不是严格调度最优解，而是用于指导 HoldUntil、ACK timeout 和路径 sleep budget 的工程近似。

### 2.4 自愈拓扑与补链

多跳远程运维系统常用树状拓扑组织节点，因为树结构便于管理、路由和权限控制。但树状拓扑的弱点也很明显：父节点失效会导致整支子树暂时失联。如果系统只依赖主树链路，则故障恢复依赖原父节点重连或子节点重新接入，恢复时间不可控。

补链自愈的思想是在主树之外建立少量冗余边。补链不应演变为完全 mesh，否则会引入过高连接成本、状态复杂度和流量噪音；也不应只做随机连接，否则可能与主路径高度重叠，对故障恢复帮助有限。因此补链需要在恢复能力和额外开销之间折中。

Shepherd 的 SupplementalPlanner 将补链选择建模为候选评分问题。候选节点的代价综合节点质量、路径重叠、睡眠预算、深度、冗余度和工作窗口。系统倾向于选择质量更好、睡眠预算更低、与现有主路径重叠更少、深度更浅的候选，以降低补链无效和过度连接的概率。

### 2.5 认证握手与形式化验证

远程运维系统涉及控制消息、节点接入和数据通道，因此握手和认证是基本安全要求。Bellare、Pointcheval 与 Rogaway 关于口令认证密钥交换的研究为预共享口令和抗字典攻击协议提供了理论基础 [13]。实际工程中，还需要结合随机 Nonce、HMAC、超时、复杂度校验和会话密钥派生等手段降低伪连接和重放风险。

安全协议仅靠自然语言描述很难覆盖并发、重放和攻击者模型。Tamarin 和 ProVerif 是常用的符号模型验证工具。Tamarin 支持多集合重写规则和一阶逻辑引理，适合分析认证、保密和可达性性质 [14]；ProVerif 适合自动验证基于 Dolev-Yao 模型的加密协议性质 [15]。Shepherd 的形式化部分目前是骨架级：将预认证挑战应答和 HI/UUID 交换抽象出来，验证共享秘密保密性和基本对应性，为后续更强性质证明打基础。

### 2.6 本文工作的定位

综上，已有研究分别解决了 Gossip 状态传播、DTN 延迟容忍路由、duty cycling 低功耗通信和安全协议验证等问题。但面向受限网络远程运维的系统，需要把这些机制整合起来：

- Gossip 提供逐步收敛的拓扑视图；
- 补链提供局部自愈能力；
- DTN 提供目标不可达时的暂存和最终交付；
- STREAM 提供长数据流能力；
- duty-cycle 感知避免把睡眠误判为故障；
- 形式化模型支撑握手机制的安全论证。

Shepherd 的贡献不在于提出某个全新的单点算法，而在于围绕“睡眠窗口 + 多跳拓扑 + 间歇连接 + 远程运维控制面”这一综合问题，完成了一个可运行、可观测、可复现实验验证的原型系统。

---

## 第 3 章 需求分析与总体架构

### 3.1 应用场景与假设

Shepherd 面向授权环境下的远程运维、实验复现和教学研究场景。系统假设操作者拥有对目标网络和节点的合法授权，不讨论未授权入侵、规避检测或恶意控制。由于远程运维工具天然具有双重用途，本文在设计与表述中将系统定位为受限网络管理原型，并把认证、审计、令牌、TOFU 指纹确认和形式化验证作为必要支撑。

系统运行环境的基本假设如下：

1. 管理端 Kelpie 能在某个入口地址上接受 root Flock 接入，或由 Stockman 先配置 Controller Listener。
2. Flock 节点之间可通过父子链路或补链进行转发，但链路可能中断。
3. 节点可能周期性睡眠，睡眠期间上游连接不可用。
4. 管理端与代理端共享初始 secret，用于预认证和会话建立。
5. 实验环境允许在本机启动多个进程模拟 mini-cluster。

### 3.2 功能需求

系统主要功能需求如下：

1. **节点接入**：Flock 可以主动连接 Kelpie 或上游 Flock，也可以通过 listener 被动等待连接。
2. **拓扑维护**：Kelpie 维护节点、父子关系、补链边、在线状态、lastSeen、sleep profile 和路由信息。
3. **Gossip 同步**：Flock 周期性传播 NodeInfo，并在拓扑变化、发现请求和故障恢复时触发更新。
4. **补链自愈**：当节点离线、链路失败或队列长时间堆积时，Kelpie 可触发补链或 repair。
5. **DTN 队列**：Kelpie 能向目标节点 enqueue bundle，支持优先级、TTL、HoldUntil、ACK、重试、过期和统计。
6. **STREAM 传输**：系统能在 DTN payload 上承载可靠流，用于代理、文件和诊断类长数据交换。
7. **Sleep 控制**：Kelpie 可向 Flock 下发 sleep/work/jitter 更新，Flock 上报 nextWake 等信息。
8. **UI 展示**：Stockman 展示连接、拓扑、节点详情、事件时间线和演示控制台。
9. **实验复现**：trace_replay 能自动启动集群、注入事件、记录指标、生成可分析数据。

### 3.3 非功能需求

非功能需求包括：

1. **可复现性**：构建、测试和实验应通过 Makefile 和脚本串行运行，避免环境状态不透明。
2. **可观测性**：系统应暴露 metrics、事件流、DTN 队列状态、补链事件和路由状态。
3. **鲁棒性**：短暂离线不应导致节点被误删，链路恢复后应能释放 HoldUntil 并继续投递。
4. **安全性**：接入前进行预认证，UI 控制面使用 token，TLS 场景支持 TOFU 指纹确认。
5. **模块化**：Kelpie、Flock、协议、UI 和实验器分目录组织，便于论文映射和测试。

### 3.4 总体架构

Shepherd 的总体架构如图 3-1 所示。

![图 3-1 Shepherd 总体架构](figures/shepherd_overview.svg)

系统分为三层：

1. **客户端展示层**：Stockman 通过 Wails v3 + Vue 3 实现，调用本地 Go facade，再由 facade 连接 Kelpie gRPC UI。
2. **管理控制层**：Kelpie 维护拓扑、会话、DTN、STREAM、补链、controller listener、pivot listener、dataplane token 和 SQLite 持久化。
3. **代理执行层**：Flock 运行在节点上，处理连接、转发、Gossip、sleep、repair、carry-forward 和流式数据。

Kelpie 与 Stockman 之间主要使用 gRPC；Kelpie 与 Flock 之间使用 Shepherd 自定义协议，支持 raw、HTTP/WebSocket 等传输包装。协议消息经过 framing、压缩和 AES 加密，并通过 header 中的 sender、accepter、messageType、route 等字段完成逐跳路由。

### 3.5 控制面与数据面

系统将控制面和数据面做了相对分离。

控制面包括：

- Flock 接入与 UUID 分配；
- Gossip NodeInfo；
- 拓扑变更；
- DTN bundle enqueue/ACK；
- Sleep update；
- 补链请求与响应；
- UI gRPC 查询和 WatchEvents。

数据面包括：

- DTN payload；
- STREAM 分片；
- dataplane TCP 多路复用；
- 文件、代理或诊断流。

图 3-2 展示了控制面与数据面的关系。

![图 3-2 控制面与数据面](figures/shepherd_control_dataplane.svg)

控制面提供状态和调度，数据面提供实际字节传输。DTN/STREAM 位于两者之间：DTN 既是控制消息的可靠暂存机制，也是 STREAM 的底层承载。

### 3.6 数据模型

Kelpie 的核心数据模型包括：

- **Node**：UUID、父节点、主机名、用户名、memo、lastSeen、isAlive、sleepSeconds、workSeconds、nextWake。
- **Edge**：父子树边与 supplemental 边。
- **RouteInfo**：目标节点的 entry、path、display、depth。
- **Bundle**：ID、target、payload、priority、enqueuedAt、holdUntil、deliverBy、attempts、meta。
- **StreamSession**：streamID、target、seq、ack、inflight、pending、window、RTO、lastActivity。
- **Supplemental Candidate**：UUID、path、depth、overlap、sleepBudget、redundancy、workSeconds。

这些模型分别落在 `internal/kelpie/topology/`、`internal/kelpie/dtn/`、`internal/kelpie/stream/` 和 `internal/kelpie/planner/` 中。SQLite 负责持久化 topology、listeners、controller_listeners、loot、dtn_bundles、collab 等信息。Flock 端则主要维护运行时 session、knownNodes、neighbors、carryQueue、upCarryQueue、streams 和 sleep 状态。

---

## 第 4 章 Gossip 拓扑维护与补链自愈设计

### 4.1 拓扑维护目标

Shepherd 的拓扑层要回答三个问题：

1. 当前系统中有哪些节点？
2. 节点之间通过哪些父子链路和补链边连接？
3. 从 Kelpie 到目标节点应该沿哪条 route 转发消息？

在受限网络中，拓扑维护不能简单依赖“连接存在即在线，连接断开即删除”。原因是节点可能睡眠、重连或经由补链恢复。因此，Kelpie 的 topology 模块将节点存在性和在线状态区分开：节点离线时保留节点记录和父子关系，只将 `isAlive` 标记为 false，并在后续重连或 Gossip 更新时重新置为在线。

### 4.2 Flock 侧 Gossip 信息生成

Flock 在 `internal/flock/process/gossip.go` 中初始化 GossipManager，并周期性发送 NodeInfo。NodeInfo 包括 UUID、IP、端口、邻居列表、状态、健康度、lastSeen、queueDepth、sleepSeconds 和 nextWake 等字段。Flock 在启动后会发送 bootstrap sleep report，避免 Kelpie 必须等待首个 Gossip 周期才能获知节点 sleep profile。

Gossip 传播采用周期触发和事件触发结合：

- 定时 ticker 到期时发送 Gossip update；
- 节点状态变化时触发 update；
- discovery loop 收集潜在邻居并发起发现请求；
- Gossip 周期中机会式发起 DTN_PULL，帮助目标主动拉取队列中的 bundle。

### 4.3 自适应 fanout 与 TTL

固定 fanout 和 TTL 在动态网络中存在两个问题：节点少时过高 fanout 会浪费带宽；节点多或发生 failover 时过低 fanout 又会导致收敛慢。Shepherd 因此实现了动态 fanout 与 TTL。

Flock 的 `dynamicFanout()` 大致遵循以下思路：

\[
f = \operatorname{clip}(f_{base} + \lceil \log_2 n \rceil + \Delta_{failover} - \Delta_{unhealthy} - \Delta_{queue} + \Delta_{sleepy}, 1, 10)
\]

其中 \(n\) 为已知节点数。若存在 pending failover，则增加 fanout；若 unhealthyRatio 或 queuePressure 较高，则降低 fanout；若 sleepyRatio 较高，则适当提高传播范围。最后通过 token bucket 限速，避免短时间内过度洪泛。

Flock 的 `dynamicTTL()` 以配置的 MaxTTL 为基础，随已知节点数按 \(\log_{10}(n+1)\) 增加，并根据本节点 sleep 状态、pending failover、unhealthyRatio、sleepyRatio 和 queuePressure 微调。该设计的目标不是提供严格最优传播，而是在工程上根据网络规模和压力调节传播半径。

### 4.4 Kelpie 侧拓扑图

Kelpie 的 `Topology` 使用内存图结构维护节点和边。核心字段包括：

- `nodes`：节点编号到 node 的映射；
- `parentByChild` 和 `childrenByParent`：父子关系；
- `edges`：邻接表；
- `edgeWeights`、`edgeLatencies`、`edgeTypes`：边权、延迟和边类型；
- `routeInfo`：路由计算结果；
- `nodeInfo` 和 `gossipCache`：Gossip 视图与去重；
- `staleTimeout` 和 `zeroSleepGrace`：离线判定策略。

拓扑操作通过 `TopoTask` 串行进入 `Topology.Run()`，避免并发修改图结构。常见任务包括 ADDNODE、ADDEDGE、CALCULATE、GETROUTE、MARKSTALEOFFLINE、MARKNODEOFFLINE、PRUNEOFFLINE 等。路由计算会根据节点、边和延迟估计更新 `routeInfo`。

### 4.5 Sleep-aware 离线判定

离线判定是 Shepherd 中容易出错的部分。如果节点短暂睡眠就被删除，则 DTN 队列和补链都失去目标；如果永远不标记离线，则 UI 和路由会保留错误状态。Shepherd 使用 sleep-aware 策略折中：

1. 当节点显式上报 sleepSeconds/workSeconds/nextWake 时，Kelpie 将其写入 topology。
2. `markStaleOffline()` 根据 lastSeen、sleep 预算和默认 grace 判断节点是否 stale。
3. 离线时优先标记节点状态，而不是删除节点记录。
4. 节点重新上线时触发 `onNodeReonline()`，释放 DTN HoldUntil 并立即 flush。

这种设计体现了“节点存在性”和“当前连通性”的区别。对于受限网络，离线状态应当是可恢复状态，而不是立即删除。

### 4.6 补链候选选择

补链自愈由 `internal/kelpie/planner/` 中的 SupplementalPlanner 实现。Planner 维护队列、失败状态、repair 状态、策略参数、指标和节点质量。它可以由节点添加、链路失败、周期检查、手动 repair 或 DTN 队列长时间堆积触发。

候选排序使用 `candidateScore()`。评分项包括：

- **节点质量**：由 health、latency、failure、queue、staleness 等 EMA 指标合成；
- **睡眠预算**：路径上累计 sleep budget 越高，候选代价越高；
- **路径重叠**：与现有主路径重叠越多，冗余价值越低；
- **深度**：候选越深，故障传播风险越高；
- **冗余度**：用于衡量补链是否真正提供不同路径；
- **工作窗口**：workSeconds 越短，作为补链节点的可用窗口越有限。

简化表示为：

\[
S(c) = w_q Q(c) + w_s Sleep(c) + w_o Overlap(c) + w_d Depth(c) + w_r Redundancy(c) + w_w Work(c)
\]

实现中分数越低越优。排序时先按综合分数，再按冗余度、重叠度、深度和 UUID 稳定排序。

### 4.7 补链事件与可观测性

Planner 记录 dispatched、success、failures、dropped、recycled、repairAttempts、repairSuccess、repairFailures、queueHigh 和 lastFailure 等指标，并保留最近事件。gRPC UI 可以通过 SupplementalAdminService 查询补链状态、指标、事件、质量和 repair 状态。Stockman 在 Metrics 和 Timeline 中展示这些信息，便于答辩演示和实验分析。

补链机制在本文实验中尚未完成完整消融验证，但它已经作为系统实现的一等模块存在，并参与节点离线、DTN 长时间堆积和 repair 场景。后续工作需要通过“启用/禁用补链”的对照实验量化其贡献。

---

## 第 5 章 DTN/STREAM 与 Duty-Cycling 协同机制

### 5.1 DTN 队列设计

Kelpie 的 DTN 管理器位于 `internal/kelpie/dtn/`。核心结构是 `Bundle`：

- `ID`：bundle 唯一标识；
- `Target`：目标节点 UUID；
- `Payload`：载荷；
- `Priority`：高、中、低优先级；
- `EnqueuedAt`：入队时间；
- `HoldUntil`：延迟到某个时刻才可投递；
- `DeliverBy`：TTL 截止时间；
- `Attempts`：重试次数；
- `Meta`：附加元数据。

Manager 按目标节点维护队列。每个队列有容量限制，容量超出时按优先级和排序策略丢弃部分 bundle。Manager 支持 Enqueue、Ready、ReadyFor、Requeue、Remove、Stats 和 List 等操作。Kelpie 启动时会从 SQLite 的 dtn_bundles 表恢复未过期 bundle，因此管理端重启后可保留 DTN 状态。Flock 侧的 carry queue 和 upCarryQueue 则主要是内存运行时缓冲，用于在上游 session 暂时不可用时暂存小型控制面消息。

### 5.2 DTN 调度流程

Kelpie 的 `initDTN()` 创建 Manager，并启动 `runDTNDispatcher()`。调度器每 500ms 执行一次 flush。流程如下：

```text
算法 5-1：Kelpie DTN dispatch

输入：当前时间 now，DTN 队列 Q
1. 扫描 inflight bundle，若 ACK 超时则重新入队
2. 从 Q 中取出 ready bundle
3. 对每个 bundle：
   3.1 若队列负载过高或 inflight 达到上限，则根据 Focus 策略延后
   3.2 若 topology 推荐 sleep-aware delay，则设置 HoldUntil 后重新入队
   3.3 查询目标 route 和 firstHop
   3.4 若 route/session 不可用，则根据 nextSendDelay 或退避重新入队
   3.5 构造 DTN_DATA 并沿 route 发送
   3.6 记录 inflight，等待 DTN_ACK
4. 更新队列指标
```

Flock 收到 DTN_DATA 后调用 `applyDTNPayload()` 解析载荷。当前诊断载荷支持 `memo:`、`log:`、`stream:` 和 `proto:` 等前缀。执行后 Flock 发送 DTN_ACK。Kelpie 收到 ACK 后，如果成功则从 inflight 和队列清理 bundle，更新 delivered 计数；如果失败且属于永久错误（如 unsupported payload、invalid proto envelope），则直接丢弃，避免无限重试挤占 Gossip 和心跳；如果是临时错误，则根据 sleep-aware delay 或退避重新入队。

### 5.3 Spray/Focus 负载控制

DTN 路由研究表明，无限制复制或无限制并发会造成队列拥塞。Shepherd 虽然不是通用 DTN 路由系统，但仍需要控制 per-target inflight 和队列压力。Kelpie 的 `preDispatchDelay()` 使用 Spray/Focus 风格的启发式策略：

- 当队列占用低于 sprayThreshold 时，允许较积极地投递；
- 当队列占用超过 focusThreshold 或 heldRatio 较高时，如果该目标已有 inflight bundle，则延迟后续 bundle；
- 当 inflight 达到 per-target 上限时，延后投递。

该策略避免在目标睡眠或路由不稳定时把大量 bundle 同时推入同一链路，降低 ACK timeout 和重复重试造成的扰动。

### 5.4 Duty-cycle 等待模型

Shepherd 在 topology 的 `latency.go` 中实现了 sleep-aware 等待估计。对于节点 \(v\)，若存在明确 `nextWake` 且 arrival 早于 nextWake，则等待时间为：

\[
W(v,t) = nextWake(v) - t
\]

若没有明确 nextWake，但存在 sleep/work 参数，则使用平均等待模型：

\[
W(v) = \frac{T_{sleep}^2}{2(T_{sleep}+T_{work})}.
\]

`RecommendSendDelay(target, base)` 会沿当前 route 估计各 hop 到达时刻，并尝试将出发时间对齐到显式 nextWake。需要注意的是，代码注释也明确说明：该算法是启发式近似，不保证全局最优。对于只有 duty-cycle 平均值、没有明确 nextWake 的节点，继续迭代会发散，因此实现不会反复把平均等待加到发送延迟上。

### 5.5 ACK timeout 与 PathSleepBudget

DTN 投递后的 ACK timeout 不能只使用固定值。若目标路径上存在睡眠节点，ACK 返回可能天然晚于普通链路。Kelpie 的 `dtnAckTimeout(target)` 使用：

\[
Timeout = BaseTimeout + PathSleepBudget(target) + DispatchInterval
\]

并限制在最小值和最大值之间。`PathSleepBudget()` 累计目标 route 上每个 hop 的 sleep budget 和 grace，防止短睡眠导致过早重试。

当节点重新上线时，`onNodeReonline()` 会清空该目标队列的 HoldUntil 并立即 flush，避免 bundle 还在等待旧的睡眠估计而错过短暂工作窗口。这一细节是实验稳定性的关键。

### 5.6 STREAM 可靠流

DTN bundle 适合离散消息，但远程运维还需要较长数据流，例如文件、代理、诊断输出。Shepherd 的 `internal/kelpie/stream/engine.go` 在 DTN 之上实现了 STREAM。

STREAM 的关键机制包括：

- **分片**：默认 ChunkBytes 为 768；
- **窗口**：InitialWindow、MinWindow、WindowFrames 控制 inflight frame 数；
- **ACK**：接收端对 seq 发送累计确认；
- **RTO**：使用 SRTT/RTTVAR 估计超时；
- **AIMD**：ACK 到达时增加窗口，超时时乘法减小；
- **重传限制**：超过 RetransLimit 后 abort；
- **诊断**：暴露 streamID、target、pending、inflight、window、seq、ack、RTO 和 lastActivity。

STREAM_OPEN、STREAM_DATA、STREAM_ACK 和 STREAM_CLOSE 都编码为协议 payload，再通过 DTN enqueue 投递。这样，长流不直接依赖稳定 TCP 端到端路径，而是继承 DTN 的重试与 sleep-aware 调度能力。

图 5-1 展示 DTN 与 STREAM 的关系。

![图 5-1 DTN 与 STREAM](figures/shepherd_dtn_stream.svg)

### 5.7 Dataplane TCP 多路复用

Kelpie 还实现了实验性的 dataplane TCP server，使用一次性 token 建立上传、下载或代理逻辑流。客户端先通过 gRPC DataplaneAdmin 准备 token，再通过 dataplane TCP 连接发送 OPEN/DATA/CLOSE frame。Kelpie 端消费 token 后调用 `Admin.OpenStream()` 建立到目标的 STREAM。这样可以把 UI 控制面授权和数据传输通道分离，同时保留 maxSize、maxRate、TTL、hash、offset 和 retries 等控制项。

---

## 第 6 章 系统实现

### 6.1 代码组织

Shepherd 使用 Go 1.25 实现核心系统，Stockman 前端使用 Vue 3、Vite、TypeScript 和 Pinia。主要目录如下：

| 目录 | 说明 |
| --- | --- |
| `cmd/kelpie` | Kelpie 管理端入口 |
| `cmd/flock` | Flock 代理端入口 |
| `internal/kelpie/process` | Kelpie 编排、路由、DTN、STREAM、listener、控制命令 |
| `internal/kelpie/topology` | 拓扑图、路由、sleep-aware latency、UI snapshot |
| `internal/kelpie/planner` | 补链自愈调度器 |
| `internal/kelpie/dtn` | DTN 队列 |
| `internal/kelpie/stream` | DTN 上的可靠流 |
| `internal/kelpie/ui/grpcserver` | gRPC UI 服务 |
| `internal/flock/process` | Flock 会话、路由、Gossip、sleep、failover、carry-forward |
| `internal/flock/gossip` | GossipManager |
| `protocol` | 自定义消息类型、raw/http/websocket 传输、payload codec |
| `proto` | gRPC proto 定义 |
| `clientui` | Wails + Vue3 Stockman 客户端 |
| `experiments/trace_replay` | Trace 回放实验器 |
| `docs` | 报告、实验数据和图表 |
| `formal` | Tamarin/ProVerif 形式化验证骨架 |

构建入口统一由 Makefile 管理。`make admin agent` 构建 Kelpie 与 Flock；`make stockman` 构建 Stockman；`make test` 执行 Go 测试；`make regress` 执行 trace replay 回归；`make check` 执行 race test 和回归。

### 6.2 Kelpie 启动流程

Kelpie 的入口在 `cmd/kelpie/main.go`。启动流程包括：

1. 解析 CLI 参数，要求 secret 和 UI token；
2. 生成预认证 token；
3. 生成并展示 TLS 证书指纹，供 Stockman TOFU 确认；
4. 初始化 protocol transports 和全局 store；
5. 初始化 SQLite 数据库、topology、listener、controller listener、loot、DTN 和 collab repository；
6. 从数据库恢复拓扑快照，并将旧在线状态统一重置为离线；
7. 根据 sleep 参数配置 stale policy；
8. 启动 topology goroutine；
9. 如果存在 pending controller listener，则使用其 bind 作为入口；
10. 创建 Admin，并启动 manager、router、supplemental planner、DTN、STREAM、listener reconciler 和 stale monitor；
11. 启动 gRPC UI 与 dataplane TCP server。

Kelpie 当前以 teamserver 模式运行，UI gRPC 使用 token 鉴权。gRPC interceptor 会先验证 token，再附加 collab claims，并记录 audit。

### 6.3 Flock 启动流程

Flock 的入口在 `cmd/flock/main.go`。启动流程包括：

1. 解析 CLI 参数，确定主动连接、被动监听、重连、SOCKS/HTTP 代理或端口复用模式；
2. 根据 secret 生成预认证 token；
3. 创建 Agent；
4. 根据模式建立初始连接；
5. 设置 UUID、初始化 store、绑定 session；
6. 启动 manager、router、repair listener、MyInfo、bootstrap sleep report、Gossip、sleep manager、carry-forward；
7. 启动 listener/SSH/shell/offline dispatch；
8. 等待子节点、补链连接，并处理上游数据。

Flock 的 runtime 状态包括 knownNodes、neighbors、pending discovery、pending failovers、sleepPredictor、childDispatchers、carryQueue、upCarryQueue、streams、file streams、proxy streams 和 sleep 控制器。

### 6.4 协议实现

Shepherd 的 raw 协议 header 包括 magic、flags、sender、accepter、messageType、routeLen、route 和 dataLen。payload 在非 pass-through 情况下经过生成 codec 序列化、gzip 压缩和 AES 加密。中间节点对于 TEMP_UUID 的逐跳路由消息不会重复解密，只在最终一跳或 Admin 侧解密。

协议 messageType 包括：

- 基础握手与控制：HI、UUID、MYINFO、NODEOFFLINE、HEARTBEAT、RUNTIMELOG；
- Gossip：GOSSIP_UPDATE、GOSSIP_REQUEST、GOSSIP_RESPONSE 等；
- 补链：SUPPLINKREQ、SUPPFAILOVER、RESCUE_REQUEST 等；
- DTN：DTN_DATA、DTN_ACK、DTN_PULL；
- STREAM：STREAM_OPEN、STREAM_DATA、STREAM_ACK、STREAM_CLOSE；
- Sleep：SLEEP_UPDATE、SLEEP_UPDATE_ACK。

payload codec 由 `tools/gencodec` 生成，生成文件不手动修改。

### 6.5 gRPC UI

UI proto 位于 `proto/kelpieui/v1/kelpieui.proto`。主要服务包括：

- KelpieUIService：GetSnapshot、GetTopology、NodeStatus、WatchEvents、GetMetrics、EnqueueDtnPayload、PruneOffline、ProxyStream 等；
- PivotListenerAdminService；
- ControllerListenerAdminService；
- SleepAdminService；
- SupplementalAdminService；
- ConnectAdminService；
- DataplaneAdmin。

gRPC server 位于 `internal/kelpie/ui/grpcserver/`。Server 将 Admin 的能力拆成 TopologyAdmin、SessionAdmin、StreamAdmin、ListenerAdmin、LootAdmin、ProxyAdmin、SupplementalAdmin 和 DTNAdmin 等接口，降低 UI 层与具体实现的耦合。

### 6.6 Stockman 客户端

Stockman 位于 `clientui/`，是面向答辩演示的轻量桌面客户端。它不复刻旧版 Qt 客户端的全部 shell、文件、SOCKS、chat、audit、loot 功能，而是聚焦五类视图：

1. 连接管理：支持 gRPC endpoint、token、TLS TOFU 指纹确认和最近连接；
2. 拓扑总览：力导向图和树状图；
3. 节点详情：节点、session、stream、sleep 信息；
4. 事件时间线：订阅 Kelpie WatchEvents；
5. 演示控制台：DTN enqueue、sleep update、prune offline。

Wails 后端 facade 位于 `clientui/backend/service/`，负责连接 Kelpie、维护 event ring、转发事件到前端。前端使用 Pinia stores 管理 connection、topology、events 和 metrics。

### 6.7 Trace 回放实验器

`experiments/trace_replay` 是本文实验的核心工具。它会：

1. 启动 Kelpie；
2. 通过 gRPC 创建 Controller Listener；
3. 启动 root Flock；
4. 根据 star 或 chain 拓扑创建 Pivot Listener 并启动更多 Flock；
5. 解析 JSONL trace，在指定 at_ms 注入事件；
6. 周期性调用 gRPC metrics 和 snapshot；
7. 输出 config.json、labels.json、metrics.jsonl、events.jsonl 和 logs。

trace 支持 sleep、dtn_enqueue、kill、metrics、kelpie_restart、dataplane_roundtrip、stream_proxy、io_burst 等事件。回归脚本会断言 DTN enqueue 与 delivered 计数匹配，并对关键事件要求 trace_result ok=true。除用于生成论文图表的实验脚本外，仓库还保留标准回归 trace，用于覆盖更高风险的路径转发、Gossip 传播、DTN ACK 和重连场景。

### 6.8 测试情况

项目包含单元测试、包级测试和 integration 测试，覆盖 protocol、DTN、STREAM、topology、planner、flock process、grpcserver、handshake 等模块。本文定稿前执行 `make test`（即 `go test ./...`）和重点包 race 检查（`go test -race ./internal/kelpie/topology ./internal/kelpie/process ./internal/flock/process ./internal/flock/manager`），均通过。对涉及 DTN ACK、Gossip memo 与 sleep/failover 的高风险路径，还补充执行了目标 trace replay 回归，结果见第 7.4 节。

---

## 第 7 章 实验设计与结果分析

### 7.1 实验环境

本文实验使用本机 Trace 回放方式。实验不依赖外部仿真器，默认环境包括：

- Go 1.25；
- Make；
- Python 3 标准库；
- Bash；
- 本仓库构建出的 `build/kelpie`、`build/flock` 和 `build/trace_replay`。

实验脚本为 `script/experiments.sh`。运行后生成：

- `docs/data/bootstrap_summary.csv`；
- `docs/data/dtn_latency_samples.csv`；
- `docs/data/dtn_latency_summary.csv`；
- `docs/figures/bootstrap_convergence.svg`；
- `docs/figures/dtn_latency.svg`。

### 7.2 实验 A：拓扑收敛

#### 7.2.1 实验目的

实验 A 评估 Gossip 驱动的拓扑 bootstrap/收敛速度。关注问题是：在不同拓扑形状和节点规模下，Kelpie 需要多长时间得到一个节点数正确、节点在线状态正确、边数达到阈值的拓扑视图。

#### 7.2.2 实验设置

实验参数如下：

| 参数 | 取值 |
| --- | --- |
| 拓扑 | star、chain |
| 节点数 | 4、6、8 |
| 重复次数 | 每组 3 次 |
| 运行时长 | 20s |
| 指标采样周期 | 500ms |
| 输出文件 | `docs/data/bootstrap_summary.csv` |

star 拓扑中，子节点尽量直接连接 root；chain 拓扑中，节点按 root → n1 → n2 → ... 形成多跳链。

#### 7.2.3 指标定义

`converged_ms` 的判据为：

1. 拓扑节点总数达到期望值；
2. 节点全部 online；
3. 边数达到实现定义的阈值。

该指标不追求形式化图一致性的严格证明，而是作为实验中可重复提取的收敛代理指标。

#### 7.2.4 实验结果

| 拓扑 | 节点数 | 收敛时间均值 ± 标准差 | 最小-最大 |
| --- | --- | --- | --- |
| star | 4 | 1.661 ± 0.116s | 1.593-1.795s |
| star | 6 | 2.517 ± 0.003s | 2.514-2.520s |
| star | 8 | 3.438 ± 0.005s | 3.433-3.441s |
| chain | 4 | 1.905 ± 0.004s | 1.901-1.908s |
| chain | 6 | 3.138 ± 0.002s | 3.136-3.139s |
| chain | 8 | 4.372 ± 0.006s | 4.368-4.378s |

图 7-1 给出了趋势图。

![图 7-1 拓扑收敛时间](figures/bootstrap_convergence.svg)

#### 7.2.5 结果分析

实验显示，节点数从 4 增加到 8 时，star 和 chain 的收敛时间都上升。这符合预期：节点越多，启动、接入、UUID 分配、Gossip 传播和路由计算需要处理的状态越多。

chain 拓扑收敛慢于 star 拓扑。原因是 chain 中后续节点依赖前序节点的 pivot listener 和父链路，拓扑形成需要逐层推进；而 star 中节点更接近 root，路径更短，Kelpie 更快观察到完整状态。

star,n=4 的标准差相对较大，可能来自本机进程调度、端口分配、进程启动顺序和 metrics 采样边界。由于当前每组只有 3 次重复，实验更多说明趋势和可复现链路，而不是统计显著性结论。

### 7.3 实验 B：Duty-cycling 下 DTN 交付时延

#### 7.3.1 实验目的

实验 B 验证 DTN 在目标节点周期睡眠情况下的最终交付能力，并分析交付时延随 sleep 参数变化的趋势。

#### 7.3.2 实验设置

| 参数 | 取值 |
| --- | --- |
| 拓扑 | chain |
| 节点数 | 4 |
| 路径 | root → n1 → n2 → n3 |
| 目标节点 | n3 |
| trace | baseline、sleep8/work2、sleep16/work2 |
| 每次 run 入队消息数 | 3 |
| 每组重复 | 2 |
| 运行时长 | 70s |
| 指标采样周期 | 500ms |

三条消息分别在约 12s、20s、28s 入队。目标 n3 在 sleep 场景中周期性睡眠。

#### 7.3.3 指标定义

交付时延定义为：

\[
Latency = deliver\_ms - enqueue\_ms.
\]

`enqueue_ms` 来自 trace_action；`deliver_ms` 来自 metrics 中 `dtn_metrics.delivered` 的增长时刻。由于 metrics 每 500ms 采样一次，交付时刻存在量化误差。

理论期望等待采用：

\[
E[T_w] = \frac{T_{sleep}^2}{2(T_{sleep}+T_{work})}.
\]

#### 7.3.4 实验结果

| 场景 | 理论等待 | 平均交付时延 ± 标准差 | p50 | 最小-最大 | 交付数 |
| --- | --- | --- | --- | --- | --- |
| 0/0 | 0.000s | 2.825 ± 1.251s | 2.491s | 1.394-4.589s | 6/6 |
| 8/2 | 3.200s | 5.401 ± 1.633s | 5.402s | 3.401-7.402s | 6/6 |
| 16/2 | 7.111s | 9.400 ± 4.320s | 11.401s | 3.400-13.400s | 6/6 |

图 7-2 展示了按 run 汇总的均值与理论期望线。

![图 7-2 DTN 交付时延](figures/dtn_latency.svg)

#### 7.3.5 结果分析

实验结果表明，三组场景均实现最终交付，交付成功率为 100%。随着 sleepSeconds 从 0 增加到 8 和 16，平均交付时延明显增加。这说明 DTN 队列没有因为目标短时不可达而丢弃消息，同时交付时延对 duty-cycle 参数敏感。

实测均值高于理论等待值。原因包括：

1. 理论模型只刻画随机到达到下一次工作窗口的平均等待，没有包含多跳转发开销。
2. DTN dispatch、ACK、重试和 carry-forward 都会引入额外时延。
3. metrics 采样周期为 500ms，交付时刻只能近似。
4. 入队相位与睡眠相位可能导致消息错过某个短工作窗口。
5. 本机多进程调度会引入抖动。

sleep16/work2 场景方差更大，说明睡眠周期越长，相位对交付时延的影响越明显。该结果符合 duty-cycling 网络的直觉，也支持系统将 sleep budget 纳入发送时机和 ACK timeout 的设计。

### 7.4 工程回归补充验证

除第 7.2 和第 7.3 节用于论文图表的实验外，本文还使用 trace replay 回归脚本补充验证更高风险场景。`gossip_memo_scale_n16` trace 会在 root + n1...n16 的 17 节点环境中连续向目标节点投递两条 memo bundle，用于检查 Gossip 规模传播、DTN 多跳路由、下行子链路写入、ACK 返回和重试判定是否一致。

本次论文定稿前执行的目标回归结果如下。`star` 结果来自 `experiments/out/regress/summary-gossip-memo-scale-n16-final6.json`，`chain` 结果来自 `experiments/out/regress/summary-gossip-memo-scale-n16-chain-final9.json`；回归脚本同时检查 DTN delivered 计数、`wait_memo` 结果和节点状态等待结果。

| Trace | 拓扑 | 节点数 | DTN 交付 | 重试次数 | 结果 |
| --- | --- | ---: | ---: | ---: | --- |
| `gossip_memo_scale_n16` | star | 17 | 2/2 | 0 | PASS |
| `gossip_memo_scale_n16` | chain | 17 | 2/2 | 1 | PASS |

此外，针对 sleep/failover 相关的 `dtn_leaf_sleep_kill_parent_n2` 回归也在 star/chain 拓扑下通过，结果记录于 `experiments/out/regress/summary-dtn-leaf-sleep-kill-parent-n2-final2.json`，每类拓扑 4 条 DTN payload 均完成交付并被 Kelpie 日志观测。该回归不替代大规模性能评估，因为它只覆盖少量确定性 trace，重复次数也较少；但它补充说明当前实现不仅能通过 4、6、8 节点的论文实验，也能在 17 节点规模下维持连续 DTN memo 投递的最终交付语义。

### 7.5 可复现性分析

本文实验的可复现性来自以下设计：

1. 所有构建和实验由 Makefile 和 Bash 脚本统一入口；
2. trace 使用 JSONL，事件时间和参数固定；
3. 每次 run 输出 config.json、labels.json、metrics.jsonl 和 logs；
4. 分析脚本只使用 Python 标准库；
5. 论文图表由 CSV 自动生成；
6. 实验数据和 SVG 图表纳入 `docs/` 管理。

### 7.6 威胁与局限

实验也存在明显局限：

1. 论文图表实验节点规模较小，最高 8 节点；虽然工程回归补充覆盖了 17 节点 trace，但仍不足以代表大规模网络；
2. 当前主要使用本机进程和 loopback 网络，没有真实 delay/loss/jitter；
3. 重复次数较少，统计显著性不足；
4. metrics 采样周期带来 500ms 级误差；
5. 补链和 Gossip 自适应策略尚未形成完整消融矩阵；
6. sleep trace 中节点相位固定，尚未覆盖随机相位分布。

因此，本文实验结论应理解为“原型可行性和趋势验证”，而不是对大规模真实网络性能的最终证明。

---

## 第 8 章 安全机制与形式化验证

### 8.1 安全目标

Shepherd 的安全目标包括：

1. 未持有共享 secret 的连接不能通过预认证；
2. 握手过程应包含随机性，避免固定明文指纹；
3. UI 控制面必须使用 token 鉴权；
4. TLS UI 场景中客户端应确认服务端证书指纹；
5. 数据通路 token 应有 TTL、大小限制、速率限制和一次性消费语义；
6. 关键操作可记录 audit。

### 8.2 预认证机制

预认证实现位于 `pkg/share/preauth.go`。Kelpie 与 Flock 共享 secret，先通过：

\[
token = Truncate(SHA256(pepper || secret))
\]

生成固定长度预认证 token。主动方发送 clientNonce 和 HMAC(token, clientLabel, clientNonce)。被动方验证后生成 serverNonce，并返回 HMAC(token, serverLabel, serverNonce, clientNonce)。主动方验证响应后才继续后续握手。

该机制具有三个作用：

1. 在进入完整协议解析前快速拒绝无效连接；
2. 使用随机 Nonce 避免简单重放；
3. 使用不同 label 区分 client 和 server MAC 方向。

预认证还设置读写 deadline，防止连接长期占用资源。

### 8.3 握手状态与复杂度校验

`pkg/share/handshake/handshake.go` 定义了 Transcript 和 Code，用于记录 start、dial、tls、negotiate、preauth、mfa、exchange、complete 等阶段。secret 至少 8 位，且必须同时包含字母和数字。可选 MFA PIN 必须为至少 4 位数字。

握手消息 HI 使用角色相关的随机 greeting。Admin 使用 ADMIN_UUID，Agent 初始使用 TEMP_UUID。后续 UUID 分配、MYINFO 和拓扑添加完成节点接入。

### 8.4 UI 与 Dataplane 安全

Kelpie gRPC UI 要求 `--ui-grpc-token`。server interceptor 支持 Authorization Bearer 和 `x-kelpie-token`。TLS 可选，启用后需要 cert/key，也可以配置 client CA 实现 mTLS。

Stockman 支持 TLS TOFU：首次连接时读取服务端证书指纹，用户确认后写入本地配置；后续连接如果指纹不匹配，则拒绝并提示 mismatch。

Dataplane 使用一次性 token。PrepareTransfer/PrepareProxy 发放 token，TCP server 消费 token 后建立逻辑流。TokenMeta 包含 target、direction、operator、tenant、maxSize、maxRate、TTL、hash、offset 和 retries 等字段。传输完成后记录 audit hook。

### 8.5 形式化验证骨架

`formal/` 目录提供 Tamarin 和 ProVerif 骨架：

- `formal/tamarin/handshake.spthy`；
- `formal/proverif/handshake.pv`；
- `formal/docker-compose.yml`。

当前模型主要覆盖：

1. PSK/共享秘密不泄露；
2. Agent 完成握手并接受 UUID 时，Admin 至少在某条执行中生成对应 UUID；
3. 预认证中 Nonce 和 MAC 的基本关系。

可以通过以下命令复现：

```sh
docker compose -f formal/docker-compose.yml run --rm proverif
docker compose -f formal/docker-compose.yml run --rm tamarin
```

当前形式化模型仍是简化模型，尚未完整覆盖并发会话、重放、错误分支、会话密钥派生和协议全部消息结构。论文中应如实表述为“形式化验证骨架”，而不是完整证明。

### 8.6 安全边界

Shepherd 的安全边界包括：

1. 不保护 secret 泄露后的系统；
2. 不声称抵抗拥有合法 token 的恶意操作者；
3. 不对 Stockman 本地配置被窃取后的 TOFU 记录安全负责；
4. 不把自定义 raw 协议等同于 TLS；
5. 不讨论未授权使用场景。

这些限制不影响本文研究目标，因为本文关注的是受限网络控制面机制，而不是完整商业安全产品。

---

## 第 9 章 局限性与改进方向

### 9.1 当前局限

Shepherd 当前仍是研究原型，主要局限如下：

1. **实验规模有限**：当前论文图表数据主要覆盖 4、6、8 节点；工程回归补充覆盖 17 节点确定性 trace，但仍无法证明大规模性能。
2. **网络真实性不足**：本机 trace replay 无法完全模拟真实 delay、loss、jitter、带宽限制和无线碰撞。
3. **统计重复不足**：拓扑实验每组 3 次，DTN 实验每组 2 次，适合趋势说明但不足以支撑严格显著性检验。
4. **补链消融不足**：当前没有完整比较“启用补链/禁用补链”的恢复时延和交付率。
5. **Gossip 自适应消融不足**：尚未量化动态 fanout/TTL 相对固定参数的收益。
6. **形式化验证不完整**：Tamarin/ProVerif 模型仍未覆盖并发、重放和全部错误分支。
7. **Stockman 功能裁剪**：当前 Wails 客户端面向答辩演示，不覆盖旧版 Qt 客户端全部运维功能。

### 9.2 改进方向

后续可以从以下方向改进：

1. **增加实验重复与统计检验**：关键场景增加到 20-30 次以上，报告 95% 置信区间、非参数检验和效应量。
2. **引入 Mininet**：通过 tc/netem 设置 delay、loss、jitter 和 bandwidth，对比本机 trace 与受控链路。
3. **扩展 ns-3 模拟**：在更大规模和随机移动/接触模型下验证 Gossip 与 DTN 机制。
4. **补链消融**：实现实验开关，比较启用/禁用补链的恢复时延、成功率和额外边数。
5. **Gossip 参数消融**：比较固定 fanout/TTL 与自适应策略在收敛时间和消息量上的差异。
6. **更细粒度事件日志**：记录每个 bundle 的准确投递、ACK、重试和 HoldUntil 变化，降低 metrics 采样误差。
7. **完善形式化模型**：加入 transcript binding、session key、并发会话和重放攻击模型。
8. **UI 完整性提升**：在 Wails 客户端中逐步恢复审计、loot、文件和代理可视化。

---

## 第 10 章 总结

本文围绕受限网络远程运维控制面问题，设计并实现了 Shepherd 原型系统。系统由 Kelpie、Flock 和 Stockman 三部分组成，核心机制包括 Gossip 拓扑维护、补链自愈、DTN store-carry-forward、DTN 上的可靠 STREAM、sleep-aware 投递和 gRPC UI 可观测性。

在实现层面，Kelpie 负责拓扑、DTN、STREAM、补链、SQLite 和 gRPC UI；Flock 负责接入、Gossip、转发、sleep、repair 和 carry-forward；Stockman 提供连接、拓扑、节点、事件和演示控制台。协议层通过统一 messageType、route 和 payload codec 支撑多种控制与数据消息。

在实验层面，本文构建了 trace replay 框架，并完成两组可复现实验。拓扑收敛实验显示，star 和 chain 拓扑在 4、6、8 节点下均能收敛，且 chain 因多跳结构收敛更慢。DTN duty-cycling 实验显示，在目标节点 sleep8/work2 和 sleep16/work2 场景下，系统仍能实现 6/6 最终交付，交付时延随 sleep 周期增加而上升，与理论等待模型趋势一致。工程回归进一步验证了 `gossip_memo_scale_n16` 在 17 节点 star/chain 拓扑下连续 DTN memo 投递 2/2 成功。

在安全层面，本文实现了基于共享 secret、Nonce 和 HMAC 的预认证，UI token 鉴权，TLS TOFU 指纹确认和 dataplane 一次性 token，并提供 Tamarin/ProVerif 形式化验证骨架。

总体而言，Shepherd 并不是简单地实现一个远程控制工具，而是围绕“多跳、睡眠、断续连接、消息最终交付”这一受限网络核心矛盾进行系统设计。当前原型已经具备可运行、可观测、可测试和可复现实验的基础。虽然仍需更大规模实验、真实网络仿真、消融分析和形式化验证精化，但本文已经证明了将 Gossip、补链、DTN 和 STREAM 组合用于受限网络远程运维控制面的可行性。

---

## 参考文献

[1] Demers, A., Greene, D., Hauser, C., Irish, W., Larson, J., Shenker, S., Sturgis, H., Swinehart, D., Terry, D. Epidemic Algorithms for Replicated Database Maintenance. PODC 1987. https://doi.org/10.1145/41840.41841  

[2] Jelasity, M., Voulgaris, S., Guerraoui, R., Kermarrec, A.-M., van Steen, M. Gossip-based Peer Sampling. ACM Transactions on Computer Systems, 2007. https://doi.org/10.1145/1275517.1275520  

[3] Das, A., Gupta, I., Motivala, A. SWIM: Scalable Weakly-consistent Infection-style Process Group Membership Protocol. DSN 2002. https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf  

[4] Fall, K. A Delay-Tolerant Network Architecture for Challenged Internets. SIGCOMM 2003. https://doi.org/10.1145/863955.863960  

[5] Cerf, V., Burleigh, S., Hooke, A., Torgerson, L., Durst, R., Scott, K., Fall, K., Weiss, H. RFC 4838: Delay-Tolerant Networking Architecture. IRTF, 2007. https://www.rfc-editor.org/rfc/rfc4838  

[6] Burleigh, S., Fall, K., Birrane, E. RFC 9171: Bundle Protocol Version 7. IETF, 2022. https://www.rfc-editor.org/rfc/rfc9171  

[7] Vahdat, A., Becker, D. Epidemic Routing for Partially Connected Ad Hoc Networks. Duke Technical Report CS-2000-06, 2000. https://cseweb.ucsd.edu/~vahdat/papers/epidemic.pdf  

[8] Lindgren, A., Doria, A., Davies, E., Grasic, S. RFC 6693: Probabilistic Routing Protocol for Intermittently Connected Networks. IRTF, 2012. https://www.rfc-editor.org/rfc/rfc6693  

[9] Spyropoulos, T., Psounis, K., Raghavendra, C. S. Spray and Wait: An Efficient Routing Scheme for Intermittently Connected Mobile Networks. WDTN 2005. https://doi.org/10.1145/1080139.1080143  

[10] Burgess, J., Gallagher, B., Jensen, D., Levine, B. N. MaxProp: Routing for Vehicle-Based Disruption-Tolerant Networks. IEEE INFOCOM 2006. https://doi.org/10.1109/INFOCOM.2006.228  

[11] Buettner, M., Yee, G. V., Anderson, E., Han, R. X-MAC: A Short Preamble MAC Protocol for Duty-Cycled Wireless Sensor Networks. SenSys 2006. https://doi.org/10.1145/1182807.1182838  

[12] Biondi, E., Boldrini, C., Passarella, A., Conti, M. What You Lose When You Snooze: How Duty Cycling Impacts on the Contact Process in Opportunistic Networks. Computer Communications, 2017. https://doi.org/10.1016/j.comcom.2017.03.006  

[13] Bellare, M., Pointcheval, D., Rogaway, P. Authenticated Key Exchange Secure Against Dictionary Attacks. EUROCRYPT 2000. https://www.iacr.org/archive/eurocrypt2000/1807/18070001-new.pdf  

[14] The Tamarin Prover Team. Tamarin Prover Manual. https://tamarin-prover.com/manual/  

[15] Blanchet, B. ProVerif: Cryptographic Protocol Verifier in the Formal Model. https://bblanche.gitlabpages.inria.fr/proverif/  

[16] Burleigh, S., Fall, K., Birrane, E. RFC 9174: Delay-Tolerant Networking TCP Convergence-Layer Protocol Version 4. IETF, 2022. https://www.rfc-editor.org/rfc/rfc9174  

---

## 附录 A：复现实验命令

```sh
make admin agent
go build -o build/trace_replay ./experiments/trace_replay
bash script/experiments.sh
```

工程测试：

```sh
make test
make regress
make check
```

形式化验证：

```sh
docker compose -f formal/docker-compose.yml run --rm proverif
docker compose -f formal/docker-compose.yml run --rm tamarin
```

---

## 附录 B：论文与代码映射

| 论文内容 | 代码/材料 |
| --- | --- |
| Kelpie 启动 | `cmd/kelpie/main.go` |
| Flock 启动 | `cmd/flock/main.go` |
| 拓扑图 | `internal/kelpie/topology/` |
| Gossip | `internal/flock/process/gossip.go`、`internal/flock/gossip/` |
| 补链自愈 | `internal/kelpie/planner/`、`internal/kelpie/supp/` |
| DTN 队列 | `internal/kelpie/dtn/`、`internal/kelpie/process/dtn.go` |
| STREAM | `internal/kelpie/stream/`、`internal/kelpie/process/stream.go` |
| Sleep 控制 | `internal/flock/process/sleep.go`、`internal/kelpie/process/control.go` |
| gRPC UI | `proto/kelpieui/v1/kelpieui.proto`、`internal/kelpie/ui/grpcserver/` |
| Dataplane | `proto/dataplane/v1/dataplane.proto`、`internal/kelpie/dataplane/` |
| Stockman | `clientui/` |
| Trace replay | `experiments/trace_replay/` |
| 实验图表 | `docs/data/`、`docs/figures/` |
| 形式化验证 | `formal/` |
