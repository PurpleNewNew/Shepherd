# 形式化验证（可复现骨架）

本目录提供 Shepherd 握手流程的形式化模型骨架，用于在中期阶段支撑“安全性论证 + 可复现证明”的研究材料。

当前模型主要对齐以下实现（以“预共享口令/令牌 + Nonce + MAC + HI/UUID 交换”为主线）：

- 预认证挑战应答（HMAC + Nonce）：`pkg/share/preauth.go`
- 握手主流程（HI、UUID 分配等）：`pkg/share/handshake/handshake.go`
- 管理端/代理端 initial：`internal/kelpie/initial/`、`internal/flock/initial/`

注意：这是一套“骨架模型”。它刻意抽象了传输层细节、错误处理与工程实现中的部分状态，以便先把性质与证明路径跑通；后续可逐步细化为更贴近 `protocol/` 的消息结构与密钥派生方式。

## 目录结构

- `formal/proverif/`: ProVerif 模型
- `formal/tamarin/`: Tamarin 模型
- `formal/docker-compose.yml`: 一键复现实验环境（Docker）

## 快速复现（Docker，推荐）

在仓库根目录执行：

```sh
docker compose -f formal/docker-compose.yml run --rm proverif
docker compose -f formal/docker-compose.yml run --rm tamarin
```

说明：

- `proverif` 会验证 `formal/proverif/handshake.pv` 中的查询（如 PSK 保密性、基本对应性性质）。
- `tamarin` 会证明 `formal/tamarin/handshake.spthy` 中的 `psk_secrecy` 引理。
- 在 Apple Silicon（arm64）上，这两个镜像可能以 `linux/amd64` 运行，Docker Desktop 会自动进行仿真，速度会慢一些，但对本仓库的模型规模通常可接受。

## 本地运行（可选）

若你希望不依赖 Docker，可本机安装工具后运行：

```sh
proverif formal/proverif/handshake.pv
tamarin-prover --prove formal/tamarin/handshake.spthy
```

## 目前已覆盖的性质（示例）

- **PSK/共享秘密不泄露**：攻击者无法直接获得预共享秘密（在模型假设下）。
- **基本对应性（Correspondence）**：Agent 完成握手并接受 UUID 时，Admin 至少在某一条执行中已“提交/生成”对应 UUID（模型中的 `AdminComplete` 事件）。

更强的认证性质（如注入一致性、会话绑定、抗重放细化）属于后续工作：需要引入更精确的消息绑定（例如将 UUID 与 Nonce/Transcript 关联）、以及对并发会话的建模约束与引理设计。

