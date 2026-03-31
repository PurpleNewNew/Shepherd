# Docker 运行环境（Kelpie + 多 Flock + Stockman）

目标：在本机用 Docker 复现一个“真实运行态”的小型网络环境：

- `kelpie` 作为控制端（teamserver），启动 gRPC UI + dataplane
- 1 个 `flock-root` 作为根节点直连 kelpie
- 若干个 `flock` 节点作为下游节点，挂到 `flock-root` 的 pivot listener 上
- `stockman` 在宿主机运行，通过 gRPC 观察拓扑/状态并下发操作

## 1. 启动（推荐）

在仓库根目录执行：

```bash
docker compose -f docker/docker-compose.yml up -d --build --scale flock=5
```

说明：

- `bootstrap` 会自动执行两步并退出：
  - 创建 Kelpie 的 controller listener：`0.0.0.0:40100`（仅容器网络内使用，不对宿主机暴露）
  - 等 root 节点上线后，在 root 节点创建 pivot listener：`0.0.0.0:41000`
- `flock` 节点以 `--reconnect` 模式启动，会在 pivot listener 就绪后自动连接并加入拓扑

查看日志（可选）：

```bash
docker compose -f docker/docker-compose.yml logs -f --tail=200 kelpie
```

## 2. 在宿主机运行 Stockman 并连接 Kelpie

Stockman 建议直接在宿主机运行（Qt GUI 不建议跑在 Docker）。

假设你已在宿主机完成 `make stockman`，启动：

```bash
./build/Stockman
```

在 Stockman 里配置连接参数：

- Host: `127.0.0.1`
- Port: `50061`
- Token: `SHEPHERD_UI_TOKEN`（默认 `stockman-dev-token`，可在 compose 里覆盖）

Dataplane（大文件传输/数据通道）端口默认映射为：

- `127.0.0.1:60080`

## 3. 常用调整

自定义 secret 与 UI token：

```bash
export SHEPHERD_SECRET='Shepherd1234'        # 至少 8 位，且必须同时包含字母与数字
export SHEPHERD_UI_TOKEN='stockman-dev-token'
docker compose -f docker/docker-compose.yml up -d --build --scale flock=8
```

调整 flock 的重连周期（秒）：

```bash
export FLOCK_RECONNECT_SECONDS=1
docker compose -f docker/docker-compose.yml up -d --build --scale flock=10
```

## 4. 关闭与清理

停止并删除容器：

```bash
docker compose -f docker/docker-compose.yml down
```

同时删除持久化数据（Kelpie SQLite）：

```bash
docker compose -f docker/docker-compose.yml down -v
```

## 5. 安全提醒（工程实践）

该环境用于本机/隔离网络下的功能验证与回归测试。默认只向宿主机暴露：

- gRPC UI: `50061`
- dataplane: `60080`

不要在不受信任网络中直接暴露这些端口；如需跨机访问，务必配置更严格的网络隔离与访问控制。

