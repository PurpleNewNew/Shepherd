# Stockman Next（答辩演示客户端）

面向**毕业答辩演示**的 Shepherd 桌面客户端，技术栈：**Wails v2 + Vue 3 + Vite + TypeScript**。

第一版聚焦演示效果，不复刻旧版 Qt Stockman 的全部功能。功能范围：

- 连接管理（TLS TOFU + 最近连接历史）
- 拓扑总览（力导向图 / 树状图 双视图切换）
- 节点详情面板
- 事件时间线（Kelpie `WatchEvents` 流实时订阅）
- 演示控制台（DTN 入队 / Sleep 参数 / 修剪离线节点）

**不实现**：shell、文件传输、SOCKS 代理、chat、audit、loot 等。如需这些功能，请使用旧版 Qt Stockman（已归档在 git 历史 `academic` 分支之前）。

## 目录结构

```
clientui/
├── main.go              # Wails 入口
├── app.go               # 应用 lifecycle + 绑定方法
├── wails.json           # Wails 项目配置
├── backend/             # Go 后端模块
│   ├── kelpie/          # gRPC 客户端封装（连 Kelpie UI/Supplemental 等服务）
│   ├── config/          # 连接历史 + TOFU 指纹持久化
│   └── service/         # 对前端暴露的 facade
├── frontend/            # Vue 3 + Vite 前端
│   ├── package.json
│   ├── vite.config.ts
│   ├── index.html
│   └── src/
└── build/               # Wails 构建产物（gitignore）
```

本目录是主 Go module `codeberg.org/agnoie/shepherd` 的子目录，可直接 import `internal/kelpie/uipb` 等包，避免重复生成 protobuf。

## 开发

### 依赖

- Go >= 1.22
- Node.js >= 20（推荐 24 LTS）
- Wails CLI（可选，用于 `wails dev` 热重载）：

```sh
go install github.com/wailsapp/wails/v2/cmd/wails@latest
```

### 运行（dev 模式）

```sh
# 终端 1：拉起一个 Kelpie 提供 gRPC UI（默认 :9090）
./build/kelpie -listen :4444 -ui-listen :9090 -ui-auth-token demo-token ...

# 终端 2：Wails dev（自动热重载前端 + Go）
cd clientui && wails dev
```

如果未安装 Wails CLI，可分开跑：

```sh
cd clientui/frontend && npm install && npm run dev    # 终端 1
cd clientui && go run -tags dev .                     # 终端 2：注意必须带 -tags dev
```

Wails 不允许裸 `go run` / `go build`；它用 build tag（`dev` / `production` / `bindings`）决定运行时行为，缺失会直接 `CreateApp` 失败。

### 构建

```sh
make stockman
# 或（官方 CLI，自动处理 build tags 和 CGO_LDFLAGS）：
cd clientui && wails build
```

手工构建时必须自己带上 build tags 和（macOS）UTType framework 链接，否则启动时 Wails 会报 "will not build without the correct build tags" 或 linker 找不到 `_OBJC_CLASS_$_UTType`：

```sh
cd clientui/frontend && npm run build && cd ..
# macOS
CGO_ENABLED=1 \
  CGO_LDFLAGS="-framework UniformTypeIdentifiers -mmacosx-version-min=10.13" \
  go build -tags production,desktop -o ../build/stockman .
# Linux（需要 libgtk-3-dev + libwebkit2gtk-4.0-dev）
CGO_ENABLED=1 go build -tags production,desktop -o ../build/stockman .
```

## 配置

客户端会把连接历史与 TOFU 指纹保存在：

- macOS：`~/Library/Application Support/Shepherd/Stockman/config.json`
- Linux：`~/.config/shepherd/stockman/config.json`
- Windows：`%APPDATA%\Shepherd\Stockman\config.json`

## 答辩演示流程（建议）

1. 启动 Kelpie（`build/kelpie -listen :4444 -ui-listen :9090 -ui-auth-token demo-token`）
2. 启动 trace_replay mini-cluster（或手动拉 Flock）
3. 打开 Stockman Next → 连接页输入 `127.0.0.1:9090` + token，回车
4. 切到拓扑视图，讲 Gossip 收敛过程
5. 切到事件时间线，讲 `WatchEvents` 是 Kelpie 推给 UI 的统一事件流
6. 控制台：向某节点发 DTN 消息 → 返回时间线看 DTN 事件
7. 控制台：改某节点 sleep=15s → 再发 DTN → 讲 duty-cycling 时延
8. 最后：PruneOffline 清理离线节点，画面复位
