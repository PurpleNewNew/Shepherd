# Stockman Next（答辩演示客户端）

面向**毕业答辩演示**的 Shepherd 桌面客户端，技术栈：**Wails v3.0.0-alpha.85 + Vue 3 + Vite + TypeScript**。

当前版本已从单窗口 Wails v2 迁移到 Wails v3 多窗口架构：启动时先打开紧凑连接窗口，连接成功后切换到主控制台窗口。功能范围：

- 连接管理（TLS TOFU + 最近连接历史）
- 拓扑总览（力导向图 / 树状图 双视图切换）
- 节点详情与会话管理（mark / repair / reconnect / terminate / diagnostics）
- 事件时间线（Kelpie `WatchEvents` 流实时订阅）
- 交互式 shell、文件列表/下载/上传、loot 导出
- SOCKS、正向/反向端口转发、SSH Session / SSH Tunnel
- Listener 创建、修改、删除与启停入口

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

- Go >= 1.25（Wails `v3.0.0-alpha.85` 的最低要求；本仓库在 Go 1.26 上验证）
- Node.js >= 20（推荐 24 LTS）
- Wails v3 CLI（可选，用于 `wails3 dev` 热重载与 bindings 生成）：

```sh
go install github.com/wailsapp/wails/v3/cmd/wails3@v3.0.0-alpha.85
```

### 运行（dev 模式）

```sh
# 终端 1：拉起一个 Kelpie 提供 gRPC UI（默认 :50061）
./build/kelpie -listen :4444 --ui-grpc-listen 127.0.0.1:50061 --ui-grpc-token demo-token ...

# 终端 2：Wails dev（自动热重载前端 + Go）
cd clientui && wails3 dev
```

如果未安装 Wails CLI，可分开跑：

```sh
cd clientui/frontend && npm install && npm run dev    # 终端 1
cd clientui && go run -tags dev,desktop .             # 终端 2
```

浏览器预览只适合调 CSS/布局；后端 API、事件流和窗口切换必须通过 Wails 桌面运行时测试。

### 构建

```sh
make stockman
# 如修改了 Go facade 方法，先重生成 TS bindings：
make stockman-bindings
```

手工构建时必须自己带上 build tags 和（macOS）UTType framework 链接：

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

1. 启动 Kelpie（`build/kelpie -listen :4444 --ui-grpc-listen 127.0.0.1:50061 --ui-grpc-token demo-token`）
2. 启动 trace_replay mini-cluster（或手动拉 Flock）
3. 打开 Stockman Next → 连接窗口输入 `127.0.0.1:50061` + token，回车
4. 切到拓扑视图，讲 Gossip 收敛过程
5. 切到事件时间线，讲 `WatchEvents` 是 Kelpie 推给 UI 的统一事件流
6. 控制台：向某节点发 DTN 消息 → 返回时间线看 DTN 事件
7. 控制台：改某节点 sleep=15s → 再发 DTN → 讲 duty-cycling 时延
8. 最后：PruneOffline 清理离线节点，画面复位
