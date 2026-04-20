# Stockman 构建与开发环境

本文档描述在 `academic` 分支上基于 **Wails v2 + Vue 3 + Vite** 的 Stockman 桌面客户端的构建与开发环境。旧版 Qt6 C++ Stockman 已在 `academic` 分支整体重写替换；历史清单仅在 `docs/stockmanchecklist.md` 里保留作为回归参考，并不适用于当前代码。

## 运行环境

macOS（Apple Silicon 已验证）、Linux、Windows 原生均可构建；开发时只需以下三件：

- **Go**：>= 1.22（已随 `go.mod` 中的 `go 1.22` 约束；仓库整体已在 Go 1.26 上验证）
- **Node.js**：>= 20（推荐使用 nvm 安装 24 LTS，仓库用 Node 24 / npm 11 验证）
- **CGO 工具链**：
  - macOS：Xcode Command Line Tools（`xcode-select --install`）
  - Linux：`libgtk-3-dev`、`libwebkit2gtk-4.0-dev`、`build-essential`、`pkg-config`
  - Windows：WebView2 Runtime（Win11 自带）

可选：Wails CLI 仅用于 `wails dev` 热重载，`make stockman` 的非热重载构建不需要它。

```sh
go install github.com/wailsapp/wails/v2/cmd/wails@latest
# 验证
wails doctor
```

## 构建

最直接的方式，使用 Makefile：

```sh
make stockman
```

该 target 做三件事：

1. 如果 `clientui/frontend/node_modules` 不存在，跑 `npm install`
2. 在 `clientui/frontend` 里 `npm run build`，产出 `frontend/dist`
3. `CGO_ENABLED=1 go build ./clientui` 输出到 `build/stockman`

手动拆开跑等价于：

```sh
cd clientui/frontend
npm install
npm run build
cd ..
CGO_ENABLED=1 go build -o ../build/stockman .
```

## 开发（热重载）

前端和后端都需要迭代时，使用 Wails dev：

```sh
cd clientui
wails dev
```

它会：

- 在 `clientui/frontend` 里跑 `npm run dev`（Vite dev server）
- 同时启动 Go 后端并在窗口中加载 Vite dev server（支持前端 HMR）
- 检测到 Go 源文件变更时会自动重启后端

如果只改前端：

```sh
cd clientui/frontend
npm run dev   # 浏览器访问 http://localhost:5173 会看到占位页面
```

但直接浏览器访问时，`window.runtime` 和 `window.go.service.API.*` 都不存在，所有后端调用都会失败——这是预期的，浏览器只适合调 CSS/布局。实际功能必须通过 Wails 窗口测试。

## 前端技术栈

- **Vue 3**（Composition API + `<script setup lang="ts">`）
- **Pinia** 状态管理
- **Vite** 构建
- **d3-force / d3-hierarchy / d3-shape / d3-zoom / d3-selection** 拓扑可视化
- **TypeScript**（`vue-tsc --noEmit` 严格模式）

类型定义在 `clientui/frontend/src/api/types.ts`，和后端 `clientui/backend/service/types.go` 的 JSON tag 一一对应。

## 后端技术栈

- **Wails v2.12+**：窗口 + 前端静态资源 embed + Go ↔ JS 双向桥接
- **google.golang.org/grpc**：与 Kelpie 的 gRPC UI 服务通信
- **`codeberg.org/agnoie/shepherd/internal/kelpie/uipb`**：直接复用主仓库的 protobuf 生成代码（clientui/ 不独立 go module，正因为要 import internal 包）

配置文件持久化位置：

- macOS：`~/Library/Application Support/Shepherd/Stockman/config.json`
- Linux：`~/.config/shepherd/stockman/config.json`
- Windows：`%APPDATA%\Shepherd\Stockman\config.json`

存储内容仅包含最近连接列表（不含 token，token 每次手输）和 TOFU 通过的 TLS 证书指纹。

## 重生成 Go protobuf 绑定

Stockman 直接用的是 `internal/kelpie/uipb`，不需要单独再生成一份。如果修改了 `proto/kelpieui/v1/kelpieui.proto`：

```sh
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.5
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
bash script/goprotos.sh
```

注意：新版 Stockman 不再使用 C++ protobuf 绑定，因此 `script/clientuiprotos.sh` 仅在需要维护旧 Qt Stockman（非本分支）时才有意义。

## 常见问题

- **`Wails applications will not build without the correct build tags.`**：Wails v2 在 `internal/app/app_default_*.go` 里要求 `-tags production`（或 `dev`/`bindings`）之一。`make stockman` 已经自动传 `-tags production,desktop`；如果手工 `go build`，必须自己带上，否则启动时 `CreateApp` 会直接返回该错误。
- **`Undefined symbols: _OBJC_CLASS_$_UTType` / `ld: framework not found UniformTypeIdentifiers`**：macOS 下需要额外链接 UniformTypeIdentifiers framework。`make stockman` 已经设置 `CGO_LDFLAGS="-framework UniformTypeIdentifiers -mmacosx-version-min=10.13"`；手工 build 也要带上。
- **`pattern all:frontend/dist: no matching files found`**：Go embed 要求 `frontend/dist/` 存在。先跑 `npm run build`（或 `make stockman-frontend`），或保留占位 `index.html`。
- **Wails 启动后白屏**：多数情况是前端资产路径问题；检查 `wails.json` 里 `frontend:build` 输出和 `main.go` 中 `//go:embed all:frontend/dist` 是否一致。
- **macOS 构建后不能启动**：若系统提示 "unidentified developer"，可临时用 `xattr -d com.apple.quarantine build/stockman`。正式分发需要 codesign + notarize。
- **链接时一堆 `object file ... was built for newer 'macOS' version (26.0)` warning**：仅是 SDK 版本标签差异（Go 1.26 toolchain 默认带 macOS 26 SDK，而 `-mmacosx-version-min=10.13` 在要求下限）。属于正常现象，不影响运行，也不需要处理。
