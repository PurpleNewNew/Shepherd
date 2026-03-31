# Repository Guidelines

## 项目结构与模块组织
`cmd/kelpie` 和 `cmd/flock` 是两个 Go 入口程序。服务端与代理端核心逻辑分别位于 `internal/kelpie/` 与 `internal/flock/`；可复用公共库位于 `pkg/`；协议与消息格式定义位于 `protocol/` 和 `proto/`。Qt6 桌面客户端位于 `clientui/`，其生成的 gRPC 代码位于 `clientui/generated/`。实验脚本与回放 trace 位于 `experiments/`，可复现实验报告与图表位于 `docs/`，形式化模型位于 `formal/`。`internal/dataplanepb/` 和 `clientui/generated/` 下的 protobuf 生成文件不要手动修改。

## 构建、测试与开发命令
统一从仓库根目录使用 `make`。

- `make admin agent`：构建 Go 二进制到 `build/`。
- `make all`：构建 `kelpie`、`flock` 和 Qt 客户端。
- `make test`：运行 Go 单元测试与包级测试。
- `make check`：执行 `go test -race ./...` 以及 trace replay 回归测试。
- `make regress`：回放标准 `star` 与 `chain` 拓扑 trace。
- `make soak`：运行高风险回放集；可用 `SOAK_REPEAT=50` 或 `SOAK_TOPOS=chain` 调整参数。
- `bash script/run_experiments_local.sh`：重新生成 `docs/` 下的实验 CSV 和 SVG 图表。

构建、测试、回放和生成类命令不要并行执行。`make all`、`make test`、`make check`、`make regress`、`make soak` 以及 protobuf 生成流程会共享 `build/`、CMake 构建目录或中间产物，并行运行时容易因锁竞争或输出互相覆盖导致失败；需要跑多项检查时请按顺序串行执行。

## 编码风格与命名约定
Go 代码遵循标准 `gofmt` 格式，包名保持小写并使用惯用命名，例如 `pkg/logging`、`internal/kelpie/ui`。测试文件统一使用 `*_test.go` 命名，并尽量与被测代码放在同一包内。Qt/C++ 代码沿用现有风格：4 空格缩进、类型名使用 `PascalCase`，并按现有目录组织，例如 `clientui/src/UserInterface/`。修改 `.proto` 后，使用 `script/gen_kelpieui_protos.sh` 重新生成绑定代码。

## 测试指南
主要测试覆盖位于 `pkg/`、`protocol/`、`internal/` 和 `integration/`。小范围改动至少运行 `make test`；涉及行为变化、传输层、协议或回放逻辑的改动，在合并前运行 `make check`。新增测试应尽量靠近变更代码；端到端或场景类验证放在 `integration/` 或 trace replay 测试中。

## 提交与 Pull Request 规范
提交信息统一使用中文前缀，格式为 `[类型] 简要说明`，其中类型限定为 `[功能]`、`[修复]`、`[优化]`、`[重构]`、`[其他]`、`[文档]`。例如：`[功能] 增加 DTN 回放统计导出`、`[修复] 修正节点重连后的会话恢复`、`[优化] 降低 gossip 同步日志噪音`。Pull Request 应说明行为变化、列出已执行命令（如 `make test`、`make check`、`make regress`），关联相关 issue 或实验记录；若修改 `clientui/` 界面或交互，应附带截图。

## 生成代码与配置说明
不要手动编辑 protobuf 生成产物。应先修改 `proto/dataplane/` 或 `proto/kelpieui/`，再运行生成脚本，并将生成结果一并提交。`clientui/config.toml` 以及 `docs/data/` 下的实验产物默认视为环境相关输出，除非本次变更明确要更新默认配置或正式结果。
