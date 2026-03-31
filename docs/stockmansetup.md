# Stockman macOS 构建环境

本文档记录当前在 macOS + Homebrew 上验证通过的一组 Stockman 构建依赖，目标是让新机器能够快速恢复到可构建状态，并避免 Homebrew 自动升级后产生版本漂移。

## 当前验证通过的版本

以下版本已在本仓库于 2026-03-31 验证通过配置阶段，并用于后续 C++ protobuf 绑定重生成：

- `cmake 4.3.1`
- `ninja 1.13.2`
- `qt 6.11.0`
- `protobuf 34.1`
- `grpc 1.78.1_3`
- `pkgconf 2.5.1`
- `fmt 12.1.0`
- `spdlog 1.17.0`
- `nlohmann-json 3.12.0`
- `tomlplusplus 3.4.0`

如果需要重生成 Go 绑定，当前使用的是：

- `protoc-gen-go v1.36.5`
- `protoc-gen-go-grpc v1.5.1`

## 安装与固定

先安装依赖：

```sh
HOMEBREW_NO_AUTO_UPDATE=1 brew install \
  cmake ninja qt protobuf grpc pkgconf fmt \
  spdlog nlohmann-json tomlplusplus
```

然后固定版本：

```sh
brew pin cmake ninja qt protobuf grpc pkgconf fmt spdlog nlohmann-json tomlplusplus
```

如果需要重生成 Go 绑定，再安装 Go 插件：

```sh
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.5
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
```

## 常用命令

构建 Stockman：

```sh
make stockman
```

重生成 Go 绑定：

```sh
bash script/goprotos.sh
```

重生成 Stockman 的 C++ 绑定：

```sh
bash script/clientuiprotos.sh
```

## 说明

- `clientui/generated/` 下的 C++ protobuf 绑定需要与本机 `protobuf`/`grpc` 版本匹配；升级这两个依赖后，应重新运行 `script/clientuiprotos.sh`。
- `internal/kelpie/uipb/` 和 `internal/dataplanepb/` 的 Go 绑定不应手工修改；如需更新，请通过 `script/goprotos.sh` 重生成。
- `clientui/CMakeLists.txt` 已改为使用 Homebrew 安装的 CMake config 包，不再依赖仓库外部的 `external/` 头文件目录。
