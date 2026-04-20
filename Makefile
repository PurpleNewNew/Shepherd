GO ?= go
PYTHON ?= python3
BUILD_ENV ?= CGO_ENABLED=0
LDFLAGS ?= -s -w
OUT ?= build

ADMIN_PKG := ./cmd/kelpie
AGENT_PKG := ./cmd/flock

# Stockman（Wails v2 + Vue3 答辩演示客户端）
CLIENTUI_DIR ?= clientui
FRONTEND_DIR ?= $(CLIENTUI_DIR)/frontend
NPM ?= npm

.PHONY: all admin agent stockman stockman-frontend stockman-deps trace_replay regress soak check test clean clean-frontend

SOAK_REPEAT ?= 10
SOAK_TOPOS ?= star,chain
SOAK_PROFILE ?= core

all: admin agent stockman

admin:
	@mkdir -p $(OUT)
	$(BUILD_ENV) $(GO) build -trimpath -ldflags "$(LDFLAGS)" -o $(OUT)/kelpie $(ADMIN_PKG)

agent:
	@mkdir -p $(OUT)
	$(BUILD_ENV) $(GO) build -trimpath -ldflags "$(LDFLAGS)" -o $(OUT)/flock $(AGENT_PKG)

trace_replay: admin agent
	@mkdir -p $(OUT)
	$(GO) build -trimpath -ldflags "$(LDFLAGS)" -o $(OUT)/trace_replay ./experiments/trace_replay

regress: trace_replay
	$(PYTHON) experiments/trace_replay/regress.py --topologies star,chain --skip-build

soak: trace_replay
	$(PYTHON) experiments/trace_replay/soak.py --topologies $(SOAK_TOPOS) --repeat $(SOAK_REPEAT) --profile $(SOAK_PROFILE) --skip-build --fail-fast

check:
	$(GO) test -race ./...
	$(MAKE) regress

# ------- Stockman（Wails v2 + Vue3）------
# 1) 安装前端依赖（如果 node_modules 不存在则 install）
stockman-deps:
	@if [ ! -d $(FRONTEND_DIR)/node_modules ]; then \
		echo "[stockman] installing frontend deps ..."; \
		cd $(FRONTEND_DIR) && $(NPM) install; \
	fi

# 2) 构建前端到 frontend/dist（Go embed 源）
stockman-frontend: stockman-deps
	cd $(FRONTEND_DIR) && $(NPM) run build

# 3) 打包 Go 二进制（Wails 需要 CGO + production build tags）
#    注：Wails v2 在 app_default_unix.go 里要求 build tag 必须有 `dev`/`production`/`bindings` 之一，
#    否则 CreateApp 会直接返回 "Wails applications will not build without the correct build tags."
#    生产构建统一使用 `-tags production,desktop`，与官方 `wails build` 行为一致。
STOCKMAN_TAGS ?= production,desktop

# Wails 需要的 CGO_LDFLAGS 与官方 `wails build` 对齐：
#   - macOS：`-framework UniformTypeIdentifiers -mmacosx-version-min=10.13`
#   - 其他平台无需额外 flag
STOCKMAN_UNAME := $(shell uname -s 2>/dev/null)
ifeq ($(STOCKMAN_UNAME),Darwin)
STOCKMAN_CGO_LDFLAGS ?= -framework UniformTypeIdentifiers -mmacosx-version-min=10.13
else
STOCKMAN_CGO_LDFLAGS ?=
endif

stockman: stockman-frontend
	@mkdir -p $(OUT)
	CGO_ENABLED=1 CGO_LDFLAGS='$(STOCKMAN_CGO_LDFLAGS)' $(GO) build -trimpath -tags '$(STOCKMAN_TAGS)' -ldflags "$(LDFLAGS)" -o $(OUT)/stockman ./$(CLIENTUI_DIR)

test:
	$(GO) test ./...

clean:
	rm -rf $(OUT)

clean-frontend:
	rm -rf $(FRONTEND_DIR)/dist $(FRONTEND_DIR)/node_modules
