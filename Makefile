GO ?= go
PYTHON ?= python3
BUILD_ENV ?= CGO_ENABLED=0
LDFLAGS ?= -s -w
OUT ?= build

ADMIN_PKG := ./cmd/kelpie
AGENT_PKG := ./cmd/flock
CMAKE ?= cmake
CMAKE_BUILD_TYPE ?= Release
CLIENTUI_BUILD ?= $(OUT)/clientui
JOBS ?= $(shell nproc 2>/dev/null || echo 4)

.PHONY: all admin agent stockman trace_replay regress soak check test clean

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
	$(PYTHON) experiments/trace_replay/run_regress.py --topologies star,chain --skip-build

soak: trace_replay
	$(PYTHON) experiments/trace_replay/run_soak.py --topologies $(SOAK_TOPOS) --repeat $(SOAK_REPEAT) --profile $(SOAK_PROFILE) --skip-build --fail-fast

check:
	$(GO) test -race ./...
	$(MAKE) regress

stockman:
	@mkdir -p $(CLIENTUI_BUILD)
	$(CMAKE) -S clientui -B $(CLIENTUI_BUILD) -DCMAKE_BUILD_TYPE=$(CMAKE_BUILD_TYPE)
	+$(CMAKE) --build $(CLIENTUI_BUILD) --config $(CMAKE_BUILD_TYPE) -- -j$(JOBS)
	@cp -f clientui/config.toml $(OUT)/config.toml

test:
	$(GO) test ./...

clean:
	rm -rf $(OUT)
