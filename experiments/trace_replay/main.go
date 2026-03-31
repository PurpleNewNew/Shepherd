package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"codeberg.org/agnoie/shepherd/internal/dataplanepb"
	uipb "codeberg.org/agnoie/shepherd/internal/kelpie/uipb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"
)

type traceEvent struct {
	AtMS int64  `json:"at_ms"`
	Type string `json:"type"`

	// sleep 事件
	Node          string   `json:"node,omitempty"`
	SleepSeconds  *int32   `json:"sleep_seconds,omitempty"`
	WorkSeconds   *int32   `json:"work_seconds,omitempty"`
	JitterPercent *float64 `json:"jitter_percent,omitempty"`

	// dtn_enqueue 事件
	Target     string `json:"target,omitempty"`
	Payload    string `json:"payload,omitempty"`
	Priority   string `json:"priority,omitempty"`    // low|normal|high
	TTLSeconds int64  `json:"ttl_seconds,omitempty"` // <=0 uses server defaults

	// dataplane_roundtrip（stream_proxy 也会复用）
	Path      string `json:"path,omitempty"`
	Expect    string `json:"expect,omitempty"`
	TimeoutMS int64  `json:"timeout_ms,omitempty"`

	// io_burst / dataplane_upload_interrupt 事件
	StreamCount    int    `json:"stream_count,omitempty"`
	DataplaneCount int    `json:"dataplane_count,omitempty"`
	PayloadBytes   int    `json:"payload_bytes,omitempty"`
	KillNode       string `json:"kill_node,omitempty"`
	KillAfterMS    int64  `json:"kill_after_ms,omitempty"`
	ChunkDelayMS   int64  `json:"chunk_delay_ms,omitempty"`
	DelayMS        int64  `json:"delay_ms,omitempty"`

	// metrics 事件
	Note string `json:"note,omitempty"`
}

type labelsFile struct {
	StartedAt string            `json:"started_at"`
	Nodes     map[string]string `json:"nodes"` // label -> uuid
}

type jsonlWriter struct {
	mu  sync.Mutex
	enc *json.Encoder
}

func newJSONLWriter(w io.Writer) *jsonlWriter {
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	return &jsonlWriter{enc: enc}
}

func (w *jsonlWriter) Write(v any) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.enc.Encode(v)
}

type diagNodeSnapshot struct {
	ParentUUID string
	Status     string
	Network    string
	Memo       string
	Depth      int32
}

type diagEdgeSnapshot struct {
	ParentUUID   string
	ChildUUID    string
	Supplemental bool
}

type suppMetricsSnapshot struct {
	Dispatched     uint64
	Success        uint64
	Failures       uint64
	Dropped        uint64
	Recycled       uint64
	QueueHigh      uint64
	EventSeq       uint64
	RepairAttempts uint64
	RepairSuccess  uint64
	RepairFailures uint64
	LastFailure    string
}

type traceDiagState struct {
	nodes           map[string]diagNodeSnapshot
	edges           map[string]diagEdgeSnapshot
	memoSource      map[string]string
	memoSeen        map[string]map[string]struct{}
	suppMetrics     suppMetricsSnapshot
	suppMetricsInit bool
	suppEventSeq    uint64
}

func newTraceDiagState() *traceDiagState {
	return &traceDiagState{
		nodes:      make(map[string]diagNodeSnapshot),
		edges:      make(map[string]diagEdgeSnapshot),
		memoSource: make(map[string]string),
		memoSeen:   make(map[string]map[string]struct{}),
	}
}

var (
	snapshotAllMu sync.Mutex
	traceDiag     = newTraceDiagState()
)

func main() {
	var (
		nodes          = flag.Int("nodes", 5, "Total agents (1 root + children)")
		duration       = flag.Duration("duration", 60*time.Second, "Total run duration")
		tailMS         = flag.Int("tail-ms", 30000, "Extra time after trace completion before exit (when --finish-when-done)")
		finishWhenDone = flag.Bool("finish-when-done", false, "Exit once all trace events are processed (plus --tail-ms), instead of waiting full --duration")
		metricsEvery   = flag.Duration("metrics-every", 1*time.Second, "Metrics snapshot interval")
		watchEvents    = flag.Bool("watch-events", false, "Record gRPC WatchEvents stream to events.jsonl")
		tracePath      = flag.String("trace", "", "Optional JSONL trace file (see experiments/trace_replay/README.md)")
		topologyShape  = flag.String("topology", "star", "Topology shape: star|chain")

		secret      = flag.String("secret", "shepherd2026", "Shared secret (must satisfy complexity rules)")
		uiToken     = flag.String("ui-token", "reprotoken", "Kelpie gRPC UI token (empty disables auth)")
		kelpieBin   = flag.String("kelpie-bin", "build/kelpie", "Path to kelpie binary")
		flockBin    = flag.String("flock-bin", "build/flock", "Path to flock binary")
		outBaseDir  = flag.String("out", "experiments/out", "Output base directory")
		uiListen    = flag.String("ui-listen", "", "Kelpie gRPC listen addr (default: auto-pick)")
		controller  = flag.String("controller-bind", "", "Kelpie controller listener bind (default: auto-pick)")
		pivotBind   = flag.String("pivot-bind", "", "Root pivot listener bind (default: auto-pick)")
		reconnectS  = flag.Int("agent-reconnect", 1, "Flock reconnect interval seconds (0 disables reconnect mode)")
		disableRate = flag.Bool("disable-ui-rate-limit", true, "Pass --ui-grpc-rate 0 to Kelpie")

		agentRepair     = flag.Bool("agent-repair", true, "Enable per-agent repair listener (improves self-heal in traces)")
		agentRepairBind = flag.String("agent-repair-bind", "127.0.0.1", "Flock repair listener bind IP (when --agent-repair)")
	)
	flag.Parse()

	if *nodes < 1 {
		fatalf("invalid --nodes: must be >= 1")
	}
	if *duration <= 0 {
		fatalf("invalid --duration: must be > 0")
	}
	if *tailMS < 0 {
		fatalf("invalid --tail-ms: must be >= 0")
	}
	if *metricsEvery <= 0 {
		fatalf("invalid --metrics-every: must be > 0")
	}
	shape := strings.ToLower(strings.TrimSpace(*topologyShape))
	if shape == "" {
		shape = "star"
	}
	if shape != "star" && shape != "chain" {
		fatalf("invalid --topology: %q (expected star|chain)", *topologyShape)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	runDir, err := makeRunDir(*outBaseDir)
	if err != nil {
		fatalf("prepare run dir: %v", err)
	}
	logDir := filepath.Join(runDir, "logs")
	mustMkdirAll(logDir)

	kelpiePath, err := filepath.Abs(*kelpieBin)
	if err != nil {
		fatalf("resolve kelpie-bin: %v", err)
	}
	flockPath, err := filepath.Abs(*flockBin)
	if err != nil {
		fatalf("resolve flock-bin: %v", err)
	}
	ensureExecutable(kelpiePath)
	ensureExecutable(flockPath)

	uiAddr := strings.TrimSpace(*uiListen)
	if uiAddr == "" {
		uiAddr = mustPickFreeAddr("127.0.0.1")
	}
	dataAddr := mustPickFreeAddr("127.0.0.1")
	controllerBind := strings.TrimSpace(*controller)
	if controllerBind == "" {
		controllerBind = mustPickFreeAddr("127.0.0.1")
	}
	pivotAddr := strings.TrimSpace(*pivotBind)
	if pivotAddr == "" {
		pivotAddr = mustPickFreeAddr("127.0.0.1")
	}

	kelpieArgs := []string{
		"-s", *secret,
		"--ui-grpc-listen", uiAddr,
		"--dataplane-listen", dataAddr,
	}
	if strings.TrimSpace(*uiToken) != "" {
		kelpieArgs = append(kelpieArgs, "--ui-grpc-token", strings.TrimSpace(*uiToken))
	}
	if *disableRate {
		kelpieArgs = append(kelpieArgs, "--ui-grpc-rate", "0")
	}

	kelpieInstance := 1
	kelpieCmd, err := startKelpie(ctx, kelpiePath, kelpieArgs, runDir, logDir, kelpieInstance)
	if err != nil {
		fatalf("start kelpie: %v", err)
	}
	defer func() { killAndWait(kelpieCmd) }()

	conn, err := dialUI(ctx, uiAddr)
	if err != nil {
		fatalf("dial kelpie gRPC UI: %v", err)
	}
	defer conn.Close()

	uiClient := uipb.NewKelpieUIServiceClient(conn)
	ctrlClient := uipb.NewControllerListenerAdminServiceClient(conn)
	pivotClient := uipb.NewPivotListenerAdminServiceClient(conn)
	sleepClient := uipb.NewSleepAdminServiceClient(conn)
	suppClient := uipb.NewSupplementalAdminServiceClient(conn)
	dpClient := dataplanepb.NewDataplaneAdminClient(conn)

	startedAt := time.Now().UTC()
	// startMono 定义 trace 调度与指标时间戳的零点。
	// 它被有意放在 bootstrap 完成之后设置，这样即使节点启动耗时不同，
	// trace 中的 at_ms 值也能保持稳定。
	var startMono time.Time

	// 记录运行参数，便于复现与后处理。
	writeJSON(filepath.Join(runDir, "config.json"), map[string]any{
		"started_at":    startedAt.Format(time.RFC3339Nano),
		"nodes":         *nodes,
		"duration":      duration.String(),
		"metrics_every": metricsEvery.String(),
		"watch_events":  *watchEvents,
		"trace":         strings.TrimSpace(*tracePath),
		"topology":      shape,
	})

	labels := map[string]string{}
	writeLabels := func() {
		out := labelsFile{
			StartedAt: startedAt.Format(time.RFC3339Nano),
			Nodes:     copyStringMap(labels),
		}
		writeJSON(filepath.Join(runDir, "labels.json"), out)
	}

	// 创建一个 controller listener，供根节点 agent 接入。
	{
		req := &uipb.CreateControllerListenerRequest{
			Spec: &uipb.ControllerListenerSpec{
				Bind:     controllerBind,
				Protocol: "tcp",
			},
		}
		if _, err := ctrlClient.CreateControllerListener(withToken(ctx, *uiToken), req); err != nil {
			fatalf("create controller listener (%s): %v", controllerBind, err)
		}
	}

	agentCmds := map[string]*exec.Cmd{}
	agentConnect := map[string]string{}

	// 启动根节点 agent。
	rootRepairPort := 0
	if *agentRepair {
		rootRepairPort = mustPickFreePort(*agentRepairBind)
	}
	rootCmd := startAgent(ctx, runDir, flockPath, filepath.Join(logDir, "flock-root"), *secret, controllerBind, *reconnectS, *agentRepairBind, rootRepairPort)
	agentCmds["root"] = rootCmd
	agentConnect["root"] = controllerBind
	defer func() { _ = rootCmd.Process.Kill() }()

	// 等待第一个节点上线。
	rootUUID, err := waitForNewNode(ctx, uiClient, *uiToken, nil, 30*time.Second)
	if err != nil {
		fatalf("wait for root node: %v", err)
	}
	labels["root"] = rootUUID
	writeLabels()

	// 按顺序启动子节点 agent，并按加入顺序把 label 映射到 UUID。
	known := map[string]struct{}{rootUUID: {}}
	switch shape {
	case "star":
		// 当前实现中的 pivot listener 是“一次性”的：接受一个子节点连接、
		// 完成握手后就退出。因此在 star 拓扑里，
		// 我们需要为每个子节点各建一个 pivot listener。
		for i := 1; i < *nodes; i++ {
			label := fmt.Sprintf("n%d", i)
			bind := ""
			if i == 1 {
				bind = pivotAddr
			} else {
				bind = mustPickFreeAddr("127.0.0.1")
			}

			var pivotListenerID string
			if l, err := createPivotWithRetry(ctx, pivotClient, *uiToken, rootUUID, bind, 8*time.Second); err != nil {
				fatalf("create pivot listener (%s): %v", bind, err)
			} else if l != nil {
				pivotListenerID = l.GetListenerId()
				if strings.EqualFold(strings.TrimSpace(l.GetStatus()), "failed") {
					fatalf("pivot listener failed immediately: %s", strings.TrimSpace(l.GetLastError()))
				}
			}
			if err := waitPivotRunning(ctx, pivotClient, *uiToken, pivotListenerID, bind, 30*time.Second); err != nil {
				fatalf("wait pivot listener running: %v", err)
			}

			repairPort := 0
			if *agentRepair {
				repairPort = mustPickFreePort(*agentRepairBind)
			}
			cmd := startAgent(ctx, runDir, flockPath, filepath.Join(logDir, "flock-"+label), *secret, bind, *reconnectS, *agentRepairBind, repairPort)
			agentCmds[label] = cmd
			agentConnect[label] = bind
			defer func(cmd *exec.Cmd) { _ = cmd.Process.Kill() }(cmd)

			uuid, err := waitForNewNode(ctx, uiClient, *uiToken, known, 45*time.Second)
			if err != nil {
				fatalf("wait for node %s: %v", label, err)
			}
			known[uuid] = struct{}{}
			labels[label] = uuid
			writeLabels()
		}
	case "chain":
		parentLabel := "root"
		parentUUID := rootUUID
		for i := 1; i < *nodes; i++ {
			childLabel := fmt.Sprintf("n%d", i)
			bind := ""
			if i == 1 {
				bind = pivotAddr
			} else {
				bind = mustPickFreeAddr("127.0.0.1")
			}
			var pivotListenerID string
			if l, err := createPivotWithRetry(ctx, pivotClient, *uiToken, parentUUID, bind, 8*time.Second); err != nil {
				fatalf("create pivot listener (%s, parent=%s): %v", bind, parentLabel, err)
			} else if l != nil {
				pivotListenerID = l.GetListenerId()
				if strings.EqualFold(strings.TrimSpace(l.GetStatus()), "failed") {
					fatalf("pivot listener failed immediately (parent=%s): %s", parentLabel, strings.TrimSpace(l.GetLastError()))
				}
			}
			if err := waitPivotRunning(ctx, pivotClient, *uiToken, pivotListenerID, bind, 30*time.Second); err != nil {
				fatalf("wait pivot listener running (parent=%s): %v", parentLabel, err)
			}

			repairPort := 0
			if *agentRepair {
				repairPort = mustPickFreePort(*agentRepairBind)
			}
			cmd := startAgent(ctx, runDir, flockPath, filepath.Join(logDir, "flock-"+childLabel), *secret, bind, *reconnectS, *agentRepairBind, repairPort)
			agentCmds[childLabel] = cmd
			agentConnect[childLabel] = bind
			defer func(cmd *exec.Cmd) { _ = cmd.Process.Kill() }(cmd)

			uuid, err := waitForNewNode(ctx, uiClient, *uiToken, known, 45*time.Second)
			if err != nil {
				fatalf("wait for node %s: %v", childLabel, err)
			}
			known[uuid] = struct{}{}
			labels[childLabel] = uuid
			writeLabels()

			parentLabel = childLabel
			parentUUID = uuid
		}
	}

	restartKelpie := func(ctx context.Context) error {
		if kelpieCmd != nil {
			killAndWait(kelpieCmd)
			kelpieCmd = nil
		}
		kelpieInstance++
		cmd, err := startKelpie(ctx, kelpiePath, kelpieArgs, runDir, logDir, kelpieInstance)
		if err != nil {
			return err
		}
		kelpieCmd = cmd
		if err := waitUIReady(ctx, uiClient, *uiToken, 30*time.Second); err != nil {
			return err
		}
		if rootUUID != "" {
			if err := waitNodeStatus(ctx, uiClient, *uiToken, rootUUID, "online", 45*time.Second); err != nil {
				return err
			}
		}
		return nil
	}

	// 为了让 at_ms 语义保持一致，trace 与 metrics 都从 bootstrap 之后开始。
	startMono = time.Now()

	metricsFile := mustCreate(filepath.Join(runDir, "metrics.jsonl"))
	defer metricsFile.Close()
	metricsW := newJSONLWriter(metricsFile)

	var eventsW *jsonlWriter
	if *watchEvents {
		eventsFile := mustCreate(filepath.Join(runDir, "events.jsonl"))
		defer eventsFile.Close()
		eventsW = newJSONLWriter(eventsFile)
		go watchUIEvents(ctx, uiClient, *uiToken, startMono, eventsW)
	}

	// 加载 trace（可选）。
	var trace []traceEvent
	if strings.TrimSpace(*tracePath) != "" {
		trace, err = loadTrace(*tracePath)
		if err != nil {
			fatalf("load trace: %v", err)
		}
	}

	// 启动周期性 metrics 快照。
	go func() {
		t := time.NewTicker(*metricsEvery)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				_ = snapshotAll(ctx, uiClient, suppClient, *uiToken, startMono, metricsW, "metrics", "")
			}
		}
	}()

	// 执行 trace 事件。
	traceDone := make(chan struct{})
	go func() {
		defer close(traceDone)
		if len(trace) == 0 {
			return
		}
		for _, evt := range trace {
			if ctx.Err() != nil {
				return
			}
			at := startMono.Add(time.Duration(evt.AtMS) * time.Millisecond)
			if d := time.Until(at); d > 0 {
				timer := time.NewTimer(d)
				select {
				case <-ctx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
			}

			switch strings.ToLower(strings.TrimSpace(evt.Type)) {
			case "sleep":
				uuid := labels[strings.TrimSpace(evt.Node)]
				if uuid == "" {
					_ = metricsW.Write(map[string]any{
						"kind":           "trace_error",
						"since_start_ms": time.Since(startMono).Milliseconds(),
						"at":             time.Now().UTC().Format(time.RFC3339Nano),
						"error":          fmt.Sprintf("unknown node label %q", evt.Node),
					})
					continue
				}
				_ = metricsW.Write(map[string]any{
					"kind":           "trace_action",
					"since_start_ms": time.Since(startMono).Milliseconds(),
					"at":             time.Now().UTC().Format(time.RFC3339Nano),
					"action":         "sleep",
					"node":           strings.TrimSpace(evt.Node),
					"uuid":           uuid,
					"sleep_seconds":  evt.SleepSeconds,
					"work_seconds":   evt.WorkSeconds,
					"jitter_percent": evt.JitterPercent,
				})
				req := &uipb.UpdateSleepRequest{TargetUuid: uuid}
				req.SleepSeconds = evt.SleepSeconds
				req.WorkSeconds = evt.WorkSeconds
				req.JitterPercent = evt.JitterPercent
				if err := updateSleepWithRetry(ctx, sleepClient, *uiToken, req, 6*time.Second); err != nil {
					_ = metricsW.Write(map[string]any{
						"kind":           "trace_error",
						"since_start_ms": time.Since(startMono).Milliseconds(),
						"at":             time.Now().UTC().Format(time.RFC3339Nano),
						"error":          fmt.Sprintf("UpdateSleep(%s) failed: %v", strings.TrimSpace(evt.Node), err),
					})
				}
			case "dtn_enqueue":
				uuid := labels[strings.TrimSpace(evt.Target)]
				if uuid == "" {
					_ = metricsW.Write(map[string]any{
						"kind":           "trace_error",
						"since_start_ms": time.Since(startMono).Milliseconds(),
						"at":             time.Now().UTC().Format(time.RFC3339Nano),
						"error":          fmt.Sprintf("unknown target label %q", evt.Target),
					})
					continue
				}
				priority := parsePriority(evt.Priority)
				_ = metricsW.Write(map[string]any{
					"kind":           "trace_action",
					"since_start_ms": time.Since(startMono).Milliseconds(),
					"at":             time.Now().UTC().Format(time.RFC3339Nano),
					"action":         "dtn_enqueue",
					"target":         strings.TrimSpace(evt.Target),
					"uuid":           uuid,
					"priority":       strings.ToLower(strings.TrimSpace(evt.Priority)),
					"ttl_seconds":    evt.TTLSeconds,
					"payload":        evt.Payload,
				})
				req := &uipb.EnqueueDtnPayloadRequest{
					TargetUuid: uuid,
					Payload:    evt.Payload,
					Priority:   priority,
					TtlSeconds: evt.TTLSeconds,
				}
				if err := enqueueDtnWithRetry(ctx, uiClient, *uiToken, req, 6*time.Second); err != nil {
					_ = metricsW.Write(map[string]any{
						"kind":           "trace_error",
						"since_start_ms": time.Since(startMono).Milliseconds(),
						"at":             time.Now().UTC().Format(time.RFC3339Nano),
						"error":          fmt.Sprintf("EnqueueDtnPayload(%s) failed: %v", strings.TrimSpace(evt.Target), err),
					})
				}
			case "dtn_expect_order":
				// 在 runner 中这里不做额外动作：顺序性由 regress.py 通过检查 Kelpie 日志来断言。
				// 保留这个分支，是为了避免生成过多无意义的 trace_error 记录。
				_ = metricsW.Write(map[string]any{
					"kind":           "trace_action",
					"since_start_ms": time.Since(startMono).Milliseconds(),
					"at":             time.Now().UTC().Format(time.RFC3339Nano),
					"action":         "dtn_expect_order",
					"payload":        evt.Payload,
				})
			case "wait_memo":
				label := strings.TrimSpace(evt.Node)
				if label == "" {
					label = strings.TrimSpace(evt.Target)
				}
				uuid := labels[label]
				expect := strings.TrimSpace(evt.Expect)
				if uuid == "" || expect == "" {
					_ = metricsW.Write(map[string]any{
						"kind":           "trace_error",
						"since_start_ms": time.Since(startMono).Milliseconds(),
						"at":             time.Now().UTC().Format(time.RFC3339Nano),
						"error":          "wait_memo missing node/expect",
					})
					continue
				}
				timeout := 45 * time.Second
				if evt.TimeoutMS > 0 {
					timeout = time.Duration(evt.TimeoutMS) * time.Millisecond
				}
				_ = metricsW.Write(map[string]any{
					"kind":           "trace_action",
					"since_start_ms": time.Since(startMono).Milliseconds(),
					"at":             time.Now().UTC().Format(time.RFC3339Nano),
					"action":         "wait_memo",
					"node":           label,
					"uuid":           uuid,
					"expect":         expect,
					"timeout_ms":     timeout.Milliseconds(),
				})
				start := time.Now()
				err := waitNodeMemo(ctx, uiClient, *uiToken, uuid, expect, timeout)
				rec := map[string]any{
					"kind":           "trace_result",
					"since_start_ms": time.Since(startMono).Milliseconds(),
					"at":             time.Now().UTC().Format(time.RFC3339Nano),
					"action":         "wait_memo",
					"node":           label,
					"uuid":           uuid,
					"elapsed_ms":     time.Since(start).Milliseconds(),
					"ok":             err == nil,
				}
				if err != nil {
					rec["error"] = err.Error()
				}
				_ = metricsW.Write(rec)
			case "wait_node_status":
				label := strings.TrimSpace(evt.Node)
				if label == "" {
					label = strings.TrimSpace(evt.Target)
				}
				uuid := labels[label]
				expect := strings.TrimSpace(evt.Expect)
				if uuid == "" || expect == "" {
					_ = metricsW.Write(map[string]any{
						"kind":           "trace_error",
						"since_start_ms": time.Since(startMono).Milliseconds(),
						"at":             time.Now().UTC().Format(time.RFC3339Nano),
						"error":          "wait_node_status missing node/expect",
					})
					continue
				}
				timeout := 45 * time.Second
				if evt.TimeoutMS > 0 {
					timeout = time.Duration(evt.TimeoutMS) * time.Millisecond
				}
				_ = metricsW.Write(map[string]any{
					"kind":           "trace_action",
					"since_start_ms": time.Since(startMono).Milliseconds(),
					"at":             time.Now().UTC().Format(time.RFC3339Nano),
					"action":         "wait_node_status",
					"node":           label,
					"uuid":           uuid,
					"expect":         expect,
					"timeout_ms":     timeout.Milliseconds(),
				})
				start := time.Now()
				err := waitNodeStatus(ctx, uiClient, *uiToken, uuid, expect, timeout)
				rec := map[string]any{
					"kind":           "trace_result",
					"since_start_ms": time.Since(startMono).Milliseconds(),
					"at":             time.Now().UTC().Format(time.RFC3339Nano),
					"action":         "wait_node_status",
					"node":           label,
					"uuid":           uuid,
					"elapsed_ms":     time.Since(start).Milliseconds(),
					"ok":             err == nil,
				}
				if err != nil {
					rec["error"] = err.Error()
				}
				_ = metricsW.Write(rec)
			case "io_burst":
				targetLabel := strings.TrimSpace(evt.Target)
				if targetLabel == "" {
					targetLabel = strings.TrimSpace(evt.Node)
				}
				uuid := labels[targetLabel]
				if uuid == "" {
					_ = metricsW.Write(map[string]any{
						"kind":           "trace_error",
						"since_start_ms": time.Since(startMono).Milliseconds(),
						"at":             time.Now().UTC().Format(time.RFC3339Nano),
						"error":          fmt.Sprintf("io_burst unknown target %q", targetLabel),
					})
					continue
				}
				spN := evt.StreamCount
				dpN := evt.DataplaneCount
				if spN < 0 {
					spN = 0
				}
				if dpN < 0 {
					dpN = 0
				}
				if spN == 0 && dpN == 0 {
					spN = 1
					dpN = 1
				}
				payloadBytes := evt.PayloadBytes
				if payloadBytes <= 0 {
					payloadBytes = 64
				}
				timeout := 45 * time.Second
				if evt.TimeoutMS > 0 {
					timeout = time.Duration(evt.TimeoutMS) * time.Millisecond
				}
				_ = metricsW.Write(map[string]any{
					"kind":            "trace_action",
					"since_start_ms":  time.Since(startMono).Milliseconds(),
					"at":              time.Now().UTC().Format(time.RFC3339Nano),
					"action":          "io_burst",
					"target":          targetLabel,
					"uuid":            uuid,
					"stream_count":    spN,
					"dataplane_count": dpN,
					"payload_bytes":   payloadBytes,
					"timeout_ms":      timeout.Milliseconds(),
				})
				start := time.Now()
				err := runIOBurst(ctx, uiClient, dpClient, *uiToken, uuid, spN, dpN, payloadBytes, timeout)
				rec := map[string]any{
					"kind":           "trace_result",
					"since_start_ms": time.Since(startMono).Milliseconds(),
					"at":             time.Now().UTC().Format(time.RFC3339Nano),
					"action":         "io_burst",
					"target":         targetLabel,
					"uuid":           uuid,
					"elapsed_ms":     time.Since(start).Milliseconds(),
					"ok":             err == nil,
				}
				if err != nil {
					rec["error"] = err.Error()
				}
				_ = metricsW.Write(rec)
			case "dataplane_upload_interrupt":
				targetLabel := strings.TrimSpace(evt.Target)
				uuid := labels[targetLabel]
				if uuid == "" || targetLabel == "" {
					_ = metricsW.Write(map[string]any{
						"kind":           "trace_error",
						"since_start_ms": time.Since(startMono).Milliseconds(),
						"at":             time.Now().UTC().Format(time.RFC3339Nano),
						"error":          "dataplane_upload_interrupt missing/unknown target",
					})
					continue
				}
				killLabel := strings.TrimSpace(evt.KillNode)
				if killLabel == "" {
					killLabel = targetLabel
				}
				killCmd := agentCmds[killLabel]
				if killCmd == nil || killCmd.Process == nil {
					_ = metricsW.Write(map[string]any{
						"kind":           "trace_error",
						"since_start_ms": time.Since(startMono).Milliseconds(),
						"at":             time.Now().UTC().Format(time.RFC3339Nano),
						"error":          fmt.Sprintf("dataplane_upload_interrupt unknown kill node %q", killLabel),
					})
					continue
				}
				payloadBytes := evt.PayloadBytes
				if payloadBytes <= 0 {
					payloadBytes = 512 * 1024
				}
				chunkDelay := time.Duration(0)
				if evt.ChunkDelayMS > 0 {
					chunkDelay = time.Duration(evt.ChunkDelayMS) * time.Millisecond
				}
				killAfter := 400 * time.Millisecond
				if evt.KillAfterMS > 0 {
					killAfter = time.Duration(evt.KillAfterMS) * time.Millisecond
				}
				timeout := 30 * time.Second
				if evt.TimeoutMS > 0 {
					timeout = time.Duration(evt.TimeoutMS) * time.Millisecond
				}
				path := strings.TrimSpace(evt.Path)
				if path == "" {
					path = fmt.Sprintf("trace/dp_interrupt_%d.bin", time.Since(startMono).Milliseconds())
				}
				_ = metricsW.Write(map[string]any{
					"kind":           "trace_action",
					"since_start_ms": time.Since(startMono).Milliseconds(),
					"at":             time.Now().UTC().Format(time.RFC3339Nano),
					"action":         "dataplane_upload_interrupt",
					"target":         targetLabel,
					"uuid":           uuid,
					"path":           path,
					"payload_bytes":  payloadBytes,
					"kill_node":      killLabel,
					"kill_after_ms":  killAfter.Milliseconds(),
					"chunk_delay_ms": chunkDelay.Milliseconds(),
					"timeout_ms":     timeout.Milliseconds(),
				})
				start := time.Now()
				err := runDataplaneUploadInterrupt(ctx, dpClient, *uiToken, uuid, path, payloadBytes, timeout, chunkDelay, killCmd, killAfter)
				rec := map[string]any{
					"kind":           "trace_result",
					"since_start_ms": time.Since(startMono).Milliseconds(),
					"at":             time.Now().UTC().Format(time.RFC3339Nano),
					"action":         "dataplane_upload_interrupt",
					"target":         targetLabel,
					"uuid":           uuid,
					"path":           path,
					"elapsed_ms":     time.Since(start).Milliseconds(),
					"ok":             err == nil,
				}
				if err != nil {
					rec["error"] = err.Error()
				}
				_ = metricsW.Write(rec)
			case "dataplane_token_replay":
				targetLabel := strings.TrimSpace(evt.Target)
				uuid := labels[targetLabel]
				if uuid == "" {
					_ = metricsW.Write(map[string]any{
						"kind":           "trace_error",
						"since_start_ms": time.Since(startMono).Milliseconds(),
						"at":             time.Now().UTC().Format(time.RFC3339Nano),
						"error":          fmt.Sprintf("unknown target label %q", targetLabel),
					})
					continue
				}
				payloadBytes := evt.PayloadBytes
				if payloadBytes <= 0 {
					payloadBytes = 1024
				}
				timeout := 30 * time.Second
				if evt.TimeoutMS > 0 {
					timeout = time.Duration(evt.TimeoutMS) * time.Millisecond
				}
				path := strings.TrimSpace(evt.Path)
				if path == "" {
					path = fmt.Sprintf("trace/dp_token_replay_%d.bin", time.Since(startMono).Milliseconds())
				}
				_ = metricsW.Write(map[string]any{
					"kind":           "trace_action",
					"since_start_ms": time.Since(startMono).Milliseconds(),
					"at":             time.Now().UTC().Format(time.RFC3339Nano),
					"action":         "dataplane_token_replay",
					"target":         targetLabel,
					"uuid":           uuid,
					"path":           path,
					"payload_bytes":  payloadBytes,
					"timeout_ms":     timeout.Milliseconds(),
				})
				start := time.Now()
				err := runDataplaneTokenReplay(ctx, dpClient, *uiToken, uuid, path, payloadBytes, timeout)
				rec := map[string]any{
					"kind":           "trace_result",
					"since_start_ms": time.Since(startMono).Milliseconds(),
					"at":             time.Now().UTC().Format(time.RFC3339Nano),
					"action":         "dataplane_token_replay",
					"target":         targetLabel,
					"uuid":           uuid,
					"path":           path,
					"elapsed_ms":     time.Since(start).Milliseconds(),
					"ok":             err == nil,
				}
				if err != nil {
					rec["error"] = err.Error()
				}
				_ = metricsW.Write(rec)
			case "dataplane_token_expire":
				targetLabel := strings.TrimSpace(evt.Target)
				uuid := labels[targetLabel]
				if uuid == "" {
					_ = metricsW.Write(map[string]any{
						"kind":           "trace_error",
						"since_start_ms": time.Since(startMono).Milliseconds(),
						"at":             time.Now().UTC().Format(time.RFC3339Nano),
						"error":          fmt.Sprintf("unknown target label %q", targetLabel),
					})
					continue
				}
				payloadBytes := evt.PayloadBytes
				if payloadBytes <= 0 {
					payloadBytes = 256
				}
				ttl := evt.TTLSeconds
				if ttl <= 0 {
					ttl = 1
				}
				delay := time.Duration(evt.DelayMS) * time.Millisecond
				if delay <= 0 {
					delay = time.Duration(ttl+1) * time.Second
				}
				timeout := 30 * time.Second
				if evt.TimeoutMS > 0 {
					timeout = time.Duration(evt.TimeoutMS) * time.Millisecond
				}
				path := strings.TrimSpace(evt.Path)
				if path == "" {
					path = fmt.Sprintf("trace/dp_token_expire_%d.bin", time.Since(startMono).Milliseconds())
				}
				_ = metricsW.Write(map[string]any{
					"kind":           "trace_action",
					"since_start_ms": time.Since(startMono).Milliseconds(),
					"at":             time.Now().UTC().Format(time.RFC3339Nano),
					"action":         "dataplane_token_expire",
					"target":         targetLabel,
					"uuid":           uuid,
					"path":           path,
					"payload_bytes":  payloadBytes,
					"ttl_seconds":    ttl,
					"delay_ms":       delay.Milliseconds(),
					"timeout_ms":     timeout.Milliseconds(),
				})
				start := time.Now()
				err := runDataplaneTokenExpire(ctx, dpClient, *uiToken, uuid, path, payloadBytes, ttl, delay, timeout)
				rec := map[string]any{
					"kind":           "trace_result",
					"since_start_ms": time.Since(startMono).Milliseconds(),
					"at":             time.Now().UTC().Format(time.RFC3339Nano),
					"action":         "dataplane_token_expire",
					"target":         targetLabel,
					"uuid":           uuid,
					"path":           path,
					"elapsed_ms":     time.Since(start).Milliseconds(),
					"ok":             err == nil,
				}
				if err != nil {
					rec["error"] = err.Error()
				}
				_ = metricsW.Write(rec)
			case "kelpie_restart":
				_ = metricsW.Write(map[string]any{
					"kind":           "trace_action",
					"since_start_ms": time.Since(startMono).Milliseconds(),
					"at":             time.Now().UTC().Format(time.RFC3339Nano),
					"action":         "kelpie_restart",
				})
				err := restartKelpie(ctx)
				if err != nil {
					_ = metricsW.Write(map[string]any{
						"kind":           "trace_result",
						"since_start_ms": time.Since(startMono).Milliseconds(),
						"at":             time.Now().UTC().Format(time.RFC3339Nano),
						"action":         "kelpie_restart",
						"ok":             false,
						"error":          err.Error(),
					})
				} else {
					_ = metricsW.Write(map[string]any{
						"kind":           "trace_result",
						"since_start_ms": time.Since(startMono).Milliseconds(),
						"at":             time.Now().UTC().Format(time.RFC3339Nano),
						"action":         "kelpie_restart",
						"ok":             true,
					})
					_ = snapshotAll(ctx, uiClient, suppClient, *uiToken, startMono, metricsW, "metrics", "kelpie_restarted")
				}
			case "dataplane_roundtrip":
				targetLabel := strings.TrimSpace(evt.Target)
				uuid := labels[targetLabel]
				if uuid == "" {
					_ = metricsW.Write(map[string]any{
						"kind":           "trace_error",
						"since_start_ms": time.Since(startMono).Milliseconds(),
						"at":             time.Now().UTC().Format(time.RFC3339Nano),
						"error":          fmt.Sprintf("unknown target label %q", targetLabel),
					})
					continue
				}
				path := strings.TrimSpace(evt.Path)
				if path == "" {
					_ = metricsW.Write(map[string]any{
						"kind":           "trace_error",
						"since_start_ms": time.Since(startMono).Milliseconds(),
						"at":             time.Now().UTC().Format(time.RFC3339Nano),
						"error":          "dataplane_roundtrip missing path",
					})
					continue
				}
				payload := evt.Payload
				if payload == "" {
					payload = fmt.Sprintf("trace-dataplane-%d", time.Since(startMono).Milliseconds())
				}
				timeout := 30 * time.Second
				if evt.TimeoutMS > 0 {
					timeout = time.Duration(evt.TimeoutMS) * time.Millisecond
				}
				_ = metricsW.Write(map[string]any{
					"kind":           "trace_action",
					"since_start_ms": time.Since(startMono).Milliseconds(),
					"at":             time.Now().UTC().Format(time.RFC3339Nano),
					"action":         "dataplane_roundtrip",
					"target":         targetLabel,
					"uuid":           uuid,
					"path":           path,
					"bytes":          len([]byte(payload)),
					"timeout_ms":     timeout.Milliseconds(),
				})
				start := time.Now()
				err := runDataplaneRoundtrip(ctx, dpClient, *uiToken, uuid, path, []byte(payload), timeout)
				rec := map[string]any{
					"kind":           "trace_result",
					"since_start_ms": time.Since(startMono).Milliseconds(),
					"at":             time.Now().UTC().Format(time.RFC3339Nano),
					"action":         "dataplane_roundtrip",
					"target":         targetLabel,
					"uuid":           uuid,
					"path":           path,
					"elapsed_ms":     time.Since(start).Milliseconds(),
					"ok":             err == nil,
				}
				if err != nil {
					rec["error"] = err.Error()
				}
				_ = metricsW.Write(rec)
			case "stream_proxy":
				label := strings.TrimSpace(evt.Node)
				if label == "" {
					label = strings.TrimSpace(evt.Target)
				}
				uuid := labels[label]
				if uuid == "" {
					_ = metricsW.Write(map[string]any{
						"kind":           "trace_error",
						"since_start_ms": time.Since(startMono).Milliseconds(),
						"at":             time.Now().UTC().Format(time.RFC3339Nano),
						"error":          fmt.Sprintf("unknown node label %q", label),
					})
					continue
				}
				payload := evt.Payload
				if payload == "" {
					payload = fmt.Sprintf("PING-%d\n", time.Since(startMono).Milliseconds())
				}
				expect := evt.Expect
				if expect == "" {
					expect = payload
				}
				timeout := 20 * time.Second
				if evt.TimeoutMS > 0 {
					timeout = time.Duration(evt.TimeoutMS) * time.Millisecond
				}
				_ = metricsW.Write(map[string]any{
					"kind":           "trace_action",
					"since_start_ms": time.Since(startMono).Milliseconds(),
					"at":             time.Now().UTC().Format(time.RFC3339Nano),
					"action":         "stream_proxy",
					"node":           label,
					"uuid":           uuid,
					"bytes":          len([]byte(payload)),
					"timeout_ms":     timeout.Milliseconds(),
				})
				start := time.Now()
				err := runStreamProxyRoundtrip(ctx, uiClient, *uiToken, uuid, []byte(payload), []byte(expect), timeout)
				rec := map[string]any{
					"kind":           "trace_result",
					"since_start_ms": time.Since(startMono).Milliseconds(),
					"at":             time.Now().UTC().Format(time.RFC3339Nano),
					"action":         "stream_proxy",
					"node":           label,
					"uuid":           uuid,
					"elapsed_ms":     time.Since(start).Milliseconds(),
					"ok":             err == nil,
				}
				if err != nil {
					rec["error"] = err.Error()
				}
				_ = metricsW.Write(rec)
			case "kill":
				label := strings.TrimSpace(evt.Node)
				if label == "" {
					_ = metricsW.Write(map[string]any{
						"kind":           "trace_error",
						"since_start_ms": time.Since(startMono).Milliseconds(),
						"at":             time.Now().UTC().Format(time.RFC3339Nano),
						"error":          "missing node label for kill event",
					})
					continue
				}
				cmd := agentCmds[label]
				if cmd == nil || cmd.Process == nil {
					_ = metricsW.Write(map[string]any{
						"kind":           "trace_error",
						"since_start_ms": time.Since(startMono).Milliseconds(),
						"at":             time.Now().UTC().Format(time.RFC3339Nano),
						"error":          fmt.Sprintf("unknown node label %q", label),
					})
					continue
				}
				_ = metricsW.Write(map[string]any{
					"kind":           "trace_action",
					"since_start_ms": time.Since(startMono).Milliseconds(),
					"at":             time.Now().UTC().Format(time.RFC3339Nano),
					"action":         "kill",
					"node":           label,
					"uuid":           labels[label],
				})
				_ = cmd.Process.Kill()
			case "metrics":
				_ = snapshotAll(ctx, uiClient, suppClient, *uiToken, startMono, metricsW, "metrics", evt.Note)
			default:
				_ = metricsW.Write(map[string]any{
					"kind":           "trace_error",
					"since_start_ms": time.Since(startMono).Milliseconds(),
					"at":             time.Now().UTC().Format(time.RFC3339Nano),
					"error":          fmt.Sprintf("unknown trace event type %q", evt.Type),
				})
			}
		}
	}()

	// bootstrap 完成后先写出一份初始快照。
	_ = snapshotAll(ctx, uiClient, suppClient, *uiToken, startMono, metricsW, "metrics", "bootstrapped")

	// 阻塞直到运行时长结束或收到中断。
	timer := time.NewTimer(*duration)
	if *finishWhenDone {
		doneByTrace := false
		select {
		case <-ctx.Done():
		case <-timer.C:
		case <-traceDone:
			doneByTrace = true
		}
		if doneByTrace && *tailMS > 0 {
			tail := time.Duration(*tailMS) * time.Millisecond
			tailTimer := time.NewTimer(tail)
			select {
			case <-ctx.Done():
			case <-timer.C:
			case <-tailTimer.C:
			}
			tailTimer.Stop()
		}
	} else {
		select {
		case <-ctx.Done():
		case <-timer.C:
		}
	}
	timer.Stop()

	// 最终快照。
	_ = snapshotAll(context.Background(), uiClient, suppClient, *uiToken, startMono, metricsW, "metrics", "final")
	writeLabels()
}

func dialUI(ctx context.Context, uiAddr string) (*grpc.ClientConn, error) {
	uiAddr = strings.TrimSpace(uiAddr)
	if uiAddr == "" {
		return nil, fmt.Errorf("empty ui address")
	}
	dialCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	conn, err := grpc.NewClient(uiAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	if err := waitForGRPCReady(dialCtx, conn); err != nil {
		conn.Close()
		return nil, err
	}
	return conn, nil
}

func waitForGRPCReady(ctx context.Context, conn *grpc.ClientConn) error {
	if conn == nil {
		return fmt.Errorf("nil grpc client connection")
	}
	conn.Connect()
	for {
		state := conn.GetState()
		if state == connectivity.Ready {
			return nil
		}
		if state == connectivity.Shutdown {
			return fmt.Errorf("grpc connection shut down")
		}
		if !conn.WaitForStateChange(ctx, state) {
			if err := ctx.Err(); err != nil {
				return err
			}
			return fmt.Errorf("grpc connection did not become ready")
		}
	}
}

func withToken(ctx context.Context, token string) context.Context {
	token = strings.TrimSpace(token)
	if token == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, "x-kelpie-token", token)
}

func snapshotAll(ctx context.Context, ui uipb.KelpieUIServiceClient, supp uipb.SupplementalAdminServiceClient, token string, startMono time.Time, out *jsonlWriter, kind, note string) error {
	snapshotAllMu.Lock()
	defer snapshotAllMu.Unlock()

	if out == nil {
		return nil
	}
	now := time.Now().UTC()
	sinceStartMS := now.Sub(startMono).Milliseconds()
	if startMono.IsZero() {
		sinceStartMS = 0
	}
	record := map[string]any{
		"kind":           kind,
		"at":             now.Format(time.RFC3339Nano),
		"since_start_ms": sinceStartMS,
	}
	if strings.TrimSpace(note) != "" {
		record["note"] = note
	}
	var (
		topologyResp    *uipb.GetTopologyResponse
		suppMetricsResp *uipb.SupplementalMetrics
		diagRecords     []map[string]any
	)
	marshal := protojson.MarshalOptions{UseProtoNames: true}

	// 拓扑指标
	if ui != nil {
		if topo, err := ui.GetTopology(withToken(ctx, token), &uipb.GetTopologyRequest{}); err == nil && topo != nil {
			topologyResp = topo
			if raw, err := marshal.Marshal(topo); err == nil {
				record["topology"] = json.RawMessage(raw)
			}
		} else if err != nil {
			record["topology_error"] = err.Error()
		}
		if metrics, err := ui.GetMetrics(withToken(ctx, token), &uipb.GetMetricsRequest{IncludeRouter: false, IncludeReconnect: true}); err == nil && metrics != nil {
			if raw, err := marshal.Marshal(metrics); err == nil {
				record["metrics"] = json.RawMessage(raw)
			}
		} else if err != nil {
			record["metrics_error"] = err.Error()
		}
		if streams, err := ui.StreamDiagnostics(withToken(ctx, token), &uipb.StreamDiagnosticsRequest{}); err == nil && streams != nil {
			if raw, err := marshal.Marshal(streams); err == nil {
				record["streams"] = json.RawMessage(raw)
			}
		} else if err != nil {
			record["streams_error"] = err.Error()
		}
	}

	// 补链指标
	if supp != nil {
		if status, err := supp.GetSupplementalStatus(withToken(ctx, token), &uipb.SupplementalEmpty{}); err == nil && status != nil {
			if raw, err := marshal.Marshal(status); err == nil {
				record["supp_status"] = json.RawMessage(raw)
			}
		} else if err != nil {
			record["supp_status_error"] = err.Error()
		}
		if sm, err := supp.GetSupplementalMetrics(withToken(ctx, token), &uipb.SupplementalEmpty{}); err == nil && sm != nil {
			suppMetricsResp = sm
			if raw, err := marshal.Marshal(sm); err == nil {
				record["supp_metrics"] = json.RawMessage(raw)
			}
		} else if err != nil {
			record["supp_metrics_error"] = err.Error()
		}
	}

	if topologyResp != nil {
		diagRecords = append(diagRecords, traceDiag.diffTopology(topologyResp, startMono, note)...)
	}
	if suppMetricsResp != nil {
		diagRecords = append(diagRecords, traceDiag.diffSuppMetrics(suppMetricsResp, startMono, note)...)
		if supp != nil {
			diagRecords = append(diagRecords, fetchSuppEventDiag(ctx, supp, token, suppMetricsResp, startMono, note)...)
		}
	}

	if err := out.Write(record); err != nil {
		return err
	}
	for _, diag := range diagRecords {
		if diag == nil {
			continue
		}
		if err := out.Write(diag); err != nil {
			return err
		}
	}
	return nil
}

func diagRecord(kind string, startMono time.Time, note string) map[string]any {
	now := time.Now().UTC()
	sinceStartMS := now.Sub(startMono).Milliseconds()
	if startMono.IsZero() {
		sinceStartMS = 0
	}
	rec := map[string]any{
		"kind":           kind,
		"at":             now.Format(time.RFC3339Nano),
		"since_start_ms": sinceStartMS,
	}
	if strings.TrimSpace(note) != "" {
		rec["note"] = strings.TrimSpace(note)
	}
	return rec
}

func diagNodeFromProto(node *uipb.NodeInfo) diagNodeSnapshot {
	if node == nil {
		return diagNodeSnapshot{}
	}
	return diagNodeSnapshot{
		ParentUUID: strings.TrimSpace(node.GetParentUuid()),
		Status:     strings.ToLower(strings.TrimSpace(node.GetStatus())),
		Network:    strings.TrimSpace(node.GetNetwork()),
		Memo:       strings.TrimSpace(node.GetMemo()),
		Depth:      node.GetDepth(),
	}
}

func diagEdgeKey(parentUUID, childUUID string, supplemental bool) string {
	parentUUID = strings.TrimSpace(parentUUID)
	childUUID = strings.TrimSpace(childUUID)
	kind := "tree"
	if supplemental {
		kind = "supp"
		// Supplemental 边在语义上近似无向；这里规范化端点顺序，
		// 避免仅仅是表示顺序翻转就被误判成真实的拓扑抖动。
		if parentUUID > childUUID {
			parentUUID, childUUID = childUUID, parentUUID
		}
	}
	return parentUUID + "->" + childUUID + "#" + kind
}

func deltaU64(after, before uint64) int64 {
	if after >= before {
		return int64(after - before)
	}
	return -int64(before - after)
}

func (state *traceDiagState) diffTopology(topo *uipb.GetTopologyResponse, startMono time.Time, note string) []map[string]any {
	if state == nil || topo == nil {
		return nil
	}
	currNodes := make(map[string]diagNodeSnapshot, len(topo.GetNodes()))
	for _, node := range topo.GetNodes() {
		if node == nil {
			continue
		}
		uuid := strings.TrimSpace(node.GetUuid())
		if uuid == "" {
			continue
		}
		currNodes[uuid] = diagNodeFromProto(node)
	}
	currEdges := make(map[string]diagEdgeSnapshot, len(topo.GetEdges()))
	for _, edge := range topo.GetEdges() {
		if edge == nil {
			continue
		}
		parentUUID := strings.TrimSpace(edge.GetParentUuid())
		childUUID := strings.TrimSpace(edge.GetChildUuid())
		if parentUUID == "" || childUUID == "" {
			continue
		}
		key := diagEdgeKey(parentUUID, childUUID, edge.GetSupplemental())
		currEdges[key] = diagEdgeSnapshot{
			ParentUUID:   parentUUID,
			ChildUUID:    childUUID,
			Supplemental: edge.GetSupplemental(),
		}
	}

	records := make([]map[string]any, 0, 16)

	uuids := make([]string, 0, len(currNodes))
	for uuid := range currNodes {
		uuids = append(uuids, uuid)
	}
	sort.Strings(uuids)

	for _, uuid := range uuids {
		curr := currNodes[uuid]
		prev, existed := state.nodes[uuid]
		if !existed {
			rec := diagRecord("diag_route_change", startMono, note)
			rec["change"] = "node_added"
			rec["uuid"] = uuid
			rec["parent_uuid"] = curr.ParentUUID
			rec["depth"] = curr.Depth
			rec["status"] = curr.Status
			rec["network"] = curr.Network
			records = append(records, rec)
		} else {
			changed := false
			rec := diagRecord("diag_route_change", startMono, note)
			rec["change"] = "node_updated"
			rec["uuid"] = uuid
			if prev.ParentUUID != curr.ParentUUID {
				rec["parent_before"] = prev.ParentUUID
				rec["parent_after"] = curr.ParentUUID
				changed = true
			}
			if prev.Depth != curr.Depth {
				rec["depth_before"] = prev.Depth
				rec["depth_after"] = curr.Depth
				changed = true
			}
			if prev.Status != curr.Status {
				rec["status_before"] = prev.Status
				rec["status_after"] = curr.Status
				changed = true
			}
			if prev.Network != curr.Network {
				rec["network_before"] = prev.Network
				rec["network_after"] = curr.Network
				changed = true
			}
			if changed {
				records = append(records, rec)
			}
			if prev.Memo != curr.Memo {
				recMemo := diagRecord("diag_memo_change", startMono, note)
				recMemo["uuid"] = uuid
				recMemo["parent_uuid"] = curr.ParentUUID
				recMemo["memo_before"] = prev.Memo
				recMemo["memo_after"] = curr.Memo
				records = append(records, recMemo)
			}
		}

		if curr.Memo == "" {
			continue
		}
		seenNodes, ok := state.memoSeen[curr.Memo]
		if !ok {
			seenNodes = make(map[string]struct{}, 4)
			state.memoSeen[curr.Memo] = seenNodes
		}
		if _, seen := seenNodes[uuid]; seen {
			continue
		}

		sourceUUID := state.memoSource[curr.Memo]
		if sourceUUID == "" {
			state.memoSource[curr.Memo] = uuid
			rec := diagRecord("diag_memo_source", startMono, note)
			rec["memo"] = curr.Memo
			rec["source_uuid"] = uuid
			rec["source_parent_uuid"] = curr.ParentUUID
			rec["source_depth"] = curr.Depth
			records = append(records, rec)
		} else if sourceUUID != uuid {
			rec := diagRecord("diag_memo_path", startMono, note)
			rec["memo"] = curr.Memo
			rec["source_uuid"] = sourceUUID
			rec["seen_uuid"] = uuid
			rec["seen_parent_uuid"] = curr.ParentUUID
			rec["seen_depth"] = curr.Depth
			rec["parent_is_source"] = curr.ParentUUID == sourceUUID
			records = append(records, rec)
		}
		seenNodes[uuid] = struct{}{}
	}

	prevUUIDs := make([]string, 0, len(state.nodes))
	for uuid := range state.nodes {
		if _, ok := currNodes[uuid]; ok {
			continue
		}
		prevUUIDs = append(prevUUIDs, uuid)
	}
	sort.Strings(prevUUIDs)
	for _, uuid := range prevUUIDs {
		prev := state.nodes[uuid]
		rec := diagRecord("diag_route_change", startMono, note)
		rec["change"] = "node_removed"
		rec["uuid"] = uuid
		rec["parent_uuid"] = prev.ParentUUID
		rec["depth"] = prev.Depth
		rec["status"] = prev.Status
		rec["network"] = prev.Network
		records = append(records, rec)
	}

	addedEdges := make([]string, 0, len(currEdges))
	for key := range currEdges {
		if _, ok := state.edges[key]; ok {
			continue
		}
		addedEdges = append(addedEdges, key)
	}
	sort.Strings(addedEdges)
	for _, key := range addedEdges {
		edge := currEdges[key]
		rec := diagRecord("diag_route_change", startMono, note)
		rec["change"] = "edge_added"
		rec["parent_uuid"] = edge.ParentUUID
		rec["child_uuid"] = edge.ChildUUID
		rec["supplemental"] = edge.Supplemental
		records = append(records, rec)
	}

	removedEdges := make([]string, 0, len(state.edges))
	for key := range state.edges {
		if _, ok := currEdges[key]; ok {
			continue
		}
		removedEdges = append(removedEdges, key)
	}
	sort.Strings(removedEdges)
	for _, key := range removedEdges {
		edge := state.edges[key]
		rec := diagRecord("diag_route_change", startMono, note)
		rec["change"] = "edge_removed"
		rec["parent_uuid"] = edge.ParentUUID
		rec["child_uuid"] = edge.ChildUUID
		rec["supplemental"] = edge.Supplemental
		records = append(records, rec)
	}

	state.nodes = currNodes
	state.edges = currEdges
	return records
}

func (state *traceDiagState) diffSuppMetrics(sm *uipb.SupplementalMetrics, startMono time.Time, note string) []map[string]any {
	if state == nil || sm == nil {
		return nil
	}
	curr := suppMetricsSnapshot{
		Dispatched:     sm.GetDispatched(),
		Success:        sm.GetSuccess(),
		Failures:       sm.GetFailures(),
		Dropped:        sm.GetDropped(),
		Recycled:       sm.GetRecycled(),
		QueueHigh:      sm.GetQueueHigh(),
		EventSeq:       sm.GetEventSeq(),
		RepairAttempts: sm.GetRepairAttempts(),
		RepairSuccess:  sm.GetRepairSuccess(),
		RepairFailures: sm.GetRepairFailures(),
		LastFailure:    strings.TrimSpace(sm.GetLastFailure()),
	}
	if !state.suppMetricsInit {
		state.suppMetrics = curr
		state.suppMetricsInit = true
		state.suppEventSeq = curr.EventSeq
		return nil
	}
	prev := state.suppMetrics
	rec := diagRecord("diag_supp_metrics_change", startMono, note)
	changed := false

	if curr.Dispatched != prev.Dispatched {
		rec["dispatched_before"] = prev.Dispatched
		rec["dispatched_after"] = curr.Dispatched
		rec["dispatched_delta"] = deltaU64(curr.Dispatched, prev.Dispatched)
		changed = true
	}
	if curr.Success != prev.Success {
		rec["success_before"] = prev.Success
		rec["success_after"] = curr.Success
		rec["success_delta"] = deltaU64(curr.Success, prev.Success)
		changed = true
	}
	if curr.Failures != prev.Failures {
		rec["failures_before"] = prev.Failures
		rec["failures_after"] = curr.Failures
		rec["failures_delta"] = deltaU64(curr.Failures, prev.Failures)
		changed = true
	}
	if curr.Dropped != prev.Dropped {
		rec["dropped_before"] = prev.Dropped
		rec["dropped_after"] = curr.Dropped
		rec["dropped_delta"] = deltaU64(curr.Dropped, prev.Dropped)
		changed = true
	}
	if curr.Recycled != prev.Recycled {
		rec["recycled_before"] = prev.Recycled
		rec["recycled_after"] = curr.Recycled
		rec["recycled_delta"] = deltaU64(curr.Recycled, prev.Recycled)
		changed = true
	}
	if curr.QueueHigh != prev.QueueHigh {
		rec["queue_high_before"] = prev.QueueHigh
		rec["queue_high_after"] = curr.QueueHigh
		rec["queue_high_delta"] = deltaU64(curr.QueueHigh, prev.QueueHigh)
		changed = true
	}
	if curr.EventSeq != prev.EventSeq {
		rec["event_seq_before"] = prev.EventSeq
		rec["event_seq_after"] = curr.EventSeq
		rec["event_seq_delta"] = deltaU64(curr.EventSeq, prev.EventSeq)
		changed = true
	}
	if curr.RepairAttempts != prev.RepairAttempts {
		rec["repair_attempts_before"] = prev.RepairAttempts
		rec["repair_attempts_after"] = curr.RepairAttempts
		rec["repair_attempts_delta"] = deltaU64(curr.RepairAttempts, prev.RepairAttempts)
		changed = true
	}
	if curr.RepairSuccess != prev.RepairSuccess {
		rec["repair_success_before"] = prev.RepairSuccess
		rec["repair_success_after"] = curr.RepairSuccess
		rec["repair_success_delta"] = deltaU64(curr.RepairSuccess, prev.RepairSuccess)
		changed = true
	}
	if curr.RepairFailures != prev.RepairFailures {
		rec["repair_failures_before"] = prev.RepairFailures
		rec["repair_failures_after"] = curr.RepairFailures
		rec["repair_failures_delta"] = deltaU64(curr.RepairFailures, prev.RepairFailures)
		changed = true
	}
	if curr.LastFailure != prev.LastFailure {
		rec["last_failure_before"] = prev.LastFailure
		rec["last_failure_after"] = curr.LastFailure
		changed = true
	}

	state.suppMetrics = curr
	if !changed {
		return nil
	}
	return []map[string]any{rec}
}

func fetchSuppEventDiag(ctx context.Context, supp uipb.SupplementalAdminServiceClient, token string, sm *uipb.SupplementalMetrics, startMono time.Time, note string) []map[string]any {
	if supp == nil || sm == nil || traceDiag == nil {
		return nil
	}
	currSeq := sm.GetEventSeq()
	if currSeq == 0 || currSeq <= traceDiag.suppEventSeq {
		return nil
	}
	delta := currSeq - traceDiag.suppEventSeq
	limit := int32(delta + 8)
	if limit < 32 {
		limit = 32
	}
	if limit > 256 {
		limit = 256
	}

	resp, err := supp.ListSupplementalEvents(withToken(ctx, token), &uipb.ListSupplementalEventsRequest{Limit: limit})
	if err != nil {
		rec := diagRecord("diag_supp_event_error", startMono, note)
		rec["error"] = err.Error()
		rec["event_seq_seen"] = currSeq
		rec["event_seq_cursor"] = traceDiag.suppEventSeq
		return []map[string]any{rec}
	}

	events := append([]*uipb.SupplementalEvent(nil), resp.GetEvents()...)
	sort.Slice(events, func(i, j int) bool {
		return events[i].GetSeq() < events[j].GetSeq()
	})

	prevSeq := traceDiag.suppEventSeq
	maxSeq := prevSeq
	firstSeq := uint64(0)
	records := make([]map[string]any, 0, len(events)+1)
	for _, evt := range events {
		if evt == nil {
			continue
		}
		seq := evt.GetSeq()
		if firstSeq == 0 || seq < firstSeq {
			firstSeq = seq
		}
		if seq <= prevSeq {
			continue
		}
		recKind := "diag_supp_event"
		if looksLikeSuppFailoverEvent(evt) {
			recKind = "diag_supp_failover_event"
		}
		rec := diagRecord(recKind, startMono, note)
		rec["seq"] = seq
		rec["event_kind"] = strings.TrimSpace(evt.GetKind())
		rec["action"] = strings.TrimSpace(evt.GetAction())
		if sourceUUID := strings.TrimSpace(evt.GetSourceUuid()); sourceUUID != "" {
			rec["source_uuid"] = sourceUUID
		}
		if targetUUID := strings.TrimSpace(evt.GetTargetUuid()); targetUUID != "" {
			rec["target_uuid"] = targetUUID
		}
		if detail := strings.TrimSpace(evt.GetDetail()); detail != "" {
			rec["detail"] = detail
		}
		if ts := strings.TrimSpace(evt.GetTimestamp()); ts != "" {
			rec["event_timestamp"] = ts
		}
		records = append(records, rec)
		if seq > maxSeq {
			maxSeq = seq
		}
	}

	if firstSeq > 0 && firstSeq > prevSeq+1 {
		gap := diagRecord("diag_supp_event_gap", startMono, note)
		gap["event_seq_cursor"] = prevSeq
		gap["first_returned_seq"] = firstSeq
		gap["event_seq_seen"] = currSeq
		records = append([]map[string]any{gap}, records...)
	}
	if maxSeq > traceDiag.suppEventSeq {
		traceDiag.suppEventSeq = maxSeq
	}
	return records
}

func looksLikeSuppFailoverEvent(evt *uipb.SupplementalEvent) bool {
	if evt == nil {
		return false
	}
	text := strings.ToLower(strings.TrimSpace(evt.GetKind()) + " " +
		strings.TrimSpace(evt.GetAction()) + " " + strings.TrimSpace(evt.GetDetail()))
	if text == "" {
		return false
	}
	keywords := []string{"failover", "promot", "repair", "rescue", "reparent"}
	for _, keyword := range keywords {
		if strings.Contains(text, keyword) {
			return true
		}
	}
	return false
}

func watchUIEvents(ctx context.Context, ui uipb.KelpieUIServiceClient, token string, startMono time.Time, out *jsonlWriter) {
	if ui == nil || out == nil {
		return
	}
	stream, err := ui.WatchEvents(withToken(ctx, token), &uipb.WatchEventsRequest{})
	if err != nil {
		_ = out.Write(map[string]any{
			"kind":           "watch_error",
			"at":             time.Now().UTC().Format(time.RFC3339Nano),
			"since_start_ms": time.Since(startMono).Milliseconds(),
			"error":          err.Error(),
		})
		return
	}
	marshal := protojson.MarshalOptions{UseProtoNames: true}
	for {
		ev, err := stream.Recv()
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, io.EOF) || ctx.Err() != nil {
				return
			}
			_ = out.Write(map[string]any{
				"kind":           "watch_error",
				"at":             time.Now().UTC().Format(time.RFC3339Nano),
				"since_start_ms": time.Since(startMono).Milliseconds(),
				"error":          err.Error(),
			})
			return
		}
		raw, mErr := marshal.Marshal(ev)
		if mErr != nil {
			continue
		}
		_ = out.Write(map[string]any{
			"kind":           "event",
			"at":             time.Now().UTC().Format(time.RFC3339Nano),
			"since_start_ms": time.Since(startMono).Milliseconds(),
			"event":          json.RawMessage(raw),
		})
	}
}

func waitForNewNode(ctx context.Context, ui uipb.KelpieUIServiceClient, token string, known map[string]struct{}, timeout time.Duration) (string, error) {
	if ui == nil {
		return "", fmt.Errorf("ui client unavailable")
	}
	if known == nil {
		known = map[string]struct{}{}
	}
	deadline := time.Now().Add(timeout)
	t := time.NewTicker(200 * time.Millisecond)
	defer t.Stop()
	for {
		if time.Now().After(deadline) {
			return "", fmt.Errorf("timeout waiting for new node")
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-t.C:
		}
		resp, err := ui.GetTopology(withToken(ctx, token), &uipb.GetTopologyRequest{})
		if err != nil || resp == nil {
			continue
		}
		for _, node := range resp.GetNodes() {
			uuid := strings.TrimSpace(node.GetUuid())
			if uuid == "" {
				continue
			}
			if _, ok := known[uuid]; ok {
				continue
			}
			return uuid, nil
		}
	}
}

func waitPivotRunning(ctx context.Context, pivot uipb.PivotListenerAdminServiceClient, token, listenerID, bind string, timeout time.Duration) error {
	if pivot == nil {
		return fmt.Errorf("pivot client unavailable")
	}
	deadline := time.Now().Add(timeout)
	t := time.NewTicker(250 * time.Millisecond)
	defer t.Stop()
	bind = strings.TrimSpace(bind)
	for {
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting pivot listener running")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
		resp, err := pivot.ListPivotListeners(withToken(ctx, token), &uipb.ListPivotListenersRequest{})
		if err != nil || resp == nil {
			continue
		}
		for _, l := range resp.GetListeners() {
			if l == nil {
				continue
			}
			if listenerID != "" && l.GetListenerId() != listenerID {
				continue
			}
			if bind != "" && strings.TrimSpace(l.GetBind()) != bind {
				continue
			}
			switch strings.ToLower(strings.TrimSpace(l.GetStatus())) {
			case "running":
				return nil
			case "failed":
				id := strings.TrimSpace(l.GetListenerId())
				if id == "" {
					id = listenerID
				}
				lastErr := strings.TrimSpace(l.GetLastError())
				if lastErr == "" {
					lastErr = "unknown error"
				}
				return fmt.Errorf("pivot listener %s failed: %s", id, lastErr)
			}
		}
	}
}

func createPivotWithRetry(ctx context.Context, pivot uipb.PivotListenerAdminServiceClient, token, targetUUID, bind string, timeout time.Duration) (*uipb.PivotListener, error) {
	if pivot == nil {
		return nil, fmt.Errorf("pivot client unavailable")
	}
	targetUUID = strings.TrimSpace(targetUUID)
	bind = strings.TrimSpace(bind)
	if targetUUID == "" {
		return nil, fmt.Errorf("empty target uuid")
	}
	if bind == "" {
		return nil, fmt.Errorf("empty bind")
	}
	if timeout <= 0 {
		timeout = 8 * time.Second
	}
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if time.Now().After(deadline) {
			if lastErr == nil {
				lastErr = fmt.Errorf("timeout creating pivot listener for %s on %s", targetUUID, bind)
			}
			return nil, lastErr
		}
		req := &uipb.CreatePivotListenerRequest{
			TargetUuid: targetUUID,
			Spec: &uipb.PivotListenerSpec{
				Protocol: "tcp",
				Bind:     bind,
				Mode:     uipb.PivotListenerMode_PIVOT_LISTENER_MODE_NORMAL,
			},
		}
		resp, err := pivot.CreatePivotListener(withToken(ctx, token), req)
		if err == nil {
			if resp == nil || resp.Listener == nil {
				lastErr = fmt.Errorf("create pivot listener returned empty response")
				time.Sleep(150 * time.Millisecond)
				continue
			}
			l := resp.Listener
			if strings.EqualFold(strings.TrimSpace(l.GetStatus()), "failed") {
				lastErr = errors.New(strings.TrimSpace(l.GetLastError()))
				// 尽力清理，避免失败的 listener 持续堆积。
				if id := strings.TrimSpace(l.GetListenerId()); id != "" {
					_, _ = pivot.DeletePivotListener(withToken(ctx, token), &uipb.DeletePivotListenerRequest{ListenerId: id})
				}
				lower := strings.ToLower(lastErr.Error())
				if strings.Contains(lower, "route unavailable") ||
					strings.Contains(lower, "session unavailable") ||
					strings.Contains(lower, "connection unavailable") {
					time.Sleep(150 * time.Millisecond)
					continue
				}
				return nil, lastErr
			}
			return l, nil
		}
		lastErr = err
		// 路由计算带有去抖；这里短暂重试一次，避免与 bootstrap 竞争。
		lower := strings.ToLower(err.Error())
		if strings.Contains(lower, "route unavailable") ||
			strings.Contains(lower, "session unavailable") ||
			strings.Contains(lower, "connection unavailable") {
			time.Sleep(150 * time.Millisecond)
			continue
		}
		return nil, err
	}
}

func updateSleepWithRetry(ctx context.Context, sleep uipb.SleepAdminServiceClient, token string, req *uipb.UpdateSleepRequest, timeout time.Duration) error {
	if sleep == nil {
		return fmt.Errorf("sleep client unavailable")
	}
	if req == nil || strings.TrimSpace(req.GetTargetUuid()) == "" {
		return fmt.Errorf("invalid sleep request")
	}
	if timeout <= 0 {
		timeout = 6 * time.Second
	}
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if time.Now().After(deadline) {
			if lastErr == nil {
				lastErr = fmt.Errorf("timeout updating sleep for %s", strings.TrimSpace(req.GetTargetUuid()))
			}
			return lastErr
		}
		_, err := sleep.UpdateSleep(withToken(ctx, token), req)
		if err == nil {
			return nil
		}
		lastErr = err
		lower := strings.ToLower(err.Error())
		if strings.Contains(lower, "route unavailable") ||
			strings.Contains(lower, "session unavailable") ||
			strings.Contains(lower, "connection unavailable") {
			time.Sleep(150 * time.Millisecond)
			continue
		}
		return err
	}
}

func enqueueDtnWithRetry(ctx context.Context, ui uipb.KelpieUIServiceClient, token string, req *uipb.EnqueueDtnPayloadRequest, timeout time.Duration) error {
	if ui == nil {
		return fmt.Errorf("ui client unavailable")
	}
	if req == nil || strings.TrimSpace(req.GetTargetUuid()) == "" {
		return fmt.Errorf("invalid DTN enqueue request")
	}
	if timeout <= 0 {
		timeout = 6 * time.Second
	}
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if time.Now().After(deadline) {
			if lastErr == nil {
				lastErr = fmt.Errorf("timeout enqueuing DTN for %s", strings.TrimSpace(req.GetTargetUuid()))
			}
			return lastErr
		}
		_, err := ui.EnqueueDtnPayload(withToken(ctx, token), req)
		if err == nil {
			return nil
		}
		lastErr = err
		lower := strings.ToLower(err.Error())
		if strings.Contains(lower, "route unavailable") ||
			strings.Contains(lower, "session unavailable") ||
			strings.Contains(lower, "connection unavailable") {
			time.Sleep(150 * time.Millisecond)
			continue
		}
		return err
	}
}

func startAgent(ctx context.Context, workDir, flockPath, logPrefix, secret, connectAddr string, reconnectSeconds int, repairBind string, repairPort int) *exec.Cmd {
	args := []string{"-s", secret, "-c", connectAddr}
	if reconnectSeconds > 0 {
		args = append(args, "-reconnect", fmt.Sprintf("%d", reconnectSeconds))
	}
	if strings.TrimSpace(repairBind) != "" && repairPort > 0 {
		args = append(args, "--repair-bind", strings.TrimSpace(repairBind), "--repair-port", fmt.Sprintf("%d", repairPort))
	}
	cmd := exec.CommandContext(ctx, flockPath, args...)
	cmd.Dir = workDir
	cmd.Stdout = mustCreate(logPrefix + ".out.log")
	cmd.Stderr = mustCreate(logPrefix + ".err.log")
	if err := cmd.Start(); err != nil {
		fatalf("start flock (%s): %v", connectAddr, err)
	}
	return cmd
}

func startKelpie(ctx context.Context, kelpiePath string, args []string, workDir, logDir string, instance int) (*exec.Cmd, error) {
	if strings.TrimSpace(kelpiePath) == "" {
		return nil, fmt.Errorf("kelpie path empty")
	}
	name := "kelpie"
	if instance > 1 {
		name = fmt.Sprintf("kelpie-%d", instance)
	}
	cmd := exec.CommandContext(ctx, kelpiePath, args...)
	cmd.Dir = workDir
	cmd.Stdout = mustCreate(filepath.Join(logDir, name+".out.log"))
	cmd.Stderr = mustCreate(filepath.Join(logDir, name+".err.log"))
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	return cmd, nil
}

func killAndWait(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = cmd.Process.Kill()
	waitDone := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(waitDone)
	}()
	select {
	case <-waitDone:
	case <-time.After(3 * time.Second):
	}
}

func waitUIReady(ctx context.Context, ui uipb.KelpieUIServiceClient, token string, timeout time.Duration) error {
	if ui == nil {
		return fmt.Errorf("ui client unavailable")
	}
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	deadline := time.Now().Add(timeout)
	t := time.NewTicker(250 * time.Millisecond)
	defer t.Stop()
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for UI ready")
		}
		_, err := ui.GetTopology(withToken(ctx, token), &uipb.GetTopologyRequest{})
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
}

func waitNodeStatus(ctx context.Context, ui uipb.KelpieUIServiceClient, token, uuid, status string, timeout time.Duration) error {
	if ui == nil {
		return fmt.Errorf("ui client unavailable")
	}
	uuid = strings.TrimSpace(uuid)
	if uuid == "" {
		return fmt.Errorf("empty uuid")
	}
	status = strings.ToLower(strings.TrimSpace(status))
	if status == "" {
		status = "online"
	}
	if timeout <= 0 {
		timeout = 45 * time.Second
	}
	deadline := time.Now().Add(timeout)
	t := time.NewTicker(300 * time.Millisecond)
	defer t.Stop()
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting node %s status=%s", uuid, status)
		}
		resp, err := ui.GetTopology(withToken(ctx, token), &uipb.GetTopologyRequest{})
		if err == nil && resp != nil {
			for _, n := range resp.GetNodes() {
				if n == nil {
					continue
				}
				if strings.TrimSpace(n.GetUuid()) != uuid {
					continue
				}
				if strings.EqualFold(strings.TrimSpace(n.GetStatus()), status) {
					return nil
				}
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
}

func waitNodeMemo(ctx context.Context, ui uipb.KelpieUIServiceClient, token, uuid, memo string, timeout time.Duration) error {
	if ui == nil {
		return fmt.Errorf("ui client unavailable")
	}
	uuid = strings.TrimSpace(uuid)
	if uuid == "" {
		return fmt.Errorf("empty uuid")
	}
	memo = strings.TrimSpace(memo)
	if memo == "" {
		return fmt.Errorf("empty memo")
	}
	if timeout <= 0 {
		timeout = 45 * time.Second
	}
	deadline := time.Now().Add(timeout)
	t := time.NewTicker(300 * time.Millisecond)
	defer t.Stop()
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting node %s memo=%q", uuid, memo)
		}
		resp, err := ui.GetTopology(withToken(ctx, token), &uipb.GetTopologyRequest{})
		if err == nil && resp != nil {
			for _, n := range resp.GetNodes() {
				if n == nil {
					continue
				}
				if strings.TrimSpace(n.GetUuid()) != uuid {
					continue
				}
				if strings.TrimSpace(n.GetMemo()) == memo {
					return nil
				}
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
}

// --- dataplane（TCP）trace 辅助函数 ---

const (
	dpFrameTypeOpen  = 1
	dpFrameTypeData  = 2
	dpFrameTypeClose = 3
)

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func runDataplaneRoundtrip(ctx context.Context, dp dataplanepb.DataplaneAdminClient, token, targetUUID, path string, payload []byte, timeout time.Duration) error {
	if dp == nil {
		return fmt.Errorf("dataplane client unavailable")
	}
	targetUUID = strings.TrimSpace(targetUUID)
	if targetUUID == "" {
		return fmt.Errorf("empty target uuid")
	}
	path = strings.TrimSpace(path)
	if path == "" {
		return fmt.Errorf("empty path")
	}
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	hash := sha256Hex(payload)
	size := int64(len(payload))
	ttlSeconds := int64(120)

	uploadResp, err := dp.PrepareTransfer(withToken(ctx, token), &dataplanepb.PrepareTransferRequest{
		TargetUuid: targetUUID,
		Direction:  dataplanepb.Direction_DIRECTION_UPLOAD,
		Path:       path,
		SizeHint:   size,
		MaxRateBps: 0,
		MaxSize:    size,
		TtlSeconds: ttlSeconds,
		Metadata:   map[string]string{},
		Hash:       hash,
		Offset:     0,
	})
	if err != nil {
		return fmt.Errorf("PrepareTransfer(upload) failed: %v", err)
	}
	if uploadResp == nil || strings.TrimSpace(uploadResp.GetToken()) == "" || strings.TrimSpace(uploadResp.GetEndpoint()) == "" {
		return fmt.Errorf("PrepareTransfer(upload) returned empty token/endpoint")
	}
	if err := dataplaneUpload(ctx, uploadResp.GetEndpoint(), uploadResp.GetToken(), path, hash, size, payload, timeout); err != nil {
		return err
	}

	downloadResp, err := dp.PrepareTransfer(withToken(ctx, token), &dataplanepb.PrepareTransferRequest{
		TargetUuid: targetUUID,
		Direction:  dataplanepb.Direction_DIRECTION_DOWNLOAD,
		Path:       path,
		SizeHint:   size,
		MaxRateBps: 0,
		MaxSize:    size,
		TtlSeconds: ttlSeconds,
		Metadata:   map[string]string{},
		Hash:       hash,
		Offset:     0,
	})
	if err != nil {
		return fmt.Errorf("PrepareTransfer(download) failed: %v", err)
	}
	if downloadResp == nil || strings.TrimSpace(downloadResp.GetToken()) == "" || strings.TrimSpace(downloadResp.GetEndpoint()) == "" {
		return fmt.Errorf("PrepareTransfer(download) returned empty token/endpoint")
	}
	got, err := dataplaneDownload(ctx, downloadResp.GetEndpoint(), downloadResp.GetToken(), path, hash, size, timeout)
	if err != nil {
		return err
	}
	if !bytes.Equal(got, payload) {
		return fmt.Errorf("dataplane download mismatch: got %d bytes, expected %d", len(got), len(payload))
	}
	return nil
}

func runDataplaneUploadOnly(ctx context.Context, dp dataplanepb.DataplaneAdminClient, token, targetUUID, path string, payload []byte, timeout time.Duration, chunkDelay time.Duration) error {
	if dp == nil {
		return fmt.Errorf("dataplane client unavailable")
	}
	targetUUID = strings.TrimSpace(targetUUID)
	if targetUUID == "" {
		return fmt.Errorf("empty target uuid")
	}
	path = strings.TrimSpace(path)
	if path == "" {
		return fmt.Errorf("empty path")
	}
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	hash := sha256Hex(payload)
	size := int64(len(payload))
	ttlSeconds := int64(120)
	// 对于“仅上传”场景（dataplane_upload_interrupt 会用到），
	// 这里根据调用方超时来限制 token TTL，
	// 这样 Kelpie 就能缩短等待上游关闭的时间，并在客户端自身超时前先失败返回。
	if timeout > 0 {
		sec := int64(timeout.Seconds())
		if sec > 0 {
			ttlSeconds = sec - 10 // safety margin
			if ttlSeconds < 15 {
				ttlSeconds = 15
			}
			if ttlSeconds > 120 {
				ttlSeconds = 120
			}
		}
	}

	uploadResp, err := dp.PrepareTransfer(withToken(ctx, token), &dataplanepb.PrepareTransferRequest{
		TargetUuid: targetUUID,
		Direction:  dataplanepb.Direction_DIRECTION_UPLOAD,
		Path:       path,
		SizeHint:   size,
		MaxRateBps: 0,
		MaxSize:    size,
		TtlSeconds: ttlSeconds,
		Metadata:   map[string]string{},
		Hash:       hash,
		Offset:     0,
	})
	if err != nil {
		return fmt.Errorf("PrepareTransfer(upload) failed: %v", err)
	}
	if uploadResp == nil || strings.TrimSpace(uploadResp.GetToken()) == "" || strings.TrimSpace(uploadResp.GetEndpoint()) == "" {
		return fmt.Errorf("PrepareTransfer(upload) returned empty token/endpoint")
	}
	if err := dataplaneUploadWithDelay(ctx, uploadResp.GetEndpoint(), uploadResp.GetToken(), path, hash, size, payload, timeout, chunkDelay); err != nil {
		return err
	}
	return nil
}

// runDataplaneUploadInterrupt 会启动一次上传，并期望在 kill 掉 killCmd 后尽快失败。
// 只有当中断语义符合预期时，它才返回 nil。
func runDataplaneUploadInterrupt(
	ctx context.Context,
	dp dataplanepb.DataplaneAdminClient,
	token string,
	targetUUID string,
	path string,
	payloadBytes int,
	timeout time.Duration,
	chunkDelay time.Duration,
	killCmd *exec.Cmd,
	killAfter time.Duration,
) error {
	if dp == nil {
		return fmt.Errorf("dataplane client unavailable")
	}
	if killCmd == nil || killCmd.Process == nil {
		return fmt.Errorf("kill command unavailable")
	}
	if payloadBytes <= 0 {
		payloadBytes = 512 * 1024
	}
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	if killAfter <= 0 {
		killAfter = 400 * time.Millisecond
	}
	// 近似确定性的载荷：先写入唯一前缀，再补齐填充。
	payload := make([]byte, payloadBytes)
	prefix := fmt.Sprintf("DP_INTERRUPT target=%s ts=%d\n", strings.TrimSpace(targetUUID), time.Now().UnixNano())
	copy(payload, []byte(prefix))
	for i := len(prefix); i < len(payload); i++ {
		payload[i] = byte(i)
	}

	cctx := withToken(ctx, token)
	cctx, cancel := context.WithTimeout(cctx, timeout)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- runDataplaneUploadOnly(cctx, dp, token, targetUUID, path, payload, timeout, chunkDelay)
	}()

	timer := time.NewTimer(killAfter)
	select {
	case <-cctx.Done():
		timer.Stop()
		return fmt.Errorf("upload did not start before deadline: %v", cctx.Err())
	case <-timer.C:
	}
	_ = killCmd.Process.Kill()

	var err error
	select {
	case <-cctx.Done():
		// 当 context deadline 与 dataplane socket deadline 几乎同时到期时，
		// goroutine 可能会在 cctx.Done() 触发之后稍晚一点才写出最终错误。
		// 这里给一个很短的宽限窗口，避免把预期中的中断误判成卡死。
		grace := 1500 * time.Millisecond
		select {
		case err = <-errCh:
		case <-time.After(grace):
			return fmt.Errorf("upload timed out after kill (potential hang): %v", cctx.Err())
		}
	case err = <-errCh:
	}
	if err == nil {
		return fmt.Errorf("upload unexpectedly succeeded (interrupt did not take effect)")
	}
	lower := strings.ToLower(err.Error())
	if strings.Contains(lower, "deadline exceeded") || strings.Contains(lower, "i/o timeout") {
		return fmt.Errorf("upload failed by timeout (potential hang): %v", err)
	}
	return nil
}

// runDataplaneTokenReplay 先用一次性 token 成功上传一次，
// 然后尝试复用同一个 token，并期望服务端拒绝该请求。
func runDataplaneTokenReplay(
	ctx context.Context,
	dp dataplanepb.DataplaneAdminClient,
	token string,
	targetUUID string,
	path string,
	payloadBytes int,
	timeout time.Duration,
) error {
	if dp == nil {
		return fmt.Errorf("dataplane client unavailable")
	}
	targetUUID = strings.TrimSpace(targetUUID)
	if targetUUID == "" {
		return fmt.Errorf("empty target uuid")
	}
	path = strings.TrimSpace(path)
	if path == "" {
		return fmt.Errorf("empty path")
	}
	if payloadBytes <= 0 {
		payloadBytes = 1024
	}
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	// 近似确定性的载荷。
	payload := make([]byte, payloadBytes)
	prefix := fmt.Sprintf("DP_TOKEN_REPLAY target=%s ts=%d\n", targetUUID, time.Now().UnixNano())
	copy(payload, []byte(prefix))
	for i := len(prefix); i < len(payload); i++ {
		payload[i] = byte(i)
	}
	hash := sha256Hex(payload)
	size := int64(len(payload))
	ttlSeconds := int64(120)

	uploadResp, err := dp.PrepareTransfer(withToken(ctx, token), &dataplanepb.PrepareTransferRequest{
		TargetUuid: targetUUID,
		Direction:  dataplanepb.Direction_DIRECTION_UPLOAD,
		Path:       path,
		SizeHint:   size,
		MaxRateBps: 0,
		MaxSize:    size,
		TtlSeconds: ttlSeconds,
		Metadata:   map[string]string{},
		Hash:       hash,
		Offset:     0,
	})
	if err != nil {
		return fmt.Errorf("PrepareTransfer(upload) failed: %v", err)
	}
	if uploadResp == nil || strings.TrimSpace(uploadResp.GetToken()) == "" || strings.TrimSpace(uploadResp.GetEndpoint()) == "" {
		return fmt.Errorf("PrepareTransfer(upload) returned empty token/endpoint")
	}
	// 第一次尝试必须成功。
	if err := dataplaneUpload(ctx, uploadResp.GetEndpoint(), uploadResp.GetToken(), path, hash, size, payload, timeout); err != nil {
		return fmt.Errorf("initial upload failed: %v", err)
	}
	// 第二次复用同一 token 必须被拒绝。
	if err := dataplaneOpenExpectInvalidToken(ctx, uploadResp.GetEndpoint(), uploadResp.GetToken(), "upload", path, 0, size, hash, timeout); err != nil {
		return fmt.Errorf("token replay check failed: %v", err)
	}
	return nil
}

// runDataplaneTokenExpire 会签发一个短生命周期 token，等待其过期后，
// 再确认 dataplane 服务端会拒绝它。
func runDataplaneTokenExpire(
	ctx context.Context,
	dp dataplanepb.DataplaneAdminClient,
	token string,
	targetUUID string,
	path string,
	payloadBytes int,
	ttlSeconds int64,
	delay time.Duration,
	timeout time.Duration,
) error {
	if dp == nil {
		return fmt.Errorf("dataplane client unavailable")
	}
	targetUUID = strings.TrimSpace(targetUUID)
	if targetUUID == "" {
		return fmt.Errorf("empty target uuid")
	}
	path = strings.TrimSpace(path)
	if path == "" {
		return fmt.Errorf("empty path")
	}
	if payloadBytes <= 0 {
		payloadBytes = 256
	}
	if ttlSeconds <= 0 {
		ttlSeconds = 1
	}
	if delay <= 0 {
		delay = time.Duration(ttlSeconds+1) * time.Second
	}
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	payload := make([]byte, payloadBytes)
	prefix := fmt.Sprintf("DP_TOKEN_EXPIRE target=%s ts=%d\n", targetUUID, time.Now().UnixNano())
	copy(payload, []byte(prefix))
	for i := len(prefix); i < len(payload); i++ {
		payload[i] = byte(i)
	}
	hash := sha256Hex(payload)
	size := int64(len(payload))

	resp, err := dp.PrepareTransfer(withToken(ctx, token), &dataplanepb.PrepareTransferRequest{
		TargetUuid: targetUUID,
		Direction:  dataplanepb.Direction_DIRECTION_UPLOAD,
		Path:       path,
		SizeHint:   size,
		MaxRateBps: 0,
		MaxSize:    size,
		TtlSeconds: ttlSeconds,
		Metadata:   map[string]string{},
		Hash:       hash,
		Offset:     0,
	})
	if err != nil {
		return fmt.Errorf("PrepareTransfer(upload) failed: %v", err)
	}
	if resp == nil || strings.TrimSpace(resp.GetToken()) == "" || strings.TrimSpace(resp.GetEndpoint()) == "" {
		return fmt.Errorf("PrepareTransfer(upload) returned empty token/endpoint")
	}
	time.Sleep(delay)
	if err := dataplaneOpenExpectInvalidToken(ctx, resp.GetEndpoint(), resp.GetToken(), "upload", path, 0, size, hash, timeout); err != nil {
		return err
	}
	return nil
}

func dataplaneUpload(ctx context.Context, endpoint, token, path, hash string, sizeHint int64, data []byte, timeout time.Duration) error {
	return dataplaneUploadWithDelay(ctx, endpoint, token, path, hash, sizeHint, data, timeout, 0)
}

func dataplaneOpenExpectInvalidToken(ctx context.Context, endpoint, token, direction, path string, offset, sizeHint int64, hash string, timeout time.Duration) error {
	conn, err := dialDataplane(ctx, endpoint, timeout)
	if err != nil {
		return fmt.Errorf("dataplane dial failed: %v", err)
	}
	defer conn.Close()
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	_ = conn.SetDeadline(time.Now().Add(timeout))
	br := bufio.NewReader(conn)

	streamID := uint32(1)
	openPayload, err := buildDataplaneOpenPayload(token, direction, path, offset, sizeHint, hash)
	if err != nil {
		return err
	}
	if err := writeDataplaneFrame(conn, dpFrameTypeOpen, streamID, openPayload); err != nil {
		return fmt.Errorf("dataplane write(open) failed: %v", err)
	}

	for {
		ft, sid, payload, err := readDataplaneFrame(br)
		if err != nil {
			return fmt.Errorf("dataplane read failed: %v", err)
		}
		if sid != streamID {
			continue
		}
		if ft != dpFrameTypeClose {
			continue
		}
		code, reason := parseDataplaneClose(payload)
		if code == 0 {
			return fmt.Errorf("expected invalid token close, got code=0 reason=%q", reason)
		}
		if !strings.Contains(strings.ToLower(reason), "invalid token") {
			return fmt.Errorf("expected invalid token reason, got code=%d reason=%q", code, reason)
		}
		return nil
	}
}

func dataplaneUploadWithDelay(ctx context.Context, endpoint, token, path, hash string, sizeHint int64, data []byte, timeout time.Duration, chunkDelay time.Duration) error {
	conn, err := dialDataplane(ctx, endpoint, timeout)
	if err != nil {
		return fmt.Errorf("dataplane dial(upload) failed: %v", err)
	}
	defer conn.Close()
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	_ = conn.SetDeadline(time.Now().Add(timeout))
	br := bufio.NewReader(conn)

	streamID := uint32(1)
	openPayload, err := buildDataplaneOpenPayload(token, "upload", path, 0, sizeHint, hash)
	if err != nil {
		return err
	}
	if err := writeDataplaneFrame(conn, dpFrameTypeOpen, streamID, openPayload); err != nil {
		return fmt.Errorf("dataplane write(open) failed: %v", err)
	}

	const chunkSize = 32 * 1024
	for off := 0; off < len(data); off += chunkSize {
		end := off + chunkSize
		if end > len(data) {
			end = len(data)
		}
		if err := writeDataplaneFrame(conn, dpFrameTypeData, streamID, data[off:end]); err != nil {
			return fmt.Errorf("dataplane write(data) failed: %v", err)
		}
		if chunkDelay > 0 {
			time.Sleep(chunkDelay)
		}
	}
	if err := writeDataplaneFrame(conn, dpFrameTypeClose, streamID, nil); err != nil {
		return fmt.Errorf("dataplane write(close) failed: %v", err)
	}

	for {
		ft, sid, payload, err := readDataplaneFrame(br)
		if err != nil {
			return fmt.Errorf("dataplane read failed: %v", err)
		}
		if sid != streamID {
			continue
		}
		if ft != dpFrameTypeClose {
			continue
		}
		code, reason := parseDataplaneClose(payload)
		if code != 0 {
			return fmt.Errorf("dataplane upload closed with code=%d reason=%q", code, reason)
		}
		return nil
	}
}

func dataplaneDownload(ctx context.Context, endpoint, token, path, hash string, sizeHint int64, timeout time.Duration) ([]byte, error) {
	conn, err := dialDataplane(ctx, endpoint, timeout)
	if err != nil {
		return nil, fmt.Errorf("dataplane dial(download) failed: %v", err)
	}
	defer conn.Close()
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	_ = conn.SetDeadline(time.Now().Add(timeout))
	br := bufio.NewReader(conn)

	streamID := uint32(1)
	openPayload, err := buildDataplaneOpenPayload(token, "download", path, 0, sizeHint, hash)
	if err != nil {
		return nil, err
	}
	if err := writeDataplaneFrame(conn, dpFrameTypeOpen, streamID, openPayload); err != nil {
		return nil, fmt.Errorf("dataplane write(open) failed: %v", err)
	}

	var out []byte
	for {
		ft, sid, payload, err := readDataplaneFrame(br)
		if err != nil {
			return nil, fmt.Errorf("dataplane read failed: %v", err)
		}
		if sid != streamID {
			continue
		}
		switch ft {
		case dpFrameTypeData:
			out = append(out, payload...)
			if sizeHint > 0 && int64(len(out)) > sizeHint {
				return nil, fmt.Errorf("dataplane download exceeded size hint (%d > %d)", len(out), sizeHint)
			}
		case dpFrameTypeClose:
			code, reason := parseDataplaneClose(payload)
			if code != 0 {
				return nil, fmt.Errorf("dataplane download closed with code=%d reason=%q", code, reason)
			}
			return out, nil
		}
	}
}

func dialDataplane(ctx context.Context, endpoint string, timeout time.Duration) (net.Conn, error) {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return nil, fmt.Errorf("empty endpoint")
	}
	switch {
	case strings.HasPrefix(endpoint, "tcp://"):
		endpoint = strings.TrimPrefix(endpoint, "tcp://")
	case strings.HasPrefix(endpoint, "tcps://"):
		return nil, fmt.Errorf("tcps endpoint not supported in trace_replay")
	}
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	d := net.Dialer{Timeout: timeout}
	return d.DialContext(ctx, "tcp", endpoint)
}

func buildDataplaneOpenPayload(token, direction, path string, offset, sizeHint int64, hash string) ([]byte, error) {
	token = strings.TrimSpace(token)
	direction = strings.TrimSpace(direction)
	path = strings.TrimSpace(path)
	hash = strings.TrimSpace(hash)
	if token == "" {
		return nil, fmt.Errorf("empty token")
	}
	if direction == "" {
		return nil, fmt.Errorf("empty direction")
	}
	if path == "" {
		return nil, fmt.Errorf("empty path")
	}
	if offset < 0 {
		offset = 0
	}
	if sizeHint < 0 {
		sizeHint = 0
	}
	if hash != "" && len(hash) != 64 {
		return nil, fmt.Errorf("invalid sha256 hex length")
	}
	var buf bytes.Buffer
	if err := dpWriteString(&buf, token); err != nil {
		return nil, err
	}
	if err := dpWriteString(&buf, direction); err != nil {
		return nil, err
	}
	if err := dpWriteString(&buf, path); err != nil {
		return nil, err
	}
	dpWriteInt64(&buf, offset)
	dpWriteInt64(&buf, sizeHint)
	if err := dpWriteString(&buf, hash); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func dpWriteString(buf *bytes.Buffer, s string) error {
	if buf == nil {
		return fmt.Errorf("buffer nil")
	}
	b := []byte(s)
	if len(b) > 0xffff {
		return fmt.Errorf("string too long: %d", len(b))
	}
	var lenBuf [2]byte
	binary.BigEndian.PutUint16(lenBuf[:], uint16(len(b)))
	buf.Write(lenBuf[:])
	if len(b) > 0 {
		buf.Write(b)
	}
	return nil
}

func dpWriteInt64(buf *bytes.Buffer, v int64) {
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], uint64(v))
	buf.Write(tmp[:])
}

func writeDataplaneFrame(w io.Writer, ft byte, streamID uint32, payload []byte) error {
	plen := 0
	if payload != nil {
		plen = len(payload)
	}
	total := 6 + plen
	buf := make([]byte, 4+total)
	binary.BigEndian.PutUint32(buf[0:4], uint32(total))
	buf[4] = ft
	binary.BigEndian.PutUint32(buf[5:9], streamID)
	buf[9] = 0 // flags reserved
	if plen > 0 {
		copy(buf[10:], payload)
	}
	_, err := w.Write(buf)
	return err
}

func readDataplaneFrame(r *bufio.Reader) (ft byte, streamID uint32, payload []byte, err error) {
	var lenBuf [4]byte
	if _, err = io.ReadFull(r, lenBuf[:]); err != nil {
		return
	}
	total := binary.BigEndian.Uint32(lenBuf[:])
	if total < 6 {
		err = fmt.Errorf("frame too short")
		return
	}
	hdr := make([]byte, 6)
	if _, err = io.ReadFull(r, hdr); err != nil {
		return
	}
	ft = hdr[0]
	streamID = binary.BigEndian.Uint32(hdr[1:5])
	payloadLen := int(total) - 6
	if payloadLen > 0 {
		payload = make([]byte, payloadLen)
		if _, err = io.ReadFull(r, payload); err != nil {
			return
		}
	}
	return
}

func parseDataplaneClose(payload []byte) (uint16, string) {
	if len(payload) < 2 {
		return 1, "close payload too short"
	}
	code := binary.BigEndian.Uint16(payload[0:2])
	reason := ""
	if len(payload) > 2 {
		reason = string(payload[2:])
	}
	return code, reason
}

func runIOBurst(
	ctx context.Context,
	ui uipb.KelpieUIServiceClient,
	dp dataplanepb.DataplaneAdminClient,
	token string,
	targetUUID string,
	streamCount int,
	dataplaneCount int,
	payloadBytes int,
	timeout time.Duration,
) error {
	if ui == nil {
		return fmt.Errorf("ui client unavailable")
	}
	if dp == nil {
		return fmt.Errorf("dataplane client unavailable")
	}
	targetUUID = strings.TrimSpace(targetUUID)
	if targetUUID == "" {
		return fmt.Errorf("empty target uuid")
	}
	if streamCount < 0 {
		streamCount = 0
	}
	if dataplaneCount < 0 {
		dataplaneCount = 0
	}
	if streamCount == 0 && dataplaneCount == 0 {
		return nil
	}
	if payloadBytes <= 0 {
		payloadBytes = 64
	}
	if timeout <= 0 {
		timeout = 45 * time.Second
	}

	cctx := withToken(ctx, token)
	cctx, cancel := context.WithTimeout(cctx, timeout)
	defer cancel()

	var wg sync.WaitGroup
	errs := make(chan error, streamCount+dataplaneCount)
	nowID := time.Now().UnixNano()

	for i := 1; i <= dataplaneCount; i++ {
		i := i
		path := fmt.Sprintf("trace/io_burst_%d_dp_%02d.bin", nowID, i)
		payload := make([]byte, payloadBytes)
		prefix := fmt.Sprintf("IO_BURST_DP i=%d ts=%d\n", i, nowID)
		copy(payload, []byte(prefix))
		for j := len(prefix); j < len(payload); j++ {
			payload[j] = byte(i)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := runDataplaneRoundtrip(cctx, dp, token, targetUUID, path, payload, timeout); err != nil {
				errs <- fmt.Errorf("dataplane#%d: %w", i, err)
			}
		}()
	}

	for i := 1; i <= streamCount; i++ {
		i := i
		payload := fmt.Sprintf("IO_BURST_SP i=%d ts=%d\n", i, nowID)
		expect := payload
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := runStreamProxyRoundtrip(cctx, ui, token, targetUUID, []byte(payload), []byte(expect), timeout); err != nil {
				errs <- fmt.Errorf("stream#%d: %w", i, err)
			}
		}()
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// --- stream_proxy trace 辅助函数 ---

func runStreamProxyRoundtrip(ctx context.Context, ui uipb.KelpieUIServiceClient, token, targetUUID string, payload, expect []byte, timeout time.Duration) error {
	if ui == nil {
		return fmt.Errorf("ui client unavailable")
	}
	targetUUID = strings.TrimSpace(targetUUID)
	if targetUUID == "" {
		return fmt.Errorf("empty target uuid")
	}
	if timeout <= 0 {
		timeout = 20 * time.Second
	}
	echoAddr, stop, err := startLocalEcho()
	if err != nil {
		return err
	}
	defer stop()

	cctx := withToken(ctx, token)
	cctx, cancel := context.WithTimeout(cctx, timeout)
	defer cancel()

	stream, err := ui.ProxyStream(cctx)
	if err != nil {
		return fmt.Errorf("ProxyStream failed: %v", err)
	}

	openReq := &uipb.StreamRequest{
		TargetUuid: targetUUID,
		Options: map[string]string{
			"kind":   "proxy",
			"target": echoAddr,
		},
	}
	if err := stream.Send(openReq); err != nil {
		return fmt.Errorf("proxy stream send(open) failed: %v", err)
	}
	openResp, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("proxy stream recv(open) failed: %v", err)
	}
	if ctrl := openResp.GetControl(); ctrl == nil || ctrl.GetKind() != uipb.StreamControl_OPEN {
		return fmt.Errorf("proxy stream expected OPEN response, got: %v", openResp)
	}

	if len(payload) > 0 {
		if err := stream.Send(&uipb.StreamRequest{Data: payload}); err != nil {
			return fmt.Errorf("proxy stream send(data) failed: %v", err)
		}
	}

	var buf bytes.Buffer
	for buf.Len() < len(expect) {
		resp, err := stream.Recv()
		if err != nil {
			return fmt.Errorf("proxy stream recv(data) failed: %v", err)
		}
		if data := resp.GetData(); len(data) > 0 {
			buf.Write(data)
		}
		if ctrl := resp.GetControl(); ctrl != nil && ctrl.GetKind() == uipb.StreamControl_ERROR {
			return fmt.Errorf("proxy stream error: %s", strings.TrimSpace(ctrl.GetError()))
		}
		if ctrl := resp.GetControl(); ctrl != nil && ctrl.GetKind() == uipb.StreamControl_CLOSE {
			break
		}
	}
	got := buf.Bytes()
	if len(expect) > 0 && !bytes.Contains(got, expect) {
		return fmt.Errorf("proxy stream mismatch: expected %q in %q", string(expect), string(got))
	}

	_ = stream.Send(&uipb.StreamRequest{Control: &uipb.StreamControl{Kind: uipb.StreamControl_CLOSE}})
	_ = stream.CloseSend()
	return nil
}

func startLocalEcho() (addr string, stop func(), err error) {
	ln, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", "0"))
	if err != nil {
		return "", func() {}, err
	}
	addr = ln.Addr().String()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				_, _ = io.Copy(conn, conn)
			}(c)
		}
	}()
	stop = func() {
		_ = ln.Close()
		<-done
	}
	return addr, stop, nil
}

func loadTrace(path string) ([]traceEvent, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	events := make([]traceEvent, 0, 64)
	scan := bufio.NewScanner(f)
	for scan.Scan() {
		line := strings.TrimSpace(scan.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		var evt traceEvent
		if err := json.Unmarshal([]byte(line), &evt); err != nil {
			return nil, fmt.Errorf("parse trace line: %w", err)
		}
		events = append(events, evt)
	}
	if err := scan.Err(); err != nil {
		return nil, err
	}
	sort.Slice(events, func(i, j int) bool {
		return events[i].AtMS < events[j].AtMS
	})
	return events, nil
}

func parsePriority(raw string) uipb.DtnPriority {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "low":
		return uipb.DtnPriority_DTN_PRIORITY_LOW
	case "high":
		return uipb.DtnPriority_DTN_PRIORITY_HIGH
	default:
		return uipb.DtnPriority_DTN_PRIORITY_NORMAL
	}
}

func makeRunDir(base string) (string, error) {
	base = strings.TrimSpace(base)
	if base == "" {
		return "", fmt.Errorf("empty out dir")
	}
	// 使用更高分辨率的时间戳，避免多个 run 在同一秒内启动时发生命名冲突
	// （这种情况在脚本化实验里很常见）。
	ts := time.Now().UTC().Format("20060102T150405.000000000Z")
	ts = strings.ReplaceAll(ts, ".", "")
	runDir := filepath.Join(base, "run-"+ts)
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		return "", err
	}
	return runDir, nil
}

func mustPickFreeAddr(host string) string {
	addr, err := pickFreeAddr(host)
	if err != nil {
		fatalf("pick free addr: %v", err)
	}
	return addr
}

func pickFreeAddr(host string) (string, error) {
	if strings.TrimSpace(host) == "" {
		host = "127.0.0.1"
	}
	l, err := net.Listen("tcp", net.JoinHostPort(host, "0"))
	if err != nil {
		return "", err
	}
	defer l.Close()
	return l.Addr().String(), nil
}

func mustPickFreePort(host string) int {
	addr := mustPickFreeAddr(host)
	_, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		fatalf("pick free port: %v", err)
	}
	p, err := strconv.Atoi(portStr)
	if err != nil || p <= 0 {
		fatalf("pick free port: invalid port %q", portStr)
	}
	return p
}

func ensureExecutable(path string) {
	st, err := os.Stat(path)
	if err != nil {
		fatalf("missing binary: %s (%v)", path, err)
	}
	if st.IsDir() {
		fatalf("binary path is a directory: %s", path)
	}
}

func mustMkdirAll(path string) {
	if err := os.MkdirAll(path, 0o755); err != nil {
		fatalf("mkdir %s: %v", path, err)
	}
}

func mustCreate(path string) *os.File {
	f, err := os.Create(path)
	if err != nil {
		fatalf("create %s: %v", path, err)
	}
	return f
}

func writeJSON(path string, v any) {
	raw, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return
	}
	_ = os.WriteFile(path, raw, 0o644)
}

func copyStringMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(2)
}
