package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	uipb "codeberg.org/agnoie/shepherd/internal/kelpie/uipb"
	"codeberg.org/agnoie/shepherd/protocol"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

const (
	defaultGRPCAddr        = "kelpie:50061"
	defaultControllerBind  = "0.0.0.0:40100"
	defaultPivotBind       = "0.0.0.0:41000"
	defaultReconnectSecond = 2
)

func main() {
	addr := envString("KELPIE_GRPC_ADDR", defaultGRPCAddr)
	token := strings.TrimSpace(os.Getenv("KELPIE_GRPC_TOKEN"))
	if token == "" {
		fatalf("KELPIE_GRPC_TOKEN is required (Kelpie UI token)")
	}

	controllerBind := envString("KELPIE_CONTROLLER_BIND", defaultControllerBind)
	pivotBinds := envCSV("PIVOT_BINDS")
	if len(pivotBinds) == 0 {
		pivotBind := envString("ROOT_PIVOT_BIND", defaultPivotBind)
		if pivotBind != "" {
			pivotBinds = []string{pivotBind}
		}
	}
	expectedOnlineNodes := envInt("EXPECTED_ONLINE_NODES", 0)

	timeout := envDuration("BOOTSTRAP_TIMEOUT", 5*time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	conn, err := dialWithRetry(ctx, addr, 250*time.Millisecond, 2*time.Second)
	if err != nil {
		fatalf("dial kelpie grpc %s: %v", addr, err)
	}
	defer conn.Close()

	rpcCtx := metadata.AppendToOutgoingContext(ctx, "x-kelpie-token", token)

	if err := ensureControllerListener(rpcCtx, conn, controllerBind); err != nil {
		fatalf("ensure controller listener: %v", err)
	}

	rootUUID, err := waitForRootNode(rpcCtx, conn, 200*time.Millisecond, 2*time.Second)
	if err != nil {
		fatalf("wait for root node: %v", err)
	}

	known := map[string]struct{}{
		rootUUID: {},
	}
	currentParent := rootUUID
	for idx, bind := range pivotBinds {
		if err := ensurePivotListener(rpcCtx, conn, currentParent, bind); err != nil {
			fatalf("ensure pivot listener for %s on %s: %v", currentParent, bind, err)
		}
		fmt.Printf("pivot_ok index=%d target_uuid=%s bind=%s\n", idx, currentParent, bind)
		if idx == len(pivotBinds)-1 {
			break
		}
		childUUID, err := waitForDirectChildNode(rpcCtx, conn, currentParent, known, 200*time.Millisecond, 2*time.Second)
		if err != nil {
			fatalf("wait for child node of %s: %v", currentParent, err)
		}
		known[childUUID] = struct{}{}
		currentParent = childUUID
	}

	onlineNodes := []string{rootUUID}
	if expectedOnlineNodes > 0 {
		onlineNodes, err = waitForOnlineNodes(rpcCtx, conn, expectedOnlineNodes, 200*time.Millisecond, 2*time.Second)
		if err != nil {
			fatalf("wait for %d online nodes: %v", expectedOnlineNodes, err)
		}
	}

	fmt.Printf("bootstrap_ok root_uuid=%s controller_bind=%s pivot_binds=%s online_nodes=%s\n",
		rootUUID,
		controllerBind,
		strings.Join(pivotBinds, ","),
		strings.Join(onlineNodes, ","))
}

func ensureControllerListener(ctx context.Context, conn *grpc.ClientConn, bind string) error {
	client := uipb.NewControllerListenerAdminServiceClient(conn)

	// 尽力而为：如果该 bind 上已经存在且未停止的 listener，就直接复用。
	current, err := client.ListControllerListeners(ctx, &uipb.ListControllerListenersRequest{})
	if err != nil {
		return err
	}
	for _, lis := range current.GetListeners() {
		if strings.TrimSpace(lis.GetBind()) == strings.TrimSpace(bind) && lis.GetStatus() != uipb.ControllerListenerStatus_CONTROLLER_LISTENER_STATUS_STOPPED {
			return nil
		}
	}

	_, err = client.CreateControllerListener(ctx, &uipb.CreateControllerListenerRequest{
		Spec: &uipb.ControllerListenerSpec{
			Bind:     bind,
			Protocol: "tcp",
		},
	})
	return err
}

func waitForRootNode(ctx context.Context, conn *grpc.ClientConn, baseDelay, maxDelay time.Duration) (string, error) {
	client := uipb.NewKelpieUIServiceClient(conn)
	delay := baseDelay
	for {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		default:
		}

		resp, err := client.GetSnapshot(ctx, &uipb.SnapshotRequest{})
		if err == nil && resp != nil && resp.GetSnapshot() != nil {
			for _, node := range resp.GetSnapshot().GetNodes() {
				if strings.TrimSpace(node.GetUuid()) == "" {
					continue
				}
				// 当 Kelpie 使用持久化 SQLite 存储时，旧的根节点会以离线记录的形式保留在快照中。
				// 只有 ONLINE 的根节点才应被视为有效的 bootstrap 目标。
				if strings.TrimSpace(node.GetParentUuid()) == protocol.ADMIN_UUID &&
					strings.EqualFold(strings.TrimSpace(node.GetStatus()), "online") {
					return node.GetUuid(), nil
				}
			}
		}

		time.Sleep(delay)
		delay *= 2
		if delay > maxDelay {
			delay = maxDelay
		}
	}
}

func waitForDirectChildNode(ctx context.Context, conn *grpc.ClientConn, parentUUID string, known map[string]struct{}, baseDelay, maxDelay time.Duration) (string, error) {
	client := uipb.NewKelpieUIServiceClient(conn)
	delay := baseDelay
	for {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		default:
		}

		resp, err := client.GetSnapshot(ctx, &uipb.SnapshotRequest{})
		if err == nil && resp != nil && resp.GetSnapshot() != nil {
			for _, node := range resp.GetSnapshot().GetNodes() {
				uuid := strings.TrimSpace(node.GetUuid())
				if uuid == "" {
					continue
				}
				if _, exists := known[uuid]; exists {
					continue
				}
				if strings.TrimSpace(node.GetParentUuid()) != parentUUID {
					continue
				}
				if !strings.EqualFold(strings.TrimSpace(node.GetStatus()), "online") {
					continue
				}
				return uuid, nil
			}
		}

		time.Sleep(delay)
		delay *= 2
		if delay > maxDelay {
			delay = maxDelay
		}
	}
}

func waitForOnlineNodes(ctx context.Context, conn *grpc.ClientConn, expected int, baseDelay, maxDelay time.Duration) ([]string, error) {
	client := uipb.NewKelpieUIServiceClient(conn)
	delay := baseDelay
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		resp, err := client.GetSnapshot(ctx, &uipb.SnapshotRequest{})
		if err == nil && resp != nil && resp.GetSnapshot() != nil {
			nodes := make([]string, 0, expected)
			for _, node := range resp.GetSnapshot().GetNodes() {
				uuid := strings.TrimSpace(node.GetUuid())
				if uuid == "" {
					continue
				}
				if !strings.EqualFold(strings.TrimSpace(node.GetStatus()), "online") {
					continue
				}
				nodes = append(nodes, uuid)
			}
			if len(nodes) >= expected {
				return nodes, nil
			}
		}

		time.Sleep(delay)
		delay *= 2
		if delay > maxDelay {
			delay = maxDelay
		}
	}
}

func ensurePivotListener(ctx context.Context, conn *grpc.ClientConn, targetUUID, bind string) error {
	client := uipb.NewPivotListenerAdminServiceClient(conn)

	existing, err := client.ListPivotListeners(ctx, &uipb.ListPivotListenersRequest{
		TargetUuids: []string{targetUUID},
	})
	if err != nil {
		return err
	}
	for _, lis := range existing.GetListeners() {
		if strings.TrimSpace(lis.GetBind()) == strings.TrimSpace(bind) && strings.TrimSpace(lis.GetTargetUuid()) == strings.TrimSpace(targetUUID) {
			// 如果它已经存在，则尽力确保它处于运行状态。
			if strings.EqualFold(strings.TrimSpace(lis.GetStatus()), "running") {
				return nil
			}
			_, err := client.UpdatePivotListener(ctx, &uipb.UpdatePivotListenerRequest{
				ListenerId:    lis.GetListenerId(),
				DesiredStatus: "restart",
			})
			return err
		}
	}

	_, err = client.CreatePivotListener(ctx, &uipb.CreatePivotListenerRequest{
		TargetUuid: targetUUID,
		Spec: &uipb.PivotListenerSpec{
			Protocol: "tcp",
			Bind:     bind,
			Mode:     uipb.PivotListenerMode_PIVOT_LISTENER_MODE_NORMAL,
		},
	})
	return err
}

func dialWithRetry(ctx context.Context, addr string, baseDelay, maxDelay time.Duration) (*grpc.ClientConn, error) {
	delay := baseDelay
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		dialCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			err = waitForGRPCReady(dialCtx, conn)
		}
		cancel()
		if err == nil {
			return conn, nil
		}
		if conn != nil {
			conn.Close()
		}

		if errors.Is(ctx.Err(), context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return nil, ctx.Err()
		}

		time.Sleep(delay)
		delay *= 2
		if delay > maxDelay {
			delay = maxDelay
		}
	}
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

func envString(key, def string) string {
	val := strings.TrimSpace(os.Getenv(key))
	if val == "" {
		return def
	}
	return val
}

func envCSV(key string) []string {
	val := strings.TrimSpace(os.Getenv(key))
	if val == "" {
		return nil
	}
	parts := strings.Split(val, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func envInt(key string, def int) int {
	val := strings.TrimSpace(os.Getenv(key))
	if val == "" {
		return def
	}
	n, err := strconv.Atoi(val)
	if err != nil {
		return def
	}
	return n
}

func envDuration(key string, def time.Duration) time.Duration {
	val := strings.TrimSpace(os.Getenv(key))
	if val == "" {
		return def
	}
	// 为了方便 compose 配置，同时接受 "300s" 和纯秒数写法。
	if d, err := time.ParseDuration(val); err == nil && d > 0 {
		return d
	}
	if secs, err := strconv.Atoi(val); err == nil && secs > 0 {
		return time.Duration(secs) * time.Second
	}
	return def
}

func fatalf(format string, args ...any) {
	_, _ = fmt.Fprintf(os.Stderr, "shepherd-lab-bootstrap: "+format+"\n", args...)
	os.Exit(2)
}
