package planner

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"codeberg.org/agnoie/shepherd/internal/kelpie/manager"
	"codeberg.org/agnoie/shepherd/internal/kelpie/printer"
	"codeberg.org/agnoie/shepherd/internal/kelpie/topology"
	"codeberg.org/agnoie/shepherd/pkg/global"
	"codeberg.org/agnoie/shepherd/pkg/session"
	"codeberg.org/agnoie/shepherd/protocol"
)

const (
	rescueQueueSize     = 32
	rescueDispatchDelay = 100 * time.Millisecond
	rescueWaitTimeout   = 15 * time.Second
)

type rescueTask struct {
	target string
	meta   *topology.Result
	result chan bool
}

// RescueCoordinator 负责协同节点级救援请求。
type RescueCoordinator struct {
	topo    *topology.Topology
	service *topology.Service
	store   *global.Store
	mgr     *manager.Manager

	queue  chan rescueTask
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	startOnce      sync.Once
	pendingMu      sync.Mutex
	pendingRes     map[string]chan *protocol.RescueResponse
	candidateGuard func(target, rescuer string) bool
}

// NewRescueCoordinator 构建实例；拓扑为空时返回 nil。
func NewRescueCoordinator(topo *topology.Topology, svc *topology.Service, mgr *manager.Manager, store *global.Store) *RescueCoordinator {
	if topo == nil {
		return nil
	}
	return &RescueCoordinator{
		topo:       topo,
		service:    svc,
		store:      store,
		mgr:        mgr,
		queue:      make(chan rescueTask, rescueQueueSize),
		pendingRes: make(map[string]chan *protocol.RescueResponse),
	}
}

// Start 启动后台消费协程。
func (c *RescueCoordinator) Start(ctx context.Context) {
	if c == nil {
		return
	}
	c.startOnce.Do(func() {
		if ctx == nil {
			ctx = context.Background()
		}
		c.ctx, c.cancel = context.WithCancel(ctx)
		c.wg.Add(1)
		go c.run()
	})
}

// Stop 停止后台协程。
func (c *RescueCoordinator) Stop() {
	if c == nil {
		return
	}
	if c.cancel != nil {
		c.cancel()
	}
	c.wg.Wait()
}

// Schedule 发起救援任务；返回 true 表示救援成功。
func (c *RescueCoordinator) Schedule(uuid string, meta *topology.Result) bool {
	if c == nil || uuid == "" {
		return false
	}
	task := rescueTask{
		target: uuid,
		meta:   meta,
		result: make(chan bool, 1),
	}
	if c.ctx == nil {
		// 协调器尚未启动时，直接同步处理当前任务。
		return c.processTask(task)
	}
	select {
	case c.queue <- task:
	default:
		printer.Warning("\r\n[*] Rescue queue full, process inline for %s\r\n", shorten(uuid))
		return c.processTask(task)
	}

	done := c.ctx.Done()
	select {
	case handled := <-task.result:
		return handled
	case <-time.After(rescueWaitTimeout):
		return false
	case <-done:
		return false
	}
}

// HandleResponse 由上层在收到 RESCUE_RESPONSE 时调用。
func (c *RescueCoordinator) HandleResponse(resp *protocol.RescueResponse) {
	if c == nil || resp == nil {
		return
	}
	c.pendingMu.Lock()
	ch, ok := c.pendingRes[resp.TargetUUID]
	if ok {
		delete(c.pendingRes, resp.TargetUUID)
	}
	c.pendingMu.Unlock()
	if !ok || ch == nil {
		return
	}
	select {
	case ch <- resp:
	default:
	}
}

func (c *RescueCoordinator) run() {
	defer c.wg.Done()
	if c.ctx == nil {
		return
	}
	for {
		select {
		case <-c.ctx.Done():
			return
		case task := <-c.queue:
			result := c.processTask(task)
			select {
			case task.result <- result:
			default:
			}
		}
	}
}

// SetCandidateGuard 设置候选节点过滤函数，用于复用补链策略。
func (c *RescueCoordinator) SetCandidateGuard(fn func(target, rescuer string) bool) {
	if c == nil {
		return
	}
	c.candidateGuard = fn
}

func (c *RescueCoordinator) processTask(task rescueTask) bool {
	candidates := c.pickCandidates(task.target, task.meta)
	if len(candidates) == 0 {
		return false
	}
	meta := task.meta
	if meta == nil {
		var err error
		meta, err = c.requestConnInfo(task.target)
		if err != nil {
			return false
		}
	}
	if !c.validDialTarget(meta) {
		printer.Warning("\r\n[*] Rescue skipped for %s: missing dial endpoint\r\n", shorten(task.target))
		return false
	}

	done := (<-chan struct{})(nil)
	if c.ctx != nil {
		done = c.ctx.Done()
	}

	for _, rescuer := range candidates {
		respCh := make(chan *protocol.RescueResponse, 1)
		if !c.registerPending(task.target, respCh) {
			continue
		}

		err := c.dispatchRequest(rescuer, task.target, meta)
		if err != nil {
			c.unregisterPending(task.target)
			printer.Warning("\r\n[*] Rescue dispatch to %s failed: %v\r\n", shorten(rescuer), err)
			continue
		}

		select {
		case resp := <-respCh:
			c.unregisterPending(task.target)
			if resp == nil {
				continue
			}
			if resp.Status != 1 {
				msg := resp.Message
				if msg == "" {
					msg = "unknown error"
				}
				printer.Warning("\r\n[*] Rescue via %s rejected: %s\r\n", shorten(rescuer), msg)
				continue
			}
			if err := c.applyRescueResult(resp); err != nil {
				printer.Warning("\r\n[*] Rescue via %s failed to apply: %v\r\n", shorten(rescuer), err)
				continue
			}
			printer.Success("\r\n[*] Rescue succeeded: %s <= %s\r\n", shorten(resp.ChildUUID), shorten(resp.ParentUUID))
			return true
		case <-time.After(rescueWaitTimeout):
			c.unregisterPending(task.target)
			printer.Warning("\r\n[*] Rescue via %s timed out\r\n", shorten(rescuer))
			continue
		case <-done:
			c.unregisterPending(task.target)
			return false
		}
	}
	return false
}

func (c *RescueCoordinator) pickCandidates(target string, meta *topology.Result) []string {
	if c.topo == nil {
		return nil
	}
	targetNet := strings.TrimSpace(c.topo.NetworkFor(target))
	candidates := make([]string, 0, 4)
	seen := make(map[string]struct{})

	addCandidate := func(uuid string) {
		if uuid == "" || uuid == target || uuid == protocol.ADMIN_UUID {
			return
		}
		if _, ok := seen[uuid]; ok {
			return
		}
		if targetNet != "" {
			netID := strings.TrimSpace(c.topo.NetworkFor(uuid))
			if netID != "" && !strings.EqualFold(netID, targetNet) {
				return
			}
		}
		if c.candidateGuard != nil && !c.candidateGuard(target, uuid) {
			return
		}
		if sess := c.sessionFor(uuid); sess == nil || sess.Conn() == nil {
			return
		}
		seen[uuid] = struct{}{}
		candidates = append(candidates, uuid)
	}

	if meta != nil {
		addCandidate(meta.Parent)
	}

	info := meta
	if info == nil {
		if res, err := c.requestNodeInfo(target); err == nil && res != nil {
			info = res
		}
	}
	if info != nil {
		addCandidate(info.Parent)
	}

	for _, neighbor := range c.fetchNeighbors(target) {
		addCandidate(neighbor)
	}

	if targetNet != "" {
		for _, entry := range c.topo.NetworkEntries(targetNet) {
			addCandidate(entry)
		}
	}

	// 优先选择稳定的（常在线）候选节点，以减少 sleep/DTN trace 中的抖动：
	// 一个“爱睡觉”的父节点即便会话仍活着，也可能正处在很窄的 work 窗口里，
	// 从而在超时前来不及完成 rescue。
	sort.SliceStable(candidates, func(i, j int) bool {
		a := candidates[i]
		b := candidates[j]
		ar, aok := c.topo.NodeRuntime(a)
		br, bok := c.topo.NodeRuntime(b)
		asleep := 1 << 30
		awork := 0
		alast := time.Time{}
		if aok {
			asleep = ar.SleepSeconds
			awork = ar.WorkSeconds
			alast = ar.LastSeen
		}
		bsleep := 1 << 30
		bwork := 0
		blast := time.Time{}
		if bok {
			bsleep = br.SleepSeconds
			bwork = br.WorkSeconds
			blast = br.LastSeen
		}
		if asleep != bsleep {
			return asleep < bsleep
		}
		if awork != bwork {
			return awork > bwork
		}
		if !alast.Equal(blast) {
			return alast.After(blast)
		}
		return a < b
	})

	return candidates
}

func (c *RescueCoordinator) dispatchRequest(rescuer, target string, meta *topology.Result) error {
	if c.mgr == nil {
		return errors.New("rescue: manager unavailable")
	}
	sess := c.sessionFor(rescuer)
	if sess == nil || sess.Conn() == nil {
		return fmt.Errorf("rescue: no active session for %s", rescuer)
	}

	route, err := c.fetchRoute(rescuer)
	if err != nil {
		return err
	}

	msg := protocol.NewDownMsg(sess.Conn(), sess.Secret(), sess.UUID())
	protocol.SetMessageMeta(msg, sess.ProtocolFlags())

	dialAddr := meta.DialAddress
	if dialAddr == "" {
		dialAddr = meta.IP
	}
	token := c.preAuthToken()
	req := &protocol.RescueRequest{
		TargetUUIDLen: uint16(len(target)),
		TargetUUID:    target,
		DialAddrLen:   uint16(len(dialAddr)),
		DialAddr:      dialAddr,
		DialPort:      uint16(clampPort(meta.FallbackPort, meta.Port)),
		TransportLen:  uint16(len(meta.Transport)),
		Transport:     meta.Transport,
		SecretLen:     uint16(len(token)),
		Secret:        token,
	}
	if meta.TLSEnabled {
		req.Flags |= protocol.RescueFlagRequireTLS
	}

	header := &protocol.Header{
		Sender:      sess.UUID(),
		Accepter:    protocol.TEMP_UUID,
		MessageType: uint16(protocol.RESCUE_REQUEST),
		RouteLen:    uint32(len(route)),
		Route:       route,
	}

	protocol.ConstructMessage(msg, header, req, false)
	msg.SendMessage()
	time.Sleep(rescueDispatchDelay)
	return nil
}

func (c *RescueCoordinator) applyRescueResult(resp *protocol.RescueResponse) error {
	if resp.ParentUUID == "" || resp.ChildUUID == "" {
		return fmt.Errorf("rescue: invalid response payload")
	}
	if c.service == nil {
		return fmt.Errorf("rescue: topology service unavailable")
	}
	ctx, cancel := context.WithTimeout(context.Background(), rescueWaitTimeout)
	defer cancel()
	_, err := c.service.Request(ctx, &topology.TopoTask{
		Mode:       topology.REPARENTNODE,
		UUID:       resp.ChildUUID,
		ParentUUID: resp.ParentUUID,
	})
	if err != nil {
		return err
	}
	// Rescue reparent 必须能够立刻路由成功，后续控制流量才能跟上。
	_, err = c.service.Request(ctx, &topology.TopoTask{Mode: topology.CALCULATE})
	return err
}

func (c *RescueCoordinator) requestConnInfo(uuid string) (*topology.Result, error) {
	if c.service == nil {
		return nil, fmt.Errorf("rescue: topology service unavailable")
	}
	ctx, cancel := context.WithTimeout(context.Background(), rescueWaitTimeout)
	defer cancel()
	return c.service.Request(ctx, &topology.TopoTask{Mode: topology.GETCONNINFO, UUID: uuid})
}

func (c *RescueCoordinator) fetchRoute(uuid string) (string, error) {
	if c.service == nil {
		return "", fmt.Errorf("rescue: topology service unavailable")
	}
	ctx, cancel := context.WithTimeout(context.Background(), rescueWaitTimeout)
	defer cancel()
	res, err := c.service.Request(ctx, &topology.TopoTask{Mode: topology.GETROUTE, UUID: uuid})
	if err != nil || res == nil {
		return "", fmt.Errorf("rescue: route lookup failed for %s", uuid)
	}
	return res.Route, nil
}

func (c *RescueCoordinator) requestNodeInfo(uuid string) (*topology.Result, error) {
	if c.service == nil {
		return nil, fmt.Errorf("rescue: topology service unavailable")
	}
	ctx, cancel := context.WithTimeout(context.Background(), rescueWaitTimeout)
	defer cancel()
	return c.service.Request(ctx, &topology.TopoTask{Mode: topology.GETNODEINFO, UUID: uuid})
}

func (c *RescueCoordinator) fetchNeighbors(uuid string) []string {
	if c.service == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), rescueWaitTimeout)
	defer cancel()
	res, err := c.service.Request(ctx, &topology.TopoTask{Mode: topology.GETNEIGHBORS, UUID: uuid})
	if err != nil || res == nil {
		return nil
	}
	return res.Neighbors
}

func (c *RescueCoordinator) registerPending(target string, ch chan *protocol.RescueResponse) bool {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	if _, exists := c.pendingRes[target]; exists {
		return false
	}
	c.pendingRes[target] = ch
	return true
}

func (c *RescueCoordinator) unregisterPending(target string) {
	c.pendingMu.Lock()
	delete(c.pendingRes, target)
	c.pendingMu.Unlock()
}

func (c *RescueCoordinator) preAuthToken() string {
	if c.store == nil {
		return ""
	}
	defer func() {
		if r := recover(); r != nil {
		}
	}()
	return c.store.PreAuthToken()
}

func (c *RescueCoordinator) sessionFor(uuid string) session.Session {
	if c.mgr == nil {
		return nil
	}
	if sess := c.mgr.SessionForTarget(uuid); sess != nil && sess.Conn() != nil {
		return sess
	}
	if sess := c.mgr.ActiveSession(); sess != nil && sess.Conn() != nil {
		return sess
	}
	return nil
}

func clampPort(primary, fallback int) int {
	port := primary
	if port <= 0 {
		port = fallback
	}
	if port < 0 {
		port = 0
	}
	if port > 65535 {
		return 65535
	}
	return port
}

func shorten(id string) string {
	if len(id) <= 8 {
		return id
	}
	return id[:8]
}

func (c *RescueCoordinator) validDialTarget(meta *topology.Result) bool {
	if meta == nil {
		return false
	}
	addr := strings.TrimSpace(meta.DialAddress)
	if addr == "" {
		addr = strings.TrimSpace(meta.IP)
	}
	if addr == "" {
		return false
	}
	port := clampPort(meta.FallbackPort, meta.Port)
	return port > 0
}
