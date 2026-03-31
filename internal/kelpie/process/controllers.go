package process

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"codeberg.org/agnoie/shepherd/internal/kelpie/initial"
	"codeberg.org/agnoie/shepherd/internal/kelpie/printer"
	"codeberg.org/agnoie/shepherd/internal/kelpie/supp"
	"codeberg.org/agnoie/shepherd/internal/kelpie/topology"
	"codeberg.org/agnoie/shepherd/pkg/share"
	"codeberg.org/agnoie/shepherd/pkg/share/handshake"
	"codeberg.org/agnoie/shepherd/pkg/share/transport"
	"codeberg.org/agnoie/shepherd/pkg/utils"
	"codeberg.org/agnoie/shepherd/pkg/utils/runtimeerr"
	"codeberg.org/agnoie/shepherd/protocol"
)

// ControllerListenerProtocol 描述 Kelpie controller listener 的绑定协议。
type ControllerListenerProtocol string

const (
	ControllerListenerProtocolTCP ControllerListenerProtocol = "tcp"
)

// ControllerListenerStatus 表示生命周期状态。
type ControllerListenerStatus string

const (
	ControllerListenerPending ControllerListenerStatus = "pending"
	ControllerListenerRunning ControllerListenerStatus = "running"
	ControllerListenerFailed  ControllerListenerStatus = "failed"
	ControllerListenerStopped ControllerListenerStatus = "stopped"
)

// ControllerListenerSpec 表示用户提供的创建或更新参数。
type ControllerListenerSpec struct {
	Bind     string
	Protocol ControllerListenerProtocol
}

// ControllerListenerRecord 存储 controller listener 的运行时状态。
type ControllerListenerRecord struct {
	ID        string
	Bind      string
	Protocol  ControllerListenerProtocol
	Status    ControllerListenerStatus
	LastError string
	CreatedAt time.Time
	UpdatedAt time.Time
}

// ControllerListenerPersistence 负责持久化 controller listener 记录。
type ControllerListenerPersistence interface {
	SaveControllerListener(rec ControllerListenerRecord) error
	DeleteControllerListener(id string) error
	LoadControllerListeners() ([]ControllerListenerRecord, error)
}

type controllerListenerRegistry struct {
	mu      sync.RWMutex
	records map[string]ControllerListenerRecord
	persist ControllerListenerPersistence
}

func (admin *Admin) registerControllerListenerCancel(id string, cancel context.CancelFunc) {
	if admin == nil || id == "" || cancel == nil {
		return
	}
	admin.controllerListenerRunMu.Lock()
	if admin.controllerListenerRuns == nil {
		admin.controllerListenerRuns = make(map[string]context.CancelFunc)
	}
	admin.controllerListenerRuns[id] = cancel
	admin.controllerListenerRunMu.Unlock()
}

func (admin *Admin) unregisterControllerListenerCancel(id string) {
	if admin == nil || id == "" {
		return
	}
	admin.controllerListenerRunMu.Lock()
	delete(admin.controllerListenerRuns, id)
	admin.controllerListenerRunMu.Unlock()
}

func (admin *Admin) cancelControllerListener(id string) {
	if admin == nil || id == "" {
		return
	}
	admin.controllerListenerRunMu.Lock()
	cancel := admin.controllerListenerRuns[id]
	delete(admin.controllerListenerRuns, id)
	admin.controllerListenerRunMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func newControllerListenerRegistry(persist ControllerListenerPersistence, seed []ControllerListenerRecord) *controllerListenerRegistry {
	recMap := make(map[string]ControllerListenerRecord)
	for _, rec := range seed {
		recMap[rec.ID] = rec
	}
	return &controllerListenerRegistry{records: recMap, persist: persist}
}

func (r *controllerListenerRegistry) list() []ControllerListenerRecord {
	r.mu.RLock()
	defer r.mu.RUnlock()
	items := make([]ControllerListenerRecord, 0, len(r.records))
	for _, rec := range r.records {
		items = append(items, rec)
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].CreatedAt.Before(items[j].CreatedAt)
	})
	return items
}

func (r *controllerListenerRegistry) get(id string) (ControllerListenerRecord, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	rec, ok := r.records[id]
	return rec, ok
}

func (r *controllerListenerRegistry) save(rec ControllerListenerRecord) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records[rec.ID] = rec
	if r.persist != nil {
		if err := r.persist.SaveControllerListener(rec); err != nil {
			return err
		}
	}
	return nil
}

func (r *controllerListenerRegistry) delete(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.records, id)
	if r.persist != nil {
		if err := r.persist.DeleteControllerListener(id); err != nil {
			return err
		}
	}
	return nil
}

func validateControllerListenerSpec(spec ControllerListenerSpec) error {
	if strings.TrimSpace(spec.Bind) == "" {
		return fmt.Errorf("listener bind missing")
	}
	if _, _, err := net.SplitHostPort(spec.Bind); err != nil {
		return fmt.Errorf("invalid bind %s: %w", spec.Bind, err)
	}
	if spec.Protocol == "" {
		spec.Protocol = ControllerListenerProtocolTCP
	}
	switch spec.Protocol {
	case ControllerListenerProtocolTCP:
	default:
		return fmt.Errorf("unsupported protocol %s", spec.Protocol)
	}
	return nil
}

// ListControllerListeners 返回所有 controller listener。
func (admin *Admin) ListControllerListeners() []ControllerListenerRecord {
	if admin == nil || admin.controllerListeners == nil {
		return nil
	}
	return admin.controllerListeners.list()
}

// CreateControllerListener 注册一个新的 Kelpie controller listener。
func (admin *Admin) CreateControllerListener(spec ControllerListenerSpec) (ControllerListenerRecord, error) {
	var empty ControllerListenerRecord
	if admin == nil {
		return empty, fmt.Errorf("admin unavailable")
	}
	if err := validateControllerListenerSpec(spec); err != nil {
		return empty, err
	}
	id := fmt.Sprintf("ctrl-%s", utils.GenerateUUID())
	now := time.Now()
	rec := ControllerListenerRecord{
		ID:        id,
		Bind:      spec.Bind,
		Protocol:  spec.Protocol,
		Status:    ControllerListenerPending,
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := admin.controllerListeners.save(rec); err != nil {
		return empty, err
	}
	return rec, nil
}

// UpdateControllerListener 更新 bind/protocol 或期望状态。
func (admin *Admin) UpdateControllerListener(id string, spec *ControllerListenerSpec, desired ControllerListenerStatus) (ControllerListenerRecord, error) {
	var empty ControllerListenerRecord
	if admin == nil || admin.controllerListeners == nil {
		return empty, fmt.Errorf("admin unavailable")
	}
	current, ok := admin.controllerListeners.get(id)
	if !ok {
		return empty, fmt.Errorf("controller listener %s not found", id)
	}
	if spec != nil {
		if err := validateControllerListenerSpec(*spec); err != nil {
			return empty, err
		}
		current.Bind = spec.Bind
		current.Protocol = spec.Protocol
		current.Status = ControllerListenerPending
	}
	if desired != "" {
		switch desired {
		case ControllerListenerPending, ControllerListenerRunning, ControllerListenerFailed, ControllerListenerStopped:
			current.Status = desired
		default:
			return empty, fmt.Errorf("unsupported desired status %s", desired)
		}
	}
	current.UpdatedAt = time.Now()
	if err := admin.controllerListeners.save(current); err != nil {
		return empty, err
	}
	return current, nil
}

// DeleteControllerListener 删除一条记录。
func (admin *Admin) DeleteControllerListener(id string) (ControllerListenerRecord, error) {
	var empty ControllerListenerRecord
	if admin == nil || admin.controllerListeners == nil {
		return empty, fmt.Errorf("admin unavailable")
	}
	current, ok := admin.controllerListeners.get(id)
	if !ok {
		return empty, fmt.Errorf("controller listener %s not found", id)
	}
	if err := admin.controllerListeners.delete(id); err != nil {
		return empty, err
	}
	return current, nil
}

// StopControllerListener 取消一个待启动或运行中的 listener（如果存在），并将状态标记为 stopped。
func (admin *Admin) StopControllerListener(id string) (ControllerListenerRecord, error) {
	if _, ok := admin.controllerListeners.get(id); !ok {
		return ControllerListenerRecord{}, fmt.Errorf("controller listener %s not found", id)
	}
	admin.cancelControllerListener(id)
	return admin.UpdateControllerListener(id, nil, ControllerListenerStopped)
}

// startPendingControllerListeners 会在启动时拉起所有 pending/running 的 controller listener。
func (admin *Admin) startPendingControllerListeners() {
	if admin == nil || admin.controllerListeners == nil {
		return
	}
	for _, rec := range admin.controllerListeners.list() {
		if rec.Status == ControllerListenerPending || rec.Status == ControllerListenerRunning {
			admin.StartControllerListenerAsync(rec.ID)
		}
	}
}

// markControllerListener 更新 status/lastError 并持久化。
func (admin *Admin) markControllerListener(id string, status ControllerListenerStatus, lastErr string) ControllerListenerRecord {
	if admin == nil || admin.controllerListeners == nil {
		return ControllerListenerRecord{}
	}
	rec, ok := admin.controllerListeners.get(id)
	if !ok {
		return ControllerListenerRecord{}
	}
	rec.Status = status
	rec.LastError = strings.TrimSpace(lastErr)
	rec.UpdatedAt = time.Now()
	_ = admin.controllerListeners.save(rec)
	return rec
}

// StartControllerListenerAsync 会立即启动一个被动 controller listener。
// 它只用于 Kelpie 在没有上游连接的情况下启动后，
// 用户再通过 gRPC UI 创建或激活 ControllerListener 的场景。
func (admin *Admin) StartControllerListenerAsync(id string) {
	if admin == nil {
		return
	}
	rec, ok := admin.controllerListeners.get(id)
	if !ok {
		return
	}
	if rec.Status == ControllerListenerStopped {
		return
	}
	if admin.options == nil {
		admin.markControllerListener(rec.ID, ControllerListenerFailed, "options unavailable")
		return
	}
	ctx, cancel := context.WithCancel(admin.context())
	admin.registerControllerListenerCancel(rec.ID, cancel)
	go func(rec ControllerListenerRecord, ctx context.Context) {
		// 如果已经连上上游，就只更新为 running。
		if admin.store != nil && admin.store.ActiveConn() != nil {
			admin.markControllerListener(rec.ID, ControllerListenerRunning, "")
			admin.unregisterControllerListenerCancel(rec.ID)
			return
		}

		// Move to running before binding to avoid重复触发。
		admin.markControllerListener(rec.ID, ControllerListenerRunning, "")

		// 复制一份 options，避免共享指针上的竞态。
		opts := *admin.options
		opts.Listen = rec.Bind
		opts.Mode = initial.NORMAL_PASSIVE

		_, _, err := admin.runControllerListener(ctx, &opts, rec)
		if err != nil {
			printer.Fail("[*] Controller listener %s failed: %v\r\n", rec.ID, err)
			admin.markControllerListener(rec.ID, ControllerListenerFailed, err.Error())
			admin.unregisterControllerListenerCancel(rec.ID)
			return
		}

		// 更新当前生效的 options，供后续重连使用。
		if admin.options != nil {
			admin.options.Listen = opts.Listen
			admin.options.Mode = opts.Mode
		}
		if recnr := admin.ensureReconnector(); recnr != nil {
			recnr.options = admin.options
		}

		admin.markControllerListener(rec.ID, ControllerListenerRunning, "")
		printer.Success("[*] Controller listener %s on %s is active\r\n", rec.ID, rec.Bind)
		admin.unregisterControllerListenerCancel(rec.ID)
	}(rec, ctx)
}

// runControllerListener 会在 rec.Bind 上绑定监听，并处理一次被动握手，同时遵守 ctx 取消信号。
func (admin *Admin) runControllerListener(ctx context.Context, opts *initial.Options, rec ControllerListenerRecord) (net.Conn, *protocol.ProtocolMeta, error) {
	if opts == nil {
		return nil, nil, fmt.Errorf("options unavailable")
	}
	listenAddr, _, err := utils.CheckIPPort(rec.Bind)
	if err != nil {
		return nil, nil, runtimeerr.Wrap(err, "ADMIN_LISTEN_ADDR", runtimeerr.SeverityError, false, "invalid listen address %s", rec.Bind)
	}

	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, nil, runtimeerr.Wrap(err, "ADMIN_LISTEN_FAILED", runtimeerr.SeverityError, true, "listen on %s failed", listenAddr)
	}
	defer listener.Close()

	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = listener.Close()
		case <-done:
		}
	}()
	defer close(done)

	baseSecret := opts.BaseSecret()
	localFlags := protocol.DefaultProtocolFlags
	if strings.ToLower(opts.Downstream) != "http" {
		localFlags &^= protocol.FlagSupportChunked
	}
	handshakeSecret := handshake.HandshakeSecret(baseSecret, opts.TlsEnable)
	greet := handshake.RandomGreeting(handshake.RoleAdmin)

	hiTemplate := &protocol.HIMess{
		GreetingLen: uint16(len(greet)),
		Greeting:    greet,
		UUIDLen:     uint16(len(protocol.ADMIN_UUID)),
		UUID:        protocol.ADMIN_UUID,
		IsAdmin:     1,
		IsReconnect: 0,
		ProtoFlags:  localFlags,
	}

	header := &protocol.Header{
		Flags:       localFlags,
		Sender:      protocol.ADMIN_UUID,
		Accepter:    protocol.TEMP_UUID,
		MessageType: protocol.HI,
		RouteLen:    uint32(len([]byte(protocol.TEMP_ROUTE))),
		Route:       protocol.TEMP_ROUTE,
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil, nil, ctx.Err()
			}
			printer.Fail("[*] Error occurred: %s\r\n", err.Error())
			continue
		}

		if opts.TlsEnable {
			var tlsConfig *tls.Config
			tlsConfig, err = transport.NewServerTLSConfig(opts.PreAuthToken, opts.Domain)
			if err != nil {
				printer.Fail("[*] Error occured: %s", err.Error())
				conn.Close()
				time.Sleep(time.Second)
				continue
			}
			conn = transport.WrapTLSServerConn(conn, tlsConfig)
		}

		param := new(protocol.NegParam)
		param.Conn = conn
		proto := protocol.NewDownProto(param)
		if err := proto.SNegotiate(); err != nil {
			printer.Fail("[*] HTTP negotiate failed: %v\r\n", err)
			conn.Close()
			continue
		}
		conn = param.Conn

		if err := share.PassivePreAuth(conn, opts.PreAuthToken); err != nil {
			printer.Fail("[*] Error occurred: %s\r\n", err.Error())
			conn.Close()
			continue
		}

		rMessage := protocol.NewDownMsgWithTransport(conn, handshakeSecret, protocol.ADMIN_UUID, opts.Downstream)
		fHeader, fMessage, err := protocol.DestructMessage(rMessage)

		if err != nil {
			printer.Fail("[*] Fail to set connection from %s, Error: %s\r\n", conn.RemoteAddr().String(), err.Error())
			conn.Close()
			continue
		}

		if fHeader.MessageType == protocol.HI {
			mmess := fMessage.(*protocol.HIMess)
			if handshake.ValidGreeting(handshake.RoleAgent, mmess.Greeting) && mmess.IsAdmin == 0 {
				meta := protocol.ResolveProtocolMeta(localFlags, mmess.ProtoFlags)
				if strings.ToLower(opts.Downstream) == "http" && meta.Flags&protocol.FlagSupportChunked == 0 {
					conn.Close()
					printer.Fail("[*] Incoming node does not support HTTP chunked transfer\r\n")
					time.Sleep(time.Second)
					continue
				}

				sMessage := protocol.NewDownMsgWithTransport(conn, handshakeSecret, protocol.ADMIN_UUID, opts.Downstream)
				protocol.SetMessageMeta(sMessage, meta.Flags)
				header.Flags = meta.Flags
				responseHI := *hiTemplate
				responseHI.ProtoFlags = meta.Flags
				protocol.ConstructMessage(sMessage, header, &responseHI, false)
				sMessage.SendMessage()
				opts.Secret = handshake.SessionSecret(baseSecret, opts.TlsEnable)
				activateConn := func() {
					downstream := opts.Downstream
					if strings.TrimSpace(downstream) == "" {
						downstream = "raw"
					}
					if admin.store != nil {
						admin.store.InitializeComponent(conn, opts.Secret, protocol.ADMIN_UUID, "raw", downstream)
						admin.store.UpdateProtocolFlags(protocol.ADMIN_UUID, meta.Flags)
					}
					admin.updateSessionConn(conn)
					admin.watchConn(ctx, conn)
				}

				if mmess.IsReconnect == 0 {
					childUUID := dispatchUUIDController(conn, opts.Secret, opts.Downstream, meta)
					activateConn()
					node := topology.NewNode(childUUID, conn.RemoteAddr().String())
					task := &topology.TopoTask{
						Mode:       topology.ADDNODE,
						Target:     node,
						ParentUUID: protocol.TEMP_UUID,
						IsFirst:    true,
					}
					if admin.topology == nil {
						conn.Close()
						printer.Fail("[*] Failed to register node: topology unavailable\r\n")
						return nil, nil, runtimeerr.New("ADMIN_REGISTER_NODE", runtimeerr.SeverityError, true, "topology unavailable")
					}
					if _, err := admin.topology.Execute(task); err != nil {
						conn.Close()
						printer.Fail("[*] Failed to register node: %v\r\n", err)
						return nil, nil, runtimeerr.Wrap(err, "ADMIN_REGISTER_NODE", runtimeerr.SeverityError, true, "failed to register node")
					}
					// 根节点通过 controller listener 加入（gRPC 路径）。
					// 这里要确保 admin-root 树边存在，让路由能够找到路径，
					// 并立刻重新计算路由，避免后续动作（pivot listener、DTN 等）
					// 因为“route unavailable”而失败。
					if _, err := admin.topology.Execute(&topology.TopoTask{
						Mode:         topology.ADDEDGE,
						UUID:         protocol.ADMIN_UUID,
						NeighborUUID: childUUID,
						EdgeType:     topology.TreeEdge,
					}); err != nil {
						conn.Close()
						printer.Fail("[*] Failed to register node edge: %v\r\n", err)
						return nil, nil, runtimeerr.Wrap(err, "ADMIN_REGISTER_NODE", runtimeerr.SeverityError, true, "failed to register node edge")
					}
					if _, err := admin.topology.Execute(&topology.TopoTask{Mode: topology.CALCULATE}); err != nil {
						conn.Close()
						printer.Fail("[*] Failed to calculate routes: %v\r\n", err)
						return nil, nil, runtimeerr.Wrap(err, "ADMIN_REGISTER_NODE", runtimeerr.SeverityError, true, "failed to calculate routes")
					}

					printer.Success("[*] Connection from node %s is set up successfully! Node id is 0\r\n", conn.RemoteAddr().String())
					supp.PublishNodeAdded(childUUID)
					return conn, &meta, nil
				}

				activateConn()
				node := topology.NewNode(mmess.UUID, conn.RemoteAddr().String())
				task := &topology.TopoTask{
					Mode:       topology.REONLINENODE,
					Target:     node,
					ParentUUID: protocol.TEMP_UUID,
					IsFirst:    true,
				}
				if admin.topology == nil {
					conn.Close()
					printer.Fail("[*] Failed to register node: topology unavailable\r\n")
					return nil, nil, runtimeerr.New("ADMIN_REGISTER_NODE", runtimeerr.SeverityError, true, "topology unavailable")
				}
				if _, err := admin.topology.Execute(task); err != nil {
					conn.Close()
					printer.Fail("[*] Failed to register node: %v\r\n", err)
					return nil, nil, runtimeerr.Wrap(err, "ADMIN_REGISTER_NODE", runtimeerr.SeverityError, true, "failed to register node")
				}
				if _, err := admin.topology.Execute(&topology.TopoTask{
					Mode:         topology.ADDEDGE,
					UUID:         protocol.ADMIN_UUID,
					NeighborUUID: mmess.UUID,
					EdgeType:     topology.TreeEdge,
				}); err != nil {
					conn.Close()
					printer.Fail("[*] Failed to register node edge: %v\r\n", err)
					return nil, nil, runtimeerr.Wrap(err, "ADMIN_REGISTER_NODE", runtimeerr.SeverityError, true, "failed to register node edge")
				}
				if _, err := admin.topology.Execute(&topology.TopoTask{Mode: topology.CALCULATE}); err != nil {
					conn.Close()
					printer.Fail("[*] Failed to calculate routes: %v\r\n", err)
					return nil, nil, runtimeerr.Wrap(err, "ADMIN_REGISTER_NODE", runtimeerr.SeverityError, true, "failed to calculate routes")
				}

				printer.Success("[*] Connection from node %s is re-established!\r\n", conn.RemoteAddr().String())
				supp.PublishNodeAdded(mmess.UUID)
				return conn, &meta, nil
			}
		}

		conn.Close()
		printer.Fail("[*] Incoming connection seems illegal!")
		time.Sleep(time.Second)
	}
}

func dispatchUUIDController(conn net.Conn, secret, transport string, meta protocol.ProtocolMeta) string {
	uuid := utils.GenerateUUID()
	uuidMess := &protocol.UUIDMess{
		UUIDLen:    uint16(len(uuid)),
		UUID:       uuid,
		ProtoFlags: meta.Flags,
	}

	header := &protocol.Header{
		Sender:      protocol.ADMIN_UUID,
		Accepter:    protocol.TEMP_UUID,
		MessageType: protocol.UUID,
		RouteLen:    uint32(len([]byte(protocol.TEMP_ROUTE))),
		Route:       protocol.TEMP_ROUTE,
	}

	sMessage := protocol.NewDownMsgWithTransport(conn, secret, protocol.ADMIN_UUID, transport)
	protocol.SetMessageMeta(sMessage, meta.Flags)

	protocol.ConstructMessage(sMessage, header, uuidMess, false)
	sMessage.SendMessage()

	return uuid
}
