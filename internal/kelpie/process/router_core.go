package process

import (
	"context"
	"errors"
	"fmt"
	"time"

	"codeberg.org/agnoie/shepherd/internal/kelpie/manager"
	"codeberg.org/agnoie/shepherd/internal/kelpie/printer"
	"codeberg.org/agnoie/shepherd/internal/kelpie/supp"
	"codeberg.org/agnoie/shepherd/internal/kelpie/topology"
	"codeberg.org/agnoie/shepherd/pkg/bus"
	"codeberg.org/agnoie/shepherd/pkg/utils"
	"codeberg.org/agnoie/shepherd/protocol"
)

type routerCore struct {
	ctx         context.Context
	bus         *bus.Router
	manager     *manager.Manager
	topo        *topology.Topology
	planner     *SupplementalPlanner
	dtnAck      func(*protocol.DTNAck)
	streamOpen  func(*protocol.Header, *protocol.StreamOpen)
	streamData  func(*protocol.Header, *protocol.StreamData)
	streamAck   func(*protocol.Header, *protocol.StreamAck)
	streamClose func(*protocol.Header, *protocol.StreamClose)
}

func newRouterCore(ctx context.Context, mgr *manager.Manager, topo *topology.Topology) *routerCore {
	core := &routerCore{
		ctx:     ctx,
		manager: mgr,
		topo:    topo,
		bus:     bus.NewRouter(),
	}
	core.bootstrap()
	return core
}

func (core *routerCore) setPlanner(planner *SupplementalPlanner) {
	if core == nil {
		return
	}
	core.planner = planner
}

func (core *routerCore) setDTNAckHandler(fn func(*protocol.DTNAck)) {
	if core == nil {
		return
	}
	core.dtnAck = fn
}

func (core *routerCore) setStreamHandlers(open func(*protocol.Header, *protocol.StreamOpen), data func(*protocol.Header, *protocol.StreamData), ack func(*protocol.Header, *protocol.StreamAck), closeFn func(*protocol.Header, *protocol.StreamClose)) {
	if core == nil {
		return
	}
	core.streamOpen = open
	core.streamData = data
	core.streamAck = ack
	core.streamClose = closeFn
}

func (core *routerCore) bootstrap() {
	if core == nil || core.manager == nil {
		return
	}
	core.registerMailboxHandlers("info", core.manager.InfoManager.Enqueue, protocol.MYINFO)
	core.registerMailboxHandlers(
		"listen",
		core.manager.ListenManager.Enqueue,
		protocol.CHILDUUIDREQ,
		protocol.LISTENRES,
	)
	core.registerMailboxHandlers(
		"connect",
		core.manager.ConnectManager.Enqueue,
		protocol.CONNECTDONE,
	)
	core.registerMailboxHandlers(
		"children",
		core.manager.ChildrenManager.Enqueue,
		protocol.NODEREONLINE,
		protocol.NODEOFFLINE,
	)
	core.bus.Register(uint16(protocol.SUPPLINKRESP), core.dispatchSuppLinkResponse())
	core.bus.Register(uint16(protocol.SUPPLINKHEARTBEAT), core.dispatchSuppLinkHeartbeat())
	core.bus.Register(uint16(protocol.RUNTIMELOG), core.dispatchRuntimeLog())
	core.bus.Register(uint16(protocol.NODECONNINFO), core.dispatchNodeConnInfo())
	core.bus.Register(uint16(protocol.RESCUE_RESPONSE), core.dispatchRescueResponse())
	core.bus.Register(uint16(protocol.DTN_ACK), core.dispatchDTNAck())
	core.bus.Register(uint16(protocol.STREAM_OPEN), core.dispatchStreamOpen())
	core.bus.Register(uint16(protocol.STREAM_DATA), core.dispatchStreamData())
	core.bus.Register(uint16(protocol.STREAM_ACK), core.dispatchStreamAck())
	core.bus.Register(uint16(protocol.STREAM_CLOSE), core.dispatchStreamClose())
	core.bus.Register(uint16(protocol.SLEEP_UPDATE_ACK), core.dispatchSleepUpdateAck())
}

func (core *routerCore) registerMailboxHandlers(name string, enqueue func(context.Context, interface{}) (bool, error), messageTypes ...uint16) {
	if core == nil || core.bus == nil || enqueue == nil {
		return
	}
	handler := core.mailboxHandler(name, enqueue)
	for _, msgType := range messageTypes {
		core.bus.Register(msgType, handler)
	}
}

func (core *routerCore) mailboxHandler(name string, enqueue func(context.Context, interface{}) (bool, error)) bus.Handler {
	return func(ctx context.Context, header *protocol.Header, payload interface{}) error {
		if header == nil {
			return bus.ErrNoHandler
		}
		dropped, err := enqueue(ctx, payload)
		if err != nil && !errors.Is(err, utils.ErrMailboxClosed) {
			return err
		}
		if dropped {
			printer.Warning("\r\n[*] %s queue dropped oldest message (type=%d)\r\n", name, header.MessageType)
		}
		return nil
	}
}

func (core *routerCore) dispatchSuppLinkResponse() bus.Handler {
	return func(ctx context.Context, header *protocol.Header, payload interface{}) error {
		resp, ok := payload.(*protocol.SuppLinkResponse)
		if !ok {
			return fmt.Errorf("expected *protocol.SuppLinkResponse, got %T", payload)
		}
		supp.HandleSuppLinkResponse(core.topo, core.manager, resp)
		return nil
	}
}

func (core *routerCore) dispatchSuppLinkHeartbeat() bus.Handler {
	return func(ctx context.Context, header *protocol.Header, payload interface{}) error {
		hb, ok := payload.(*protocol.SuppLinkHeartbeat)
		if !ok {
			return fmt.Errorf("expected *protocol.SuppLinkHeartbeat, got %T", payload)
		}
		sender := ""
		if header != nil {
			sender = header.Sender
		}
		supp.HandleSuppLinkHeartbeat(core.topo, core.manager, sender, hb)
		return nil
	}
}

func (core *routerCore) dispatchRuntimeLog() bus.Handler {
	return func(ctx context.Context, header *protocol.Header, payload interface{}) error {
		logPayload, ok := payload.(*protocol.RuntimeLog)
		if !ok {
			return fmt.Errorf("expected *protocol.RuntimeLog, got %T", payload)
		}
		entry := ""
		if core.topo != nil && logPayload != nil && logPayload.UUID != "" {
			entry = core.topo.NetworkFor(logPayload.UUID)
			if entry == "" {
				entry = logPayload.UUID
			}
		}
		printRuntimeLog(entry, logPayload)
		return nil
	}
}

func (core *routerCore) dispatchRescueResponse() bus.Handler {
	return func(ctx context.Context, header *protocol.Header, payload interface{}) error {
		resp, ok := payload.(*protocol.RescueResponse)
		if !ok {
			return fmt.Errorf("expected *protocol.RescueResponse, got %T", payload)
		}
		if core.planner != nil {
			core.planner.HandleRescueResponse(resp)
		}
		return nil
	}
}

func (core *routerCore) dispatchDTNAck() bus.Handler {
	return func(ctx context.Context, header *protocol.Header, payload interface{}) error {
		ack, ok := payload.(*protocol.DTNAck)
		if !ok {
			return fmt.Errorf("expected *protocol.DTNAck, got %T", payload)
		}
		if ack == nil {
			return nil
		}
		status := "OK"
		if ack.OK == 0 {
			status = "ERR"
		}
		if ack.OK == 0 && ack.Error != "" {
			printer.Warning("\r\n[*] DTN ack bundle=%s status=%s error=%s\r\n", shorten(ack.BundleID), status, ack.Error)
		} else {
			printer.Success("\r\n[*] DTN ack bundle=%s status=%s\r\n", shorten(ack.BundleID), status)
		}

		// Trace helper: for successful DTN "log:" payloads we echo the log message via
		// DTN_ACK.Error (on OK=1) so it inherits ACK retry semantics. When present,
		// render it as a normal RuntimeLog for regression visibility.
		if ack.OK != 0 && ack.Error != "" {
			sender := ""
			if header != nil {
				sender = header.Sender
			}
			if sender != "" {
				entry := ""
				if core.topo != nil {
					entry = core.topo.NetworkFor(sender)
					if entry == "" {
						entry = sender
					}
				}
				logPayload := &protocol.RuntimeLog{
					UUIDLen:     uint16(len(sender)),
					UUID:        sender,
					SeverityLen: uint16(len("INFO")),
					Severity:    "INFO",
					CodeLen:     uint16(len("DTN")),
					Code:        "DTN",
					MessageLen:  uint64(len(ack.Error)),
					Message:     ack.Error,
					Retryable:   0,
					CauseLen:    0,
					Cause:       "",
				}
				printRuntimeLog(entry, logPayload)
			}
		}

		if core.dtnAck != nil {
			core.dtnAck(ack)
		}
		return nil
	}
}

func (core *routerCore) dispatchStreamOpen() bus.Handler {
	return func(ctx context.Context, header *protocol.Header, payload interface{}) error {
		msg, ok := payload.(*protocol.StreamOpen)
		if !ok {
			return fmt.Errorf("expected *protocol.StreamOpen, got %T", payload)
		}
		if core.streamOpen != nil {
			core.streamOpen(header, msg)
		}
		return nil
	}
}

func (core *routerCore) dispatchStreamData() bus.Handler {
	return func(ctx context.Context, header *protocol.Header, payload interface{}) error {
		msg, ok := payload.(*protocol.StreamData)
		if !ok {
			return fmt.Errorf("expected *protocol.StreamData, got %T", payload)
		}
		if core.streamData != nil {
			core.streamData(header, msg)
		}
		return nil
	}
}

func (core *routerCore) dispatchStreamAck() bus.Handler {
	return func(ctx context.Context, header *protocol.Header, payload interface{}) error {
		msg, ok := payload.(*protocol.StreamAck)
		if !ok {
			return fmt.Errorf("expected *protocol.StreamAck, got %T", payload)
		}
		if core.streamAck != nil {
			core.streamAck(header, msg)
		}
		return nil
	}
}

func (core *routerCore) dispatchStreamClose() bus.Handler {
	return func(ctx context.Context, header *protocol.Header, payload interface{}) error {
		msg, ok := payload.(*protocol.StreamClose)
		if !ok {
			return fmt.Errorf("expected *protocol.StreamClose, got %T", payload)
		}
		if core.streamClose != nil {
			core.streamClose(header, msg)
		}
		return nil
	}
}

func (core *routerCore) dispatchSleepUpdateAck() bus.Handler {
	return func(ctx context.Context, header *protocol.Header, payload interface{}) error {
		ack, ok := payload.(*protocol.SleepUpdateAck)
		if !ok {
			return fmt.Errorf("expected *protocol.SleepUpdateAck, got %T", payload)
		}
		target := "node"
		if header != nil && header.Sender != "" {
			target = shortUUID(header.Sender)
		}
		if ack.OK == 1 {
			printer.Success("\r\n[*] Sleep update applied on %s.\r\n", target)
		} else if ack.Error != "" {
			printer.Fail("\r\n[*] Sleep update failed on %s: %s\r\n", target, ack.Error)
		} else {
			printer.Fail("\r\n[*] Sleep update failed on %s\r\n", target)
		}
		return nil
	}
}

func (core *routerCore) dispatchNodeConnInfo() bus.Handler {
	return func(ctx context.Context, header *protocol.Header, payload interface{}) error {
		info, ok := payload.(*protocol.NodeConnInfo)
		if !ok {
			return fmt.Errorf("expected *protocol.NodeConnInfo, got %T", payload)
		}
		if info == nil || info.UUID == "" {
			return nil
		}
		if core.topo == nil {
			return nil
		}
		lastSuccess := time.Unix(info.LastSuccessUnix, 0)
		if info.LastSuccessUnix == 0 {
			lastSuccess = time.Time{}
		}
		task := &topology.TopoTask{
			Mode:         topology.UPDATECONNINFO,
			UUID:         info.UUID,
			Port:         int(info.ListenPort),
			DialAddress:  info.DialAddress,
			FallbackPort: int(info.FallbackPort),
			Transport:    info.Transport,
			LastSuccess:  lastSuccess,
			TlsEnabled:   info.TlsEnabled != 0,
		}
		_, err := core.topo.Execute(task)
		return err
	}
}

func (core *routerCore) Router() *bus.Router {
	if core == nil {
		return nil
	}
	return core.bus
}

func (core *routerCore) handleFromDownstream(admin *Admin) {
	if core == nil || admin == nil {
		return
	}
	ctx := admin.context()
	conn, secret, uuid := admin.connectionTriple()
	if conn == nil {
		printer.Fail("\r\n[*] No active upstream connection")
	}
	for conn == nil {
		if ctx.Err() != nil {
			return
		}
		time.Sleep(200 * time.Millisecond)
		conn, secret, uuid = admin.connectionTriple()
	}
	admin.watchConn(ctx, conn)
	rMessage := protocol.NewUpMsg(conn, secret, uuid)
	for {
		if ctx.Err() != nil {
			return
		}
		header, message, err := protocol.DestructMessage(rMessage)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			printer.Fail("\r\n[*] Upstream connection error: %v\r\n", err)
			if !admin.reconnectUpstream() {
				printer.Fail("\r\n[*] Unable to recover upstream connection. Use `exit` to terminate.\r\n")
				admin.markEntriesOffline()
				return
			}
			conn, secret, uuid = admin.connectionTriple()
			if conn == nil {
				printer.Fail("\r\n[*] Upstream session unavailable after reconnect\r\n")
				admin.markEntriesOffline()
				return
			}
			admin.watchConn(ctx, conn)
			rMessage = protocol.NewUpMsg(conn, secret, uuid)
			continue
		}
		if !admin.messageAllowed(header) {
			continue
		}
		if err := core.bus.Dispatch(ctx, header, message); err != nil {
			switch {
			case errors.Is(err, bus.ErrNoHandler):
				printer.Fail("\r\n[*] Unknown Message Type: %d\r\n", header.MessageType)
			case errors.Is(err, bus.ErrBackpressure):
				printer.Warning("\r\n[*] Message %d dropped due to backpressure\r\n", header.MessageType)
			default:
				printer.Fail("\r\n[*] Message dispatch error: %v\r\n", err)
			}
		}
	}
}
