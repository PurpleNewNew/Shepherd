package kelpie

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"codeberg.org/agnoie/shepherd/internal/kelpie/uipb"
)

// EventKind 描述一条标准化后的 UI 事件类别。
// 客户端不直接使用 protobuf oneof，改用该枚举 + 扁平字段，便于 JSON 传给前端。
type EventKind string

const (
	EventKindNode         EventKind = "node"
	EventKindLog          EventKind = "log"
	EventKindStream       EventKind = "stream"
	EventKindListener     EventKind = "listener"
	EventKindSession      EventKind = "session"
	EventKindDial         EventKind = "dial"
	EventKindProxy        EventKind = "proxy"
	EventKindSleep        EventKind = "sleep"
	EventKindSupplemental EventKind = "supplemental"
	EventKindChat         EventKind = "chat"
	EventKindAudit        EventKind = "audit"
	EventKindLoot         EventKind = "loot"
	EventKindUnknown      EventKind = "unknown"
)

// Event 是"Kelpie WatchEvents → 前端时间线"所需要的扁平事件结构。
// 字段只保留在答辩演示中会用到的部分，保持 JSON 序列化短小。
type Event struct {
	Seq       uint64            `json:"seq"`
	Kind      EventKind         `json:"kind"`
	Action    string            `json:"action"`
	Timestamp time.Time         `json:"timestamp"`
	Summary   string            `json:"summary"`
	Target    string            `json:"target,omitempty"`
	Source    string            `json:"source,omitempty"`
	Level     string            `json:"level,omitempty"`
	Reason    string            `json:"reason,omitempty"`
	Extras    map[string]string `json:"extras,omitempty"`
}

// EventStream 管理一个 WatchEvents 订阅的 goroutine，负责把事件转换成 Event 并
// 通过 channel 下发给上层（facade），上层再用 wails runtime 推送到前端。
type EventStream struct {
	cancel context.CancelFunc
	out    chan Event
	done   chan struct{}
	err    error
	errMu  sync.RWMutex
	seq    uint64
}

// Out 返回只读事件 channel。
func (s *EventStream) Out() <-chan Event { return s.out }

// Err 返回流中断的错误（若有）。
func (s *EventStream) Err() error {
	s.errMu.RLock()
	defer s.errMu.RUnlock()
	return s.err
}

// Done 返回一个在 goroutine 退出后关闭的 channel。
func (s *EventStream) Done() <-chan struct{} { return s.done }

// Close 停止订阅并回收资源。
func (s *EventStream) Close() {
	if s == nil || s.cancel == nil {
		return
	}
	s.cancel()
	<-s.done
}

// Subscribe 在 parent 上下文之下启动一个 WatchEvents goroutine。
// buffer 控制 channel 大小，建议 >= 128；过小会阻塞 gRPC 接收。
func (c *Client) Subscribe(parent context.Context, buffer int) (*EventStream, error) {
	if c == nil {
		return nil, errors.New("nil client")
	}
	if buffer < 16 {
		buffer = 128
	}
	ctx, cancel := context.WithCancel(parent)
	stream, err := c.WatchEvents(ctx)
	if err != nil {
		cancel()
		return nil, err
	}
	s := &EventStream{
		cancel: cancel,
		out:    make(chan Event, buffer),
		done:   make(chan struct{}),
	}
	go s.loop(stream)
	return s, nil
}

func (s *EventStream) loop(stream uipb.KelpieUIService_WatchEventsClient) {
	defer close(s.out)
	defer close(s.done)
	for {
		ev, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
				return
			}
			s.errMu.Lock()
			s.err = err
			s.errMu.Unlock()
			return
		}
		flat := flattenEvent(ev)
		flat.Seq = s.nextSeq()
		select {
		case s.out <- flat:
		default:
			// 满了就丢掉最旧一条（演示场景，优先保证"最新"事件可见）。
			select {
			case <-s.out:
			default:
			}
			s.out <- flat
		}
	}
}

func (s *EventStream) nextSeq() uint64 {
	s.seq++
	return s.seq
}

// flattenEvent 把一个 UiEvent 扁平化为 Event。
func flattenEvent(raw *uipb.UiEvent) Event {
	now := time.Now()
	out := Event{Timestamp: now, Kind: EventKindUnknown, Extras: map[string]string{}}
	if raw == nil {
		return out
	}
	switch payload := raw.GetPayload().(type) {
	case *uipb.UiEvent_NodeEvent:
		ev := payload.NodeEvent
		out.Kind = EventKindNode
		out.Action = nodeKindString(ev.GetKind())
		if node := ev.GetNode(); node != nil {
			out.Target = node.GetUuid()
			out.Summary = formatNodeSummary(ev.GetKind(), node)
			if node.GetAlias() != "" {
				out.Extras["alias"] = node.GetAlias()
			}
			if node.GetStatus() != "" {
				out.Extras["status"] = node.GetStatus()
			}
			if node.GetSleep() != "" {
				out.Extras["sleep"] = node.GetSleep()
			}
		}
	case *uipb.UiEvent_LogEvent:
		ev := payload.LogEvent
		out.Kind = EventKindLog
		out.Action = "log"
		out.Target = ev.GetNodeUuid()
		out.Level = ev.GetLevel()
		out.Summary = ev.GetMessage()
		if ts := ev.GetTimestamp(); ts != "" {
			out.Extras["timestamp"] = ts
		}
	case *uipb.UiEvent_StreamEvent:
		ev := payload.StreamEvent
		out.Kind = EventKindStream
		out.Action = streamKindString(ev.GetKind())
		out.Reason = ev.GetReason()
		if s := ev.GetStream(); s != nil {
			out.Target = s.GetTargetUuid()
			out.Summary = formatStreamSummary(ev.GetKind(), s)
		}
	case *uipb.UiEvent_ListenerEvent:
		ev := payload.ListenerEvent
		out.Kind = EventKindListener
		out.Action = listenerKindString(ev.GetKind())
		if l := ev.GetListener(); l != nil {
			out.Target = l.GetTargetUuid()
			out.Summary = "listener " + l.GetBind() + " (" + l.GetProtocol() + ")"
		}
	case *uipb.UiEvent_SessionEvent:
		ev := payload.SessionEvent
		out.Kind = EventKindSession
		out.Action = sessionKindString(ev.GetKind())
		if ss := ev.GetSession(); ss != nil {
			out.Target = ss.GetTargetUuid()
			out.Summary = "session " + ss.GetStatus().String()
			out.Reason = ev.GetReason()
		}
	case *uipb.UiEvent_DialEvent:
		ev := payload.DialEvent
		out.Kind = EventKindDial
		out.Action = ev.GetKind().String()
		if st := ev.GetStatus(); st != nil {
			out.Target = st.GetTargetUuid()
			out.Summary = "dial " + st.GetState().String() + " -> " + st.GetAddress()
			out.Reason = st.GetReason()
		}
	case *uipb.UiEvent_ProxyEvent:
		ev := payload.ProxyEvent
		out.Kind = EventKindProxy
		out.Action = ev.GetKind().String()
		if p := ev.GetProxy(); p != nil {
			out.Target = p.GetTargetUuid()
			out.Summary = "proxy " + p.GetKind() + " " + p.GetBind()
		}
		out.Reason = ev.GetReason()
	case *uipb.UiEvent_SleepEvent:
		ev := payload.SleepEvent
		out.Kind = EventKindSleep
		out.Action = ev.GetKind().String()
		out.Target = ev.GetTargetUuid()
		out.Summary = formatSleepSummary(ev)
		out.Reason = ev.GetReason()
	case *uipb.UiEvent_SupplementalEvent:
		ev := payload.SupplementalEvent
		out.Kind = EventKindSupplemental
		out.Action = ev.GetKind()
		out.Target = ev.GetTargetUuid()
		out.Source = ev.GetSourceUuid()
		out.Summary = "supp " + ev.GetAction() + " " + ev.GetDetail()
		if ts := ev.GetTimestamp(); ts != "" {
			out.Extras["timestamp"] = ts
		}
	case *uipb.UiEvent_ChatEvent:
		out.Kind = EventKindChat
		if m := payload.ChatEvent.GetMessage(); m != nil {
			out.Action = "message"
			out.Source = m.GetUsername()
			out.Summary = m.GetMessage()
		}
	case *uipb.UiEvent_AuditEvent:
		out.Kind = EventKindAudit
		if entry := payload.AuditEvent.GetEntry(); entry != nil {
			out.Action = entry.GetMethod()
			out.Source = entry.GetUsername()
			out.Target = entry.GetTarget()
			out.Summary = entry.GetStatus() + " " + entry.GetMethod()
			out.Reason = entry.GetError()
		}
	case *uipb.UiEvent_LootEvent:
		out.Kind = EventKindLoot
		ev := payload.LootEvent
		out.Action = ev.GetKind().String()
		if item := ev.GetItem(); item != nil {
			out.Target = item.GetTargetUuid()
			out.Summary = item.GetName()
		}
	default:
		// 未知 payload 直接保留 unknown，前端仍可显示一条通用条目。
	}
	return out
}

func nodeKindString(k uipb.NodeEvent_Kind) string {
	switch k {
	case uipb.NodeEvent_ADDED:
		return "added"
	case uipb.NodeEvent_UPDATED:
		return "updated"
	case uipb.NodeEvent_REMOVED:
		return "removed"
	}
	return "unknown"
}

func streamKindString(k uipb.StreamEvent_Kind) string {
	switch k {
	case uipb.StreamEvent_STREAM_OPENED:
		return "opened"
	case uipb.StreamEvent_STREAM_UPDATED:
		return "updated"
	case uipb.StreamEvent_STREAM_CLOSED:
		return "closed"
	}
	return "unknown"
}

func listenerKindString(k uipb.PivotListenerEvent_Kind) string {
	switch k {
	case uipb.PivotListenerEvent_PIVOT_LISTENER_ADDED:
		return "added"
	case uipb.PivotListenerEvent_PIVOT_LISTENER_UPDATED:
		return "updated"
	case uipb.PivotListenerEvent_PIVOT_LISTENER_REMOVED:
		return "removed"
	}
	return "unknown"
}

func sessionKindString(k uipb.SessionEvent_Kind) string {
	switch k {
	case uipb.SessionEvent_SESSION_EVENT_ADDED:
		return "added"
	case uipb.SessionEvent_SESSION_EVENT_UPDATED:
		return "updated"
	case uipb.SessionEvent_SESSION_EVENT_REMOVED:
		return "removed"
	case uipb.SessionEvent_SESSION_EVENT_MARKED:
		return "marked"
	case uipb.SessionEvent_SESSION_EVENT_REPAIR_STARTED:
		return "repair-started"
	case uipb.SessionEvent_SESSION_EVENT_REPAIR_COMPLETED:
		return "repair-completed"
	case uipb.SessionEvent_SESSION_EVENT_TERMINATED:
		return "terminated"
	}
	return "unknown"
}

func formatNodeSummary(k uipb.NodeEvent_Kind, n *uipb.NodeInfo) string {
	alias := n.GetAlias()
	if alias == "" {
		alias = n.GetUuid()
	}
	switch k {
	case uipb.NodeEvent_ADDED:
		return alias + " joined (" + n.GetStatus() + ")"
	case uipb.NodeEvent_UPDATED:
		return alias + " updated (" + n.GetStatus() + ")"
	case uipb.NodeEvent_REMOVED:
		return alias + " removed"
	}
	return alias
}

func formatStreamSummary(k uipb.StreamEvent_Kind, s *uipb.StreamDiag) string {
	switch k {
	case uipb.StreamEvent_STREAM_OPENED:
		return "stream " + s.GetKind() + " open → " + s.GetTargetUuid()
	case uipb.StreamEvent_STREAM_CLOSED:
		return "stream " + s.GetKind() + " closed"
	default:
		return "stream " + s.GetKind() + " @ " + s.GetTargetUuid()
	}
}

func formatSleepSummary(ev *uipb.SleepEvent) string {
	out := "sleep"
	if s := ev.SleepSeconds; s != nil {
		out += " s=" + itoa(int64(*s))
	}
	if w := ev.WorkSeconds; w != nil {
		out += " w=" + itoa(int64(*w))
	}
	if j := ev.JitterPercent; j != nil {
		out += " j=" + ftoa(*j)
	}
	return out
}

func itoa(v int64) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	buf := make([]byte, 0, 20)
	for v > 0 {
		buf = append([]byte{byte('0' + v%10)}, buf...)
		v /= 10
	}
	if neg {
		buf = append([]byte{'-'}, buf...)
	}
	return string(buf)
}

func ftoa(v float64) string {
	// 仅供 UI 展示，保留两位小数即可。
	intPart := int64(v)
	frac := int64((v - float64(intPart)) * 100)
	if frac < 0 {
		frac = -frac
	}
	fracBuf := itoa(frac)
	if len(fracBuf) < 2 {
		fracBuf = "0" + fracBuf
	}
	return itoa(intPart) + "." + fracBuf
}
