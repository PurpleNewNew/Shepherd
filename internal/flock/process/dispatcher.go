package process

import (
	"context"
	"errors"
	"sync"
)

type childDispatcher struct {
	agent  *Agent
	uuid   string
	inbox  chan *ChildrenMess
	stop   sync.Once
	closed chan struct{}
}

func newChildDispatcher(agent *Agent, uuid string) *childDispatcher {
	d := &childDispatcher{
		agent:  agent,
		uuid:   uuid,
		inbox:  make(chan *ChildrenMess, childDispatcherQueueSize),
		closed: make(chan struct{}),
	}
	go d.run(agent.context())
	return d
}

func (d *childDispatcher) enqueue(msg *ChildrenMess) bool {
	if d == nil || msg == nil {
		return false
	}
	var done <-chan struct{}
	if d.agent != nil {
		if ctx := d.agent.context(); ctx != nil {
			done = ctx.Done()
		}
	}
	select {
	case <-d.closed:
		return false
	case <-done:
		return false
	case d.inbox <- msg:
		return true
	}
}

func (d *childDispatcher) run(ctx context.Context) {
	if d == nil {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-d.closed:
			return
		case msg, ok := <-d.inbox:
			if !ok {
				return
			}
			if msg == nil {
				continue
			}
			if err := d.agent.sendChildMessage(msg); err != nil {
				d.handleForwardError(err, msg)
			}
		}
	}
}

func (d *childDispatcher) handleForwardError(err error, msg *ChildrenMess) {
	if d == nil || err == nil {
		return
	}
	var routeErr *childRouteError
	hasRouteErr := errors.As(err, &routeErr)
	if d.agent != nil && d.agent.shouldCarryRetry(err, msg) {
		d.agent.enqueueCarry(msg, err)
		logger.Warnf("forward to child %s queued for retry: %v", d.uuid, err)
		if shouldMarkPrimaryChildOffline(routeErr) {
			downStreamOffline(d.agent, d.uuid, d, routeErr.conn)
			d.agent.removeDispatcher(d.uuid, d)
		}
		return
	}

	logger.Warnf("failed to forward message to child %s: %v", d.uuid, err)
	if hasRouteErr && d.agent != nil && shouldMarkPrimaryChildOffline(routeErr) {
		downStreamOffline(d.agent, d.uuid, d, routeErr.conn)
		d.agent.removeDispatcher(d.uuid, d)
	}
}

func shouldMarkPrimaryChildOffline(err *childRouteError) bool {
	if err == nil || err.fromSupp {
		return false
	}
	if errors.Is(err, ErrNoUpstreamSession) || errors.Is(err, ErrInvalidDownstreamMessage) {
		return false
	}
	return true
}

func (d *childDispatcher) stopDispatcher() {
	if d == nil {
		return
	}
	d.stop.Do(func() {
		close(d.closed)
		// 不要关闭 inbox：enqueue 可能与 stop 并发竞争，进而触发 send-to-closed panic。
	})
}
