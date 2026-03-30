package process

import (
	"context"
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
				if d.agent != nil && d.agent.shouldCarryRetry(err, msg) {
					d.agent.enqueueCarry(msg, err)
					logger.Warnf("forward to child %s queued for retry: %v", d.uuid, err)
					continue
				}
				logger.Warnf("failed to forward message to child %s: %v", d.uuid, err)
			}
		}
	}
}

func (d *childDispatcher) stopDispatcher() {
	if d == nil {
		return
	}
	d.stop.Do(func() {
		close(d.closed)
		// Do not close inbox: enqueue may race with stop and panic on send-to-closed.
	})
}
