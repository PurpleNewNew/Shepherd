package utils

import (
	"context"
	"errors"
	"sync"
)

var ErrMailboxClosed = errors.New("mailbox: closed")

type Mailbox struct {
	name     string
	out      chan<- interface{}
	capacity int

	mu      sync.Mutex
	cond    *sync.Cond
	queue   []interface{}
	closed  bool
	dropped uint64
}

func NewMailbox(name string, out chan<- interface{}, capacity int) *Mailbox {
	if capacity <= 0 {
		capacity = 1
	}
	mb := &Mailbox{
		name:     name,
		out:      out,
		capacity: capacity,
	}
	mb.cond = sync.NewCond(&mb.mu)
	go mb.loop()
	return mb
}

func (mb *Mailbox) loop() {
	for {
		mb.mu.Lock()
		for len(mb.queue) == 0 && !mb.closed {
			mb.cond.Wait()
		}
		if len(mb.queue) == 0 && mb.closed {
			mb.mu.Unlock()
			return
		}
		msg := mb.queue[0]
		mb.queue = mb.queue[1:]
		mb.mu.Unlock()

		func() {
			defer func() {
				if recover() != nil {
					mb.mu.Lock()
					mb.closed = true
					mb.mu.Unlock()
				}
			}()
			mb.out <- msg
		}()
	}
}

func (mb *Mailbox) Enqueue(ctx context.Context, msg interface{}) (bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case mb.out <- msg:
		return false, nil
	case <-ctx.Done():
		return false, ctx.Err()
	default:
	}

	mb.mu.Lock()
	defer mb.mu.Unlock()
	if mb.closed {
		return false, ErrMailboxClosed
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}

	dropped := false
	if len(mb.queue) >= mb.capacity {
		if len(mb.queue) > 0 {
			mb.queue = mb.queue[1:]
			mb.dropped++
			dropped = true
		}
	}

	mb.queue = append(mb.queue, msg)
	mb.cond.Signal()
	return dropped, nil
}

func (mb *Mailbox) Close() {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	if mb.closed {
		return
	}
	mb.closed = true
	mb.queue = nil
	mb.cond.Broadcast()
}

func (mb *Mailbox) Dropped() uint64 {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	return mb.dropped
}

func (mb *Mailbox) Name() string {
	return mb.name
}
