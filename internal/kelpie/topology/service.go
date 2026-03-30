package topology

import (
	"context"
	"errors"
)

// ErrNilTopology 表示拓扑服务在创建时传入了空的拓扑实例。
var ErrNilTopology = errors.New("topology: nil topology")

// ErrNilTask 表示向拓扑服务提交了空任务。
var ErrNilTask = errors.New("topology: nil task")

// ErrStopped 表示拓扑调度器已停止工作。
var ErrStopped = errors.New("topology: stopped")

type asyncResult struct {
	value *Result
	err   error
}

// Future 允许调用者在支持取消的前提下等待结果。
type Future struct {
	ch <-chan asyncResult
}

// Await 等待拓扑结果返回或上下文取消。
func (f *Future) Await(ctx context.Context) (*Result, error) {
	if f == nil {
		return nil, errors.New("topology: nil future")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case res, ok := <-f.ch:
		if !ok {
			return nil, nil
		}
		return res.value, res.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Service 封装拓扑对象，并基于旧版通道接口提供异步辅助方法。
type Service struct {
	topo *Topology
}

// NewService 为给定的拓扑构建服务实例。
func NewService(topo *Topology) *Service {
	return &Service{topo: topo}
}

// Submit 将任务入队并返回可等待的 future。
func (s *Service) Submit(ctx context.Context, task *TopoTask) (*Future, error) {
	if s == nil || s.topo == nil {
		return nil, ErrNilTopology
	}
	if task == nil {
		return nil, ErrNilTask
	}
	if ctx == nil {
		ctx = context.Background()
	}

	response := make(chan *topoResult, 1)
	taskCopy := *task
	taskCopy.Response = response

	out := make(chan asyncResult, 1)

	go func() {
		defer close(out)
		if err := s.topo.EnqueueContext(ctx, &taskCopy); err != nil {
			out <- asyncResult{err: err}
			return
		}

		select {
		case res := <-response:
			out <- asyncResult{value: res}
		case <-ctx.Done():
			out <- asyncResult{err: ctx.Err()}
		}
	}()

	return &Future{ch: out}, nil
}

// Request 入队任务并阻塞直到收到结果或上下文取消。
func (s *Service) Request(ctx context.Context, task *TopoTask) (*Result, error) {
	future, err := s.Submit(ctx, task)
	if err != nil {
		return nil, err
	}
	return future.Await(ctx)
}
