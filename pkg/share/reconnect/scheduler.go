package reconnect

import (
	"context"
	"math/rand"
	"time"
)

type Strategy struct {
	BaseDelay      time.Duration
	MaxDelay       time.Duration
	MaxAttempts    int
	JitterFactor   float64
	Multiplier     float64
	ImmediateFirst bool
}

var DefaultStrategy = Strategy{
	BaseDelay:      2 * time.Second,
	MaxDelay:       60 * time.Second,
	MaxAttempts:    0,
	JitterFactor:   0.5,
	Multiplier:     2.0,
	ImmediateFirst: true,
}

type Attempt struct {
	Index   int
	Delay   time.Duration
	Context context.Context
}

type Scheduler struct {
	strategy Strategy
	rand     *rand.Rand
}

func NewScheduler(strategy Strategy) *Scheduler {
	if strategy.BaseDelay <= 0 {
		strategy.BaseDelay = DefaultStrategy.BaseDelay
	}
	if strategy.MaxDelay <= 0 {
		strategy.MaxDelay = DefaultStrategy.MaxDelay
	}
	if strategy.Multiplier <= 1 {
		strategy.Multiplier = DefaultStrategy.Multiplier
	}
	if strategy.JitterFactor < 0 {
		strategy.JitterFactor = 0
	}
	return &Scheduler{
		strategy: strategy,
		rand:     rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (s *Scheduler) Attempts(ctx context.Context) <-chan Attempt {
	out := make(chan Attempt)
	go func() {
		defer close(out)
		delay := s.strategy.BaseDelay
		for i := 0; ; i++ {
			if s.strategy.MaxAttempts > 0 && i >= s.strategy.MaxAttempts {
				return
			}
			immediate := i == 0 && s.strategy.ImmediateFirst
			wait := delay
			if immediate {
				wait = 0
			}
			attemptCtx, cancel := context.WithCancel(ctx)
			select {
			case out <- Attempt{Index: i + 1, Delay: wait, Context: attemptCtx}:
			case <-ctx.Done():
				cancel()
				return
			}
			if wait > 0 {
				timer := time.NewTimer(wait)
				select {
				case <-timer.C:
				case <-ctx.Done():
					timer.Stop()
					cancel()
					return
				}
				timer.Stop()
			}
			cancel()
			if immediate {
				delay = s.strategy.BaseDelay
			} else {
				delay = s.nextDelay(delay)
			}
		}
	}()
	return out
}

func (s *Scheduler) nextDelay(prev time.Duration) time.Duration {
	next := time.Duration(float64(prev) * s.strategy.Multiplier)
	if next < s.strategy.BaseDelay {
		next = s.strategy.BaseDelay
	}
	if next > s.strategy.MaxDelay {
		next = s.strategy.MaxDelay
	}
	if s.strategy.JitterFactor > 0 {
		jitter := s.rand.Float64()*2 - 1
		delta := time.Duration(float64(next) * s.strategy.JitterFactor * jitter)
		next += delta
	}
	if next < s.strategy.BaseDelay {
		next = s.strategy.BaseDelay
	}
	if next > s.strategy.MaxDelay {
		next = s.strategy.MaxDelay
	}
	if next <= 0 {
		next = s.strategy.BaseDelay
	}
	return next
}
