package process

import (
	"crypto/sha256"
	"encoding/binary"
	"math"
	"sync"
	"time"
)

const defaultSleepJitterRatio = 0.2 // 20% 抖动上限

type sleepPredictor struct {
	mu          sync.Mutex
	uuid        string
	sleep       time.Duration
	jitterRatio float64
	phaseOffset time.Duration
}

func newSleepPredictor(uuid string, sleepSeconds int) *sleepPredictor {
	if sleepSeconds <= 0 {
		return nil
	}
	sleep := time.Duration(sleepSeconds) * time.Second
	p := &sleepPredictor{
		uuid:        uuid,
		sleep:       sleep,
		jitterRatio: defaultSleepJitterRatio,
	}
	p.phaseOffset = p.computePhaseOffset(uuid)
	return p
}

func (p *sleepPredictor) UpdateUUID(uuid string) {
	if p == nil || uuid == "" {
		return
	}
	p.mu.Lock()
	if uuid != p.uuid {
		p.uuid = uuid
		p.phaseOffset = p.computePhaseOffset(uuid)
	}
	p.mu.Unlock()
}

func (p *sleepPredictor) computePhaseOffset(uuid string) time.Duration {
	if uuid == "" || p.sleep <= 0 {
		return 0
	}
	sum := sha256.Sum256([]byte(uuid))
	value := binary.BigEndian.Uint64(sum[:8])
	return time.Duration(value % uint64(p.sleep))
}

func (p *sleepPredictor) jitterForCycle(cycle int64) time.Duration {
	if p == nil || p.sleep <= 0 || p.jitterRatio <= 0 {
		return 0
	}
	ratio := p.jitterRatio
	if ratio > 0.5 {
		ratio = 0.5
	}
	// 构造输入: uuid + cycle index
	buf := make([]byte, len(p.uuid)+8)
	copy(buf, p.uuid)
	binary.BigEndian.PutUint64(buf[len(p.uuid):], uint64(cycle))
	sum := sha256.Sum256(buf)
	value := binary.BigEndian.Uint64(sum[:8])
	// 映射到 [-1, 1]
	frac := float64(value) / float64(math.MaxUint64)
	frac = frac*2 - 1
	jitter := frac * ratio * float64(p.sleep)
	return time.Duration(jitter)
}

func (p *sleepPredictor) NextWake(now time.Time) time.Time {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.sleep <= 0 {
		return time.Time{}
	}
	base := time.Unix(0, 0).Add(p.phaseOffset)
	if now.Before(base) {
		jitter := p.jitterForCycle(0)
		next := base.Add(jitter)
		if next.Before(now) {
			next = base.Add(p.sleep + jitter)
		}
		return next
	}
	elapsed := now.Sub(base)
	cycles := int64(elapsed / p.sleep)
	nextStart := base.Add(time.Duration(cycles+1) * p.sleep)
	jitter := p.jitterForCycle(cycles + 1)
	next := nextStart.Add(jitter)
	if !next.After(now) {
		next = next.Add(p.sleep)
	}
	return next
}

func (p *sleepPredictor) UpdateSleepSeconds(seconds int) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if seconds <= 0 {
		p.sleep = 0
		p.phaseOffset = 0
		return
	}
	p.sleep = time.Duration(seconds) * time.Second
	p.phaseOffset = p.computePhaseOffset(p.uuid)
}

func (p *sleepPredictor) SetJitterRatio(r float64) {
	if p == nil {
		return
	}
	if r < 0 {
		r = 0
	}
	if r > 0.5 {
		r = 0.5
	}
	p.mu.Lock()
	p.jitterRatio = r
	p.mu.Unlock()
}
