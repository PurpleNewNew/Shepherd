package process

import (
	"fmt"
	"math"
	"math/rand"
	"time"

	"codeberg.org/agnoie/shepherd/internal/flock/initial"
	"codeberg.org/agnoie/shepherd/protocol"
)

type sleepConfig struct {
	sleepSeconds int
	workSeconds  int
	jitter       float64
}

const (
	criticalSleepFactor   = 0.6
	criticalJitterFactor  = 0.4
	criticalNeighborFloor = 3
)

// startSleepManager 会启动一个 goroutine，跟踪当前 sleep 配置，
// 并据此进入或离开睡眠窗口。
func (agent *Agent) startSleepManager() {
	if agent == nil {
		return
	}
	agent.sleepManagerOnce.Do(func() {
		go agent.sleepLoop()
	})
}

func (agent *Agent) sleepLoop() {
	if agent == nil {
		return
	}
	agent.noteActivity()
	ctx := agent.context()
	for {
		if ctx.Err() != nil {
			return
		}
		sleepSeconds, workSeconds, jitter := agent.currentSleepConfig()
		if sleepSeconds <= 0 {
			time.Sleep(time.Second)
			continue
		}
		// 等到节点至少空闲满 workSeconds。
		// 注意：等待期间 lastActivity 可能继续前进；睡眠前要再次检查，
		// 以免在有活跃流量时误关上游连接。
		for {
			agent.sleepMu.Lock()
			last := agent.lastActivity
			agent.sleepMu.Unlock()
			deadline := last.Add(time.Duration(workSeconds) * time.Second)
			if d := time.Until(deadline); d > 0 {
				timer := time.NewTimer(d)
				select {
				case <-ctx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
			}
			agent.sleepMu.Lock()
			latest := agent.lastActivity
			agent.sleepMu.Unlock()
			if latest.After(last) {
				continue
			}
			break
		}
		if cfg := agent.loadSleepConfig(); cfg.sleepSeconds <= 0 {
			continue
		}
		// 如果最近刚发生关键控制面操作（如 sleep update、rescue 等），
		// 即使此刻其余部分都空闲，也要在短暂宽限期内保持上游会话存活。
		agent.sleepMu.Lock()
		graceUntil := agent.sleepGraceUntil
		agent.sleepMu.Unlock()
		now := time.Now()
		if !graceUntil.IsZero() && graceUntil.After(now) {
			timer := time.NewTimer(time.Until(graceUntil))
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			continue
		}
		factor := 1.0
		if jitter > 0 {
			// 抖动范围：[-jitter, +jitter]
			factor += (rand.Float64()*2 - 1) * jitter
		}
		sleepDur := time.Duration(sleepSeconds) * time.Second
		until := time.Now().Add(time.Duration(float64(sleepDur) * factor))
		agent.sleepMu.Lock()
		agent.sleepingUntil = until
		agent.sleepMu.Unlock()
		if sess := agent.currentSession(); sess != nil && sess.Conn() != nil {
			_ = sess.Conn().Close()
		}
		for {
			if ctx.Err() != nil {
				return
			}
			if cfg := agent.loadSleepConfig(); cfg.sleepSeconds <= 0 {
				agent.sleepMu.Lock()
				agent.sleepingUntil = time.Time{}
				agent.sleepMu.Unlock()
				break
			}
			agent.sleepMu.Lock()
			wake := agent.sleepingUntil
			agent.sleepMu.Unlock()
			now := time.Now()
			if wake.IsZero() || now.After(wake) {
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
		agent.noteActivity()
	}
}

func (agent *Agent) currentSleepConfig() (sleepSeconds int, workSeconds int, jitter float64) {
	cfg := agent.loadSleepConfig()
	return cfg.sleepSeconds, cfg.workSeconds, cfg.jitter
}

func (agent *Agent) applySleepUpdate(update *protocol.SleepUpdate) error {
	if agent == nil || update == nil {
		return fmt.Errorf("invalid sleep update")
	}
	if update.Flags == 0 {
		return fmt.Errorf("sleep update missing fields")
	}
	if agent.options == nil {
		agent.options = &initial.Options{}
	}
	changed := false
	if update.Flags&protocol.SleepUpdateFlagSleepSeconds != 0 {
		if update.SleepSeconds < 0 {
			return fmt.Errorf("sleep seconds must be >= 0")
		}
		agent.options.SleepSeconds = int(update.SleepSeconds)
		if update.SleepSeconds <= 0 {
			agent.sleepMu.Lock()
			agent.sleepingUntil = time.Time{}
			agent.sleepMu.Unlock()
		}
		changed = true
	}
	if update.Flags&protocol.SleepUpdateFlagWorkSeconds != 0 {
		if update.WorkSeconds < 0 {
			return fmt.Errorf("work seconds must be >= 0")
		}
		agent.options.WorkSeconds = int(update.WorkSeconds)
		changed = true
	}
	if update.Flags&protocol.SleepUpdateFlagJitter != 0 {
		agent.options.SleepJitter = clampJitterValue(float64(update.JitterPermille) / 1000)
		changed = true
	}
	if !changed {
		return fmt.Errorf("sleep update made no changes")
	}
	agent.publishSleepConfig()
	agent.rebuildSleepPredictor()
	if agent.gossipMgr != nil {
		sleepSeconds, _, _ := agent.currentSleepConfig()
		agent.gossipMgr.SetSleepSeconds(int32(sleepSeconds))
		agent.triggerGossipUpdate()
	}
	return nil
}

func (agent *Agent) rebuildSleepPredictor() {
	if agent == nil {
		return
	}
	agent.sleepPredMu.Lock()
	defer agent.sleepPredMu.Unlock()
	cfg := agent.loadSleepConfig()
	if cfg.sleepSeconds <= 0 {
		agent.sleepPredictor = nil
		return
	}
	if agent.sleepPredictor == nil {
		agent.sleepPredictor = newSleepPredictor(agent.UUID, cfg.sleepSeconds)
	} else {
		agent.sleepPredictor.UpdateSleepSeconds(cfg.sleepSeconds)
	}
	agent.sleepPredictor.SetJitterRatio(cfg.jitter)
	agent.sleepPredictor.UpdateUUID(agent.UUID)
}

func (agent *Agent) publishSleepConfig() {
	if agent == nil {
		return
	}
	cfg := sleepConfig{workSeconds: 10}
	if agent.options != nil {
		cfg.sleepSeconds = agent.options.SleepSeconds
		cfg.workSeconds = agent.options.WorkSeconds
		if cfg.workSeconds <= 0 {
			cfg.workSeconds = 10
		}
		cfg.jitter = clampJitterValue(agent.options.SleepJitter)
	}
	agent.sleepCfg.Store(cfg)
}

func (agent *Agent) loadSleepConfig() sleepConfig {
	if agent == nil {
		return sleepConfig{workSeconds: 10}
	}
	cfg := sleepConfig{workSeconds: 10}
	if stored, ok := agent.sleepCfg.Load().(sleepConfig); ok {
		cfg = stored
	}
	if cfg.workSeconds <= 0 {
		cfg.workSeconds = 10
	}
	return agent.applyCriticalSleepOverrides(cfg)
}

func clampJitterValue(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 0.5 {
		return 0.5
	}
	return v
}

func (agent *Agent) applyCriticalSleepOverrides(cfg sleepConfig) sleepConfig {
	if cfg.sleepSeconds <= 0 {
		return cfg
	}
	if !agent.isCriticalNode() {
		return cfg
	}
	adjusted := int(math.Max(1, float64(cfg.sleepSeconds)*criticalSleepFactor))
	if adjusted < cfg.sleepSeconds {
		cfg.sleepSeconds = adjusted
	}
	cfg.jitter = clampJitterValue(cfg.jitter * criticalJitterFactor)
	return cfg
}

func (agent *Agent) isCriticalNode() bool {
	if agent == nil {
		return false
	}
	if len(agent.currentChildren()) > 0 {
		return true
	}
	if agent.pendingFailoverCount() > 0 {
		return true
	}
	agent.neighborsMu.RLock()
	neighborCount := len(agent.neighbors)
	extra := len(agent.extraNeighbors)
	agent.neighborsMu.RUnlock()
	if neighborCount >= criticalNeighborFloor || extra > 0 {
		return true
	}
	return false
}
