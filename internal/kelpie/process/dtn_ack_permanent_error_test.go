package process

import (
	"strings"
	"testing"
	"time"

	"codeberg.org/agnoie/shepherd/internal/kelpie/dtn"
	"codeberg.org/agnoie/shepherd/protocol"
)

// TestOnDTNAckPermanentErrorDropsBundle 守住 "unsupported payload" 这类
// 永久性错误立即丢弃的约束。历史问题：flock 回 "unsupported payload" 时
// Kelpie 会无限 Requeue，持续占用 session 写路径，使 gossip/heartbeat 被
// 挤掉，从而让其他节点被 markStaleOffline（表现为 UI 上"节点莫名其妙掉线"）。
func TestOnDTNAckPermanentErrorDropsBundle(t *testing.T) {
	admin := &Admin{
		adminDTNState: adminDTNState{
			dtnManager:              dtn.NewManager(dtn.DefaultConfig()),
			dtnInflight:             make(map[string]*dtnInflightRecord),
			dtnMaxInflightPerTarget: 2,
		},
	}

	target := "leaf-node"
	if _, err := admin.dtnManager.Enqueue(
		target,
		[]byte("random text without reserved prefix"),
		dtn.WithTTL(10*time.Minute),
	); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	ready := admin.dtnManager.Ready(time.Now(), 1)
	if len(ready) != 1 {
		t.Fatalf("expected 1 ready bundle, got %d", len(ready))
	}
	bundle := ready[0]
	admin.rememberInflightAt(bundle, time.Now())

	ack := &protocol.DTNAck{
		BundleIDLen: uint16(len(bundle.ID)),
		BundleID:    bundle.ID,
		OK:          0,
		Error:       "unsupported payload",
	}
	admin.onDTNAck(ack)

	if admin.dtnFailed != 1 {
		t.Fatalf("expected dtnFailed=1, got %d", admin.dtnFailed)
	}
	if admin.dtnRetried != 0 {
		t.Fatalf("expected dtnRetried=0 (permanent errors must not requeue), got %d",
			admin.dtnRetried)
	}
	if got := admin.inflightForTarget(target); got != 0 {
		t.Fatalf("expected inflight cleared after permanent drop, got %d", got)
	}
	for _, s := range admin.dtnManager.List(target, 10) {
		if s.ID == bundle.ID {
			t.Fatalf("expected bundle %s dropped, still present in queue", bundle.ID)
		}
	}
}

// TestOnDTNAckTransientErrorStillRequeues 守住另一侧：非永久错误仍然会
// 退避重试，以免 permanent 规则被写得过宽而误杀瞬时故障场景。
func TestOnDTNAckTransientErrorStillRequeues(t *testing.T) {
	admin := &Admin{
		adminDTNState: adminDTNState{
			dtnManager:              dtn.NewManager(dtn.DefaultConfig()),
			dtnInflight:             make(map[string]*dtnInflightRecord),
			dtnMaxInflightPerTarget: 2,
		},
	}

	target := "leaf-node"
	if _, err := admin.dtnManager.Enqueue(
		target,
		[]byte("memo:hi"),
		dtn.WithTTL(10*time.Minute),
	); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	ready := admin.dtnManager.Ready(time.Now(), 1)
	if len(ready) != 1 {
		t.Fatalf("expected 1 ready bundle, got %d", len(ready))
	}
	bundle := ready[0]
	admin.rememberInflightAt(bundle, time.Now())

	ack := &protocol.DTNAck{
		BundleIDLen: uint16(len(bundle.ID)),
		BundleID:    bundle.ID,
		OK:          0,
		Error:       "session temporarily unavailable",
	}
	admin.onDTNAck(ack)

	if admin.dtnRetried != 1 {
		t.Fatalf("expected dtnRetried=1 for transient error, got %d", admin.dtnRetried)
	}
	found := false
	for _, s := range admin.dtnManager.List(target, 10) {
		if s.ID == bundle.ID {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected transient failure to leave bundle queued for retry")
	}
}

func TestIsPermanentDTNAckErrorMatchesFlockErrorStrings(t *testing.T) {
	// 这些 error 字面量都来自 internal/flock/process/router.go::applyDTNPayload。
	// 如果 flock 侧的 error 文案改了，也要同步更新这里的白名单。
	permanent := []string{
		"unsupported payload",
		"Unsupported Payload",
		"unsupported stream subcommand: foo",
		"invalid payload",
		"invalid proto envelope",
		"invalid message type",
		"invalid payload hex",
		"decode payload: unexpected EOF",
	}
	for _, s := range permanent {
		if !isPermanentDTNAckError(s) {
			t.Errorf("expected %q to be permanent", s)
		}
	}
	transient := []string{
		"",
		"session temporarily unavailable",
		"write tcp: broken pipe",
		"context deadline exceeded",
	}
	for _, s := range transient {
		if isPermanentDTNAckError(s) {
			t.Errorf("expected %q to be transient", s)
		}
	}
	// 防御：确保常量字符串小写后确实没有任何白名单项包含空串之类的
	// 迂回匹配（isPermanentDTNAckError 使用 strings.Contains）。
	for _, needle := range dtnPermanentAckErrors {
		if strings.TrimSpace(needle) == "" {
			t.Fatalf("dtnPermanentAckErrors must not contain empty needle")
		}
	}
}
