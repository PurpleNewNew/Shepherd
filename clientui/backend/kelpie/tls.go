package kelpie

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
)

// TOFUVerifier 实现 "Trust On First Use" 的证书校验：
//   - 若本地没有记录过该 endpoint 的指纹，则首次会收集到 LastSeen 里，连接被拒绝，
//     由上层根据用户操作决定是否调用 store.TrustFingerprint() 信任。
//   - 若已经记录过指纹，则要求本次握手的指纹必须匹配，否则连接失败。
//
// 这是一个为 gRPC dial 准备的 tls.Config.VerifyPeerCertificate 回调。
type TOFUVerifier struct {
	expected string
	lastSeen atomic.Value // string
}

// NewTOFUVerifier 创建一个 TOFU 校验器。
// expected 为期望的 SHA256 指纹（大写 hex，无分隔符）；为空时表示尚未记录，首次连接总是会被拒绝。
func NewTOFUVerifier(expected string) *TOFUVerifier {
	return &TOFUVerifier{expected: normalizeFingerprint(expected)}
}

// LastSeen 返回本次（或最近一次）握手中观察到的对端证书指纹。
// 若连接尚未发生任何握手则返回空串。
func (v *TOFUVerifier) LastSeen() string {
	if v == nil {
		return ""
	}
	if raw, ok := v.lastSeen.Load().(string); ok {
		return raw
	}
	return ""
}

// Verify 作为 tls.Config.VerifyPeerCertificate 使用。
// 策略：
//  1. 总是记录第一份 cert（leaf）的 sha256 指纹到 LastSeen。
//  2. 若 expected 为空，返回 ErrTOFUUnrecorded，让上层引导用户确认。
//  3. 若 expected 不匹配 leaf 指纹，返回 ErrTOFUMismatch。
//  4. 匹配则放行。
func (v *TOFUVerifier) Verify(rawCerts [][]byte, _ [][]*x509.Certificate) error {
	if v == nil {
		return errors.New("nil TOFU verifier")
	}
	if len(rawCerts) == 0 {
		return errors.New("server presented no certificate")
	}
	// 只校验 leaf 证书（rawCerts[0]）。
	sum := sha256.Sum256(rawCerts[0])
	fp := strings.ToUpper(hex.EncodeToString(sum[:]))
	v.lastSeen.Store(fp)
	if v.expected == "" {
		return &TOFUDecisionRequired{Fingerprint: fp}
	}
	if fp != v.expected {
		return &TOFUMismatch{Expected: v.expected, Got: fp}
	}
	return nil
}

// TLSConfig 生成一个供 gRPC dial 使用的 tls.Config，禁用系统 CA 校验，只依赖 TOFU 指纹。
func (v *TOFUVerifier) TLSConfig(serverName string) *tls.Config {
	return &tls.Config{
		ServerName:            serverName,
		InsecureSkipVerify:    true,
		VerifyPeerCertificate: v.Verify,
		MinVersion:            tls.VersionTLS12,
	}
}

// normalizeFingerprint 将可能包含冒号/空白/大小写的指纹规整为纯大写 hex。
func normalizeFingerprint(raw string) string {
	if raw == "" {
		return ""
	}
	s := strings.Map(func(r rune) rune {
		switch {
		case r >= '0' && r <= '9', r >= 'A' && r <= 'F', r >= 'a' && r <= 'f':
			return r
		default:
			return -1
		}
	}, raw)
	return strings.ToUpper(s)
}

// FormatFingerprint 将 64 位 hex 按每 2 个字符加冒号分隔，便于人读显示。
func FormatFingerprint(fp string) string {
	fp = normalizeFingerprint(fp)
	if len(fp) == 0 {
		return ""
	}
	parts := make([]string, 0, len(fp)/2)
	for i := 0; i < len(fp); i += 2 {
		end := i + 2
		if end > len(fp) {
			end = len(fp)
		}
		parts = append(parts, fp[i:end])
	}
	return strings.Join(parts, ":")
}

// -------- 错误类型 --------

// TOFUDecisionRequired 表示本地尚未记录过这个 endpoint 的指纹，需要用户确认。
// UI 上应当提示“首次连接，请确认指纹”并提供信任按钮。
type TOFUDecisionRequired struct {
	Fingerprint string
}

func (e *TOFUDecisionRequired) Error() string {
	return fmt.Sprintf("tofu: fingerprint unrecorded (got %s); user confirmation required", e.Fingerprint)
}

// TOFUMismatch 表示本地记录的指纹与本次握手不匹配，可能是配置变更或中间人。
type TOFUMismatch struct {
	Expected string
	Got      string
}

func (e *TOFUMismatch) Error() string {
	return fmt.Sprintf("tofu: fingerprint mismatch (expected %s, got %s)", e.Expected, e.Got)
}

// IsTOFUDecisionRequired 判断 error 是否为首次连接待确认。
func IsTOFUDecisionRequired(err error) (*TOFUDecisionRequired, bool) {
	var d *TOFUDecisionRequired
	if errors.As(err, &d) {
		return d, true
	}
	return nil, false
}

// IsTOFUMismatch 判断 error 是否为指纹不匹配。
func IsTOFUMismatch(err error) (*TOFUMismatch, bool) {
	var d *TOFUMismatch
	if errors.As(err, &d) {
		return d, true
	}
	return nil, false
}
