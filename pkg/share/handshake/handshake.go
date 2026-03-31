package handshake

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"strings"
	"time"

	"codeberg.org/agnoie/shepherd/pkg/share"
	"codeberg.org/agnoie/shepherd/protocol"
)

// Role 表示握手消息的发送方身份。
type Role int

const (
	RoleAgent Role = iota
	RoleAdmin
)

var (
	adminGreetings = []string{"Keep silent"}
	agentGreetings = []string{"Shhh...", "Shh...", "Shhh."}
)

// Code 表示结构化握手阶段标识。
type Code string

const (
	CodeStart     Code = "start"
	CodeDial      Code = "dial"
	CodeTLS       Code = "tls"
	CodeNegotiate Code = "negotiate"
	CodePreAuth   Code = "preauth"
	CodeMFA       Code = "mfa"
	CodeExchange  Code = "exchange"
	CodeComplete  Code = "complete"
)

// Event 描述握手过程中的关键事件。
type Event struct {
	Time  time.Time
	Stage Code
	Error string
}

// Transcript 保存结构化的握手事件，便于日志或追踪。
type Transcript struct {
	events []Event
}

// NewTranscript 创建空的握手 transcript。
func NewTranscript() *Transcript {
	return &Transcript{events: make([]Event, 0, 8)}
}

// Record 向 transcript 添加事件。
func (t *Transcript) Record(stage Code, err error) {
	if t == nil {
		return
	}
	entry := Event{Time: time.Now(), Stage: stage}
	if err != nil {
		entry.Error = err.Error()
	}
	t.events = append(t.events, entry)
}

// Annotate 将 transcript 附加到错误中，便于排障。
func (t *Transcript) Annotate(err error) error {
	if err == nil || t == nil {
		return err
	}
	return fmt.Errorf("%w (trace=%s)", err, t.String())
}

// String 返回简要的事件串。
func (t *Transcript) String() string {
	if t == nil || len(t.events) == 0 {
		return ""
	}
	parts := make([]string, 0, len(t.events))
	for _, evt := range t.events {
		if evt.Error != "" {
			parts = append(parts, fmt.Sprintf("%s!%s", evt.Stage, evt.Error))
		} else {
			parts = append(parts, string(evt.Stage))
		}
	}
	return strings.Join(parts, ">")
}

// NewHIMess 根据角色构造标准 HI 消息模板。
func NewHIMess(role Role, version, flags uint16, isReconnect bool) *protocol.HIMess {
	uuid := protocol.TEMP_UUID
	isAdmin := uint16(0)
	switch role {
	case RoleAdmin:
		uuid = protocol.ADMIN_UUID
		isAdmin = 1
	}
	greet := RandomGreeting(role)
	return &protocol.HIMess{
		GreetingLen:  uint16(len(greet)),
		Greeting:     greet,
		UUIDLen:      uint16(len(uuid)),
		UUID:         uuid,
		IsAdmin:      isAdmin,
		IsReconnect:  boolFlag(isReconnect),
		ProtoVersion: version,
		ProtoFlags:   flags,
	}
}

// HandshakeSecret 计算握手阶段使用的密钥。
func HandshakeSecret(secret string, _ bool) string {
	return secret
}

// ValidateSecretComplexity 确保密钥同时包含字母与数字且长度>=8。
func ValidateSecretComplexity(secret string) error {
	if len(secret) < 8 {
		return fmt.Errorf("secret must be at least 8 characters")
	}
	hasLetter := false
	hasDigit := false
	for _, r := range secret {
		if r >= '0' && r <= '9' {
			hasDigit = true
		} else if r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z' {
			hasLetter = true
		}
	}
	if !hasLetter || !hasDigit {
		return fmt.Errorf("secret must contain letters and digits")
	}
	return nil
}

// ValidateMFAPin 校验多因素 PIN（仅数字且长度>=4）。
func ValidateMFAPin(pin string) error {
	if pin == "" {
		return nil
	}
	if len(pin) < 4 {
		return fmt.Errorf("mfa pin must be at least 4 digits")
	}
	for _, r := range pin {
		if r < '0' || r > '9' {
			return fmt.Errorf("mfa pin must contain digits only")
		}
	}
	return nil
}

// SessionSecret 返回会话期间使用的加密密钥。
func SessionSecret(secret string, tlsEnabled bool) string {
	return share.DeriveSessionSecret(secret, tlsEnabled)
}

// RandomGreeting 返回角色对应的随机问候语，减少固定指纹；若随机失败回退第一个。
func RandomGreeting(role Role) string {
	switch role {
	case RoleAdmin:
		return pickGreeting(adminGreetings)
	default:
		return pickGreeting(agentGreetings)
	}
}

// ValidGreeting 校验收到的问候是否属于允许集合。
func ValidGreeting(role Role, g string) bool {
	g = strings.TrimSpace(g)
	if g == "" {
		return false
	}
	var list []string
	switch role {
	case RoleAdmin:
		list = adminGreetings
	default:
		list = agentGreetings
	}
	for _, candidate := range list {
		if strings.EqualFold(candidate, g) {
			return true
		}
	}
	return false
}

func pickGreeting(list []string) string {
	if len(list) == 0 {
		return ""
	}
	if len(list) == 1 {
		return list[0]
	}
	idx, err := rand.Int(rand.Reader, big.NewInt(int64(len(list))))
	if err != nil {
		return list[0]
	}
	return list[idx.Int64()]
}

func boolFlag(v bool) uint16 {
	if v {
		return 1
	}
	return 0
}
