package manager

import (
	"context"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"codeberg.org/agnoie/shepherd/pkg/utils"
)

const (
	mailboxCapacitySmall  = 64
	mailboxCapacityMedium = 128
	mailboxCapacityLarge  = 256
)

type sshManager struct {
	SSHMessChan chan interface{}
	mailbox     *utils.Mailbox
}

func newSSHManager() *sshManager {
	manager := new(sshManager)
	manager.SSHMessChan = make(chan interface{})
	manager.mailbox = utils.NewMailbox("agent-ssh", manager.SSHMessChan, mailboxCapacityMedium)
	return manager
}

func (manager *sshManager) Enqueue(ctx context.Context, payload interface{}) (bool, error) {
	if manager == nil || manager.mailbox == nil {
		return false, utils.ErrMailboxClosed
	}
	return manager.mailbox.Enqueue(ctx, payload)
}

func (manager *sshManager) Close() {
	if manager == nil {
		return
	}
	if manager.mailbox != nil {
		manager.mailbox.Close()
		manager.mailbox = nil
	}
}

type shellManager struct {
	ShellMessChan chan interface{}
	mailbox       *utils.Mailbox
	sessionMu     sync.Mutex
	sessions      map[string]*ShellSession
	idleTimeout   time.Duration
	// 将 shell sessionID 映射到 streamID，用于输出走 STREAM_DATA。
	streamBySessionID map[string]uint32
	seqBySessionID    map[string]uint32
}

func newShellManager() *shellManager {
	manager := new(shellManager)
	manager.ShellMessChan = make(chan interface{})
	manager.mailbox = utils.NewMailbox("agent-shell", manager.ShellMessChan, mailboxCapacityLarge)
	manager.sessions = make(map[string]*ShellSession)
	manager.idleTimeout = 10 * time.Minute
	manager.streamBySessionID = make(map[string]uint32)
	manager.seqBySessionID = make(map[string]uint32)
	return manager
}

func (manager *shellManager) Enqueue(ctx context.Context, payload interface{}) (bool, error) {
	if manager == nil || manager.mailbox == nil {
		return false, utils.ErrMailboxClosed
	}
	return manager.mailbox.Enqueue(ctx, payload)
}

func (manager *shellManager) Close() {
	if manager == nil {
		return
	}
	if manager.mailbox != nil {
		manager.mailbox.Close()
		manager.mailbox = nil
	}
}

// SetStreamForSession 记录 sessionID 对应的 streamID
func (manager *shellManager) SetStreamForSession(sessionID string, streamID uint32) {
	if manager == nil || sessionID == "" || streamID == 0 {
		return
	}
	manager.sessionMu.Lock()
	if manager.streamBySessionID == nil {
		manager.streamBySessionID = make(map[string]uint32)
	}
	manager.streamBySessionID[sessionID] = streamID
	manager.sessionMu.Unlock()
}

// StreamForSession 返回 sessionID 绑定的 streamID（0 表示无绑定）
func (manager *shellManager) StreamForSession(sessionID string) uint32 {
	if manager == nil || sessionID == "" {
		return 0
	}
	manager.sessionMu.Lock()
	defer manager.sessionMu.Unlock()
	if id, ok := manager.streamBySessionID[sessionID]; ok {
		return id
	}
	return 0
}

// ClearStreamForSession 清理 sessionID 绑定的 streamID
func (manager *shellManager) ClearStreamForSession(sessionID string) {
	if manager == nil || sessionID == "" {
		return
	}
	manager.sessionMu.Lock()
	if manager.streamBySessionID != nil {
		delete(manager.streamBySessionID, sessionID)
	}
	if manager.seqBySessionID != nil {
		delete(manager.seqBySessionID, sessionID)
	}
	manager.sessionMu.Unlock()
}

// NextSeqForSession 返回自增序号（从 1 开始）
func (manager *shellManager) NextSeqForSession(sessionID string) uint32 {
	if manager == nil || sessionID == "" {
		return 0
	}
	manager.sessionMu.Lock()
	defer manager.sessionMu.Unlock()
	if manager.seqBySessionID == nil {
		manager.seqBySessionID = make(map[string]uint32)
	}
	manager.seqBySessionID[sessionID] = manager.seqBySessionID[sessionID] + 1
	return manager.seqBySessionID[sessionID]
}

type ShellSession struct {
	ID           string
	Cmd          *exec.Cmd
	Process      *os.Process
	Stdin        io.WriteCloser
	Stdout       io.ReadCloser
	PTY          *os.File
	PTYIn        io.WriteCloser
	PTYOut       io.ReadCloser
	PTYControl   PTYControl
	Mode         uint16
	WinRows      uint16
	WinCols      uint16
	Charset      string
	CreatedAt    time.Time
	activityMu   sync.RWMutex
	lastActivity time.Time
	Closed       bool
	CloseReason  string
	Done         chan struct{}
}

// SetLastActivity records the provided timestamp as the latest activity moment.
func (session *ShellSession) SetLastActivity(ts time.Time) {
	if session == nil {
		return
	}
	session.activityMu.Lock()
	session.lastActivity = ts
	session.activityMu.Unlock()
}

// TouchActivity updates the activity timestamp to now.
func (session *ShellSession) TouchActivity() {
	session.SetLastActivity(time.Now())
}

// LastActivity returns the most recently recorded activity time.
func (session *ShellSession) LastActivity() time.Time {
	if session == nil {
		return time.Time{}
	}
	session.activityMu.RLock()
	ts := session.lastActivity
	session.activityMu.RUnlock()
	return ts
}

type PTYControl interface {
	Resize(cols, rows uint16) error
	Close() error
}

func (manager *shellManager) GetSession(id string) (*ShellSession, bool) {
	manager.sessionMu.Lock()
	defer manager.sessionMu.Unlock()
	session, ok := manager.sessions[id]
	return session, ok
}

func (manager *shellManager) SetSession(id string, session *ShellSession) {
	if manager == nil {
		return
	}
	manager.sessionMu.Lock()
	if manager.sessions == nil {
		manager.sessions = make(map[string]*ShellSession)
	}
	manager.sessions[id] = session
	manager.sessionMu.Unlock()
}

func (manager *shellManager) DeleteSession(id, reason string) *ShellSession {
	if manager == nil {
		return nil
	}
	manager.sessionMu.Lock()
	defer manager.sessionMu.Unlock()
	session, ok := manager.sessions[id]
	if ok {
		delete(manager.sessions, id)
		if session != nil {
			session.Closed = true
			if reason != "" {
				session.CloseReason = reason
			}
		}
	}
	return session
}

func (manager *shellManager) IdleTimeout() time.Duration {
	if manager == nil {
		return 0
	}
	return manager.idleTimeout
}

func (manager *shellManager) SetIdleTimeout(timeout time.Duration) {
	if manager == nil {
		return
	}
	manager.sessionMu.Lock()
	manager.idleTimeout = timeout
	manager.sessionMu.Unlock()
}

type listenManager struct {
	ListenMessChan chan interface{}
	childMu        sync.Mutex
	childWaiters   map[string]chan string
	mailbox        *utils.Mailbox
	activeMu       sync.Mutex
	active         map[string]context.CancelFunc
}

func newListenManager() *listenManager {
	manager := new(listenManager)
	manager.ListenMessChan = make(chan interface{})
	manager.childWaiters = make(map[string]chan string)
	manager.mailbox = utils.NewMailbox("agent-listen", manager.ListenMessChan, mailboxCapacitySmall)
	return manager
}

func (manager *listenManager) Enqueue(ctx context.Context, payload interface{}) (bool, error) {
	if manager == nil || manager.mailbox == nil {
		return false, utils.ErrMailboxClosed
	}
	return manager.mailbox.Enqueue(ctx, payload)
}

func (manager *listenManager) Close() {
	if manager == nil {
		return
	}
	if manager.mailbox != nil {
		manager.mailbox.Close()
		manager.mailbox = nil
	}
}

func (manager *listenManager) RegisterChildWaiter(requestID string) chan string {
	if manager == nil || requestID == "" {
		ch := make(chan string, 1)
		close(ch)
		return ch
	}
	manager.childMu.Lock()
	defer manager.childMu.Unlock()
	if manager.childWaiters == nil {
		manager.childWaiters = make(map[string]chan string)
	}
	if existing, ok := manager.childWaiters[requestID]; ok {
		return existing
	}
	ch := make(chan string, 1)
	manager.childWaiters[requestID] = ch
	return ch
}

func (manager *listenManager) DeliverChildUUID(requestID, uuid string) {
	if manager == nil || requestID == "" {
		return
	}
	manager.childMu.Lock()
	ch, ok := manager.childWaiters[requestID]
	if ok {
		delete(manager.childWaiters, requestID)
	}
	manager.childMu.Unlock()
	if ok {
		ch <- uuid
		close(ch)
	}
}

func (manager *listenManager) CancelChildWaiter(requestID string) {
	if manager == nil || requestID == "" {
		return
	}
	manager.childMu.Lock()
	ch, ok := manager.childWaiters[requestID]
	if ok {
		delete(manager.childWaiters, requestID)
	}
	manager.childMu.Unlock()
	if ok {
		close(ch)
	}
}

func (manager *listenManager) RegisterActive(id string, cancel context.CancelFunc) {
	if manager == nil || id == "" || cancel == nil {
		return
	}
	manager.activeMu.Lock()
	if manager.active == nil {
		manager.active = make(map[string]context.CancelFunc)
	}
	manager.active[strings.ToLower(id)] = cancel
	manager.activeMu.Unlock()
}

func (manager *listenManager) StopActive(id string) bool {
	if manager == nil || id == "" {
		return false
	}
	manager.activeMu.Lock()
	cancel := manager.active[strings.ToLower(id)]
	if cancel != nil {
		delete(manager.active, strings.ToLower(id))
	}
	manager.activeMu.Unlock()
	if cancel != nil {
		cancel()
		return true
	}
	return false
}

func (manager *listenManager) ClearActive(id string) {
	if manager == nil || id == "" {
		return
	}
	manager.activeMu.Lock()
	delete(manager.active, strings.ToLower(id))
	manager.activeMu.Unlock()
}

type offlineManager struct {
	OfflineMessChan chan interface{}
	mailbox         *utils.Mailbox
}

func newOfflineManager() *offlineManager {
	manager := new(offlineManager)
	manager.OfflineMessChan = make(chan interface{})
	manager.mailbox = utils.NewMailbox("agent-offline", manager.OfflineMessChan, mailboxCapacitySmall)
	return manager
}

func (manager *offlineManager) Enqueue(ctx context.Context, payload interface{}) (bool, error) {
	if manager == nil || manager.mailbox == nil {
		return false, utils.ErrMailboxClosed
	}
	return manager.mailbox.Enqueue(ctx, payload)
}

func (manager *offlineManager) Close() {
	if manager == nil {
		return
	}
	if manager.mailbox != nil {
		manager.mailbox.Close()
		manager.mailbox = nil
	}
}
