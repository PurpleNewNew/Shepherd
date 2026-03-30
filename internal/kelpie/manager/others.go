package manager

import (
	"context"
	"strings"
	"sync"

	"codeberg.org/agnoie/shepherd/pkg/utils"
	"codeberg.org/agnoie/shepherd/protocol"
)

const (
	mailboxCapacitySmall  = 64
	mailboxCapacityMedium = 128
	mailboxCapacityLarge  = 256
)

type consoleManager struct {
	OK   chan bool
	Exit chan bool
}

func newConsoleManager() *consoleManager {
	manager := new(consoleManager)
	manager.OK = make(chan bool, 1)
	manager.Exit = make(chan bool, 1)
	return manager
}

type sshManager struct {
	SSHMessChan chan interface{}
	mailbox     *utils.Mailbox
}

func newSSHManager() *sshManager {
	manager := new(sshManager)
	manager.SSHMessChan = make(chan interface{})
	manager.mailbox = utils.NewMailbox("admin-ssh", manager.SSHMessChan, mailboxCapacityMedium)
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
	sessions      map[string]string
	sessionsByID  map[string]string
	sessionModes  map[string]uint16
	modesByID     map[string]uint16
	pending       map[string]pendingShell
}

func newShellManager() *shellManager {
	manager := new(shellManager)
	manager.ShellMessChan = make(chan interface{})
	manager.mailbox = utils.NewMailbox("admin-shell", manager.ShellMessChan, mailboxCapacityLarge)
	manager.sessions = make(map[string]string)
	manager.sessionsByID = make(map[string]string)
	manager.sessionModes = make(map[string]uint16)
	manager.modesByID = make(map[string]uint16)
	manager.pending = make(map[string]pendingShell)
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

type pendingShell struct {
	uuid  string
	route string
	mode  uint16
}

func (manager *shellManager) SessionID(uuid string) string {
	if manager == nil {
		return ""
	}
	manager.sessionMu.Lock()
	defer manager.sessionMu.Unlock()
	return manager.sessions[uuid]
}

func (manager *shellManager) SetSessionID(uuid, sessionID string) {
	if manager == nil || uuid == "" {
		return
	}
	manager.sessionMu.Lock()
	if manager.sessions == nil {
		manager.sessions = make(map[string]string)
	}
	if manager.sessionsByID == nil {
		manager.sessionsByID = make(map[string]string)
	}
	if manager.sessionModes == nil {
		manager.sessionModes = make(map[string]uint16)
	}
	if manager.modesByID == nil {
		manager.modesByID = make(map[string]uint16)
	}
	manager.sessions[uuid] = sessionID
	if sessionID != "" {
		manager.sessionsByID[sessionID] = uuid
		if mode, ok := manager.sessionModes[uuid]; ok {
			manager.modesByID[sessionID] = mode
		}
	}
	manager.sessionMu.Unlock()
}

func (manager *shellManager) ClearSessionID(uuid string) {
	if manager == nil || uuid == "" {
		return
	}
	manager.sessionMu.Lock()
	if manager.sessions != nil {
		if sessionID, ok := manager.sessions[uuid]; ok {
			delete(manager.sessionsByID, sessionID)
			delete(manager.modesByID, sessionID)
		}
		delete(manager.sessions, uuid)
	}
	if manager.sessionModes != nil {
		delete(manager.sessionModes, uuid)
	}
	manager.sessionMu.Unlock()
}

func (manager *shellManager) ClearSessionByID(sessionID string) {
	if manager == nil || sessionID == "" {
		return
	}
	manager.sessionMu.Lock()
	if manager.sessionsByID != nil {
		if uuid, ok := manager.sessionsByID[sessionID]; ok {
			delete(manager.sessions, uuid)
			delete(manager.sessionsByID, sessionID)
			delete(manager.sessionModes, uuid)
		}
	}
	if manager.modesByID != nil {
		delete(manager.modesByID, sessionID)
	}
	if manager.pending != nil {
		delete(manager.pending, sessionID)
	}
	manager.sessionMu.Unlock()
}

func (manager *shellManager) SetPending(uuid, route, sessionID string, mode uint16) {
	if manager == nil || uuid == "" || sessionID == "" {
		return
	}
	manager.sessionMu.Lock()
	if manager.pending == nil {
		manager.pending = make(map[string]pendingShell)
	}
	manager.pending[sessionID] = pendingShell{
		uuid:  uuid,
		route: route,
		mode:  mode,
	}
	manager.sessionMu.Unlock()
}

func (manager *shellManager) ConfirmPending(sessionID string, mode uint16) (uuid string) {
	if manager == nil {
		return ""
	}
	if sessionID == "" {
		return manager.confirmAnyPending(mode)
	}
	manager.sessionMu.Lock()
	defer manager.sessionMu.Unlock()
	if manager.pending == nil {
		return ""
	}
	pending, ok := manager.pending[sessionID]
	if !ok {
		return ""
	}
	delete(manager.pending, sessionID)
	if manager.sessions == nil {
		manager.sessions = make(map[string]string)
	}
	if manager.sessionsByID == nil {
		manager.sessionsByID = make(map[string]string)
	}
	if manager.sessionModes == nil {
		manager.sessionModes = make(map[string]uint16)
	}
	if manager.modesByID == nil {
		manager.modesByID = make(map[string]uint16)
	}
	manager.sessions[pending.uuid] = sessionID
	manager.sessionsByID[sessionID] = pending.uuid
	manager.sessionModes[pending.uuid] = mode
	manager.modesByID[sessionID] = mode
	return pending.uuid
}

func (manager *shellManager) FailPending(sessionID string) (uuid, route string, mode uint16) {
	if manager == nil {
		return "", "", 0
	}
	if sessionID == "" {
		return manager.failAnyPending()
	}
	manager.sessionMu.Lock()
	defer manager.sessionMu.Unlock()
	if manager.pending == nil {
		return "", "", 0
	}
	if pending, ok := manager.pending[sessionID]; ok {
		delete(manager.pending, sessionID)
		return pending.uuid, pending.route, pending.mode
	}
	return "", "", 0
}

func (manager *shellManager) confirmAnyPending(mode uint16) string {
	manager.sessionMu.Lock()
	defer manager.sessionMu.Unlock()
	if manager.pending == nil {
		return ""
	}
	for sessionID, pending := range manager.pending {
		delete(manager.pending, sessionID)
		return pending.uuid
	}
	return ""
}

func (manager *shellManager) failAnyPending() (string, string, uint16) {
	manager.sessionMu.Lock()
	defer manager.sessionMu.Unlock()
	for sessionID, pending := range manager.pending {
		delete(manager.pending, sessionID)
		return pending.uuid, pending.route, pending.mode
	}
	return "", "", 0
}

func (manager *shellManager) SetSessionMode(uuid string, mode uint16) {
	if manager == nil || uuid == "" {
		return
	}
	manager.sessionMu.Lock()
	if manager.sessionModes == nil {
		manager.sessionModes = make(map[string]uint16)
	}
	manager.sessionModes[uuid] = mode
	if sessionID, ok := manager.sessions[uuid]; ok {
		if manager.modesByID == nil {
			manager.modesByID = make(map[string]uint16)
		}
		manager.modesByID[sessionID] = mode
	}
	manager.sessionMu.Unlock()
}

func (manager *shellManager) SessionMode(uuid string) uint16 {
	if manager == nil || uuid == "" {
		return protocol.ShellModePipe
	}
	manager.sessionMu.Lock()
	defer manager.sessionMu.Unlock()
	if mode, ok := manager.sessionModes[uuid]; ok {
		return mode
	}
	return protocol.ShellModePipe
}

func (manager *shellManager) SessionModeByID(sessionID string) uint16 {
	if manager == nil || sessionID == "" {
		return protocol.ShellModePipe
	}
	manager.sessionMu.Lock()
	defer manager.sessionMu.Unlock()
	if mode, ok := manager.modesByID[sessionID]; ok {
		return mode
	}
	return protocol.ShellModePipe
}

type infoManager struct {
	InfoMessChan chan interface{}
	mailbox      *utils.Mailbox
}

func newInfoManager() *infoManager {
	manager := new(infoManager)
	manager.InfoMessChan = make(chan interface{})
	manager.mailbox = utils.NewMailbox("admin-info", manager.InfoMessChan, mailboxCapacitySmall)
	return manager
}

func (manager *infoManager) Enqueue(ctx context.Context, payload interface{}) (bool, error) {
	if manager == nil || manager.mailbox == nil {
		return false, utils.ErrMailboxClosed
	}
	return manager.mailbox.Enqueue(ctx, payload)
}

func (manager *infoManager) Close() {
	if manager == nil {
		return
	}
	if manager.mailbox != nil {
		manager.mailbox.Close()
		manager.mailbox = nil
	}
}

type listenManager struct {
	ListenMessChan chan interface{}
	mailbox        *utils.Mailbox

	ackMu      sync.Mutex
	ackWaiters map[string]chan bool
}

func newListenManager() *listenManager {
	manager := new(listenManager)
	manager.ListenMessChan = make(chan interface{})
	manager.mailbox = utils.NewMailbox("admin-listen", manager.ListenMessChan, mailboxCapacitySmall)
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

func (manager *listenManager) RegisterAck(id string) chan bool {
	ch := make(chan bool, 1)
	if manager == nil || strings.TrimSpace(id) == "" {
		close(ch)
		return ch
	}
	manager.ackMu.Lock()
	if manager.ackWaiters == nil {
		manager.ackWaiters = make(map[string]chan bool)
	}
	manager.ackWaiters[strings.ToLower(id)] = ch
	manager.ackMu.Unlock()
	return ch
}

func (manager *listenManager) DeliverAck(id string, ok bool) {
	if manager == nil || strings.TrimSpace(id) == "" {
		return
	}
	manager.ackMu.Lock()
	ch := manager.ackWaiters[strings.ToLower(id)]
	if ch != nil {
		delete(manager.ackWaiters, strings.ToLower(id))
	}
	manager.ackMu.Unlock()
	if ch != nil {
		ch <- ok
		close(ch)
	}
}

type connectManager struct {
	ConnectMessChan chan interface{}
	mailbox         *utils.Mailbox
	ackMu           sync.Mutex
	ackWaiters      []chan bool
}

func newConnectManager() *connectManager {
	manager := new(connectManager)
	manager.ConnectMessChan = make(chan interface{})
	manager.mailbox = utils.NewMailbox("admin-connect", manager.ConnectMessChan, mailboxCapacitySmall)
	return manager
}

func (manager *connectManager) Enqueue(ctx context.Context, payload interface{}) (bool, error) {
	if manager == nil || manager.mailbox == nil {
		return false, utils.ErrMailboxClosed
	}
	return manager.mailbox.Enqueue(ctx, payload)
}

func (manager *connectManager) Close() {
	if manager == nil {
		return
	}
	if manager.mailbox != nil {
		manager.mailbox.Close()
		manager.mailbox = nil
	}
}

func (manager *connectManager) RegisterAck() chan bool {
	ch := make(chan bool, 1)
	if manager == nil {
		close(ch)
		return ch
	}
	manager.ackMu.Lock()
	manager.ackWaiters = append(manager.ackWaiters, ch)
	manager.ackMu.Unlock()
	return ch
}

func (manager *connectManager) DeliverAck(ok bool) {
	if manager == nil {
		return
	}
	manager.ackMu.Lock()
	var ch chan bool
	if len(manager.ackWaiters) > 0 {
		ch = manager.ackWaiters[0]
		manager.ackWaiters = manager.ackWaiters[1:]
	}
	manager.ackMu.Unlock()
	if ch != nil {
		ch <- ok
		close(ch)
	}
}

func (manager *connectManager) UnregisterAck(ch chan bool) {
	if manager == nil || ch == nil {
		return
	}
	manager.ackMu.Lock()
	for i, waiter := range manager.ackWaiters {
		if waiter == ch {
			manager.ackWaiters = append(manager.ackWaiters[:i], manager.ackWaiters[i+1:]...)
			close(ch)
			break
		}
	}
	manager.ackMu.Unlock()
}

type childrenManager struct {
	ChildrenMessChan chan interface{}
	mailbox          *utils.Mailbox
}

func newchildrenManager() *childrenManager {
	manager := new(childrenManager)
	manager.ChildrenMessChan = make(chan interface{})
	manager.mailbox = utils.NewMailbox("admin-children", manager.ChildrenMessChan, mailboxCapacitySmall)
	return manager
}

func (manager *childrenManager) Enqueue(ctx context.Context, payload interface{}) (bool, error) {
	if manager == nil || manager.mailbox == nil {
		return false, utils.ErrMailboxClosed
	}
	return manager.mailbox.Enqueue(ctx, payload)
}

func (manager *childrenManager) Close() {
	if manager == nil {
		return
	}
	if manager.mailbox != nil {
		manager.mailbox.Close()
		manager.mailbox = nil
	}
}
