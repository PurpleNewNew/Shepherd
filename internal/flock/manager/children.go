package manager

import (
	"net"
	"sync"

	"codeberg.org/agnoie/shepherd/pkg/utils"
)

type childrenManager struct {
	mu            sync.RWMutex
	children      map[string]*child
	ChildComeChan chan *ChildInfo
	closeOnce     sync.Once
}

type ChildInfo struct {
	UUID string
	Conn net.Conn
}

type child struct {
	conn net.Conn
}

func newChildrenManager() *childrenManager {
	return &childrenManager{
		children:      make(map[string]*child),
		ChildComeChan: make(chan *ChildInfo),
	}
}

func (manager *childrenManager) AddChild(uuid string, conn net.Conn) {
	if manager == nil || uuid == "" || conn == nil {
		return
	}
	manager.mu.Lock()
	if manager.children == nil {
		manager.children = make(map[string]*child)
	}
	manager.children[uuid] = &child{conn: utils.NewSafeConn(conn)}
	manager.mu.Unlock()
}

func (manager *childrenManager) GetConn(uuid string) (net.Conn, bool) {
	if manager == nil || uuid == "" {
		return nil, false
	}
	manager.mu.RLock()
	child, ok := manager.children[uuid]
	manager.mu.RUnlock()
	if !ok || child == nil || child.conn == nil {
		return nil, false
	}
	return child.conn, true
}

func (manager *childrenManager) AllChildren() []string {
	if manager == nil {
		return nil
	}
	manager.mu.RLock()
	defer manager.mu.RUnlock()
	if len(manager.children) == 0 {
		return nil
	}
	result := make([]string, 0, len(manager.children))
	for uuid := range manager.children {
		result = append(result, uuid)
	}
	return result
}

func (manager *childrenManager) RemoveChild(uuid string) {
	if manager == nil || uuid == "" {
		return
	}
	manager.mu.Lock()
	delete(manager.children, uuid)
	manager.mu.Unlock()
}

func (manager *childrenManager) NotifyChild(info *ChildInfo) {
	if manager == nil || info == nil {
		return
	}
	manager.mu.RLock()
	ch := manager.ChildComeChan
	manager.mu.RUnlock()
	if ch == nil {
		return
	}
	select {
	case ch <- info:
	default:
		go func(ch chan *ChildInfo, payload *ChildInfo) {
			defer func() {
				_ = recover()
			}()
			ch <- payload
		}(ch, info)
	}
}

func (manager *childrenManager) Close() {
	if manager == nil {
		return
	}
	manager.closeOnce.Do(func() {
		close(manager.ChildComeChan)
		manager.mu.Lock()
		manager.children = nil
		manager.ChildComeChan = nil
		manager.mu.Unlock()
	})
}

type LinkProvider interface {
	GetConn(uuid string) (net.Conn, bool)
}

type ConnectionRouter struct {
	providers []LinkProvider
}

func NewConnectionRouter(providers ...LinkProvider) *ConnectionRouter {
	return &ConnectionRouter{providers: providers}
}

func (router *ConnectionRouter) AddProvider(provider LinkProvider) {
	router.providers = append(router.providers, provider)
}

func (router *ConnectionRouter) GetConn(uuid string) (net.Conn, bool) {
	for _, provider := range router.providers {
		if conn, ok := provider.GetConn(uuid); ok {
			return conn, true
		}
	}
	return nil, false
}
