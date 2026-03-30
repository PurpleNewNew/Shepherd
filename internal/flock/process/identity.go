package process

import "sync"

type agentIdentity struct {
	mu       sync.RWMutex
	parent   string
	username string
	hostname string
}

func (id *agentIdentity) Parent() string {
	if id == nil {
		return ""
	}
	id.mu.RLock()
	defer id.mu.RUnlock()
	return id.parent
}

func (id *agentIdentity) SetParent(uuid string) bool {
	if id == nil || uuid == "" {
		return false
	}
	id.mu.Lock()
	changed := id.parent != uuid
	id.parent = uuid
	id.mu.Unlock()
	return changed
}

func (id *agentIdentity) Username() string {
	if id == nil {
		return ""
	}
	id.mu.RLock()
	defer id.mu.RUnlock()
	return id.username
}

func (id *agentIdentity) Hostname() string {
	if id == nil {
		return ""
	}
	id.mu.RLock()
	defer id.mu.RUnlock()
	return id.hostname
}

func (id *agentIdentity) SetSystemInfo(hostname, username string) bool {
	if id == nil {
		return false
	}
	id.mu.Lock()
	changed := id.hostname != hostname || id.username != username
	id.hostname = hostname
	id.username = username
	id.mu.Unlock()
	return changed
}
