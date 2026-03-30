package state

import "sync"

var (
	currentListenAddr string
	listenMu          sync.RWMutex
)

func SetCurrentListenAddr(addr string) {
	listenMu.Lock()
	currentListenAddr = addr
	listenMu.Unlock()
}

func GetCurrentListenAddr() string {
	listenMu.RLock()
	defer listenMu.RUnlock()
	return currentListenAddr
}
