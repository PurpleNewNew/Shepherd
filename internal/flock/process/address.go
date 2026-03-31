package process

import "sync"

var (
	currentListenAddr string
	listenAddrMu      sync.RWMutex
)

func setCurrentListenAddr(addr string) {
	listenAddrMu.Lock()
	currentListenAddr = addr
	listenAddrMu.Unlock()
}

func currentListenAddrValue() string {
	listenAddrMu.RLock()
	defer listenAddrMu.RUnlock()
	return currentListenAddr
}
