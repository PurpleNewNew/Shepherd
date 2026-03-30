package manager

import (
	"context"
	"net"

    "codeberg.org/agnoie/shepherd/pkg/global"
	"codeberg.org/agnoie/shepherd/pkg/session"
)

type Manager struct {
	ChildrenManager     *childrenManager
	SSHManager          *sshManager
	ShellManager        *shellManager
	ListenManager       *listenManager
	OfflineManager      *offlineManager
	SupplementalManager *supplementalManager
	ConnRouter          *ConnectionRouter
	session             session.Session
	store               *global.Store
	ctx                 context.Context
}

func NewManager(store *global.Store) *Manager {
	if store == nil {
		panic("agent manager requires a non-nil store")
	}
	manager := new(Manager)
	manager.ChildrenManager = newChildrenManager()
	manager.SSHManager = newSSHManager()
	manager.ShellManager = newShellManager()
	manager.ListenManager = newListenManager()
	manager.OfflineManager = newOfflineManager()
	manager.SupplementalManager = newSupplementalManager()
	manager.ConnRouter = NewConnectionRouter(manager.ChildrenManager, manager.SupplementalManager)
	manager.store = store
	return manager
}

func (manager *Manager) Run(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	manager.ctx = ctx
	go manager.SupplementalManager.run(ctx)
}

func (manager *Manager) SetSession(sess session.Session) {
	manager.session = sess
}

func (manager *Manager) Close() {
	if manager == nil {
		return
	}
	if manager.SSHManager != nil {
		manager.SSHManager.Close()
	}
	if manager.ShellManager != nil {
		manager.ShellManager.Close()
	}
	if manager.ListenManager != nil {
		manager.ListenManager.Close()
	}
	if manager.OfflineManager != nil {
		manager.OfflineManager.Close()
	}
	if manager.ChildrenManager != nil {
		manager.ChildrenManager.Close()
	}
	manager.ctx = nil
}

func (manager *Manager) Context() context.Context {
	if manager == nil {
		return context.Background()
	}
	if manager.ctx != nil {
		return manager.ctx
	}
	return context.Background()
}

func (manager *Manager) ActiveSession() session.Session {
	if manager == nil {
		return nil
	}
	if manager.session != nil {
		return manager.session
	}
	if manager.store != nil {
		return manager.store.ActiveSession()
	}
	return nil
}

func (manager *Manager) ActiveConn() net.Conn {
	sess := manager.ActiveSession()
	if sess != nil {
		return sess.Conn()
	}
	if manager.store != nil {
		return manager.store.ActiveConn()
	}
	return nil
}

func (manager *Manager) ActiveSecret() string {
	sess := manager.ActiveSession()
	if sess != nil {
		return sess.Secret()
	}
	if manager.store != nil {
		return manager.store.ActiveSecret()
	}
	return ""
}

func (manager *Manager) ActiveUUID() string {
	sess := manager.ActiveSession()
	if sess != nil {
		return sess.UUID()
	}
	if manager.store != nil {
		return manager.store.ActiveUUID()
	}
	return ""
}

func (manager *Manager) Store() *global.Store {
	if manager == nil {
		return nil
	}
	return manager.store
}

func (manager *Manager) PreAuthToken() string {
	if manager == nil || manager.store == nil {
		return ""
	}
	return manager.store.PreAuthToken()
}

func (manager *Manager) TLSEnabled() bool {
	if manager.store != nil {
		return manager.store.TLSEnabled()
	}
	return false
}

func (manager *Manager) SetTLSEnabled(enabled bool) {
	if manager.store != nil {
		manager.store.SetTLSEnabled(enabled)
	}
}
