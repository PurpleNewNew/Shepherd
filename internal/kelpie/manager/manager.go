/*
 * @作者: ph4ntom
 * @日期: 2021-03-23 19:01:26
 * @最后编辑: ph4ntom
 * @最后编辑时间: 2021-04-02 17:24:21
 */
package manager

import (
	"context"
	"net"
	"strings"

	"codeberg.org/agnoie/shepherd/pkg/global"
	"codeberg.org/agnoie/shepherd/pkg/session"
)

type Manager struct {
	ConsoleManager  *consoleManager
	SSHManager      *sshManager
	ShellManager    *shellManager
	InfoManager     *infoManager
	ListenManager   *listenManager
	ConnectManager  *connectManager
	ChildrenManager *childrenManager
	session         session.Session
	store           *global.Store
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

func NewManager(store *global.Store) *Manager {
	if store == nil {
		panic("admin manager requires a non-nil store")
	}
	manager := new(Manager)
	manager.ConsoleManager = newConsoleManager()
	manager.SSHManager = newSSHManager()
	manager.ShellManager = newShellManager()
	manager.InfoManager = newInfoManager()
	manager.ListenManager = newListenManager()
	manager.ConnectManager = newConnectManager()
	manager.ChildrenManager = newchildrenManager()
	manager.store = store
	return manager
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
	if manager.InfoManager != nil {
		manager.InfoManager.Close()
	}
	if manager.ListenManager != nil {
		manager.ListenManager.Close()
	}
	if manager.ConnectManager != nil {
		manager.ConnectManager.Close()
	}
	if manager.ChildrenManager != nil {
		manager.ChildrenManager.Close()
	}
}

func (manager *Manager) Session() session.Session {
	return manager.session
}

func (manager *Manager) SessionForTarget(target string) session.Session {
	if manager == nil {
		return nil
	}
	target = strings.TrimSpace(target)
	if target == "" {
		return manager.ActiveSession()
	}
	if manager.session != nil && manager.session.UUID() == target {
		return manager.session
	}
	if manager.store != nil {
		return manager.store.SessionFor(target)
	}
	return nil
}

func (manager *Manager) Run(ctx context.Context) {
}
