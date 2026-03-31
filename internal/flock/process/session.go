package process

import (
	"codeberg.org/agnoie/shepherd/internal/flock/manager"
	"codeberg.org/agnoie/shepherd/pkg/session"
	"codeberg.org/agnoie/shepherd/protocol"
)

func newAgentUpMsg(sess session.Session) (protocol.Message, session.Session, bool) {
	if sess == nil {
		return nil, nil, false
	}
	conn := sess.Conn()
	if conn == nil {
		return nil, sess, false
	}
	msg := protocol.NewUpMsg(conn, sess.Secret(), sess.UUID())
	protocol.SetMessageMeta(msg, sess.ProtocolVersion(), sess.ProtocolFlags())
	return msg, sess, true
}

func activeSession(mgr *manager.Manager) session.Session {
	if mgr != nil {
		if sess := mgr.ActiveSession(); sess != nil {
			return sess
		}
	}
	return nil
}

func newAgentUpMsgForManager(mgr *manager.Manager) (protocol.Message, session.Session, bool) {
	return newAgentUpMsg(activeSession(mgr))
}
