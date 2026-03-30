package process

import (
	"context"

	"codeberg.org/agnoie/shepherd/internal/kelpie/manager"
	"codeberg.org/agnoie/shepherd/protocol"
)

// DispatchConnectMess consumes connect-related mailbox messages and notifies waiting callers.
func DispatchConnectMess(ctx context.Context, mgr *manager.Manager) {
	if mgr == nil || mgr.ConnectManager == nil {
		return
	}
	for {
		var message interface{}
		select {
		case <-ctx.Done():
			return
		case message = <-mgr.ConnectManager.ConnectMessChan:
		}
		switch mess := message.(type) {
		case *protocol.ConnectDone:
			ok := mess.OK == 1
			mgr.ConnectManager.DeliverAck(ok)
		}
	}
}
