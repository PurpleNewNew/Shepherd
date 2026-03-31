package process

import (
	"context"

	"codeberg.org/agnoie/shepherd/internal/kelpie/manager"
	"codeberg.org/agnoie/shepherd/protocol"
)

// DispatchConnectMess 负责消费 connect 相关邮箱消息，并通知正在等待的调用方。
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
