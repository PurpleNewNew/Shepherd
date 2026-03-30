package process

import (
	"context"

	"codeberg.org/agnoie/shepherd/internal/kelpie/manager"
	"codeberg.org/agnoie/shepherd/internal/kelpie/topology"
	"codeberg.org/agnoie/shepherd/protocol"
)

// DispatchInfoMess consumes upstream info messages and updates the topology cache.
func DispatchInfoMess(ctx context.Context, mgr *manager.Manager, topo *topology.Topology) {
	for {
		var message interface{}
		select {
		case <-ctx.Done():
			return
		case message = <-mgr.InfoManager.InfoMessChan:
		}

		switch mess := message.(type) {
		case *protocol.MyInfo:
			task := &topology.TopoTask{
				Mode:         topology.UPDATEDETAIL,
				UUID:         mess.UUID,
				UserName:     mess.Username,
				HostName:     mess.Hostname,
				Memo:         mess.Memo,
				SleepSeconds: -1,
				WorkSeconds:  -1,
			}
			if topo != nil {
				topo.Execute(task)
			}
		}
	}
}
