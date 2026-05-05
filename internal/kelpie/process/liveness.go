package process

import (
	"strings"

	"codeberg.org/agnoie/shepherd/internal/kelpie/printer"
	"codeberg.org/agnoie/shepherd/internal/kelpie/topology"
	"codeberg.org/agnoie/shepherd/protocol"
)

func markTopologyNodeAlive(topo *topology.Topology, uuid string) {
	if topo == nil {
		return
	}
	uuid = strings.TrimSpace(uuid)
	if uuid == "" || uuid == protocol.ADMIN_UUID || uuid == protocol.TEMP_UUID {
		return
	}
	_, err := topoRequestDefault(topo, &topology.TopoTask{
		Mode:         topology.UPDATEDETAIL,
		UUID:         uuid,
		SleepSeconds: -1,
		WorkSeconds:  -1,
	})
	if err != nil {
		printer.Warning("\r\n[*] Failed to refresh node liveness for %s: %v\r\n", shortID(uuid), err)
	}
}

func (admin *Admin) markNodeAlive(uuid string) {
	if admin == nil {
		return
	}
	markTopologyNodeAlive(admin.topology, uuid)
}

func (core *routerCore) markSenderAlive(header *protocol.Header) {
	if core == nil || header == nil {
		return
	}
	markTopologyNodeAlive(core.topo, header.Sender)
}
