package process

import (
	"context"
	"fmt"

	"codeberg.org/agnoie/shepherd/internal/kelpie/manager"
	"codeberg.org/agnoie/shepherd/internal/kelpie/printer"
	"codeberg.org/agnoie/shepherd/internal/kelpie/supp"
	"codeberg.org/agnoie/shepherd/internal/kelpie/topology"
	"codeberg.org/agnoie/shepherd/pkg/session"
	"codeberg.org/agnoie/shepherd/pkg/utils"
	"codeberg.org/agnoie/shepherd/protocol"
)

func DispatchListenMess(ctx context.Context, mgr *manager.Manager, topo *topology.Topology) {
	for {
		var message interface{}
		select {
		case <-ctx.Done():
			return
		case message = <-mgr.ListenManager.ListenMessChan:
		}

		switch mess := message.(type) {
		case *protocol.ListenRes:
			mgr.ListenManager.DeliverAck(mess.ListenerID, mess.OK == 1)
		case *protocol.ChildUUIDReq:
			go dispatchChildUUID(mgr, topo, mess.ParentUUID, mess.IP, mess.RequestID)
		}
	}
}

func dispatchChildUUID(mgr *manager.Manager, topo *topology.Topology, parentUUID, ip, requestID string) {
	uuid := utils.GenerateUUID()
	node := topology.NewNode(uuid, ip)
	result, err := topoRequestDefault(topo, &topology.TopoTask{Mode: topology.ADDNODE, Target: node, ParentUUID: parentUUID, IsFirst: false})
	if err != nil || result == nil {
		printer.Warning("\r\n[*] failed to register child node %s for parent %s (ip=%s)\r\n", uuid, parentUUID, ip)
		return
	}
	childID := result.IDNum
	if err := topoExecute(topo, &topology.TopoTask{Mode: topology.ADDEDGE, UUID: parentUUID, NeighborUUID: uuid}); err != nil {
		printer.Warning("\r\n[*] failed to add tree edge between parent %s and child %s\r\n", parentUUID, uuid)
		return
	}
	// 新节点加入后，路由计算必须立即可用。
	// 如果重算请求被去抖，可能会与后续动作（supplemental 规划、DTN 调度、
	// listener 创建）发生竞争，进而出现 "route unavailable" 或 "no candidates"。
	if topo != nil {
		if _, err := topo.Execute(&topology.TopoTask{Mode: topology.CALCULATE}); err != nil {
			printer.Warning("\r\n[*] failed to calculate routes after adding child %s: %v\r\n", uuid, err)
		}
	}
	routeRes, err := topoRequestDefault(topo, &topology.TopoTask{Mode: topology.GETROUTE, UUID: parentUUID})
	if err != nil || routeRes == nil {
		printer.Warning("\r\n[*] failed to fetch route for parent %s when dispatching child\r\n", parentUUID)
		return
	}
	route := routeRes.Route
	if err := sendChildUUIDResponse(mgr, uuid, parentUUID, route, requestID); err != nil {
		return
	}
	printer.Success("\r\n[*] New node online! Node id is %d\r\n", childID)
	supp.PublishNodeAdded(uuid)
}

func sendChildUUIDResponse(mgr *manager.Manager, uuid, parentUUID, route, requestID string) error {
	sess := sessionForTarget(mgr, uuid)
	if sess == nil {
		return fmt.Errorf("session unavailable for %s", uuid)
	}
	conn := sess.Conn()
	if conn == nil {
		return fmt.Errorf("connection unavailable for %s", uuid)
	}
	msg := protocol.NewDownMsg(conn, sess.Secret(), sess.UUID())
	protocol.SetMessageMeta(msg, sess.ProtocolFlags())
	header := &protocol.Header{Sender: protocol.ADMIN_UUID, Accepter: protocol.TEMP_UUID, MessageType: protocol.CHILDUUIDRES, RouteLen: uint32(len([]byte(route))), Route: route}
	payload := &protocol.ChildUUIDRes{UUIDLen: uint16(len(uuid)), UUID: uuid, RequestIDLen: uint16(len(requestID)), RequestID: requestID}
	protocol.ConstructMessage(msg, header, payload, false)
	msg.SendMessage()
	return nil
}

func sessionForTarget(mgr *manager.Manager, uuid string) session.Session {
	if mgr == nil {
		return nil
	}
	if sess := mgr.SessionForTarget(uuid); sess != nil {
		return sess
	}
	return mgr.ActiveSession()
}
