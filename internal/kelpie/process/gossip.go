package process

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"codeberg.org/agnoie/shepherd/internal/kelpie/printer"
	"codeberg.org/agnoie/shepherd/internal/kelpie/topology"
	"codeberg.org/agnoie/shepherd/pkg/bus"
	"codeberg.org/agnoie/shepherd/protocol"
)

func normalizeGossipRequest(req *protocol.GossipRequest) {
	if req == nil {
		return
	}
	req.RequestUUIDLen = uint16(len(req.RequestUUID))
	req.RequesterUUIDLen = uint16(len(req.RequesterUUID))
	req.TargetUUIDLen = uint16(len(req.TargetUUID))
}

func normalizeGossipResponse(resp *protocol.GossipResponse) {
	if resp == nil {
		return
	}
	resp.RequestUUIDLen = uint16(len(resp.RequestUUID))
	resp.RequesterUUIDLen = uint16(len(resp.RequesterUUID))
	resp.ResponderUUIDLen = uint16(len(resp.ResponderUUID))
	resp.TargetUUIDLen = uint16(len(resp.TargetUUID))
	resp.NodeDataLen = uint64(len(resp.NodeData))
}

func (admin *Admin) handleGossipRequest(header *protocol.Header, req *protocol.GossipRequest) {
	if req == nil {
		return
	}

	if req.RequesterUUID == "" {
		req.RequesterUUID = header.Sender
	}
	normalizeGossipRequest(req)

	if req.TargetUUID == "" || req.RequesterUUID == "" || req.TargetUUID == protocol.ADMIN_UUID {
		admin.sendGossipFailure(req)
		return
	}

	if admin.topology != nil {
		reqNet := admin.topology.NetworkFor(req.RequesterUUID)
		targetNet := admin.topology.NetworkFor(req.TargetUUID)
		if reqNet != "" && targetNet != "" && !strings.EqualFold(reqNet, targetNet) {
			admin.sendGossipFailure(req)
			return
		}
	}

	route, ok := admin.fetchRoute(req.TargetUUID)
	if !ok {
		admin.sendGossipFailure(req)
		return
	}

	sess := admin.sessionForRoute(route)
	if sess == nil || sess.Conn() == nil {
		admin.sendGossipFailure(req)
		return
	}
	sMessage := protocol.NewDownMsg(sess.Conn(), sess.Secret(), sess.UUID())
	protocol.SetMessageMeta(sMessage, sess.ProtocolVersion(), sess.ProtocolFlags())
	outHeader := &protocol.Header{
		Sender:      sess.UUID(),
		Accepter:    protocol.TEMP_UUID,
		MessageType: uint16(protocol.GOSSIP_REQUEST),
		RouteLen:    uint32(len([]byte(route))),
		Route:       route,
	}

	protocol.ConstructMessage(sMessage, outHeader, req, false)
	sMessage.SendMessage()
}

func (admin *Admin) handleGossipResponse(resp *protocol.GossipResponse) {
	if resp == nil || resp.RequesterUUID == "" {
		return
	}
	normalizeGossipResponse(resp)
	admin.forwardGossipResponse(resp)
}

func (admin *Admin) sendGossipFailure(req *protocol.GossipRequest) {
	if req == nil || req.RequesterUUID == "" {
		return
	}
	resp := &protocol.GossipResponse{
		RequestUUID:   req.RequestUUID,
		RequesterUUID: req.RequesterUUID,
		ResponderUUID: protocol.ADMIN_UUID,
		TargetUUID:    req.TargetUUID,
		Reachable:     false,
		Timestamp:     time.Now().UnixNano(),
	}
	normalizeGossipResponse(resp)
	admin.forwardGossipResponse(resp)
}

func (admin *Admin) forwardGossipResponse(resp *protocol.GossipResponse) {
	if resp == nil || resp.RequesterUUID == "" {
		return
	}
	normalizeGossipResponse(resp)

	if admin.topology != nil {
		reqNet := admin.topology.NetworkFor(resp.RequesterUUID)
		targetNet := admin.topology.NetworkFor(resp.TargetUUID)
		if reqNet != "" && targetNet != "" && !strings.EqualFold(reqNet, targetNet) {
			return
		}
	}

	route, ok := admin.fetchRoute(resp.RequesterUUID)
	if !ok {
		return
	}
	sess := admin.sessionForRoute(route)
	if sess == nil || sess.Conn() == nil {
		return
	}
	sMessage := protocol.NewDownMsg(sess.Conn(), sess.Secret(), sess.UUID())
	protocol.SetMessageMeta(sMessage, sess.ProtocolVersion(), sess.ProtocolFlags())
	header := &protocol.Header{
		Sender:      sess.UUID(),
		Accepter:    protocol.TEMP_UUID,
		MessageType: uint16(protocol.GOSSIP_RESPONSE),
		RouteLen:    uint32(len([]byte(route))),
		Route:       route,
	}

	protocol.ConstructMessage(sMessage, header, resp, false)
	sMessage.SendMessage()
}

func (admin *Admin) enqueueGossipUpdate(update *protocol.GossipUpdate) {
	if admin == nil || update == nil || admin.gossipUpdateChan == nil {
		return
	}
	if ctx := admin.context(); ctx.Err() != nil {
		return
	}
	cloned := *update
	if update.NodeData != nil {
		cloned.NodeData = append([]byte(nil), update.NodeData...)
	}
	select {
	case admin.gossipUpdateChan <- &cloned:
	default:
		printer.Warning("\r\n[*] Gossip update queue full, dropping update from %s\r\n", cloned.SenderUUID)
	}
}

func (admin *Admin) processGossipUpdates(ctx context.Context) {
	if admin == nil {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case update, ok := <-admin.gossipUpdateChan:
			if !ok {
				return
			}
			admin.applyGossipUpdate(update)
		}
	}
}

func (admin *Admin) applyGossipUpdate(gossipMsg *protocol.GossipUpdate) {
	if admin == nil || gossipMsg == nil {
		return
	}

	var nodeInfo protocol.NodeInfo
	if err := json.Unmarshal(gossipMsg.NodeData, &nodeInfo); err != nil {
		printer.Fail("\r\n[*] Failed to parse gossip node data: %v", err)
		return
	}

	nodeNetwork := ""
	if admin.topology != nil {
		nodeNetwork = admin.topology.NetworkFor(nodeInfo.UUID)
		if nodeNetwork == "" && gossipMsg.SenderUUID != "" {
			nodeNetwork = admin.topology.NetworkFor(gossipMsg.SenderUUID)
		}
	}

	if admin.suppPlanner != nil {
		admin.suppPlanner.OnGossipTelemetry(&nodeInfo)
	}

	topoTask := &topology.TopoTask{
		Mode:         topology.UPDATEDETAIL,
		UUID:         nodeInfo.UUID,
		UserName:     nodeInfo.Username,
		HostName:     nodeInfo.Hostname,
		Memo:         nodeInfo.Memo,
		Port:         nodeInfo.Port,
		LatencyMs:    nodeInfo.LatencyMs,
		SleepSeconds: int(nodeInfo.SleepSeconds),
		WorkSeconds:  int(nodeInfo.WorkSeconds),
		NextWakeUnix: nodeInfo.NextWake,
	}
	if _, err := admin.topoRequest(topoTask); err != nil {
		printer.Fail("\r\n[*] Failed to update node detail: %v", err)
		return
	}

	newNeighborSet := make(map[string]struct{})
	for _, neighborUUID := range nodeInfo.Neighbors {
		if neighborUUID == "" || neighborUUID == nodeInfo.UUID {
			continue
		}
		newNeighborSet[neighborUUID] = struct{}{}
	}

	getNeighborTask := &topology.TopoTask{
		Mode: topology.GETNEIGHBORS,
		UUID: nodeInfo.UUID,
	}
	currentNeighborResult, err := admin.topoRequest(getNeighborTask)
	if err != nil || currentNeighborResult == nil {
		printer.Fail("\r\n[*] Failed to fetch neighbors for %s: %v", nodeInfo.UUID, err)
		return
	}

	currentNeighborSet := make(map[string]struct{})
	for _, neighborUUID := range currentNeighborResult.Neighbors {
		if neighborUUID == "" || neighborUUID == nodeInfo.UUID {
			continue
		}
		currentNeighborSet[neighborUUID] = struct{}{}
	}

	getMetaTask := &topology.TopoTask{
		Mode: topology.GETNODEMETA,
		UUID: nodeInfo.UUID,
	}
	metaResult, err := admin.topoRequest(getMetaTask)
	if err != nil || metaResult == nil {
		printer.Fail("\r\n[*] Failed to fetch node metadata for %s: %v", nodeInfo.UUID, err)
		return
	}

	baseEdges := make(map[string]struct{})
	if metaResult.Parent != "" {
		baseEdges[metaResult.Parent] = struct{}{}
	}
	for _, child := range metaResult.Children {
		baseEdges[child] = struct{}{}
	}
	if nodeNetwork == "" && metaResult != nil && metaResult.Parent != "" {
		nodeNetwork = admin.topology.NetworkFor(metaResult.Parent)
	}

	sameNetwork := func(uuid string) bool {
		if uuid == "" || admin.topology == nil {
			return true
		}
		otherNet := admin.topology.NetworkFor(uuid)
		if nodeNetwork == "" || otherNet == "" {
			return true
		}
		return strings.EqualFold(nodeNetwork, otherNet)
	}

	validNeighborSet := make(map[string]struct{})
	for neighborUUID := range newNeighborSet {
		if _, isBase := baseEdges[neighborUUID]; isBase {
			validNeighborSet[neighborUUID] = struct{}{}
		}
	}
	for neighborUUID := range validNeighborSet {
		if !sameNetwork(neighborUUID) {
			delete(validNeighborSet, neighborUUID)
		}
	}

	healthScore := nodeInfo.Health
	weightValue := healthScore + 1
	changed := healthScore > 0 || nodeInfo.LatencyMs > 0
	for neighborUUID := range validNeighborSet {
		if !sameNetwork(neighborUUID) {
			continue
		}
		weightTask := &topology.TopoTask{
			Mode:         topology.SETEDGEWEIGHT,
			UUID:         nodeInfo.UUID,
			NeighborUUID: neighborUUID,
			Weight:       weightValue,
		}
		if _, err := admin.topoRequest(weightTask); err != nil {
			printer.Warning("\r\n[*] Failed to set edge weight for %s -> %s: %v", nodeInfo.UUID, neighborUUID, err)
		}
	}

	for neighborUUID := range currentNeighborSet {
		if _, isBase := baseEdges[neighborUUID]; isBase {
			continue
		}
		if admin.topology.IsSupplementalEdge(nodeInfo.UUID, neighborUUID) {
			continue
		}
		if !sameNetwork(neighborUUID) {
			continue
		}
		removeTask := &topology.TopoTask{
			Mode:         topology.REMOVEEDGE,
			UUID:         nodeInfo.UUID,
			NeighborUUID: neighborUUID,
		}
		if _, err := admin.topoRequest(removeTask); err != nil {
			printer.Warning("\r\n[*] Failed to remove edge %s -> %s: %v", nodeInfo.UUID, neighborUUID, err)
		}
		changed = true
	}

	for neighborUUID := range validNeighborSet {
		if _, exists := currentNeighborSet[neighborUUID]; !exists {
			if !sameNetwork(neighborUUID) {
				continue
			}
			edgeTask := &topology.TopoTask{
				Mode:         topology.ADDEDGE,
				UUID:         nodeInfo.UUID,
				NeighborUUID: neighborUUID,
			}
			if _, err := admin.topoRequest(edgeTask); err != nil {
				printer.Warning("\r\n[*] Failed to add edge %s -> %s: %v", nodeInfo.UUID, neighborUUID, err)
			} else {
				changed = true
			}
		}
	}

	if changed {
		if admin != nil && admin.topology != nil {
			admin.topology.ScheduleCalculate()
		}
	}

	// 根据最新 telemetry 与 route 实时重排 DTN hold：
	// - 重新计算发往该节点 bundle 的 hold_until
	// - 尝试立即 flush，利用新对齐出来的发送窗口
	admin.recalculateHoldForTarget(nodeInfo.UUID)
	admin.flushDTNBundles(admin.context())
}

func (admin *Admin) dispatchGossipUpdate() bus.Handler {
	return func(ctx context.Context, header *protocol.Header, payload interface{}) error {
		update, ok := payload.(*protocol.GossipUpdate)
		if !ok {
			return fmt.Errorf("expected *protocol.GossipUpdate, got %T", payload)
		}
		admin.enqueueGossipUpdate(update)
		return nil
	}
}

func (admin *Admin) dispatchGossipRequest() bus.Handler {
	return func(ctx context.Context, header *protocol.Header, payload interface{}) error {
		req, ok := payload.(*protocol.GossipRequest)
		if !ok {
			return fmt.Errorf("expected *protocol.GossipRequest, got %T", payload)
		}
		admin.handleGossipRequest(header, req)
		return nil
	}
}

func (admin *Admin) dispatchGossipResponse() bus.Handler {
	return func(ctx context.Context, header *protocol.Header, payload interface{}) error {
		resp, ok := payload.(*protocol.GossipResponse)
		if !ok {
			return fmt.Errorf("expected *protocol.GossipResponse, got %T", payload)
		}
		admin.handleGossipResponse(resp)
		return nil
	}
}
