package process

import (
	"context"
	"fmt"
	"time"

	"codeberg.org/agnoie/shepherd/internal/kelpie/printer"
	"codeberg.org/agnoie/shepherd/internal/kelpie/topology"
	"codeberg.org/agnoie/shepherd/pkg/config/defaults"
	"codeberg.org/agnoie/shepherd/protocol"
)

// runHeartbeat 会周期性探测上游会话，以发现失效的 admin 路由。
func (admin *Admin) runHeartbeat(ctx context.Context) {
	if admin == nil || admin.topology == nil || admin.mgr == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ticker := time.NewTicker(defaults.AdminHeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			uuid, route, err := admin.resolveHeartbeatRoute()
			if err != nil {
				printer.Warning("\r\n[!] Admin heartbeat skipped: %v\r\n", err)
				continue
			}
			if err := admin.sendHeartbeat(uuid, route); err != nil {
				printer.Warning("\r\n[!] Admin heartbeat failed for %s: %v\r\n", uuid, err)
			}
		}
	}
}

func (admin *Admin) resolveHeartbeatRoute() (string, string, error) {
	if admin == nil || admin.topology == nil {
		return "", "", fmt.Errorf("topology unavailable")
	}
	task := &topology.TopoTask{
		Mode:    topology.GETUUID,
		UUIDNum: 0,
	}
	result, err := admin.topoRequest(task)
	if err != nil {
		return "", "", fmt.Errorf("resolve admin uuid: %w", err)
	}
	if result == nil || result.UUID == "" {
		return "", "", fmt.Errorf("admin uuid unavailable")
	}
	uuid := result.UUID
	route, ok := admin.fetchRoute(uuid)
	if !ok || route == "" {
		return "", "", fmt.Errorf("route unavailable for %s", uuid)
	}
	return uuid, route, nil
}

func (admin *Admin) sendHeartbeat(targetUUID, route string) error {
	msg, err := admin.newDownstreamMessageForRoute(targetUUID, route)
	if err != nil {
		return err
	}
	header := &protocol.Header{
		Sender:      protocol.ADMIN_UUID,
		Accepter:    protocol.TEMP_UUID,
		MessageType: protocol.HEARTBEAT,
		RouteLen:    uint32(len([]byte(route))),
		Route:       route,
	}
	payload := &protocol.HeartbeatMsg{Ping: 1}
	protocol.ConstructMessage(msg, header, payload, false)
	msg.SendMessage()
	return nil
}
