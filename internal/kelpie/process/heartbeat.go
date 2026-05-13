package process

import (
	"context"
	"fmt"
	"strings"
	"time"

	"codeberg.org/agnoie/shepherd/internal/kelpie/printer"
	"codeberg.org/agnoie/shepherd/pkg/config/defaults"
	"codeberg.org/agnoie/shepherd/protocol"
)

type heartbeatTarget struct {
	uuid  string
	route string
}

// runHeartbeat 会周期性探测所有已知可路由节点。只有收到 Flock 回执时，
// Kelpie 才刷新 last_seen/is_alive，避免 Stockman 把可操作节点误判离线。
func (admin *Admin) runHeartbeat(ctx context.Context) {
	if admin == nil || admin.topology == nil {
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
			targets := admin.resolveHeartbeatTargets()
			for _, target := range targets {
				if err := admin.sendHeartbeat(target.uuid, target.route); err != nil {
					printer.Warning("\r\n[!] Admin heartbeat failed for %s: %v\r\n", target.uuid, err)
				}
			}
		}
	}
}

func (admin *Admin) resolveHeartbeatRoute() (string, string, error) {
	targets := admin.resolveHeartbeatTargets()
	if len(targets) == 0 {
		return "", "", fmt.Errorf("no heartbeat targets available")
	}
	return targets[0].uuid, targets[0].route, nil
}

func (admin *Admin) resolveHeartbeatTargets() []heartbeatTarget {
	if admin == nil || admin.topology == nil {
		return nil
	}
	snapshot := admin.topology.UISnapshot("", "")
	targets := make([]heartbeatTarget, 0, len(snapshot.Nodes))
	for _, node := range snapshot.Nodes {
		uuid := strings.TrimSpace(node.UUID)
		if uuid == "" || uuid == protocol.ADMIN_UUID || uuid == protocol.TEMP_UUID {
			continue
		}
		route, ok := admin.fetchRoute(uuid)
		if !ok || strings.TrimSpace(route) == "" {
			continue
		}
		targets = append(targets, heartbeatTarget{uuid: uuid, route: route})
	}
	return targets
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
