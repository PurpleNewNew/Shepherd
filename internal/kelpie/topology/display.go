package topology

import (
	"fmt"
	"strings"

	"codeberg.org/agnoie/shepherd/pkg/utils"
	"codeberg.org/agnoie/shepherd/protocol"
)

func (topology *Topology) showDetail(task *TopoTask) {
	filter := ""
	network := ""
	if task != nil {
		filter = task.FilterID
		network = strings.TrimSpace(task.NetworkID)
	}

	var nodes []int
	for uuidNum, node := range topology.nodes {
		if filter != "" && !topology.matchesEntry(node.uuid, filter) && node.uuid != protocol.ADMIN_UUID {
			continue
		}
		if network != "" && !topology.matchesNetwork(node.uuid, network) && node.uuid != protocol.ADMIN_UUID {
			continue
		}
		nodes = append(nodes, uuidNum)
	}

	utils.CheckRange(nodes)

	for _, uuidNum := range nodes {
		status := "Offline"
		if topology.nodes[uuidNum].isAlive {
			status = "Online"
		}

		node := topology.nodes[uuidNum]

		fmt.Printf("\r\nNode[%d] -> IP: %s  Port: %d  Hostname: %s  User: %s  Status: %s\r\nMemo: %s\r\n",
			uuidNum,
			node.currentIP,
			node.listenPort,
			node.currentHostname,
			node.currentUser,
			status,
			node.memo,
		)
	}

	topology.ResultChan <- &topoResult{} // 只是告诉上游：工作完成！
}

func (topology *Topology) showTopo(task *TopoTask) {
	filter := ""
	network := ""
	if task != nil {
		filter = task.FilterID
		network = strings.TrimSpace(task.NetworkID)
	}

	fmt.Printf("\r\nGraph Network Topology:\r\n")
	fmt.Printf("Last updated: %s\r\n", topology.lastUpdateTime.Format("2006-01-02 15:04:05"))

	var nodes []int
	for uuidNum, node := range topology.nodes {
		if filter != "" && !topology.matchesEntry(node.uuid, filter) && node.uuid != protocol.ADMIN_UUID {
			continue
		}
		nodes = append(nodes, uuidNum)
	}

	utils.CheckRange(nodes)

	for _, uuidNum := range nodes {
		node := topology.nodes[uuidNum]
		if network != "" && !topology.matchesNetwork(node.uuid, network) && node.uuid != protocol.ADMIN_UUID {
			continue
		}
		nodeUUID := node.uuid
		fmt.Printf("\r\nNode[%d] (%s) neighbors ->\r\n", uuidNum, nodeUUID[:8]) // 显示UUID的前8个字符
		if neighbors, exists := topology.edges[nodeUUID]; exists {
			for _, neighborUUID := range neighbors {
				if filter != "" && !topology.matchesEntry(neighborUUID, filter) && neighborUUID != protocol.ADMIN_UUID {
					continue
				}
				if network != "" && !topology.matchesNetwork(neighborUUID, network) && neighborUUID != protocol.ADMIN_UUID {
					continue
				}
				neighborIDNum := topology.id2IDNum(neighborUUID)
				label := "[tree]"
				if topology.isSupplementalEdge(nodeUUID, neighborUUID) {
					label = "[supp]"
				}
				if neighborIDNum >= 0 {
					fmt.Printf("  Node[%d] (%s) %s\r\n", neighborIDNum, neighborUUID[:8], label)
				} else {
					fmt.Printf("  Node (%s) %s\r\n", neighborUUID[:8], label)
				}
			}
		} else {
			fmt.Printf("  (no neighbors)\r\n")
		}
	}

	topology.ResultChan <- &topoResult{} // 通知上游处理完成
}
