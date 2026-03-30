package manager

import (
	"context"
	"net"
	"time"

	"codeberg.org/agnoie/shepherd/pkg/utils"
)

const (
	SuppAddOrUpdate = iota
	SuppRemove
	SuppGet
	SuppList
	SuppMarkReady
	SuppAttachConn
	SuppMarkFailed
	SuppGetByPeer
	SuppDetach
)

type SupplementalState int

const (
	SuppStatePending SupplementalState = iota
	SuppStateReady
	SuppStateFailed
)

type SupplementalLink struct {
	LinkUUID    string
	PeerUUID    string
	Role        uint16
	State       SupplementalState
	LastUpdated time.Time
	Conn        net.Conn
}

type SupplementalTask struct {
	Mode     int
	Link     *SupplementalLink
	LinkUUID string
	PeerUUID string
	Conn     net.Conn
}

type SupplementalResult struct {
	OK    bool
	Link  *SupplementalLink
	Links []*SupplementalLink
	Conn  net.Conn
}

type supplementalManager struct {
	ctx           context.Context
	links         map[string]*SupplementalLink
	TaskChan      chan *SupplementalTask
	ResultChan    chan *SupplementalResult
	ConnReadyChan chan *SupplementalLink
}

func newSupplementalManager() *supplementalManager {
	return &supplementalManager{
		links:         make(map[string]*SupplementalLink),
		TaskChan:      make(chan *SupplementalTask),
		ResultChan:    make(chan *SupplementalResult),
		ConnReadyChan: make(chan *SupplementalLink, 4),
	}
}

func (manager *supplementalManager) run(ctx context.Context) {
	manager.ctx = ctx
	defer func() {
		manager.ctx = nil
	}()
	for {
		var task *SupplementalTask
		select {
		case <-ctx.Done():
			return
		case task = <-manager.TaskChan:
		}
		switch task.Mode {
		case SuppAddOrUpdate:
			manager.addOrUpdate(task.Link)
		case SuppRemove:
			manager.remove(task.LinkUUID)
		case SuppGet:
			manager.get(task.LinkUUID)
		case SuppList:
			manager.list()
		case SuppMarkReady:
			manager.markReady(task.LinkUUID)
		case SuppAttachConn:
			manager.attachConn(task.LinkUUID, task.Conn)
		case SuppMarkFailed:
			manager.markFailed(task.LinkUUID)
		case SuppGetByPeer:
			manager.getByPeer(task.PeerUUID)
		case SuppDetach:
			manager.detach(task.LinkUUID)
		default:
			manager.ResultChan <- &SupplementalResult{OK: false}
		}
	}
}

func (manager *supplementalManager) addOrUpdate(link *SupplementalLink) {
	if link == nil || link.LinkUUID == "" {
		manager.ResultChan <- &SupplementalResult{OK: false}
		return
	}
	existing, ok := manager.links[link.LinkUUID]
	if ok {
		existing.PeerUUID = link.PeerUUID
		existing.Role = link.Role
		existing.State = link.State
		existing.LastUpdated = time.Now()
		manager.ResultChan <- &SupplementalResult{OK: true, Link: cloneLink(existing, false)}
		return
	}
	link.LastUpdated = time.Now()
	manager.links[link.LinkUUID] = link
	manager.ResultChan <- &SupplementalResult{OK: true, Link: cloneLink(link, false)}
}

func (manager *supplementalManager) remove(linkUUID string) {
	if _, ok := manager.links[linkUUID]; ok {
		link := manager.links[linkUUID]
		if link.Conn != nil {
			link.Conn.Close()
		}
		delete(manager.links, linkUUID)
		manager.ResultChan <- &SupplementalResult{OK: true}
		return
	}
	manager.ResultChan <- &SupplementalResult{OK: false}
}

func (manager *supplementalManager) get(linkUUID string) {
	if link, ok := manager.links[linkUUID]; ok {
		manager.ResultChan <- &SupplementalResult{OK: true, Link: cloneLink(link, false)}
		return
	}
	manager.ResultChan <- &SupplementalResult{OK: false}
}

func (manager *supplementalManager) list() {
	links := make([]*SupplementalLink, 0, len(manager.links))
	for _, link := range manager.links {
		links = append(links, cloneLink(link, false))
	}
	manager.ResultChan <- &SupplementalResult{OK: true, Links: links}
}

func (manager *supplementalManager) markReady(linkUUID string) {
	if link, ok := manager.links[linkUUID]; ok {
		link.State = SuppStateReady
		link.LastUpdated = time.Now()
		manager.ResultChan <- &SupplementalResult{OK: true, Link: cloneLink(link, false)}
		return
	}
	manager.ResultChan <- &SupplementalResult{OK: false}
}

func (manager *supplementalManager) attachConn(linkUUID string, conn net.Conn) {
	if conn == nil {
		manager.ResultChan <- &SupplementalResult{OK: false}
		return
	}
	safeConn := utils.NewSafeConn(conn)
	if link, ok := manager.links[linkUUID]; ok {
		link.Conn = safeConn
		link.LastUpdated = time.Now()
		manager.publishConnReady(link)
		manager.ResultChan <- &SupplementalResult{OK: true, Link: cloneLink(link, false)}
		return
	}
	safeConn.Close()
	manager.ResultChan <- &SupplementalResult{OK: false}
}

func (manager *supplementalManager) markFailed(linkUUID string) {
	if link, ok := manager.links[linkUUID]; ok {
		link.State = SuppStateFailed
		if link.Conn != nil {
			link.Conn.Close()
			link.Conn = nil
		}
		manager.ResultChan <- &SupplementalResult{OK: true, Link: cloneLink(link, false)}
		return
	}
	manager.ResultChan <- &SupplementalResult{OK: false}
}

func (manager *supplementalManager) getByPeer(peerUUID string) {
	if peerUUID == "" {
		manager.ResultChan <- &SupplementalResult{OK: false}
		return
	}
	for _, link := range manager.links {
		if link.PeerUUID == peerUUID && link.Conn != nil && link.State == SuppStateReady {
			manager.ResultChan <- &SupplementalResult{OK: true, Link: cloneLink(link, false), Conn: link.Conn}
			return
		}
	}
	manager.ResultChan <- &SupplementalResult{OK: false}
}

func (manager *supplementalManager) detach(linkUUID string) {
	if link, ok := manager.links[linkUUID]; ok {
		delete(manager.links, linkUUID)
		manager.ResultChan <- &SupplementalResult{OK: true, Link: cloneLink(link, false)}
		return
	}
	manager.ResultChan <- &SupplementalResult{OK: false}
}

func (manager *supplementalManager) publishConnReady(link *SupplementalLink) {
	if link == nil || manager.ConnReadyChan == nil {
		return
	}
	clone := cloneLink(link, true)
	if clone == nil {
		return
	}
	var done <-chan struct{}
	if manager.ctx != nil {
		done = manager.ctx.Done()
	}
	select {
	case manager.ConnReadyChan <- clone:
	case <-done:
		if clone.Conn != nil {
			_ = clone.Conn.Close()
		}
	}
}

func (manager *supplementalManager) GetConn(uuid string) (net.Conn, bool) {
	manager.TaskChan <- &SupplementalTask{Mode: SuppGetByPeer, PeerUUID: uuid}
	result := <-manager.ResultChan
	if !result.OK || result.Conn == nil {
		return nil, false
	}
	return result.Conn, true
}

func cloneLink(link *SupplementalLink, includeConn bool) *SupplementalLink {
	if link == nil {
		return nil
	}
	clone := *link
	if !includeConn {
		clone.Conn = nil
	}
	return &clone
}
