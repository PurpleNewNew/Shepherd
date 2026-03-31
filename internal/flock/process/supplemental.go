package process

import (
	"context"
	"errors"
	"io"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"time"

	"codeberg.org/agnoie/shepherd/internal/flock/manager"
	"codeberg.org/agnoie/shepherd/pkg/utils"
	"codeberg.org/agnoie/shepherd/protocol"
)

const supplementalHandshakeMagic = "SHEPHERD_SUPP"

var (
	listenerMu      sync.Mutex
	activeListeners = make(map[string]net.Listener)
	heartbeatMu     sync.Mutex
	heartbeatStops  = make(map[string]chan struct{})
	suppActivityMu  sync.Mutex
	suppActivity    = make(map[string]time.Time)
)

const (
	suppHeartbeatActiveInterval = 20 * time.Second
	suppHeartbeatIdleInterval   = 50 * time.Second
	suppHeartbeatIdleThreshold  = 45 * time.Second
	suppHeartbeatJitter         = 0.10
	suppHeartbeatMinInterval    = 10 * time.Second
)

func HandleSuppLinkRequest(mgr *manager.Manager, req *protocol.SuppLinkRequest) {
	if req == nil || mgr == nil || mgr.SupplementalManager == nil {
		return
	}
	peerUUID := req.TargetUUID
	if req.Role == protocol.SuppLinkRoleResponder {
		peerUUID = req.InitiatorUUID
	}
	link := &manager.SupplementalLink{
		LinkUUID:    req.LinkUUID,
		PeerUUID:    peerUUID,
		Role:        req.Role,
		State:       manager.SuppStatePending,
		LastUpdated: time.Now(),
	}
	task := &manager.SupplementalTask{Mode: manager.SuppAddOrUpdate, Link: link}
	mgr.SupplementalManager.TaskChan <- task
	<-mgr.SupplementalManager.ResultChan

	switch req.Action {
	case protocol.SuppLinkActionListen:
		port, err := startSupplementalListener(mgr, req)
		if err != nil {
			sendSuppLinkResponse(mgr, req, false, err.Error(), "", 0)
			return
		}
		sendSuppLinkResponse(mgr, req, true, "listening", listenerIPForResponse(mgr), port)
	case protocol.SuppLinkActionDial:
		if req.PeerIP == "" || req.PeerPort == 0 {
			sendSuppLinkResponse(mgr, req, false, "missing dial target", "", 0)
			return
		}
		if err := performSupplementalDial(mgr, req); err != nil {
			sendSuppLinkResponse(mgr, req, false, err.Error(), "", 0)
			return
		}
		sendSuppLinkResponse(mgr, req, true, "dial success", "", 0)
	default:
		sendSuppLinkResponse(mgr, req, false, "unknown action", "", 0)
	}
}

func HandleSuppLinkTeardown(mgr *manager.Manager, td *protocol.SuppLinkTeardown) {
	if td == nil || mgr == nil || mgr.SupplementalManager == nil {
		return
	}
	closeActiveListener(td.LinkUUID)
	cleanupSupplementalLink(mgr, td.LinkUUID)
	mgr.SupplementalManager.TaskChan <- &manager.SupplementalTask{Mode: manager.SuppRemove, LinkUUID: td.LinkUUID}
	<-mgr.SupplementalManager.ResultChan
}

func sendSuppLinkResponse(mgr *manager.Manager, req *protocol.SuppLinkRequest, ok bool, message string, listenIP string, listenPort uint16) {
	sess := activeSession(mgr)
	if sess == nil {
		return
	}
	msg, sess, okMsg := newAgentUpMsg(sess)
	if !okMsg {
		return
	}
	agentUUID := sess.UUID()
	if agentUUID == "" {
		return
	}

	status := uint16(0)
	if ok {
		status = 1
		if message == "" {
			message = "link recorded"
		}
	} else if message == "" {
		message = "link rejected"
	}
	resp := &protocol.SuppLinkResponse{
		RequestUUIDLen: uint16(len(req.RequestUUID)),
		RequestUUID:    req.RequestUUID,
		LinkUUIDLen:    uint16(len(req.LinkUUID)),
		LinkUUID:       req.LinkUUID,
		AgentUUIDLen:   uint16(len(agentUUID)),
		AgentUUID:      agentUUID,
		PeerUUIDLen:    uint16(len(req.TargetUUID)),
		PeerUUID:       req.TargetUUID,
		Role:           req.Role,
		Status:         status,
		MessageLen:     uint16(len(message)),
		Message:        message,
		ListenIPLen:    uint16(len(listenIP)),
		ListenIP:       listenIP,
		ListenPort:     listenPort,
	}
	if req.Role == protocol.SuppLinkRoleResponder {
		resp.PeerUUIDLen = uint16(len(req.InitiatorUUID))
		resp.PeerUUID = req.InitiatorUUID
	}
	header := &protocol.Header{
		Sender:      agentUUID,
		Accepter:    protocol.ADMIN_UUID,
		MessageType: uint16(protocol.SUPPLINKRESP),
		RouteLen:    uint32(len([]byte(protocol.TEMP_ROUTE))),
		Route:       protocol.TEMP_ROUTE,
	}
	protocol.ConstructMessage(msg, header, resp, false)
	msg.SendMessage()
}

func startSupplementalListener(mgr *manager.Manager, req *protocol.SuppLinkRequest) (uint16, error) {
	ln, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		return 0, err
	}

	tcpAddr, _ := ln.Addr().(*net.TCPAddr)
	port := uint16(tcpAddr.Port)

	listenerMu.Lock()
	activeListeners[req.LinkUUID] = ln
	listenerMu.Unlock()

	ctx := managerContext(mgr)
	stopClose := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = ln.Close()
		case <-stopClose:
		}
	}()

	go func() {
		defer close(stopClose)
		awaitSupplementalDial(mgr, req.LinkUUID, ln)
	}()

	return port, nil
}

func managerContext(mgr *manager.Manager) context.Context {
	if mgr == nil {
		return context.Background()
	}
	return mgr.Context()
}

func awaitSupplementalDial(mgr *manager.Manager, linkUUID string, ln net.Listener) {
	defer closeActiveListener(linkUUID)

	conn, err := ln.Accept()
	if err != nil {
		return
	}

	if err := acceptSupplementalHandshake(mgr, conn); err != nil {
		conn.Close()
		return
	}

	conn.SetDeadline(time.Time{})
	if !attachSupplementalConn(mgr, linkUUID, conn) {
		conn.Close()
		failSupplementalLink(mgr, linkUUID)
		return
	}
	startSuppHeartbeat(mgr, linkUUID)
}

func closeActiveListener(linkUUID string) {
	listenerMu.Lock()
	ln := activeListeners[linkUUID]
	delete(activeListeners, linkUUID)
	listenerMu.Unlock()
	if ln != nil {
		ln.Close()
	}
}

func listenerIPForResponse(mgr *manager.Manager) string {
	addr := currentListenAddrValue()
	if host, _, err := net.SplitHostPort(addr); err == nil && host != "" {
		if ip := net.ParseIP(host); ip != nil && !ip.IsUnspecified() {
			return ip.String()
		}
	}
	if sess := activeSession(mgr); sess != nil {
		if conn := sess.Conn(); conn != nil {
			if tcp, ok := conn.LocalAddr().(*net.TCPAddr); ok && tcp.IP != nil {
				if !tcp.IP.IsUnspecified() {
					return tcp.IP.String()
				}
			}
		}
	}
	if mgr != nil {
		if conn := mgr.ActiveConn(); conn != nil {
			if tcp, ok := conn.LocalAddr().(*net.TCPAddr); ok && tcp.IP != nil {
				if !tcp.IP.IsUnspecified() {
					return tcp.IP.String()
				}
			}
		}
	}
	return ""
}

func performSupplementalDial(mgr *manager.Manager, req *protocol.SuppLinkRequest) error {
	address := net.JoinHostPort(req.PeerIP, strconv.Itoa(int(req.PeerPort)))
	conn, err := net.DialTimeout("tcp", address, 10*time.Second)
	if err != nil {
		return err
	}

	conn.SetDeadline(time.Now().Add(10 * time.Second))
	if err := utils.WriteFull(conn, []byte(supplementalHandshakeMagic)); err != nil {
		conn.Close()
		return err
	}
	buf := make([]byte, len(supplementalHandshakeMagic))
	if _, err := io.ReadFull(conn, buf); err != nil {
		conn.Close()
		return err
	}
	if string(buf) != supplementalHandshakeMagic {
		conn.Close()
		return errors.New("unexpected handshake response")
	}

	conn.SetDeadline(time.Time{})
	if !attachSupplementalConn(mgr, req.LinkUUID, conn) {
		conn.Close()
		failSupplementalLink(mgr, req.LinkUUID)
		return errors.New("unable to register supplemental connection")
	}
	startSuppHeartbeat(mgr, req.LinkUUID)
	return nil
}

func acceptSupplementalHandshake(mgr *manager.Manager, conn net.Conn) error {
	conn.SetDeadline(time.Now().Add(10 * time.Second))
	buf := make([]byte, len(supplementalHandshakeMagic))
	if _, err := io.ReadFull(conn, buf); err != nil {
		return err
	}
	if string(buf) != supplementalHandshakeMagic {
		return errors.New("invalid handshake magic")
	}
	if err := utils.WriteFull(conn, []byte(supplementalHandshakeMagic)); err != nil {
		WarnRuntime(mgr, "AGENT_SUPP_HANDSHAKE_ACK", true, err, "failed to acknowledge supplemental handshake")
		return err
	}
	return nil
}

func startSuppHeartbeat(mgr *manager.Manager, linkUUID string) {
	if mgr == nil || mgr.SupplementalManager == nil {
		return
	}
	link := fetchSupplementalLink(mgr, linkUUID)
	if link == nil {
		return
	}
	heartbeatMu.Lock()
	if stop, ok := heartbeatStops[linkUUID]; ok {
		close(stop)
	}
	stopCh := make(chan struct{})
	heartbeatStops[linkUUID] = stopCh
	heartbeatMu.Unlock()
	go runSuppHeartbeat(mgr, linkUUID, link.PeerUUID, stopCh)
}

func stopSuppHeartbeat(linkUUID string) {
	heartbeatMu.Lock()
	if stop, ok := heartbeatStops[linkUUID]; ok {
		close(stop)
		delete(heartbeatStops, linkUUID)
	}
	heartbeatMu.Unlock()
}

func runSuppHeartbeat(mgr *manager.Manager, linkUUID, peerUUID string, stopCh chan struct{}) {
	sendSuppHeartbeat(mgr, linkUUID, peerUUID, 1)
	ctx := managerContext(mgr)
	for {
		interval := nextSuppHeartbeatInterval(peerUUID)
		timer := time.NewTimer(interval)
		select {
		case <-stopCh:
			timer.Stop()
			sendSuppHeartbeat(mgr, linkUUID, peerUUID, 0)
			return
		case <-ctx.Done():
			timer.Stop()
			sendSuppHeartbeat(mgr, linkUUID, peerUUID, 0)
			return
		case <-timer.C:
			sendSuppHeartbeat(mgr, linkUUID, peerUUID, 1)
		}
	}
}

func noteSupplementalActivity(peerUUID string) {
	if peerUUID == "" {
		return
	}
	suppActivityMu.Lock()
	suppActivity[peerUUID] = time.Now()
	suppActivityMu.Unlock()
}

func lastSupplementalActivity(peerUUID string) time.Time {
	if peerUUID == "" {
		return time.Time{}
	}
	suppActivityMu.Lock()
	last := suppActivity[peerUUID]
	suppActivityMu.Unlock()
	return last
}

func nextSuppHeartbeatInterval(peerUUID string) time.Duration {
	interval := suppHeartbeatActiveInterval
	last := lastSupplementalActivity(peerUUID)
	if last.IsZero() || time.Since(last) > suppHeartbeatIdleThreshold {
		interval = suppHeartbeatIdleInterval
	}
	jitter := (rand.Float64()*2 - 1) * suppHeartbeatJitter
	withJitter := time.Duration(float64(interval) * (1 + jitter))
	if withJitter < suppHeartbeatMinInterval {
		return suppHeartbeatMinInterval
	}
	return withJitter
}

func sendSuppHeartbeat(mgr *manager.Manager, linkUUID, peerUUID string, status uint16) {
	sess := activeSession(mgr)
	if sess == nil {
		return
	}
	msg, sess, ok := newAgentUpMsg(sess)
	if !ok {
		return
	}
	agentUUID := sess.UUID()
	if agentUUID == "" {
		return
	}
	hb := &protocol.SuppLinkHeartbeat{
		LinkUUIDLen: uint16(len(linkUUID)),
		LinkUUID:    linkUUID,
		PeerUUIDLen: uint16(len(peerUUID)),
		PeerUUID:    peerUUID,
		Status:      status,
		Timestamp:   time.Now().Unix(),
	}
	header := &protocol.Header{
		Sender:      agentUUID,
		Accepter:    protocol.ADMIN_UUID,
		MessageType: uint16(protocol.SUPPLINKHEARTBEAT),
		RouteLen:    uint32(len([]byte(protocol.TEMP_ROUTE))),
		Route:       protocol.TEMP_ROUTE,
	}
	protocol.ConstructMessage(msg, header, hb, false)
	msg.SendMessage()
}

func fetchSupplementalLink(mgr *manager.Manager, linkUUID string) *manager.SupplementalLink {
	if mgr == nil || mgr.SupplementalManager == nil {
		return nil
	}
	mgr.SupplementalManager.TaskChan <- &manager.SupplementalTask{Mode: manager.SuppGet, LinkUUID: linkUUID}
	result := <-mgr.SupplementalManager.ResultChan
	if !result.OK {
		return nil
	}
	return result.Link
}

func attachSupplementalConn(mgr *manager.Manager, linkUUID string, conn net.Conn) bool {
	if mgr == nil || mgr.SupplementalManager == nil {
		return false
	}
	mgr.SupplementalManager.TaskChan <- &manager.SupplementalTask{Mode: manager.SuppMarkReady, LinkUUID: linkUUID}
	if result := <-mgr.SupplementalManager.ResultChan; !result.OK {
		return false
	}
	mgr.SupplementalManager.TaskChan <- &manager.SupplementalTask{Mode: manager.SuppAttachConn, LinkUUID: linkUUID, Conn: conn}
	if result := <-mgr.SupplementalManager.ResultChan; !result.OK {
		return false
	}
	return true
}

func failSupplementalLink(mgr *manager.Manager, linkUUID string) {
	if mgr == nil || mgr.SupplementalManager == nil {
		return
	}
	mgr.SupplementalManager.TaskChan <- &manager.SupplementalTask{Mode: manager.SuppMarkFailed, LinkUUID: linkUUID}
	<-mgr.SupplementalManager.ResultChan
}

func cleanupSupplementalLink(mgr *manager.Manager, linkUUID string) {
	stopSuppHeartbeat(linkUUID)
	failSupplementalLink(mgr, linkUUID)
}

func CleanupSupplementalLink(mgr *manager.Manager, linkUUID string) {
	cleanupSupplementalLink(mgr, linkUUID)
}

func DetachSupplementalLink(mgr *manager.Manager, linkUUID string) *manager.SupplementalLink {
	if mgr == nil || mgr.SupplementalManager == nil {
		return nil
	}
	stopSuppHeartbeat(linkUUID)
	mgr.SupplementalManager.TaskChan <- &manager.SupplementalTask{Mode: manager.SuppDetach, LinkUUID: linkUUID}
	result := <-mgr.SupplementalManager.ResultChan
	if !result.OK {
		return nil
	}
	return result.Link
}
