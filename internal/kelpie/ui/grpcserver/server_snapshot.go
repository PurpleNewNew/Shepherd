package grpcserver

import (
	"context"
	"strings"
	"time"

	"codeberg.org/agnoie/shepherd/internal/kelpie/process"
	"codeberg.org/agnoie/shepherd/internal/kelpie/stream"
	"codeberg.org/agnoie/shepherd/internal/kelpie/topology"
	"codeberg.org/agnoie/shepherd/internal/kelpie/uipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *service) GetSnapshot(ctx context.Context, _ *uipb.SnapshotRequest) (*uipb.SnapshotResponse, error) {
	if s == nil || s.admin == nil {
		return nil, status.Error(codes.Unavailable, "admin unavailable")
	}
	topoSnap := s.admin.TopologySnapshot("", "")
	streams := s.admin.StreamDiagnostics()
	snapshot := &uipb.Snapshot{
		Nodes:               make([]*uipb.NodeInfo, 0, len(topoSnap.Nodes)),
		Edges:               make([]*uipb.Edge, 0, len(topoSnap.Edges)),
		Streams:             make([]*uipb.StreamDiag, 0, len(streams)),
		RecentLogs:          []*uipb.LogEntry{},
		PivotListeners:      nil,
		Sessions:            nil,
		ControllerListeners: nil,
	}
	for _, node := range topoSnap.Nodes {
		snapshot.Nodes = append(snapshot.Nodes, convertNodeInfo(node))
	}
	for _, edge := range topoSnap.Edges {
		snapshot.Edges = append(snapshot.Edges, convertEdge(edge))
	}
	for _, diag := range streams {
		snapshot.Streams = append(snapshot.Streams, convertStreamDiag(diag))
	}
	if listeners := s.admin.ListListeners(process.ListenerFilter{}); len(listeners) > 0 {
		snapshot.PivotListeners = convertPivotListeners(listeners)
	}
	if ctrl := s.admin.ListControllerListeners(); len(ctrl) > 0 {
		snapshot.ControllerListeners = convertControllerListeners(ctrl)
	}
	if sessions := s.admin.Sessions(process.SessionFilter{}); len(sessions) > 0 {
		snapshot.Sessions = convertSessions(sessions)
	}
	return &uipb.SnapshotResponse{Snapshot: snapshot}, nil
}

func (s *service) ListNodes(ctx context.Context, req *uipb.ListNodesRequest) (*uipb.ListNodesResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "missing request")
	}
	if s == nil || s.admin == nil {
		return nil, status.Error(codes.Unavailable, "admin unavailable")
	}
	target := strings.TrimSpace(req.GetTargetUuid())
	network := strings.TrimSpace(req.GetNetworkId())
	snapshot := s.snapshotView(target, network)
	resp := &uipb.ListNodesResponse{Nodes: make([]*uipb.NodeInfo, 0, len(snapshot.Nodes))}
	for _, node := range snapshot.Nodes {
		resp.Nodes = append(resp.Nodes, convertNodeInfo(node))
	}
	return resp, nil
}

func (s *service) GetTopology(ctx context.Context, req *uipb.GetTopologyRequest) (*uipb.GetTopologyResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "missing request")
	}
	if s == nil || s.admin == nil {
		return nil, status.Error(codes.Unavailable, "admin unavailable")
	}
	target := strings.TrimSpace(req.GetTargetUuid())
	network := strings.TrimSpace(req.GetNetworkId())
	snapshot := s.snapshotView(target, network)
	resp := &uipb.GetTopologyResponse{
		Nodes: make([]*uipb.NodeInfo, 0, len(snapshot.Nodes)),
		Edges: make([]*uipb.Edge, 0, len(snapshot.Edges)),
	}
	for _, node := range snapshot.Nodes {
		resp.Nodes = append(resp.Nodes, convertNodeInfo(node))
	}
	for _, edge := range snapshot.Edges {
		resp.Edges = append(resp.Edges, convertEdge(edge))
	}
	if !snapshot.LastUpdated.IsZero() {
		resp.LastUpdated = snapshot.LastUpdated.UTC().Format(time.RFC3339Nano)
	}
	return resp, nil
}

func (s *service) WatchEvents(_ *uipb.WatchEventsRequest, srv uipb.KelpieUIService_WatchEventsServer) error {
	if s == nil || s.admin == nil {
		return status.Error(codes.Unavailable, "admin unavailable")
	}
	subID, subCh := s.registerSubscriber()
	defer s.unregisterSubscriber(subID)
	prevNodes := make(map[string]topology.UINodeSnapshot)
	prevStreams := make(map[uint32]stream.SessionDiag)
	prevListeners := make(map[string]process.ListenerRecord)
	prevSessions := make(map[string]process.SessionInfo)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	emitNodeEvent := func(kind uipb.NodeEvent_Kind, node topology.UINodeSnapshot) error {
		event := &uipb.UiEvent{
			Payload: &uipb.UiEvent_NodeEvent{
				NodeEvent: &uipb.NodeEvent{
					Kind: kind,
					Node: convertNodeInfo(node),
				},
			},
		}
		return srv.Send(event)
	}
	emitStreamEvent := func(kind uipb.StreamEvent_Kind, diag stream.SessionDiag, reason string) error {
		event := &uipb.UiEvent{
			Payload: &uipb.UiEvent_StreamEvent{
				StreamEvent: &uipb.StreamEvent{
					Kind:   kind,
					Stream: convertStreamDiag(diag),
					Reason: reason,
				},
			},
		}
		return srv.Send(event)
	}
	emitListenerEvent := func(kind uipb.PivotListenerEvent_Kind, rec process.ListenerRecord) error {
		event := &uipb.UiEvent{
			Payload: &uipb.UiEvent_ListenerEvent{
				ListenerEvent: &uipb.PivotListenerEvent{
					Kind:     kind,
					Listener: convertPivotListener(rec),
				},
			},
		}
		return srv.Send(event)
	}
	sendSession := func(kind uipb.SessionEvent_Kind, session process.SessionInfo, reason string) error {
		event := &uipb.UiEvent{
			Payload: &uipb.UiEvent_SessionEvent{
				SessionEvent: &uipb.SessionEvent{
					Kind:    kind,
					Session: convertSession(session),
					Reason:  reason,
				},
			},
		}
		return srv.Send(event)
	}

	initialSnap := s.admin.TopologySnapshot("", "")
	for _, node := range initialSnap.Nodes {
		prevNodes[node.UUID] = node
		if err := emitNodeEvent(uipb.NodeEvent_ADDED, node); err != nil {
			return err
		}
	}
	for _, diag := range s.admin.StreamDiagnostics() {
		prevStreams[diag.ID] = diag
		if err := emitStreamEvent(uipb.StreamEvent_STREAM_OPENED, diag, ""); err != nil {
			return err
		}
	}
	for _, rec := range s.admin.ListListeners(process.ListenerFilter{}) {
		prevListeners[rec.ID] = rec
		if err := emitListenerEvent(uipb.PivotListenerEvent_PIVOT_LISTENER_ADDED, rec); err != nil {
			return err
		}
	}
	for _, sess := range s.admin.Sessions(process.SessionFilter{}) {
		prevSessions[strings.ToLower(sess.TargetUUID)] = sess
		if err := sendSession(uipb.SessionEvent_SESSION_EVENT_ADDED, sess, ""); err != nil {
			return err
		}
	}

	for {
		select {
		case <-srv.Context().Done():
			return srv.Context().Err()
		case event := <-subCh:
			if event != nil {
				if err := srv.Send(event); err != nil {
					return err
				}
			}
		case <-ticker.C:
			snap := s.admin.TopologySnapshot("", "")
			currentNodes := make(map[string]topology.UINodeSnapshot, len(snap.Nodes))
			for _, node := range snap.Nodes {
				currentNodes[node.UUID] = node
				prev, ok := prevNodes[node.UUID]
				if !ok {
					if err := emitNodeEvent(uipb.NodeEvent_ADDED, node); err != nil {
						return err
					}
					continue
				}
				if nodeChanged(prev, node) {
					if err := emitNodeEvent(uipb.NodeEvent_UPDATED, node); err != nil {
						return err
					}
				}
			}
			for uuid, prev := range prevNodes {
				if _, ok := currentNodes[uuid]; !ok {
					if err := emitNodeEvent(uipb.NodeEvent_REMOVED, prev); err != nil {
						return err
					}
				}
			}
			prevNodes = currentNodes

			streamDiags := s.admin.StreamDiagnostics()
			currentStreams := make(map[uint32]stream.SessionDiag, len(streamDiags))
			for _, diag := range streamDiags {
				currentStreams[diag.ID] = diag
				if prev, ok := prevStreams[diag.ID]; ok {
					if streamChanged(prev, diag) {
						if err := emitStreamEvent(uipb.StreamEvent_STREAM_UPDATED, diag, ""); err != nil {
							return err
						}
					}
				} else {
					if err := emitStreamEvent(uipb.StreamEvent_STREAM_OPENED, diag, ""); err != nil {
						return err
					}
				}
			}
			for id, diag := range prevStreams {
				if _, ok := currentStreams[id]; !ok {
					reason := s.admin.StreamCloseReason(id)
					if err := emitStreamEvent(uipb.StreamEvent_STREAM_CLOSED, diag, reason); err != nil {
						return err
					}
				}
			}
			prevStreams = currentStreams

			listenerList := s.admin.ListListeners(process.ListenerFilter{})
			currentListeners := make(map[string]process.ListenerRecord, len(listenerList))
			for _, rec := range listenerList {
				currentListeners[rec.ID] = rec
				if prev, ok := prevListeners[rec.ID]; ok {
					if listenerChanged(prev, rec) {
						if err := emitListenerEvent(uipb.PivotListenerEvent_PIVOT_LISTENER_UPDATED, rec); err != nil {
							return err
						}
					}
				} else {
					if err := emitListenerEvent(uipb.PivotListenerEvent_PIVOT_LISTENER_ADDED, rec); err != nil {
						return err
					}
				}
			}
			for id, prev := range prevListeners {
				if _, ok := currentListeners[id]; !ok {
					if err := emitListenerEvent(uipb.PivotListenerEvent_PIVOT_LISTENER_REMOVED, prev); err != nil {
						return err
					}
				}
			}
			prevListeners = currentListeners

			sessionList := s.admin.Sessions(process.SessionFilter{})
			currentSessions := make(map[string]process.SessionInfo, len(sessionList))
			for _, sess := range sessionList {
				key := strings.ToLower(sess.TargetUUID)
				currentSessions[key] = sess
				if prev, ok := prevSessions[key]; ok {
					if sessionChanged(prev, sess) {
						if err := sendSession(uipb.SessionEvent_SESSION_EVENT_UPDATED, sess, ""); err != nil {
							return err
						}
					}
				} else {
					if err := sendSession(uipb.SessionEvent_SESSION_EVENT_ADDED, sess, ""); err != nil {
						return err
					}
				}
			}
			for id, prev := range prevSessions {
				if _, ok := currentSessions[id]; !ok {
					if err := sendSession(uipb.SessionEvent_SESSION_EVENT_REMOVED, prev, ""); err != nil {
						return err
					}
				}
			}
			prevSessions = currentSessions
		}
	}
}
