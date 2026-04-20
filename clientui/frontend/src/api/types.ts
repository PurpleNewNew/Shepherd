// 前端共享的 TypeScript 类型，镜像 backend/service/types.go。
// 保持字段命名与 JSON tag 一致。
export type ConnectionPhase =
  | 'disconnected'
  | 'awaiting-trust'
  | 'connecting'
  | 'connected';

export interface ConnectRequest {
  endpoint: string;
  token: string;
  useTLS: boolean;
  serverName?: string;
  remember: boolean;
  label?: string;
  forceAccept: boolean;
}

export interface ConnectionStatus {
  phase: ConnectionPhase;
  endpoint?: string;
  useTLS: boolean;
  fingerprint?: string;
  connectedAt?: string;
  error?: string;
}

export interface ConnectResult {
  phase: ConnectionPhase;
  pendingFingerprint?: string;
  mismatchExpected?: string;
  endpoint?: string;
  message?: string;
  status?: ConnectionStatus;
}

export interface RecentConnection {
  id: string;
  label: string;
  endpoint: string;
  useTLS: boolean;
  serverName?: string;
  fingerprint?: string;
  lastUsedAt: string;
}

export interface NodeSummary {
  uuid: string;
  alias?: string;
  parentUuid?: string;
  status?: string;
  network?: string;
  sleep?: string;
  memo?: string;
  depth: number;
  tags?: string[];
  activeStreams: number;
  workProfile?: string;
}

export interface EdgeSummary {
  parentUuid: string;
  childUuid: string;
  supplemental: boolean;
}

export interface StreamDiag {
  streamId: number;
  targetUuid: string;
  kind: string;
  outbound: boolean;
  pending: number;
  inFlight: number;
  window: number;
  seq: number;
  ack: number;
  rto: string;
  lastActivity: string;
  sessionId: string;
}

export interface SessionSummary {
  targetUuid: string;
  status: string;
  active: boolean;
  connected: boolean;
  remoteAddr?: string;
  upstream?: string;
  downstream?: string;
  networkId?: string;
  lastSeen?: string;
  lastError?: string;
  sleepSeconds?: number;
  workSeconds?: number;
  jitter?: number;
}

export interface SleepProfile {
  targetUuid: string;
  sleepSeconds?: number;
  workSeconds?: number;
  jitter?: number;
  profile?: string;
  lastUpdated?: string;
  nextWakeAt?: string;
  status?: string;
}

export interface Snapshot {
  nodes: NodeSummary[];
  edges: EdgeSummary[];
  streams: StreamDiag[];
  sessions: SessionSummary[];
  sleepProfiles?: SleepProfile[];
  fetchedAt: string;
}

export interface PivotListenerDTO {
  listenerId: string;
  protocol: string;
  bind: string;
  status: string;
  mode: string;
}

export interface NodeDetail {
  node: NodeSummary;
  sessions: SessionSummary[];
  streams: StreamDiag[];
  pivotListeners?: PivotListenerDTO[];
  sleep?: SleepProfile;
  fetchedAt: string;
}

export interface DTNMetrics {
  enqueued: number;
  delivered: number;
  failed: number;
  retried: number;
  global: {
    total: number;
    ready: number;
    held: number;
    capacity: number;
    highWatermark: number;
    averageWait: string;
    droppedTotal: number;
    expiredTotal: number;
  };
}

export interface ReconnectMetrics {
  attempts: number;
  success: number;
  failures: number;
  lastError?: string;
}

export interface SupplementalSnapshot {
  enabled: boolean;
  queueLength: number;
  pendingActions: number;
  activeLinks: number;
  dispatched: number;
  success: number;
  failures: number;
  dropped: number;
  recycled: number;
  queueHigh: number;
  lastFailure?: string;
}

export interface MetricsBundle {
  dtn: DTNMetrics;
  reconnect: ReconnectMetrics;
  supplemental: SupplementalSnapshot;
  capturedAt: string;
}

export interface EnqueueDTNRequest {
  target: string;
  payload: string;
  priority: 'low' | 'normal' | 'high';
  ttlSeconds: number;
}

export interface EnqueueDTNResult {
  bundleId: string;
  enqueuedAt: string;
}

export interface UpdateSleepRequest {
  target: string;
  sleepSeconds?: number;
  workSeconds?: number;
  jitter?: number;
}

export interface PruneOfflineResult {
  removed: number;
}

export interface SupplementalEventDTO {
  seq: number;
  kind: string;
  action: string;
  source?: string;
  target?: string;
  detail?: string;
  timestamp: string;
}

export type EventKind =
  | 'node'
  | 'log'
  | 'stream'
  | 'listener'
  | 'session'
  | 'dial'
  | 'proxy'
  | 'sleep'
  | 'supplemental'
  | 'chat'
  | 'audit'
  | 'loot'
  | 'unknown';

export interface TimelineEvent {
  seq: number;
  kind: EventKind;
  action: string;
  timestamp: string;
  summary: string;
  target?: string;
  source?: string;
  level?: string;
  reason?: string;
  extras?: Record<string, string>;
}
