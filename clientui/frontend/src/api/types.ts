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
  pivotListeners?: PivotListenerDTO[];
  controllerListeners?: ControllerListenerDTO[];
  sleepProfiles?: SleepProfile[];
  fetchedAt: string;
}

export interface PivotListenerDTO {
  listenerId: string;
  targetUuid?: string;
  route?: string;
  protocol: string;
  bind: string;
  status: string;
  mode: string;
  lastError?: string;
  metadata?: Record<string, string>;
  createdAt?: string;
  updatedAt?: string;
}

export interface ControllerListenerDTO {
  listenerId: string;
  protocol: string;
  bind: string;
  status: string;
  lastError?: string;
  createdAt?: string;
  updatedAt?: string;
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

export interface StreamHandle {
  handleId: string;
  targetUuid: string;
  sessionId: string;
  kind: string;
  streamId?: number;
  options?: Record<string, string>;
  status: string;
}

export interface StartShellRequest {
  target: string;
  mode?: 'pipe' | 'pty';
  resumeSessionId?: string;
}

export interface StartSocksProxyRequest {
  target: string;
  auth?: 'none' | 'userpass';
  username?: string;
  password?: string;
}

export interface StartSshSessionRequest {
  target: string;
  serverAddr: string;
  username: string;
  password: string;
}

export interface StartSshTunnelRequest {
  target: string;
  serverAddr: string;
  agentPort: string;
  authMethod?: 'password' | 'cert';
  username: string;
  password?: string;
  privateKey?: string;
}

export interface StartForwardProxyRequest {
  target: string;
  localBind: string;
  remoteAddr: string;
}

export interface StartForwardProxyResult {
  handle: StreamHandle;
  proxyId: string;
  kind?: string;
  bind: string;
  remoteAddr: string;
}

export interface StopForwardProxyRequest {
  target: string;
  proxyId: string;
}

export interface StopForwardProxyResult {
  stopped: number;
}

export interface StartBackwardProxyRequest {
  target: string;
  remotePort: string;
  localPort: string;
}

export interface StartBackwardProxyResult {
  handle: StreamHandle;
  proxyId: string;
  kind?: string;
  remotePort: string;
  localPort: string;
}

export interface StopBackwardProxyRequest {
  target: string;
  proxyId: string;
}

export interface StopBackwardProxyResult {
  stopped: number;
}

export interface StreamDataRequest {
  handleId: string;
  data: string;
}

export interface StreamResizeRequest {
  handleId: string;
  rows: number;
  cols: number;
}

export interface CloseInteractiveStreamRequest {
  handleId: string;
  reason?: string;
}

export interface CloseStreamByIDRequest {
  streamId: number;
  reason?: string;
}

export interface StreamPingRequest {
  target: string;
  count: number;
  payloadSize: number;
}

export interface StreamEventDTO {
  handleId: string;
  targetUuid?: string;
  sessionId?: string;
  kind?: string;
  streamId?: number;
  type: 'open' | 'data' | 'control' | 'closed' | 'error';
  data?: string;
  error?: string;
}

export interface RemoteFileEntry {
  name: string;
  path: string;
  isDir: boolean;
  isDrive?: boolean;
  isSymlink?: boolean;
  size?: number;
  mode?: string;
  modifiedAt?: string;
  hidden?: boolean;
}

export interface RemoteFileListing {
  requestedPath?: string;
  resolvedPath?: string;
  displayPath?: string;
  rootPath?: string;
  parentPath?: string;
  canGoUp: boolean;
  virtualRoot: boolean;
  entries?: RemoteFileEntry[];
}

export interface ListRemoteFilesRequest {
  target: string;
  path?: string;
}

export interface CollectRemoteFileRequest {
  target: string;
  remotePath: string;
  tags?: string[];
}

export interface UploadRemoteFileRequest {
  target: string;
  localPath: string;
  remotePath: string;
}

export interface UploadRemoteFileResult {
  remotePath: string;
  size?: number;
  sha256?: string;
  mime?: string;
  message?: string;
}

export interface LootItem {
  lootId: string;
  targetUuid?: string;
  operator?: string;
  category?: string;
  name?: string;
  storageRef?: string;
  originPath?: string;
  hash?: string;
  size?: number;
  mime?: string;
  metadata?: Record<string, string>;
  tags?: string[];
  createdAt?: string;
}

export interface CollectRemoteFileResult {
  item: LootItem;
}

export interface ListLootRequest {
  target?: string;
  limit?: number;
}

export interface ListLootResult {
  items: LootItem[];
}

export interface ExportLootRequest {
  lootId: string;
  localPath?: string;
}

export interface ExportLootResult {
  item: LootItem;
  localPath: string;
  bytes: number;
}

export interface SessionActionRequest {
  target: string;
  action?: 'alive' | 'dead' | 'maintenance';
  force?: boolean;
  reason?: string;
}

export interface SessionActionResult {
  session?: SessionSummary;
  accepted?: boolean;
  terminated?: boolean;
  message?: string;
}

export interface SessionMetricDTO {
  name: string;
  value: string;
}

export interface SessionIssueDTO {
  code: string;
  message: string;
  detail?: string;
}

export interface SessionProcessDTO {
  pid: string;
  name: string;
  user?: string;
  status?: string;
  path?: string;
  startedAt?: string;
}

export interface SessionDiagnosticsDTO {
  session: SessionSummary;
  metrics?: SessionMetricDTO[];
  issues?: SessionIssueDTO[];
  processes?: SessionProcessDTO[];
}

export interface ListenerSpecRequest {
  listenerId?: string;
  target?: string;
  protocol?: string;
  bind?: string;
  mode?: 'normal' | 'iptables' | 'soreuse';
  desiredStatus?: 'running' | 'stopped' | 'pending' | 'failed' | 'resume' | 'pause';
  includeSpec?: boolean;
}

export interface ListPivotListenersRequest {
  target?: string;
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
