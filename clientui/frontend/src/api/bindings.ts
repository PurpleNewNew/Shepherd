// Wails v2 通过 Bind 把 Go 方法挂在 window.go.<pkg>.<Struct>.<Method> 下。
// 对 *service.API，路径是 window.go.service.API.XXX。
// 此处做一层薄封装，便于类型化与 mock。

import type {
  ConnectRequest,
  ConnectResult,
  ConnectionStatus,
  RecentConnection,
  Snapshot,
  MetricsBundle,
  EnqueueDTNRequest,
  EnqueueDTNResult,
  UpdateSleepRequest,
  PruneOfflineResult,
  StartShellRequest,
  StreamHandle,
  StartSocksProxyRequest,
  StartSshSessionRequest,
  StartSshTunnelRequest,
  StartForwardProxyRequest,
  StartForwardProxyResult,
  StopForwardProxyRequest,
  StopForwardProxyResult,
  StartBackwardProxyRequest,
  StartBackwardProxyResult,
  StopBackwardProxyRequest,
  StopBackwardProxyResult,
  StreamDataRequest,
  StreamResizeRequest,
  CloseInteractiveStreamRequest,
  CloseStreamByIDRequest,
  StreamPingRequest,
  StreamDiag,
  ListRemoteFilesRequest,
  RemoteFileListing,
  CollectRemoteFileRequest,
  CollectRemoteFileResult,
  UploadRemoteFileRequest,
  UploadRemoteFileResult,
  ListLootRequest,
  ListLootResult,
  ExportLootRequest,
  ExportLootResult,
  SessionActionRequest,
  SessionActionResult,
  SessionDiagnosticsDTO,
  ListenerSpecRequest,
  ListPivotListenersRequest,
  PivotListenerDTO,
  ControllerListenerDTO,
  StreamEventDTO,
  SupplementalEventDTO,
  TimelineEvent,
} from './types';

type WailsGo = Record<string, Record<string, Record<string, (...args: any[]) => Promise<any>>>>;

function goAPI() {
  const raw = (window as any).go as WailsGo | undefined;
  if (!raw || !raw.service || !raw.service.API) {
    return null;
  }
  return raw.service.API;
}

function notAvailable<T>(method: string): Promise<T> {
  return Promise.reject(
    new Error(
      `Wails binding service.API.${method} is not available; Stockman must be launched through the Wails runtime.`,
    ),
  );
}

function call<T>(method: string, ...args: unknown[]): Promise<T> {
  const api = goAPI();
  if (!api || typeof api[method] !== 'function') {
    return notAvailable<T>(method);
  }
  return api[method](...args) as Promise<T>;
}

export function hasBindings(): boolean {
  return goAPI() !== null;
}

/* ---------- 连接管理 ---------- */

export const listRecentConnections = () =>
  call<RecentConnection[]>('ListRecentConnections');

export const removeRecentConnection = (id: string) =>
  call<void>('RemoveRecentConnection', id);

export const connect = (req: ConnectRequest) =>
  call<ConnectResult>('Connect', req);

export const disconnect = () => call<void>('Disconnect');

export const getConnectionStatus = () =>
  call<ConnectionStatus>('GetConnectionStatus');

export const forgetFingerprint = (endpoint: string) =>
  call<void>('ForgetFingerprint', endpoint);

/* ---------- 拓扑 / 节点 / 指标 ---------- */

export const getSnapshot = () => call<Snapshot>('GetSnapshot');

export const getMetrics = () => call<MetricsBundle>('GetMetrics');

export const listSupplementalEvents = (limit: number) =>
  call<SupplementalEventDTO[]>('ListSupplementalEvents', limit);

export const recentEvents = (limit: number) =>
  call<TimelineEvent[]>('RecentEvents', limit);

/* ---------- 演示控制台 ---------- */

export const enqueueDTN = (req: EnqueueDTNRequest) =>
  call<EnqueueDTNResult>('EnqueueDTN', req);

export const updateSleep = (req: UpdateSleepRequest) =>
  call<void>('UpdateSleep', req);

export const pruneOffline = () => call<PruneOfflineResult>('PruneOffline');

/* ---------- 目标交互能力 ---------- */

export const startShell = (req: StartShellRequest) =>
  call<StreamHandle>('StartShell', req);

export const startSocksProxy = (req: StartSocksProxyRequest) =>
  call<StreamHandle>('StartSocksProxy', req);

export const startSshSession = (req: StartSshSessionRequest) =>
  call<StreamHandle>('StartSshSession', req);

export const startSshTunnel = (req: StartSshTunnelRequest) =>
  call<void>('StartSshTunnel', req);

export const startForwardProxy = (req: StartForwardProxyRequest) =>
  call<StartForwardProxyResult>('StartForwardProxy', req);

export const stopForwardProxy = (req: StopForwardProxyRequest) =>
  call<StopForwardProxyResult>('StopForwardProxy', req);

export const startBackwardProxy = (req: StartBackwardProxyRequest) =>
  call<StartBackwardProxyResult>('StartBackwardProxy', req);

export const stopBackwardProxy = (req: StopBackwardProxyRequest) =>
  call<StopBackwardProxyResult>('StopBackwardProxy', req);

export const sendStreamData = (req: StreamDataRequest) =>
  call<void>('SendStreamData', req);

export const resizeStream = (req: StreamResizeRequest) =>
  call<void>('ResizeStream', req);

export const closeInteractiveStream = (req: CloseInteractiveStreamRequest) =>
  call<void>('CloseInteractiveStream', req);

export const closeStreamByID = (req: CloseStreamByIDRequest) =>
  call<void>('CloseStreamByID', req);

export const streamDiagnostics = () =>
  call<StreamDiag[]>('StreamDiagnostics');

export const streamPing = (req: StreamPingRequest) =>
  call<void>('StreamPing', req);

export const listRemoteFiles = (req: ListRemoteFilesRequest) =>
  call<RemoteFileListing>('ListRemoteFiles', req);

export const collectRemoteFile = (req: CollectRemoteFileRequest) =>
  call<CollectRemoteFileResult>('CollectRemoteFile', req);

export const uploadRemoteFile = (req: UploadRemoteFileRequest) =>
  call<UploadRemoteFileResult>('UploadRemoteFile', req);

export const listLoot = (req: ListLootRequest) =>
  call<ListLootResult>('ListLoot', req);

export const exportLoot = (req: ExportLootRequest) =>
  call<ExportLootResult>('ExportLoot', req);

export const markSession = (req: SessionActionRequest) =>
  call<SessionActionResult>('MarkSession', req);

export const repairSession = (req: SessionActionRequest) =>
  call<SessionActionResult>('RepairSession', req);

export const reconnectSession = (req: SessionActionRequest) =>
  call<SessionActionResult>('ReconnectSession', req);

export const terminateSession = (req: SessionActionRequest) =>
  call<SessionActionResult>('TerminateSession', req);

export const getSessionDiagnostics = (req: SessionActionRequest) =>
  call<SessionDiagnosticsDTO>('GetSessionDiagnostics', req);

export const listPivotListeners = (req: ListPivotListenersRequest = {}) =>
  call<PivotListenerDTO[]>('ListPivotListeners', req);

export const createPivotListener = (req: ListenerSpecRequest) =>
  call<PivotListenerDTO>('CreatePivotListener', req);

export const updatePivotListener = (req: ListenerSpecRequest) =>
  call<PivotListenerDTO>('UpdatePivotListener', req);

export const deletePivotListener = (req: ListenerSpecRequest) =>
  call<void>('DeletePivotListener', req);

export const listControllerListeners = () =>
  call<ControllerListenerDTO[]>('ListControllerListeners');

export const createControllerListener = (req: ListenerSpecRequest) =>
  call<ControllerListenerDTO>('CreateControllerListener', req);

export const updateControllerListener = (req: ListenerSpecRequest) =>
  call<ControllerListenerDTO>('UpdateControllerListener', req);

export const deleteControllerListener = (req: ListenerSpecRequest) =>
  call<ControllerListenerDTO>('DeleteControllerListener', req);

/* ---------- 事件订阅（Wails runtime） ---------- */

type EventsRuntime = {
  EventsOn: (name: string, cb: (data: unknown) => void) => () => void;
  EventsOff?: (name: string) => void;
  EventsEmit?: (name: string, ...data: unknown[]) => void;
};

function eventsRuntime(): EventsRuntime | null {
  const rt = (window as any).runtime;
  if (rt && typeof rt.EventsOn === 'function') return rt as EventsRuntime;
  return null;
}

export function onKelpieEvent(cb: (ev: TimelineEvent) => void): () => void {
  const rt = eventsRuntime();
  if (!rt) return () => undefined;
  return rt.EventsOn('kelpie:event', (raw) => cb(raw as TimelineEvent));
}

export function onConnectionStatus(
  cb: (status: ConnectionStatus) => void,
): () => void {
  const rt = eventsRuntime();
  if (!rt) return () => undefined;
  return rt.EventsOn('kelpie:status', (raw) => cb(raw as ConnectionStatus));
}

export function onStreamEvent(cb: (ev: StreamEventDTO) => void): () => void {
  const rt = eventsRuntime();
  if (!rt) return () => undefined;
  return rt.EventsOn('kelpie:stream', (raw) => cb(raw as StreamEventDTO));
}
