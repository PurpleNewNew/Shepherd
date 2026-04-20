// Wails v2 通过 Bind 把 Go 方法挂在 window.go.<pkg>.<Struct>.<Method> 下。
// 对 *service.API，路径是 window.go.service.API.XXX。
// 此处做一层薄封装，便于类型化与 mock。

import type {
  ConnectRequest,
  ConnectResult,
  ConnectionStatus,
  RecentConnection,
  Snapshot,
  NodeDetail,
  MetricsBundle,
  EnqueueDTNRequest,
  EnqueueDTNResult,
  UpdateSleepRequest,
  PruneOfflineResult,
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

export const getNodeDetail = (uuid: string) =>
  call<NodeDetail>('GetNodeDetail', uuid);

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
