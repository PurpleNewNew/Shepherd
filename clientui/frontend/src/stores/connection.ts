import { defineStore } from 'pinia';
import { ref, computed } from 'vue';
import type {
  ConnectRequest,
  ConnectResult,
  ConnectionPhase,
  ConnectionStatus,
  RecentConnection,
} from '@/api/types';
import {
  connect as apiConnect,
  disconnect as apiDisconnect,
  getConnectionStatus,
  listRecentConnections,
  onConnectionStatus,
  removeRecentConnection,
  forgetFingerprint,
  hasBindings,
} from '@/api/bindings';

export const useConnectionStore = defineStore('connection', () => {
  const phase = ref<ConnectionPhase>('disconnected');
  const endpoint = ref<string>('');
  const useTLS = ref<boolean>(false);
  const fingerprint = ref<string>('');
  const connectedAt = ref<string>('');
  const lastError = ref<string>('');

  // TOFU pending 决策
  const pendingFingerprint = ref<string>('');
  const pendingMismatch = ref<string>('');

  const recent = ref<RecentConnection[]>([]);
  const bindingsReady = ref<boolean>(hasBindings());

  let unsubStatus: (() => void) | null = null;

  const isConnected = computed(() => phase.value === 'connected');
  const awaitingTrust = computed(() => phase.value === 'awaiting-trust');
  const connecting = computed(() => phase.value === 'connecting');

  function applyStatus(s: ConnectionStatus) {
    phase.value = s.phase;
    endpoint.value = s.endpoint ?? '';
    useTLS.value = !!s.useTLS;
    fingerprint.value = s.fingerprint ?? '';
    connectedAt.value = s.connectedAt ?? '';
    lastError.value = s.error ?? '';
  }

  async function refreshStatus() {
    if (!bindingsReady.value) return;
    try {
      applyStatus(await getConnectionStatus());
    } catch (err) {
      console.warn('refreshStatus error', err);
    }
  }

  async function refreshRecent() {
    if (!bindingsReady.value) return;
    try {
      recent.value = (await listRecentConnections()) ?? [];
    } catch (err) {
      console.warn('listRecentConnections error', err);
    }
  }

  async function connect(req: ConnectRequest): Promise<ConnectResult> {
    pendingFingerprint.value = '';
    pendingMismatch.value = '';
    lastError.value = '';
    phase.value = 'connecting';
    try {
      const res = await apiConnect(req);
      if (res.status) applyStatus(res.status);
      if (res.phase === 'awaiting-trust') {
        phase.value = 'awaiting-trust';
        pendingFingerprint.value = res.pendingFingerprint ?? '';
      }
      if (res.mismatchExpected) {
        pendingMismatch.value = res.mismatchExpected;
      }
      if (res.phase === 'disconnected') {
        lastError.value = res.message ?? 'connection failed';
      }
      return res;
    } catch (err: any) {
      phase.value = 'disconnected';
      lastError.value = err?.message ?? String(err);
      throw err;
    } finally {
      await refreshRecent();
    }
  }

  async function disconnect() {
    try {
      await apiDisconnect();
    } finally {
      applyStatus({ phase: 'disconnected', useTLS: false });
    }
  }

  async function forget(ep: string) {
    try {
      await forgetFingerprint(ep);
    } catch (err) {
      console.warn('forgetFingerprint error', err);
    }
  }

  async function remove(id: string) {
    try {
      await removeRecentConnection(id);
    } finally {
      await refreshRecent();
    }
  }

  function bootstrap() {
    if (!bindingsReady.value) return;
    refreshStatus();
    refreshRecent();
    unsubStatus?.();
    unsubStatus = onConnectionStatus((s) => applyStatus(s));
  }

  function dispose() {
    unsubStatus?.();
    unsubStatus = null;
  }

  return {
    phase,
    endpoint,
    useTLS,
    fingerprint,
    connectedAt,
    lastError,
    pendingFingerprint,
    pendingMismatch,
    recent,
    bindingsReady,
    isConnected,
    awaitingTrust,
    connecting,
    connect,
    disconnect,
    refreshStatus,
    refreshRecent,
    remove,
    forget,
    bootstrap,
    dispose,
  };
});
