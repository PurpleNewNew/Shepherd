import { defineStore } from 'pinia';
import { ref, computed } from 'vue';
import type {
  EdgeSummary,
  NodeSummary,
  SessionSummary,
  StreamDiag,
  Snapshot,
  PivotListenerDTO,
  ControllerListenerDTO,
} from '@/api/types';
import { getSnapshot } from '@/api/bindings';

export const useTopologyStore = defineStore('topology', () => {
  const nodes = ref<NodeSummary[]>([]);
  const edges = ref<EdgeSummary[]>([]);
  const streams = ref<StreamDiag[]>([]);
  const sessions = ref<SessionSummary[]>([]);
  const pivotListeners = ref<PivotListenerDTO[]>([]);
  const controllerListeners = ref<ControllerListenerDTO[]>([]);

  const loading = ref(false);
  const error = ref<string>('');

  const selectedUUID = ref<string>('');

  const nodeMap = computed(() => {
    const m = new Map<string, NodeSummary>();
    for (const n of nodes.value) m.set(n.uuid, n);
    return m;
  });

  function applySnapshot(s: Snapshot) {
    nodes.value = s.nodes ?? [];
    edges.value = s.edges ?? [];
    streams.value = s.streams ?? [];
    sessions.value = s.sessions ?? [];
    pivotListeners.value = s.pivotListeners ?? [];
    controllerListeners.value = s.controllerListeners ?? [];
  }

  async function refresh() {
    loading.value = true;
    error.value = '';
    try {
      const snap = await getSnapshot();
      applySnapshot(snap);
    } catch (err: any) {
      error.value = err?.message ?? String(err);
    } finally {
      loading.value = false;
    }
  }

  function select(uuid: string) {
    selectedUUID.value = uuid;
  }

  function clear() {
    nodes.value = [];
    edges.value = [];
    streams.value = [];
    sessions.value = [];
    pivotListeners.value = [];
    controllerListeners.value = [];
    selectedUUID.value = '';
    error.value = '';
  }

  return {
    nodes,
    edges,
    streams,
    sessions,
    pivotListeners,
    controllerListeners,
    loading,
    error,
    selectedUUID,
    nodeMap,
    refresh,
    applySnapshot,
    select,
    clear,
  };
});
