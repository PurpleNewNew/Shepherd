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

  function applySnapshot(s: Snapshot, options: { preserveTopologyIdentity?: boolean } = {}) {
    const nextNodes = s.nodes ?? [];
    const nextEdges = s.edges ?? [];
    if (!options.preserveTopologyIdentity || topologyChanged(nextNodes, nextEdges)) {
      nodes.value = nextNodes;
      edges.value = nextEdges;
    } else {
      mergeNodes(nextNodes);
      mergeEdges(nextEdges);
    }
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

  async function refreshQuietly() {
    error.value = '';
    try {
      const snap = await getSnapshot();
      applySnapshot(snap, { preserveTopologyIdentity: true });
    } catch (err: any) {
      error.value = err?.message ?? String(err);
    }
  }

  function topologyChanged(nextNodes: NodeSummary[], nextEdges: EdgeSummary[]) {
    return topologySignature(nodes.value, edges.value) !== topologySignature(nextNodes, nextEdges);
  }

  function topologySignature(inputNodes: NodeSummary[], inputEdges: EdgeSummary[]) {
    const nodeKeys = inputNodes
      .map((n) => n.uuid)
      .sort()
      .join('|');
    const edgeKeys = inputEdges
      .map((e) => [e.parentUuid, e.childUuid, e.supplemental ? 's' : 'p'].join(':'))
      .sort()
      .join('|');
    return `${nodeKeys}#${edgeKeys}`;
  }

  function mergeNodes(nextNodes: NodeSummary[]) {
    const byUUID = new Map(nextNodes.map((n) => [n.uuid, n]));
    for (let i = 0; i < nodes.value.length; i++) {
      const next = byUUID.get(nodes.value[i].uuid);
      if (next) nodes.value[i] = next;
    }
  }

  function mergeEdges(nextEdges: EdgeSummary[]) {
    for (let i = 0; i < edges.value.length; i++) {
      const next = nextEdges[i];
      if (next) edges.value[i] = next;
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
    refreshQuietly,
    applySnapshot,
    select,
    clear,
  };
});
