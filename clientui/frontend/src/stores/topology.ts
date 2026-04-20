import { defineStore } from 'pinia';
import { ref, computed } from 'vue';
import type {
  EdgeSummary,
  NodeSummary,
  SessionSummary,
  SleepProfile,
  StreamDiag,
  Snapshot,
  NodeDetail,
} from '@/api/types';
import { getSnapshot, getNodeDetail } from '@/api/bindings';

export const useTopologyStore = defineStore('topology', () => {
  const nodes = ref<NodeSummary[]>([]);
  const edges = ref<EdgeSummary[]>([]);
  const streams = ref<StreamDiag[]>([]);
  const sessions = ref<SessionSummary[]>([]);
  const sleepProfiles = ref<SleepProfile[]>([]);
  const fetchedAt = ref<string>('');

  const loading = ref(false);
  const error = ref<string>('');

  const selectedUUID = ref<string>('');
  const detail = ref<NodeDetail | null>(null);
  const detailLoading = ref(false);
  const detailError = ref<string>('');

  const nodeMap = computed(() => {
    const m = new Map<string, NodeSummary>();
    for (const n of nodes.value) m.set(n.uuid, n);
    return m;
  });

  const childrenByParent = computed(() => {
    const m = new Map<string, string[]>();
    for (const e of edges.value) {
      if (e.supplemental) continue;
      const arr = m.get(e.parentUuid) ?? [];
      arr.push(e.childUuid);
      m.set(e.parentUuid, arr);
    }
    return m;
  });

  const rootUUIDs = computed(() => {
    const present = new Set(nodes.value.map((n) => n.uuid));
    const parents = new Set<string>();
    for (const e of edges.value) parents.add(e.childUuid);
    return nodes.value
      .filter((n) => !parents.has(n.uuid) || !present.has(n.parentUuid ?? ''))
      .map((n) => n.uuid);
  });

  function applySnapshot(s: Snapshot) {
    nodes.value = s.nodes ?? [];
    edges.value = s.edges ?? [];
    streams.value = s.streams ?? [];
    sessions.value = s.sessions ?? [];
    sleepProfiles.value = s.sleepProfiles ?? [];
    fetchedAt.value = s.fetchedAt;
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
    if (uuid) loadDetail(uuid);
  }

  async function loadDetail(uuid: string) {
    if (!uuid) {
      detail.value = null;
      return;
    }
    detailLoading.value = true;
    detailError.value = '';
    try {
      detail.value = await getNodeDetail(uuid);
    } catch (err: any) {
      detail.value = null;
      detailError.value = err?.message ?? String(err);
    } finally {
      detailLoading.value = false;
    }
  }

  function clear() {
    nodes.value = [];
    edges.value = [];
    streams.value = [];
    sessions.value = [];
    sleepProfiles.value = [];
    fetchedAt.value = '';
    detail.value = null;
    selectedUUID.value = '';
    error.value = '';
  }

  return {
    nodes,
    edges,
    streams,
    sessions,
    sleepProfiles,
    fetchedAt,
    loading,
    error,
    selectedUUID,
    detail,
    detailLoading,
    detailError,
    nodeMap,
    childrenByParent,
    rootUUIDs,
    refresh,
    applySnapshot,
    select,
    loadDetail,
    clear,
  };
});
