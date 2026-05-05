<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, reactive, ref } from 'vue';
import { useConnectionStore } from '@/stores/connection';
import { useTopologyStore } from '@/stores/topology';
import { useEventsStore } from '@/stores/events';
import { useMetricsStore } from '@/stores/metrics';
import {
  closeInteractiveStream,
  closeStreamByID,
  collectRemoteFile,
  enqueueDTN,
  listRemoteFiles,
  onStreamEvent,
  pruneOffline,
  sendStreamData,
  startForwardProxy,
  startShell,
  startSocksProxy,
  stopForwardProxy,
  streamPing,
  updateSleep,
} from '@/api/bindings';
import type {
  CollectRemoteFileResult,
  EnqueueDTNResult,
  RemoteFileEntry,
  RemoteFileListing,
  NodeSummary,
  SessionSummary,
  StartForwardProxyResult,
  StreamEventDTO,
  StreamHandle,
  TimelineEvent,
} from '@/api/types';

type BuiltinTab = 'events' | 'sessions' | 'dtn' | 'control' | 'streams';
type BottomTab = BuiltinTab | `target:${string}`;
type SideMode = 'targets' | 'networks' | 'listeners';
type MainView = 'table' | 'graph';
type TargetMode = 'overview' | 'shell' | 'files' | 'proxy';
type MenuKey = 'stockman' | 'view' | 'operations' | 'listeners' | 'sessions' | 'reports' | 'help';

const conn = useConnectionStore();
const topo = useTopologyStore();
const events = useEventsStore();
const metrics = useMetricsStore();

const bottomTab = ref<BottomTab>('events');
const sideMode = ref<SideMode>('targets');
const mainView = ref<MainView>('table');
const activeMenu = ref<MenuKey | ''>('');
const targetTabs = ref<string[]>([]);
const targetMode = ref<TargetMode>('overview');
const commandLine = ref('');
const dtnPayload = ref('memo:');
const dtnPriority = ref<'low' | 'normal' | 'high'>('normal');
const sleepSeconds = ref(15);
const workSeconds = ref(5);
const jitter = ref(10);
const shellInput = ref('');
const filePath = ref('/');
const proxyLocalBind = ref('127.0.0.1:1080');
const proxyRemoteAddr = ref('127.0.0.1:80');
const socksUsername = ref('');
const socksPassword = ref('');
const shellHandles = reactive<Record<string, StreamHandle>>({});
const shellLines = reactive<Record<string, string[]>>({});
const fileListings = reactive<Record<string, RemoteFileListing>>({});
const fileErrors = reactive<Record<string, string>>({});
const fileLoading = reactive<Record<string, boolean>>({});
const proxyResults = reactive<Record<string, StartForwardProxyResult[]>>({});

const action = reactive({
  busy: false,
  message: '',
  error: '',
});

const contextMenu = reactive<{
  open: boolean;
  x: number;
  y: number;
  node: NodeSummary | null;
}>({
  open: false,
  x: 0,
  y: 0,
  node: null,
});

const connectedDisplay = computed(() => {
  const prefix = conn.useTLS ? 'TLS ' : '';
  return `${prefix}${conn.endpoint}`;
});

const selectedNode = computed(() => {
  if (!topo.selectedUUID) return null;
  return topo.nodeMap.get(topo.selectedUUID) ?? null;
});

const selectedSessions = computed(() => {
  const uuid = topo.selectedUUID;
  if (!uuid) return [];
  return topo.sessions.filter((s) => s.targetUuid === uuid);
});

const selectedStreams = computed(() => {
  const uuid = topo.selectedUUID;
  if (!uuid) return [];
  return topo.streams.filter((s) => s.targetUuid === uuid);
});

const activeTargetUUID = computed(() =>
  bottomTab.value.startsWith('target:') ? bottomTab.value.slice('target:'.length) : '',
);

const activeTargetNode = computed(() => {
  if (!activeTargetUUID.value) return null;
  return topo.nodeMap.get(activeTargetUUID.value) ?? null;
});

const activeTargetSessions = computed(() => {
  const uuid = activeTargetUUID.value;
  if (!uuid) return [];
  return topo.sessions.filter((s) => s.targetUuid === uuid);
});

const activeTargetStreams = computed(() => {
  const uuid = activeTargetUUID.value;
  if (!uuid) return [];
  return topo.streams.filter((s) => s.targetUuid === uuid);
});

const activeShellHandle = computed(() => {
  const uuid = activeTargetUUID.value;
  return uuid ? shellHandles[uuid] : undefined;
});

const activeShellLines = computed(() => {
  const uuid = activeTargetUUID.value;
  return uuid ? (shellLines[uuid] ?? []) : [];
});

const activeFileListing = computed(() => {
  const uuid = activeTargetUUID.value;
  return uuid ? fileListings[uuid] : undefined;
});

const activeFileError = computed(() => {
  const uuid = activeTargetUUID.value;
  return uuid ? fileErrors[uuid] : '';
});

const activeFileLoading = computed(() => {
  const uuid = activeTargetUUID.value;
  return uuid ? fileLoading[uuid] : false;
});

const activeProxyResults = computed(() => {
  const uuid = activeTargetUUID.value;
  return uuid ? (proxyResults[uuid] ?? []) : [];
});

const orderedNodes = computed(() => {
  return [...topo.nodes].sort((a, b) => {
    const ad = a.depth ?? 0;
    const bd = b.depth ?? 0;
    if (ad !== bd) return ad - bd;
    return labelForNode(a).localeCompare(labelForNode(b));
  });
});

const networkGroups = computed(() => {
  const groups = new Map<string, NodeSummary[]>();
  for (const node of topo.nodes) {
    const key = node.network || 'default';
    groups.set(key, [...(groups.get(key) ?? []), node]);
  }
  return [...groups.entries()].sort(([a], [b]) => a.localeCompare(b));
});

const listeners = computed(() => {
  const map = new Map<string, { key: string; count: number; nodes: string[] }>();
  for (const node of topo.nodes) {
    const key = node.workProfile || node.network || 'implicit';
    const item = map.get(key) ?? { key, count: 0, nodes: [] };
    item.count += 1;
    item.nodes.push(labelForNode(node));
    map.set(key, item);
  }
  return [...map.values()].sort((a, b) => a.key.localeCompare(b.key));
});

const sessionByNode = computed(() => {
  const map = new Map<string, SessionSummary>();
  for (const session of topo.sessions) {
    if (!map.has(session.targetUuid)) map.set(session.targetUuid, session);
  }
  return map;
});

const recentEvents = computed(() => events.events.slice(0, 120));

const stats = computed(() => {
  const dtn = metrics.bundle.dtn;
  const supp = metrics.bundle.supplemental;
  return {
    nodes: topo.nodes.length,
    online: topo.nodes.filter((n) => isOnline(n.status)).length,
    streams: topo.streams.length,
    sessions: topo.sessions.length,
    dtnHeld: dtn.global.held,
    dtnDelivered: dtn.delivered,
    suppActive: supp.activeLinks,
    failures: dtn.failed + supp.failures,
  };
});

onMounted(async () => {
  await refreshAll();
  events.bootstrap();
  metrics.start(3000);
  const offStream = onStreamEvent(handleStreamEvent);
  (window as any).__stockmanOffStream = offStream;
});

onBeforeUnmount(() => {
  metrics.stop();
  events.dispose();
  (window as any).__stockmanOffStream?.();
});

async function refreshAll() {
  await Promise.all([topo.refresh(), metrics.refresh()]);
  if (topo.selectedUUID) {
    await topo.loadDetail(topo.selectedUUID);
  }
}

async function logout() {
  await conn.disconnect();
  topo.clear();
  events.dispose();
  metrics.reset();
}

function labelForNode(node: NodeSummary): string {
  return node.alias || node.uuid.slice(0, 8);
}

function isOnline(status?: string): boolean {
  const normalized = (status || '').toLowerCase();
  return normalized.includes('online') || normalized.includes('connected') || normalized === 'ready';
}

function statusTone(status?: string): 'ok' | 'warn' | 'bad' | 'idle' {
  const normalized = (status || '').toLowerCase();
  if (isOnline(status)) return 'ok';
  if (normalized.includes('sleep') || normalized.includes('hold')) return 'warn';
  if (normalized.includes('error') || normalized.includes('fail') || normalized.includes('lost')) return 'bad';
  return 'idle';
}

function eventTone(ev: TimelineEvent): 'ok' | 'warn' | 'bad' | 'idle' {
  const text = `${ev.level || ''} ${ev.action || ''} ${ev.summary || ''}`.toLowerCase();
  if (text.includes('error') || text.includes('fail') || text.includes('denied')) return 'bad';
  if (text.includes('warn') || text.includes('retry') || text.includes('held')) return 'warn';
  if (text.includes('success') || text.includes('connected') || text.includes('delivered')) return 'ok';
  return 'idle';
}

function selectNode(uuid: string) {
  topo.select(uuid);
}

function toggleMenu(menu: MenuKey) {
  activeMenu.value = activeMenu.value === menu ? '' : menu;
}

function openTargetMenu(ev: MouseEvent, node: NodeSummary) {
  ev.preventDefault();
  ev.stopPropagation();
  topo.select(node.uuid);
  contextMenu.node = node;
  contextMenu.open = true;
  contextMenu.x = Math.min(ev.clientX, window.innerWidth - 248);
  contextMenu.y = Math.min(ev.clientY, window.innerHeight - 302);
}

function closeContextMenu() {
  contextMenu.open = false;
}

function closeOverlays() {
  closeContextMenu();
  activeMenu.value = '';
}

function openTargetTab(uuid: string, mode: TargetMode = 'overview') {
  if (!targetTabs.value.includes(uuid)) {
    targetTabs.value.push(uuid);
  }
  bottomTab.value = `target:${uuid}`;
  targetMode.value = mode;
  topo.select(uuid);
  topo.loadDetail(uuid);
}

function closeTargetTab(uuid: string) {
  targetTabs.value = targetTabs.value.filter((id) => id !== uuid);
  if (bottomTab.value === `target:${uuid}`) {
    bottomTab.value = targetTabs.value.length
      ? `target:${targetTabs.value[targetTabs.value.length - 1]}`
      : 'events';
  }
}

async function chooseTargetAction(kind: 'detail' | 'shell' | 'files' | 'proxy' | 'events' | 'sessions' | 'streams' | 'dtn' | 'sleep' | 'refresh' | 'copy' | 'prune') {
  const node = contextMenu.node;
  closeContextMenu();
  if (!node) return;
  topo.select(node.uuid);
  if (kind === 'detail') {
    openTargetTab(node.uuid);
  } else if (kind === 'shell') {
    openTargetTab(node.uuid, 'shell');
    await ensureShell(node.uuid);
  } else if (kind === 'files') {
    openTargetTab(node.uuid, 'files');
    await loadFiles(node.uuid);
  } else if (kind === 'proxy') {
    openTargetTab(node.uuid, 'proxy');
  } else if (kind === 'events') {
    bottomTab.value = 'events';
  } else if (kind === 'sessions') {
    bottomTab.value = 'sessions';
  } else if (kind === 'streams') {
    bottomTab.value = 'streams';
  } else if (kind === 'dtn') {
    bottomTab.value = 'dtn';
  } else if (kind === 'sleep') {
    bottomTab.value = 'control';
  } else if (kind === 'refresh') {
    await refreshAll();
  } else if (kind === 'copy') {
    await navigator.clipboard?.writeText(node.uuid);
    action.message = `Copied node UUID: ${node.uuid}`;
  } else if (kind === 'prune') {
    await submitPrune();
  }
}

function chooseMenuAction(actionName: 'refresh' | 'table' | 'graph' | 'events' | 'sessions' | 'streams' | 'dtn' | 'sleep' | 'prune' | 'disconnect') {
  activeMenu.value = '';
  if (actionName === 'refresh') refreshAll();
  else if (actionName === 'table') mainView.value = 'table';
  else if (actionName === 'graph') mainView.value = 'graph';
  else if (actionName === 'events') bottomTab.value = 'events';
  else if (actionName === 'sessions') bottomTab.value = 'sessions';
  else if (actionName === 'streams') bottomTab.value = 'streams';
  else if (actionName === 'dtn') bottomTab.value = 'dtn';
  else if (actionName === 'sleep') bottomTab.value = 'control';
  else if (actionName === 'prune') submitPrune();
  else if (actionName === 'disconnect') logout();
}

function resetAction() {
  action.error = '';
  action.message = '';
}

async function submitDTN() {
  resetAction();
  if (!topo.selectedUUID) {
    action.error = '先选择一个目标节点。';
    return;
  }
  if (!dtnPayload.value.trim()) {
    action.error = 'DTN payload 不能为空。';
    return;
  }
  action.busy = true;
  try {
    const result: EnqueueDTNResult = await enqueueDTN({
      target: topo.selectedUUID,
      payload: normalizeDTNPayload(dtnPayload.value),
      priority: dtnPriority.value,
      ttlSeconds: 600,
    });
    action.message = `DTN bundle queued: ${result.bundleId}`;
    await metrics.refresh();
  } catch (err: any) {
    action.error = err?.message ?? String(err);
  } finally {
    action.busy = false;
  }
}

async function submitSleep() {
  resetAction();
  if (!topo.selectedUUID) {
    action.error = '先选择一个目标节点。';
    return;
  }
  action.busy = true;
  try {
    await updateSleep({
      target: topo.selectedUUID,
      sleepSeconds: sleepSeconds.value,
      workSeconds: workSeconds.value,
      jitter: jitter.value,
    });
    action.message = `Sleep profile updated for ${topo.selectedUUID.slice(0, 8)}.`;
    await refreshAll();
  } catch (err: any) {
    action.error = err?.message ?? String(err);
  } finally {
    action.busy = false;
  }
}

async function submitPrune() {
  resetAction();
  action.busy = true;
  try {
    const result = await pruneOffline();
    action.message = `Pruned ${result.removed} offline node(s).`;
    await refreshAll();
  } catch (err: any) {
    action.error = err?.message ?? String(err);
  } finally {
    action.busy = false;
  }
}

function handleStreamEvent(ev: StreamEventDTO) {
  const uuid = ev.targetUuid || activeTargetUUID.value;
  if (!uuid) return;
  if (!shellLines[uuid]) shellLines[uuid] = [];
  if (ev.type === 'open') {
    shellLines[uuid].push(`[stream ${ev.streamId || '-'} opened]`);
    if (shellHandles[uuid] && ev.streamId) shellHandles[uuid].streamId = ev.streamId;
  } else if (ev.type === 'data' && ev.data) {
    shellLines[uuid].push(...ev.data.replace(/\r/g, '').split('\n').filter(Boolean));
  } else if (ev.type === 'closed') {
    shellLines[uuid].push('[stream closed]');
  } else if (ev.type === 'error') {
    shellLines[uuid].push(`[error] ${ev.error || 'stream error'}`);
  }
  shellLines[uuid] = shellLines[uuid].slice(-160);
}

async function ensureShell(uuid = activeTargetUUID.value) {
  if (!uuid) return;
  targetMode.value = 'shell';
  if (shellHandles[uuid]) return;
  if (!shellLines[uuid]) shellLines[uuid] = [];
  try {
    const handle = await startShell({ target: uuid, mode: 'pty' });
    shellHandles[uuid] = handle;
    shellLines[uuid].push(`[shell requested] ${handle.sessionId || handle.handleId}`);
  } catch (err: any) {
    action.error = err?.message ?? String(err);
    shellLines[uuid].push(`[error] ${action.error}`);
  }
}

async function submitShellCommand() {
  const uuid = activeTargetUUID.value;
  const cmd = shellInput.value;
  if (!uuid || !cmd.trim()) return;
  await ensureShell(uuid);
  const handle = shellHandles[uuid];
  shellLines[uuid].push(`$ ${cmd}`);
  shellInput.value = '';
  try {
    await sendStreamData({ handleId: handle.handleId, data: `${cmd}\n` });
  } catch (err: any) {
    action.error = err?.message ?? String(err);
    shellLines[uuid].push(`[error] ${action.error}`);
  }
}

async function closeActiveShell() {
  const uuid = activeTargetUUID.value;
  const handle = uuid ? shellHandles[uuid] : undefined;
  if (!uuid || !handle) return;
  try {
    await closeInteractiveStream({ handleId: handle.handleId, reason: 'operator closed shell' });
  } catch (err: any) {
    action.error = err?.message ?? String(err);
  }
  delete shellHandles[uuid];
}

async function loadFiles(uuid = activeTargetUUID.value, path = filePath.value) {
  if (!uuid) return;
  targetMode.value = 'files';
  const requestedPath = path.trim() || '/';
  fileLoading[uuid] = true;
  fileErrors[uuid] = '';
  try {
    const listing = await listRemoteFiles({ target: uuid, path: requestedPath });
    fileListings[uuid] = listing;
    filePath.value = listing.displayPath || listing.resolvedPath || requestedPath;
  } catch (err: any) {
    const message = err?.message ?? String(err);
    fileErrors[uuid] = message;
    action.error = message;
  } finally {
    fileLoading[uuid] = false;
  }
}

async function openFileEntry(entry: RemoteFileEntry) {
  if (entry.isDir || entry.isDrive) {
    await loadFiles(activeTargetUUID.value, entry.path);
  } else {
    await collectFile(entry);
  }
}

async function collectFile(entry: RemoteFileEntry) {
  const uuid = activeTargetUUID.value;
  if (!uuid) return;
  try {
    const result: CollectRemoteFileResult = await collectRemoteFile({
      target: uuid,
      remotePath: entry.path,
      tags: ['stockman'],
    });
    action.message = `Collected ${result.item.name || entry.name}`;
  } catch (err: any) {
    action.error = err?.message ?? String(err);
  }
}

async function startForwardForActive() {
  const uuid = activeTargetUUID.value;
  if (!uuid) return;
  targetMode.value = 'proxy';
  if (!proxyResults[uuid]) proxyResults[uuid] = [];
  try {
    const result = await startForwardProxy({
      target: uuid,
      localBind: proxyLocalBind.value,
      remoteAddr: proxyRemoteAddr.value,
    });
    proxyResults[uuid].push(result);
  } catch (err: any) {
    action.error = err?.message ?? String(err);
  }
}

async function stopForwardForActive(proxyId: string) {
  const uuid = activeTargetUUID.value;
  if (!uuid) return;
  try {
    await stopForwardProxy({ target: uuid, proxyId });
  } catch (err: any) {
    action.error = err?.message ?? String(err);
    return;
  }
  proxyResults[uuid] = (proxyResults[uuid] || []).filter((p) => p.proxyId !== proxyId);
}

async function startSocksForActive() {
  const uuid = activeTargetUUID.value;
  if (!uuid) return;
  targetMode.value = 'proxy';
  try {
    const handle = await startSocksProxy({
      target: uuid,
      auth: socksUsername.value ? 'userpass' : 'none',
      username: socksUsername.value,
      password: socksPassword.value,
    });
    action.message = `SOCKS stream opened: ${handle.handleId}`;
  } catch (err: any) {
    action.error = err?.message ?? String(err);
  }
}

async function closeStream(streamId: number) {
  if (!streamId) return;
  try {
    await closeStreamByID({ streamId, reason: 'operator closed from Stockman' });
    await refreshAll();
  } catch (err: any) {
    action.error = err?.message ?? String(err);
  }
}

async function pingActiveStream() {
  const uuid = activeTargetUUID.value || topo.selectedUUID;
  if (!uuid) return;
  try {
    await streamPing({ target: uuid, count: 3, payloadSize: 32 });
    action.message = `Stream ping queued for ${uuid.slice(0, 8)}.`;
  } catch (err: any) {
    action.error = err?.message ?? String(err);
  }
}

function submitCommand() {
  const raw = commandLine.value.trim();
  if (!raw) return;
  if (raw === 'clear') {
    events.clear();
  } else if (raw === 'refresh') {
    refreshAll();
  } else if (raw === 'prune') {
    submitPrune();
  } else {
    action.message = `Command staged: ${raw}`;
  }
  commandLine.value = '';
}

const DTN_RESERVED_PREFIXES = ['memo:', 'log:', 'stream:', 'proto:'];

function normalizeDTNPayload(raw: string): string {
  const trimmed = raw.trim();
  const lower = trimmed.toLowerCase();
  if (DTN_RESERVED_PREFIXES.some((prefix) => lower.startsWith(prefix))) return trimmed;
  return `memo:${trimmed}`;
}
</script>

<template>
  <section class="ops-shell" @click="closeOverlays" @keydown.esc="closeOverlays">
    <header class="menubar">
      <nav class="menus" aria-label="Application menu">
        <div class="menu-item" @click.stop>
          <button :class="{ active: activeMenu === 'stockman' }" @click="toggleMenu('stockman')">File</button>
          <div v-if="activeMenu === 'stockman'" class="app-menu">
            <button @click="chooseMenuAction('refresh')"><span>Refresh Workspace</span><kbd>⌘R</kbd></button>
            <button @click="chooseMenuAction('disconnect')"><span>Disconnect</span><kbd>⌘Q</kbd></button>
          </div>
        </div>
        <div class="menu-item" @click.stop>
          <button :class="{ active: activeMenu === 'view' }" @click="toggleMenu('view')">View</button>
          <div v-if="activeMenu === 'view'" class="app-menu">
            <button @click="chooseMenuAction('table')"><span>Target Table</span><kbd>⌘1</kbd></button>
            <button @click="chooseMenuAction('graph')"><span>Session Graph</span><kbd>⌘2</kbd></button>
            <button @click="chooseMenuAction('events')"><span>Event Log</span><kbd>⌘E</kbd></button>
          </div>
        </div>
        <div class="menu-item" @click.stop>
          <button :class="{ active: activeMenu === 'operations' }" @click="toggleMenu('operations')">Operations</button>
          <div v-if="activeMenu === 'operations'" class="app-menu">
            <button @click="chooseMenuAction('dtn')"><span>Queue DTN Payload</span><kbd>⇄</kbd></button>
            <button @click="chooseMenuAction('sleep')"><span>Sleep / Work Profile</span><kbd>◐</kbd></button>
            <button @click="chooseMenuAction('prune')"><span>Prune Offline Nodes</span><kbd>⌫</kbd></button>
          </div>
        </div>
        <div class="menu-item" @click.stop>
          <button :class="{ active: activeMenu === 'listeners' }" @click="toggleMenu('listeners')">Listeners</button>
          <div v-if="activeMenu === 'listeners'" class="app-menu">
            <button @click="sideMode = 'listeners'; activeMenu = ''"><span>Show Listener Groups</span><kbd>⌘L</kbd></button>
            <button @click="bottomTab = 'events'; activeMenu = ''"><span>Listener Events</span><kbd>⌘⇧L</kbd></button>
          </div>
        </div>
        <div class="menu-item" @click.stop>
          <button :class="{ active: activeMenu === 'sessions' }" @click="toggleMenu('sessions')">Sessions</button>
          <div v-if="activeMenu === 'sessions'" class="app-menu">
            <button @click="chooseMenuAction('sessions')"><span>Session List</span><kbd>⌘S</kbd></button>
            <button @click="chooseMenuAction('streams')"><span>Stream Diagnostics</span><kbd>⌘D</kbd></button>
          </div>
        </div>
        <div class="menu-item" @click.stop>
          <button :class="{ active: activeMenu === 'reports' }" @click="toggleMenu('reports')">Reports</button>
          <div v-if="activeMenu === 'reports'" class="app-menu">
            <button @click="bottomTab = 'events'; activeMenu = ''"><span>Audit/Event Evidence</span><kbd>⌘P</kbd></button>
            <button @click="chooseMenuAction('refresh')"><span>Refresh Metrics</span><kbd>F5</kbd></button>
          </div>
        </div>
        <div class="menu-item" @click.stop>
          <button :class="{ active: activeMenu === 'help' }" @click="toggleMenu('help')">Help</button>
          <div v-if="activeMenu === 'help'" class="app-menu">
            <button @click="bottomTab = 'control'; activeMenu = ''"><span>Operator Commands</span><kbd>?</kbd></button>
            <button @click="mainView = 'graph'; activeMenu = ''"><span>Graph View</span><kbd>G</kbd></button>
          </div>
        </div>
      </nav>
      <div class="connection-strip">
        <span class="dot ok"></span>
        <span class="sf-mono">{{ connectedDisplay }}</span>
      </div>
    </header>

    <div class="toolbar">
      <button data-tip="Refresh snapshot and metrics" @click="refreshAll">↻</button>
      <button data-tip="Show target table" @click="mainView = 'table'">▦</button>
      <button data-tip="Group nodes by network" @click="sideMode = 'networks'">◎</button>
      <button data-tip="Show listener groups" @click="sideMode = 'listeners'">◌</button>
      <span class="tool-sep"></span>
      <button data-tip="Queue DTN payload for selected target" @click="bottomTab = 'dtn'">⇄</button>
      <button data-tip="Edit sleep/work/jitter for selected target" @click="bottomTab = 'control'">◐</button>
      <button data-tip="Prune offline nodes" @click="submitPrune">⌫</button>
      <span class="tool-sep"></span>
      <button data-tip="Open event log" @click="bottomTab = 'events'">▤</button>
      <button data-tip="Open stream diagnostics" @click="bottomTab = 'streams'">≋</button>
      <button data-tip="Disconnect from Kelpie UI server" @click="logout">⏻</button>
      <div class="toolbar-stats">
        <span>Nodes: <strong>{{ stats.nodes }}</strong></span>
        <span>Online: <strong>{{ stats.online }}</strong></span>
        <span>Streams: <strong>{{ stats.streams }}</strong></span>
        <span>Held: <strong>{{ stats.dtnHeld }}</strong></span>
      </div>
    </div>

    <main class="ops-grid">
      <aside class="left-pane">
        <div class="pane-tabs">
          <button :class="{ active: sideMode === 'targets' }" @click="sideMode = 'targets'">Targets</button>
          <button :class="{ active: sideMode === 'networks' }" @click="sideMode = 'networks'">Networks</button>
          <button :class="{ active: sideMode === 'listeners' }" @click="sideMode = 'listeners'">Listeners</button>
        </div>

        <section v-if="sideMode === 'targets'" class="tree-list">
          <button
            v-for="node in orderedNodes"
            :key="node.uuid"
            :class="['tree-row', { active: topo.selectedUUID === node.uuid }]"
            :style="{ paddingLeft: `${10 + Math.max(0, node.depth) * 14}px` }"
            :title="`Right-click ${labelForNode(node)} for target actions`"
            @click="selectNode(node.uuid)"
            @contextmenu="openTargetMenu($event, node)"
          >
            <span :class="['dot', statusTone(node.status)]"></span>
            <span class="tree-main">{{ labelForNode(node) }}</span>
            <span class="tree-meta">{{ node.activeStreams }}</span>
          </button>
          <p v-if="!orderedNodes.length" class="empty">No nodes reported.</p>
        </section>

        <section v-else-if="sideMode === 'networks'" class="side-groups">
          <button
            v-for="[network, nodes] in networkGroups"
            :key="network"
            class="group-row"
          >
            <span>{{ network }}</span>
            <strong>{{ nodes.length }}</strong>
          </button>
          <p v-if="!networkGroups.length" class="empty">No networks.</p>
        </section>

        <section v-else class="side-groups">
          <button
            v-for="listener in listeners"
            :key="listener.key"
            class="group-row"
          >
            <span>{{ listener.key }}</span>
            <strong>{{ listener.count }}</strong>
          </button>
          <p v-if="!listeners.length" class="empty">No listeners.</p>
        </section>
      </aside>

      <section class="center-pane">
        <header class="table-head">
          <div>
            <p class="eyebrow">Operational Beacons</p>
            <h1>{{ mainView === 'table' ? `Targets: ${stats.nodes}` : 'Session Graph' }}</h1>
          </div>
          <div class="view-switch">
            <button :class="{ active: mainView === 'table' }" @click="mainView = 'table'">Table</button>
            <button :class="{ active: mainView === 'graph' }" @click="mainView = 'graph'">Graph</button>
          </div>
          <div class="mini-stats">
            <span>Delivered {{ stats.dtnDelivered }}</span>
            <span>Supplemental {{ stats.suppActive }}</span>
            <span>Failures {{ stats.failures }}</span>
          </div>
        </header>

        <div v-if="mainView === 'table'" class="beacon-table-wrap">
          <table class="beacon-table">
            <thead>
              <tr>
                <th>Status</th>
                <th>Node</th>
                <th>Network</th>
                <th>User</th>
                <th>Parent</th>
                <th>Sleep</th>
                <th>Streams</th>
                <th>Memo</th>
                <th>Last</th>
              </tr>
            </thead>
            <tbody>
              <tr
                v-for="node in orderedNodes"
                :key="node.uuid"
                :class="{ selected: topo.selectedUUID === node.uuid }"
                :title="`Right-click ${labelForNode(node)} for target actions`"
                @click="selectNode(node.uuid)"
                @contextmenu="openTargetMenu($event, node)"
              >
                <td>
                  <span :class="['pill', statusTone(node.status)]">
                    {{ node.status || 'unknown' }}
                  </span>
                </td>
                <td>
                  <strong>{{ labelForNode(node) }}</strong>
                  <small class="sf-mono">{{ node.uuid }}</small>
                </td>
                <td>{{ node.network || 'default' }}</td>
                <td>{{ sessionByNode.get(node.uuid)?.remoteAddr || '-' }}</td>
                <td class="sf-mono">{{ node.parentUuid?.slice(0, 8) || 'root' }}</td>
                <td>{{ node.sleep || '-' }}</td>
                <td>{{ node.activeStreams }}</td>
                <td>{{ node.memo || '-' }}</td>
                <td>{{ sessionByNode.get(node.uuid)?.lastSeen || '-' }}</td>
              </tr>
            </tbody>
          </table>
          <div v-if="!orderedNodes.length" class="empty table-empty">
            Waiting for Kelpie snapshot. Use Refresh after agents join.
          </div>
        </div>

        <div v-else class="session-graph">
          <div
            v-for="node in orderedNodes"
            :key="node.uuid"
            :class="['graph-node', { selected: topo.selectedUUID === node.uuid }]"
            :style="{ marginLeft: `${Math.max(0, node.depth) * 54}px` }"
            @click="selectNode(node.uuid)"
            @dblclick="openTargetTab(node.uuid)"
            @contextmenu="openTargetMenu($event, node)"
          >
            <span :class="['dot', statusTone(node.status)]"></span>
            <strong>{{ labelForNode(node) }}</strong>
            <small>{{ node.network || 'default' }}</small>
            <em>{{ node.parentUuid ? `via ${node.parentUuid.slice(0, 8)}` : 'root session' }}</em>
          </div>
          <p v-if="!orderedNodes.length" class="empty table-empty">
            Waiting for graph data.
          </p>
        </div>
      </section>
    </main>

    <section class="bottom-pane">
      <nav class="bottom-tabs">
        <button data-tip="Chronological operator and node event log" :class="{ active: bottomTab === 'events' }" @click="bottomTab = 'events'">Event Log</button>
        <button data-tip="Session status, remote address, and errors" :class="{ active: bottomTab === 'sessions' }" @click="bottomTab = 'sessions'">Sessions</button>
        <button data-tip="Queue store-carry-forward payloads" :class="{ active: bottomTab === 'dtn' }" @click="bottomTab = 'dtn'">DTN Queue</button>
        <button data-tip="Active stream diagnostics and flow control" :class="{ active: bottomTab === 'streams' }" @click="bottomTab = 'streams'">Streams</button>
        <button data-tip="Operator command scratchpad" :class="{ active: bottomTab === 'control' }" @click="bottomTab = 'control'">Console</button>
        <button
          v-for="uuid in targetTabs"
          :key="uuid"
          class="target-tab"
          :class="{ active: bottomTab === `target:${uuid}` }"
          @click="bottomTab = `target:${uuid}`"
        >
          <span>Target: {{ labelForNode(topo.nodeMap.get(uuid) || { uuid, depth: 0, activeStreams: 0 }) }}</span>
          <i @click.stop="closeTargetTab(uuid)">×</i>
        </button>
        <button data-tip="Pause or resume event rendering" class="tab-tool" @click="events.togglePause">{{ events.paused ? 'Resume' : 'Pause' }}</button>
        <button data-tip="Clear local event view" class="tab-tool" @click="events.clear">Clear</button>
      </nav>

      <div class="bottom-content">
        <section v-if="bottomTab === 'events'" class="event-console">
          <p
            v-for="ev in recentEvents"
            :key="ev.seq"
            :class="['event-line', eventTone(ev)]"
          >
            <span class="time">{{ ev.timestamp }}</span>
            <span class="kind">{{ ev.kind }}</span>
            <span class="action">{{ ev.action }}</span>
            <span class="summary">{{ ev.summary }}</span>
          </p>
          <p v-if="!recentEvents.length" class="empty">No events yet.</p>
        </section>

        <section v-else-if="bottomTab === 'sessions'" class="data-list">
          <div v-for="session in topo.sessions" :key="`${session.targetUuid}-${session.status}`" class="data-row">
            <span :class="['dot', session.active ? 'ok' : 'idle']"></span>
            <strong>{{ session.targetUuid.slice(0, 8) }}</strong>
            <span>{{ session.status }}</span>
            <span>{{ session.remoteAddr || '-' }}</span>
            <span>{{ session.lastError || session.lastSeen || '-' }}</span>
          </div>
          <p v-if="!topo.sessions.length" class="empty">No sessions.</p>
        </section>

        <section v-else-if="bottomTab === 'dtn'" class="command-panel">
          <label>
            <span>Payload</span>
            <input v-model="dtnPayload" class="sf-mono" placeholder="memo:operator note" />
          </label>
          <label>
            <span>Priority</span>
            <select v-model="dtnPriority">
              <option value="low">low</option>
              <option value="normal">normal</option>
              <option value="high">high</option>
            </select>
          </label>
          <button :disabled="!selectedNode || action.busy" @click="submitDTN">Enqueue</button>
        </section>

        <section v-else-if="bottomTab === 'streams'" class="data-list">
          <div v-for="stream in topo.streams" :key="stream.streamId" class="data-row">
            <strong>#{{ stream.streamId }}</strong>
            <span>{{ stream.targetUuid.slice(0, 8) }}</span>
            <span>{{ stream.kind }}</span>
            <span>pending {{ stream.pending }}</span>
            <span>window {{ stream.window }}</span>
            <span>{{ stream.lastActivity }}</span>
          </div>
          <p v-if="!topo.streams.length" class="empty">No active streams.</p>
        </section>

        <section v-else-if="bottomTab.startsWith('target:')" class="target-workspace">
          <header>
            <div>
              <p class="eyebrow">Target Workspace</p>
              <h2>{{ activeTargetNode ? labelForNode(activeTargetNode) : activeTargetUUID }}</h2>
            </div>
            <div class="target-actions">
              <button @click="ensureShell()">Shell</button>
              <button @click="loadFiles()">Files</button>
              <button @click="targetMode = 'proxy'">Proxy</button>
              <button @click="bottomTab = 'dtn'">Queue DTN</button>
              <button @click="submitSleep">Apply Sleep</button>
              <button @click="topo.loadDetail(activeTargetUUID)">Refresh Detail</button>
            </div>
          </header>
          <nav class="target-mode-tabs">
            <button :class="{ active: targetMode === 'overview' }" @click="targetMode = 'overview'">Overview</button>
            <button :class="{ active: targetMode === 'shell' }" @click="ensureShell()">Shell</button>
            <button :class="{ active: targetMode === 'files' }" @click="loadFiles()">Files</button>
            <button :class="{ active: targetMode === 'proxy' }" @click="targetMode = 'proxy'">Proxy</button>
          </nav>
          <div v-if="activeTargetNode && targetMode === 'overview'" class="target-grid">
            <dl class="facts">
              <div>
                <dt>UUID</dt>
                <dd class="sf-mono">{{ activeTargetNode.uuid }}</dd>
              </div>
              <div>
                <dt>Status</dt>
                <dd><span :class="['pill', statusTone(activeTargetNode.status)]">{{ activeTargetNode.status || 'unknown' }}</span></dd>
              </div>
              <div>
                <dt>Network</dt>
                <dd>{{ activeTargetNode.network || 'default' }}</dd>
              </div>
              <div>
                <dt>Parent</dt>
                <dd class="sf-mono">{{ activeTargetNode.parentUuid || 'root' }}</dd>
              </div>
              <div>
                <dt>Sleep</dt>
                <dd>{{ activeTargetNode.sleep || '-' }}</dd>
              </div>
              <div>
                <dt>Memo</dt>
                <dd>{{ activeTargetNode.memo || '-' }}</dd>
              </div>
            </dl>
            <section class="target-panel">
              <h3>Sessions</h3>
              <p v-for="session in activeTargetSessions" :key="`${session.targetUuid}-${session.status}`">
                <span :class="['dot', session.active ? 'ok' : 'idle']"></span>
                <strong>{{ session.status }}</strong>
                <span>{{ session.remoteAddr || '-' }}</span>
                <small>{{ session.lastError || session.lastSeen || '-' }}</small>
              </p>
              <p v-if="!activeTargetSessions.length" class="empty">No sessions for this target.</p>
            </section>
            <section class="target-panel">
              <h3>Streams</h3>
              <p v-for="stream in activeTargetStreams" :key="stream.streamId">
                <strong>#{{ stream.streamId }}</strong>
                <span>{{ stream.kind }}</span>
                <span>pending {{ stream.pending }}</span>
                <small>{{ stream.lastActivity }}</small>
                <button @click="closeStream(stream.streamId)">Close</button>
              </p>
              <p v-if="!activeTargetStreams.length" class="empty">No streams for this target.</p>
              <button @click="pingActiveStream">Ping Stream</button>
            </section>
            <section class="target-panel sleep-editor">
              <h3>Sleep Profile</h3>
              <label><span>Sleep</span><input v-model.number="sleepSeconds" type="number" min="0" /></label>
              <label><span>Work</span><input v-model.number="workSeconds" type="number" min="0" /></label>
              <label><span>Jitter</span><input v-model.number="jitter" type="number" min="0" max="100" /></label>
            </section>
          </div>
          <div v-else-if="targetMode === 'shell'" class="shell-workspace">
            <div class="shell-head">
              <span>{{ activeShellHandle ? activeShellHandle.status : 'not started' }}</span>
              <span class="sf-mono">{{ activeShellHandle?.sessionId || activeTargetUUID }}</span>
              <button @click="ensureShell()">Start</button>
              <button @click="closeActiveShell">Close</button>
            </div>
            <div class="shell-output">
              <p v-for="(line, idx) in activeShellLines" :key="idx">{{ line }}</p>
              <p v-if="!activeShellLines.length" class="empty">No shell output yet.</p>
            </div>
            <form class="shell-input" @submit.prevent="submitShellCommand">
              <span>$</span>
              <input v-model="shellInput" class="sf-mono" placeholder="whoami" />
              <button>Send</button>
            </form>
          </div>
          <div v-else-if="targetMode === 'files'" class="files-workspace">
            <form class="file-path" @submit.prevent="loadFiles(activeTargetUUID, filePath)">
              <input v-model="filePath" class="sf-mono" placeholder="/" />
              <button :disabled="activeFileLoading">{{ activeFileLoading ? 'Listing' : 'List' }}</button>
              <button
                type="button"
                :disabled="activeFileLoading || !activeFileListing?.canGoUp"
                @click="loadFiles(activeTargetUUID, activeFileListing?.parentPath || '/')"
              >
                Up
              </button>
            </form>
            <div class="file-table">
              <p v-if="activeFileError" class="empty error-text">{{ activeFileError }}</p>
              <button
                v-for="entry in activeFileListing?.entries || []"
                :key="entry.path"
                class="file-row"
                @click="openFileEntry(entry)"
              >
                <span>{{ entry.isDir || entry.isDrive ? 'DIR' : 'FILE' }}</span>
                <strong>{{ entry.name }}</strong>
                <em>{{ entry.mode || '-' }}</em>
                <small>{{ entry.size || '' }}</small>
                <small>{{ entry.modifiedAt || '' }}</small>
              </button>
              <p v-if="!activeFileError && !(activeFileListing?.entries || []).length" class="empty">
                {{ activeFileLoading ? 'Loading remote directory...' : 'No file listing loaded.' }}
              </p>
            </div>
          </div>
          <div v-else-if="targetMode === 'proxy'" class="proxy-workspace">
            <section class="proxy-panel">
              <h3>Forward Proxy</h3>
              <label><span>Local bind</span><input v-model="proxyLocalBind" class="sf-mono" /></label>
              <label><span>Remote address</span><input v-model="proxyRemoteAddr" class="sf-mono" /></label>
              <button @click="startForwardForActive">Start Forward</button>
            </section>
            <section class="proxy-panel">
              <h3>SOCKS</h3>
              <label><span>Username</span><input v-model="socksUsername" /></label>
              <label><span>Password</span><input v-model="socksPassword" type="password" /></label>
              <button @click="startSocksForActive">Start SOCKS</button>
            </section>
            <section class="proxy-panel active-proxies">
              <h3>Active Proxies</h3>
              <p v-for="proxy in activeProxyResults" :key="proxy.proxyId">
                <strong>{{ proxy.proxyId }}</strong>
                <span>{{ proxy.bind }}</span>
                <span>{{ proxy.remoteAddr }}</span>
                <button @click="stopForwardForActive(proxy.proxyId)">Stop</button>
              </p>
              <p v-if="!activeProxyResults.length" class="empty">No proxies for this target.</p>
            </section>
          </div>
        </section>

        <section v-else class="terminal">
          <div class="terminal-lines">
            <p>Connected to {{ connectedDisplay }}</p>
            <p>Type: refresh, clear, prune. Target actions use the selected row.</p>
          </div>
          <form @submit.prevent="submitCommand">
            <span>stockman&gt;</span>
            <input v-model="commandLine" class="sf-mono" autofocus />
          </form>
        </section>
      </div>
    </section>

    <footer class="statusbar">
      <span>[{{ new Date().toLocaleTimeString() }}]</span>
      <span>operator: stockman</span>
      <span>teamserver: {{ connectedDisplay }}</span>
      <span>nodes: {{ stats.nodes }}</span>
      <span>sessions: {{ stats.sessions }}</span>
      <span>lag: {{ metrics.loading ? 'polling' : 'ok' }}</span>
    </footer>

    <div
      v-if="contextMenu.open && contextMenu.node"
      class="target-menu"
      :style="{ left: `${contextMenu.x}px`, top: `${contextMenu.y}px` }"
      @click.stop
      @contextmenu.prevent
    >
      <header>
        <strong>{{ labelForNode(contextMenu.node) }}</strong>
        <span class="sf-mono">{{ contextMenu.node.uuid }}</span>
      </header>
      <button @click="chooseTargetAction('detail')">
        <span>Inspect Target</span>
        <kbd>Enter</kbd>
      </button>
      <button @click="chooseTargetAction('shell')">
        <span>Interact / Shell</span>
        <kbd>⌘I</kbd>
      </button>
      <button @click="chooseTargetAction('files')">
        <span>Browse Files</span>
        <kbd>⌘F</kbd>
      </button>
      <button @click="chooseTargetAction('proxy')">
        <span>Port Forward / SOCKS</span>
        <kbd>⌘P</kbd>
      </button>
      <button @click="chooseTargetAction('dtn')">
        <span>Queue DTN Payload</span>
        <kbd>⇄</kbd>
      </button>
      <button @click="chooseTargetAction('sleep')">
        <span>Sleep / Work Profile</span>
        <kbd>◐</kbd>
      </button>
      <button @click="chooseTargetAction('streams')">
        <span>Stream Diagnostics</span>
        <kbd>≋</kbd>
      </button>
      <button @click="chooseTargetAction('sessions')">
        <span>Session Details</span>
        <kbd>⌘S</kbd>
      </button>
      <hr />
      <button @click="chooseTargetAction('refresh')">
        <span>Refresh Node</span>
        <kbd>↻</kbd>
      </button>
      <button @click="chooseTargetAction('copy')">
        <span>Copy UUID</span>
        <kbd>⌘C</kbd>
      </button>
      <button @click="chooseTargetAction('prune')">
        <span>Prune Offline Nodes</span>
        <kbd>⌫</kbd>
      </button>
    </div>
  </section>
</template>

<style scoped>
.ops-shell {
  --ops-bg: #111417;
  --ops-panel: #181d22;
  --ops-panel-2: #20262d;
  --ops-panel-3: #252c34;
  --ops-line: #343d47;
  --ops-line-soft: #29313a;
  --ops-text: #e8edf2;
  --ops-muted: #9aa7b4;
  --ops-faint: #6e7a86;
  --ops-accent: #4da3ff;
  --ops-accent-2: #7dd3c7;
  --ops-ok: #5bd38f;
  --ops-warn: #f0bd5b;
  --ops-bad: #ff6b6b;
  --ops-idle: #7c8792;

  display: grid;
  grid-template-rows: 36px 42px minmax(0, 1fr) 246px 28px;
  height: 100vh;
  width: 100vw;
  overflow: hidden;
  background: var(--ops-bg);
  color: var(--ops-text);
  font-family: var(--sf-font-sans);
}

button,
input,
select {
  font: inherit;
}

[data-tip] {
  position: relative;
}

[data-tip]::after {
  content: attr(data-tip);
  position: absolute;
  left: 0;
  top: calc(100% + 7px);
  z-index: 40;
  min-width: 170px;
  max-width: 260px;
  padding: 7px 9px;
  border: 1px solid var(--ops-line);
  background: #0a0d10;
  color: var(--ops-text);
  font-size: 0.72rem;
  line-height: 1.35;
  box-shadow: 0 10px 22px rgba(0, 0, 0, 0.34);
  opacity: 0;
  pointer-events: none;
  transform: translateY(-3px);
  transition: opacity 120ms var(--sf-ease), transform 120ms var(--sf-ease);
  white-space: normal;
}

[data-tip]:hover::after {
  opacity: 1;
  transform: translateY(0);
}

.bottom-tabs [data-tip]::after {
  top: auto;
  bottom: calc(100% + 7px);
}

.menubar {
  display: flex;
  align-items: center;
  gap: 22px;
  padding: 0 14px;
  border-bottom: 1px solid var(--ops-line);
  background: #15191e;
}

.menus {
  display: flex;
  align-items: center;
  gap: 2px;
}

.menu-item {
  position: relative;
}

.menus button,
.toolbar button,
.pane-tabs button,
.bottom-tabs button {
  border: 0;
  color: var(--ops-text);
  background: transparent;
  cursor: pointer;
}

.menus button {
  height: 28px;
  padding: 0 10px;
}

.menus button:hover {
  background: var(--ops-panel-2);
}

.menus button.active {
  background: var(--ops-panel-2);
  color: var(--ops-text);
}

.app-menu {
  position: absolute;
  top: 31px;
  left: 0;
  z-index: 80;
  width: 232px;
  border: 1px solid var(--ops-line);
  background: #101419;
  box-shadow: 0 18px 38px rgba(0, 0, 0, 0.42);
  padding: 4px;
}

.menus .app-menu button {
  width: 100%;
  height: 30px;
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  align-items: center;
  gap: 10px;
  padding: 0 8px;
  color: var(--ops-muted);
  text-align: left;
}

.menus .app-menu button:hover {
  color: var(--ops-text);
  background: rgba(77, 163, 255, 0.14);
}

.app-menu kbd {
  color: var(--ops-faint);
  font-family: var(--sf-font-mono);
  font-size: 0.66rem;
  font-weight: 400;
}

.connection-strip {
  margin-left: auto;
  display: inline-flex;
  align-items: center;
  gap: 8px;
  min-width: 0;
  color: var(--ops-muted);
}

.toolbar {
  display: flex;
  align-items: center;
  gap: 6px;
  padding: 5px 10px;
  border-bottom: 1px solid var(--ops-line);
  background: var(--ops-panel);
}

.toolbar button {
  display: grid;
  place-items: center;
  width: 30px;
  height: 30px;
  border: 1px solid var(--ops-line-soft);
  background: var(--ops-panel-2);
  color: var(--ops-muted);
}

.toolbar button:hover {
  color: var(--ops-text);
  border-color: var(--ops-accent);
}

.tool-sep {
  width: 1px;
  align-self: stretch;
  background: var(--ops-line);
  margin: 0 4px;
}

.toolbar-stats {
  margin-left: auto;
  display: flex;
  gap: 14px;
  color: var(--ops-muted);
  font-size: 0.78rem;
}

.toolbar-stats strong {
  color: var(--ops-text);
}

.ops-grid {
  display: grid;
  grid-template-columns: 250px minmax(0, 1fr);
  min-height: 0;
}

.left-pane,
.center-pane {
  min-height: 0;
  overflow: hidden;
  background: var(--ops-panel);
}

.left-pane {
  border-right: 1px solid var(--ops-line);
}

.pane-tabs {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  border-bottom: 1px solid var(--ops-line);
}

.pane-tabs button {
  height: 34px;
  color: var(--ops-muted);
  border-right: 1px solid var(--ops-line-soft);
}

.pane-tabs button.active {
  color: var(--ops-text);
  background: var(--ops-panel-2);
  box-shadow: inset 0 -2px 0 var(--ops-accent);
}

.tree-list,
.side-groups {
  height: 100%;
  overflow: auto;
  padding: 8px;
}

.tree-row,
.group-row {
  width: 100%;
  min-height: 30px;
  display: grid;
  grid-template-columns: auto minmax(0, 1fr) auto;
  align-items: center;
  gap: 8px;
  border: 1px solid transparent;
  color: var(--ops-muted);
  background: transparent;
  text-align: left;
  cursor: pointer;
}

.tree-row:hover,
.tree-row.active,
.group-row:hover {
  background: var(--ops-panel-2);
  border-color: var(--ops-line);
  color: var(--ops-text);
}

.tree-main {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.tree-meta {
  color: var(--ops-faint);
  font-size: 0.72rem;
}

.group-row {
  padding: 0 10px;
}

.center-pane {
  display: grid;
  grid-template-rows: auto minmax(0, 1fr);
  background: var(--ops-bg);
}

.table-head {
  display: flex;
  align-items: end;
  justify-content: space-between;
  gap: 16px;
  padding: 14px 16px 12px;
  border-bottom: 1px solid var(--ops-line);
  background: #14191e;
}

.eyebrow {
  margin: 0 0 4px;
  color: var(--ops-accent-2);
  text-transform: uppercase;
  letter-spacing: 0.08em;
  font-size: 0.68rem;
  font-weight: 700;
}

.table-head h1,
.target-workspace h2 {
  margin: 0;
  font-size: 1.05rem;
  letter-spacing: 0;
}

.view-switch {
  display: inline-flex;
  border: 1px solid var(--ops-line);
  background: var(--ops-panel);
}

.view-switch button {
  height: 28px;
  padding: 0 12px;
  border: 0;
  border-right: 1px solid var(--ops-line-soft);
  background: transparent;
  color: var(--ops-muted);
  cursor: pointer;
}

.view-switch button:last-child {
  border-right: 0;
}

.view-switch button:hover,
.view-switch button.active {
  color: var(--ops-text);
  background: var(--ops-panel-2);
}

.mini-stats {
  display: flex;
  gap: 10px;
  color: var(--ops-muted);
  font-size: 0.76rem;
}

.beacon-table-wrap {
  min-height: 0;
  overflow: auto;
}

.beacon-table {
  width: 100%;
  border-collapse: collapse;
  table-layout: fixed;
  font-size: 0.82rem;
}

.beacon-table th {
  position: sticky;
  top: 0;
  z-index: 1;
  height: 32px;
  padding: 0 9px;
  color: var(--ops-muted);
  background: var(--ops-panel-2);
  border-bottom: 1px solid var(--ops-line);
  border-right: 1px solid var(--ops-line-soft);
  text-align: left;
  font-weight: 600;
}

.beacon-table td {
  height: 38px;
  padding: 4px 9px;
  border-bottom: 1px solid var(--ops-line-soft);
  color: var(--ops-muted);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.beacon-table tr {
  cursor: pointer;
}

.beacon-table tr:hover td,
.beacon-table tr.selected td {
  background: rgba(77, 163, 255, 0.08);
  color: var(--ops-text);
}

.beacon-table strong {
  display: block;
  color: var(--ops-text);
  font-weight: 650;
}

.beacon-table small {
  display: block;
  margin-top: 2px;
  color: var(--ops-faint);
  overflow: hidden;
  text-overflow: ellipsis;
}

.session-graph {
  min-height: 0;
  overflow: auto;
  padding: 18px 18px 36px;
  background:
    linear-gradient(var(--ops-line-soft) 1px, transparent 1px),
    linear-gradient(90deg, var(--ops-line-soft) 1px, transparent 1px);
  background-size: 28px 28px;
  background-color: #101419;
}

.graph-node {
  position: relative;
  width: min(440px, calc(100% - 18px));
  min-height: 46px;
  display: grid;
  grid-template-columns: auto 120px 110px minmax(0, 1fr);
  align-items: center;
  gap: 10px;
  margin-bottom: 12px;
  padding: 0 12px;
  border: 1px solid var(--ops-line);
  background: rgba(24, 29, 34, 0.96);
  color: var(--ops-muted);
  cursor: pointer;
}

.graph-node::before {
  content: '';
  position: absolute;
  left: -54px;
  top: 22px;
  width: 54px;
  height: 1px;
  background: var(--ops-line);
}

.graph-node:first-child::before {
  display: none;
}

.graph-node:hover,
.graph-node.selected {
  border-color: var(--ops-accent);
  color: var(--ops-text);
  background: rgba(32, 38, 45, 0.98);
}

.graph-node strong {
  color: var(--ops-text);
}

.graph-node small,
.graph-node em {
  overflow: hidden;
  color: var(--ops-faint);
  text-overflow: ellipsis;
  white-space: nowrap;
  font-style: normal;
  font-size: 0.74rem;
}

.facts {
  display: grid;
  gap: 10px;
  margin: 14px 0;
}

.facts div {
  display: grid;
  gap: 4px;
}

.facts dt {
  color: var(--ops-faint);
  text-transform: uppercase;
  letter-spacing: 0.08em;
  font-size: 0.66rem;
}

.facts dd {
  margin: 0;
  color: var(--ops-text);
  overflow-wrap: anywhere;
}

.quick-actions {
  display: grid;
  grid-template-columns: 1fr;
  gap: 8px;
  margin-top: 14px;
}

.quick-actions button,
.action-form input,
.command-panel button,
.command-panel input,
.command-panel select,
.terminal input {
  border: 1px solid var(--ops-line);
  background: var(--ops-panel-2);
  color: var(--ops-text);
}

.quick-actions button,
.command-panel button {
  height: 34px;
  cursor: pointer;
}

.quick-actions button:hover,
.command-panel button:hover {
  border-color: var(--ops-accent);
}

.quick-actions button:disabled,
.command-panel button:disabled {
  cursor: not-allowed;
  opacity: 0.55;
}

.action-form {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 8px;
  margin-top: 14px;
}

.action-form label,
.command-panel label {
  display: grid;
  gap: 5px;
  color: var(--ops-faint);
  font-size: 0.72rem;
}

.action-form input,
.command-panel input,
.command-panel select {
  width: 100%;
  height: 30px;
  padding: 0 8px;
}

.bottom-pane {
  min-height: 0;
  border-top: 1px solid var(--ops-line);
  background: #0b0e11;
  display: grid;
  grid-template-rows: 36px minmax(0, 1fr);
}

.bottom-tabs {
  display: flex;
  align-items: end;
  gap: 2px;
  padding: 0 10px;
  border-bottom: 1px solid var(--ops-line);
  background: var(--ops-panel);
}

.bottom-tabs button {
  height: 32px;
  padding: 0 12px;
  color: var(--ops-muted);
}

.bottom-tabs button.active {
  color: var(--ops-text);
  background: #0b0e11;
  box-shadow: inset 0 -2px 0 var(--ops-accent);
}

.bottom-tabs .target-tab {
  max-width: 190px;
  display: inline-grid;
  grid-template-columns: minmax(0, 1fr) auto;
  align-items: center;
  gap: 8px;
  border-left: 1px solid var(--ops-line-soft);
  border-right: 1px solid var(--ops-line-soft);
}

.target-tab span {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.target-tab i {
  color: var(--ops-faint);
  font-style: normal;
}

.target-tab i:hover {
  color: var(--ops-bad);
}

.bottom-tabs .tab-tool {
  margin-left: 4px;
  height: 26px;
  align-self: center;
  border: 1px solid var(--ops-line);
}

.bottom-content {
  min-height: 0;
  overflow: auto;
}

.event-console,
.terminal {
  height: 100%;
  padding: 8px 10px;
  font-family: var(--sf-font-mono);
  font-size: 0.78rem;
}

.event-line {
  display: grid;
  grid-template-columns: 150px 90px 120px minmax(0, 1fr);
  gap: 10px;
  margin: 0;
  min-height: 24px;
  align-items: center;
  color: var(--ops-muted);
}

.event-line.ok .summary,
.event-line.ok .action {
  color: var(--ops-ok);
}

.event-line.warn .summary,
.event-line.warn .action {
  color: var(--ops-warn);
}

.event-line.bad .summary,
.event-line.bad .action {
  color: var(--ops-bad);
}

.event-line .time,
.event-line .kind {
  color: var(--ops-faint);
}

.data-list {
  display: grid;
  align-content: start;
  padding: 8px 10px;
  gap: 4px;
}

.data-row {
  min-height: 30px;
  display: grid;
  grid-template-columns: auto 110px 120px 160px minmax(0, 1fr);
  align-items: center;
  gap: 10px;
  padding: 0 8px;
  border: 1px solid var(--ops-line-soft);
  color: var(--ops-muted);
  background: rgba(255, 255, 255, 0.015);
}

.data-row strong {
  color: var(--ops-text);
}

.command-panel {
  display: grid;
  grid-template-columns: minmax(240px, 1fr) 140px 120px;
  align-items: end;
  gap: 12px;
  padding: 16px;
}

.target-workspace {
  display: grid;
  grid-template-rows: auto auto minmax(0, 1fr);
  gap: 12px;
  padding: 12px;
}

.target-workspace > header {
  display: flex;
  align-items: end;
  justify-content: space-between;
  gap: 16px;
  border-bottom: 1px solid var(--ops-line);
  padding-bottom: 10px;
}

.target-actions {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
}

.target-mode-tabs {
  display: flex;
  gap: 2px;
  border-bottom: 1px solid var(--ops-line);
}

.target-mode-tabs button {
  height: 28px;
  padding: 0 12px;
  border: 0;
  border-left: 1px solid var(--ops-line-soft);
  background: transparent;
  color: var(--ops-muted);
  cursor: pointer;
}

.target-mode-tabs button.active,
.target-mode-tabs button:hover {
  background: var(--ops-panel-2);
  color: var(--ops-text);
}

.target-actions button,
.sleep-editor input,
.shell-head button,
.shell-input button,
.shell-input input,
.file-path input,
.file-path button,
.proxy-panel input,
.proxy-panel button,
.target-panel button {
  border: 1px solid var(--ops-line);
  background: var(--ops-panel-2);
  color: var(--ops-text);
}

.target-actions button,
.shell-head button,
.file-path button,
.proxy-panel button,
.target-panel button {
  height: 30px;
  padding: 0 10px;
  cursor: pointer;
}

.target-actions button:hover,
.shell-head button:hover,
.shell-input button:hover,
.file-path button:hover,
.proxy-panel button:hover,
.target-panel button:hover {
  border-color: var(--ops-accent);
}

.target-grid {
  min-height: 0;
  display: grid;
  grid-template-columns: minmax(240px, 1fr) repeat(3, minmax(180px, 1fr));
  gap: 10px;
  overflow: auto;
}

.target-panel,
.target-grid .facts {
  min-width: 0;
  margin: 0;
  border: 1px solid var(--ops-line);
  background: rgba(255, 255, 255, 0.015);
  padding: 10px;
}

.target-panel h3 {
  margin: 0 0 10px;
  color: var(--ops-text);
  font-size: 0.86rem;
}

.target-panel p {
  display: grid;
  grid-template-columns: auto minmax(0, 1fr);
  gap: 8px;
  align-items: center;
  margin: 0 0 8px;
  color: var(--ops-muted);
}

.target-panel p span,
.target-panel p small {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.sleep-editor {
  align-content: start;
}

.sleep-editor label {
  display: grid;
  gap: 5px;
  margin-bottom: 8px;
  color: var(--ops-faint);
  font-size: 0.72rem;
}

.sleep-editor input {
  height: 28px;
  padding: 0 8px;
}

.shell-workspace,
.files-workspace,
.proxy-workspace {
  min-height: 0;
  display: grid;
  gap: 10px;
}

.shell-workspace {
  grid-template-rows: 30px minmax(0, 1fr) 32px;
}

.shell-head {
  display: flex;
  align-items: center;
  gap: 10px;
  color: var(--ops-muted);
}

.shell-head .sf-mono {
  margin-right: auto;
}

.shell-output {
  min-height: 0;
  overflow: auto;
  border: 1px solid var(--ops-line);
  background: #070a0d;
  padding: 10px;
  font-family: var(--sf-font-mono);
  font-size: 0.78rem;
}

.shell-output p {
  margin: 0 0 4px;
  color: var(--ops-text);
  white-space: pre-wrap;
}

.shell-input,
.file-path {
  display: grid;
  align-items: center;
  gap: 8px;
}

.shell-input {
  grid-template-columns: auto minmax(0, 1fr) 88px;
}

.shell-input input,
.file-path input,
.proxy-panel input {
  height: 30px;
  padding: 0 8px;
}

.files-workspace {
  grid-template-rows: 32px minmax(0, 1fr);
}

.file-path {
  grid-template-columns: minmax(0, 1fr) 80px 70px;
}

.file-table {
  min-height: 0;
  overflow: auto;
  border: 1px solid var(--ops-line);
}

.file-row {
  width: 100%;
  min-height: 30px;
  display: grid;
  grid-template-columns: 58px minmax(0, 1fr) 110px 86px 160px;
  align-items: center;
  gap: 10px;
  padding: 0 10px;
  border: 0;
  border-bottom: 1px solid var(--ops-line-soft);
  background: transparent;
  color: var(--ops-muted);
  text-align: left;
  cursor: pointer;
}

.file-row:hover {
  background: var(--ops-panel-2);
  color: var(--ops-text);
}

.file-row strong,
.file-row em,
.file-row small {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.file-row em {
  font-style: normal;
}

.proxy-workspace {
  grid-template-columns: repeat(3, minmax(220px, 1fr));
  align-content: start;
}

.proxy-panel {
  border: 1px solid var(--ops-line);
  background: rgba(255, 255, 255, 0.015);
  padding: 10px;
}

.proxy-panel h3 {
  margin: 0 0 10px;
  color: var(--ops-text);
  font-size: 0.86rem;
}

.proxy-panel label {
  display: grid;
  gap: 5px;
  margin-bottom: 8px;
  color: var(--ops-faint);
  font-size: 0.72rem;
}

.active-proxies p {
  display: grid;
  grid-template-columns: minmax(0, 1fr) minmax(0, 1fr) minmax(0, 1fr) auto;
  gap: 8px;
  align-items: center;
  margin: 0 0 8px;
  color: var(--ops-muted);
}

.terminal {
  display: grid;
  grid-template-rows: 1fr auto;
}

.terminal-lines p {
  margin: 0 0 6px;
  color: var(--ops-muted);
}

.terminal form {
  display: grid;
  grid-template-columns: auto minmax(0, 1fr);
  align-items: center;
  gap: 8px;
}

.terminal input {
  height: 30px;
  padding: 0 8px;
}

.statusbar {
  display: flex;
  align-items: center;
  gap: 16px;
  padding: 0 10px;
  border-top: 1px solid var(--ops-line);
  background: #d9dee3;
  color: #12161a;
  font-family: var(--sf-font-mono);
  font-size: 0.74rem;
}

.target-menu {
  position: fixed;
  z-index: 100;
  width: 244px;
  border: 1px solid var(--ops-line);
  background: #101419;
  color: var(--ops-text);
  box-shadow: 0 18px 38px rgba(0, 0, 0, 0.46);
}

.target-menu header {
  display: grid;
  gap: 3px;
  padding: 10px 11px;
  border-bottom: 1px solid var(--ops-line);
  background: var(--ops-panel);
}

.target-menu header strong {
  font-size: 0.86rem;
}

.target-menu header span {
  color: var(--ops-faint);
  font-size: 0.68rem;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.target-menu button {
  width: 100%;
  height: 31px;
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  align-items: center;
  gap: 10px;
  padding: 0 10px;
  border: 0;
  background: transparent;
  color: var(--ops-muted);
  text-align: left;
  cursor: pointer;
}

.target-menu button:hover {
  background: rgba(77, 163, 255, 0.14);
  color: var(--ops-text);
}

.target-menu kbd {
  color: var(--ops-faint);
  font-family: var(--sf-font-mono);
  font-size: 0.66rem;
  font-weight: 400;
}

.target-menu hr {
  margin: 5px 0;
  border: 0;
  border-top: 1px solid var(--ops-line-soft);
}

.dot {
  width: 8px;
  height: 8px;
  background: var(--ops-idle);
  box-shadow: 0 0 0 2px rgba(124, 135, 146, 0.15);
}

.dot.ok,
.pill.ok {
  background: rgba(91, 211, 143, 0.12);
  color: var(--ops-ok);
}

.dot.ok {
  background: var(--ops-ok);
}

.dot.warn,
.pill.warn {
  background: rgba(240, 189, 91, 0.14);
  color: var(--ops-warn);
}

.dot.warn {
  background: var(--ops-warn);
}

.dot.bad,
.pill.bad {
  background: rgba(255, 107, 107, 0.14);
  color: var(--ops-bad);
}

.dot.bad {
  background: var(--ops-bad);
}

.dot.idle,
.pill.idle {
  background: rgba(124, 135, 146, 0.16);
  color: var(--ops-muted);
}

.pill {
  display: inline-flex;
  max-width: 100%;
  align-items: center;
  min-height: 22px;
  padding: 0 8px;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  font-size: 0.72rem;
}

.empty {
  margin: 12px;
  color: var(--ops-faint);
  font-size: 0.82rem;
}

.error-text {
  color: var(--ops-bad);
}

.table-empty {
  padding: 28px;
}

.notice {
  margin: 12px 0 0;
  padding: 9px 10px;
  font-size: 0.78rem;
}

.notice.ok {
  color: var(--ops-ok);
  background: rgba(91, 211, 143, 0.1);
}

.notice.bad {
  color: var(--ops-bad);
  background: rgba(255, 107, 107, 0.1);
}

.sf-mono {
  font-family: var(--sf-font-mono);
}

@media (max-width: 1180px) {
  .ops-grid {
    grid-template-columns: 210px minmax(0, 1fr);
  }

  .toolbar-stats {
    display: none;
  }

  .event-line {
    grid-template-columns: 120px 80px 100px minmax(0, 1fr);
  }

  .target-grid {
    grid-template-columns: repeat(2, minmax(220px, 1fr));
  }
}
</style>
