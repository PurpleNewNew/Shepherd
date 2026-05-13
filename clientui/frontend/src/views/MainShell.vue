<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, reactive, ref } from 'vue';
import { useConnectionStore } from '@/stores/connection';
import { useTopologyStore } from '@/stores/topology';
import { useEventsStore } from '@/stores/events';
import { useMetricsStore } from '@/stores/metrics';
import ForceGraph from '@/components/topology/ForceGraph.vue';
import {
  closeInteractiveStream,
  closeStreamByID,
  collectRemoteFile,
  createControllerListener,
  createPivotListener,
  deleteControllerListener,
  deletePivotListener,
  enqueueDTN,
  exportLoot,
  getSessionDiagnostics,
  listControllerListeners,
  listLoot,
  listPivotListeners,
  listRemoteFiles,
  markSession,
  onStreamEvent,
  pruneOffline,
  reconnectSession,
  repairSession,
  sendStreamData,
  startBackwardProxy,
  startForwardProxy,
  startShell,
  startSocksProxy,
  startSshSession,
  startSshTunnel,
  stopBackwardProxy,
  stopForwardProxy,
  streamPing,
  terminateSession,
  updateControllerListener,
  updatePivotListener,
  updateSleep,
  uploadRemoteFile,
} from '@/api/bindings';
import type {
  ControllerListenerDTO,
  CollectRemoteFileResult,
  EnqueueDTNResult,
  ExportLootResult,
  ListLootResult,
  LootItem,
  RemoteFileEntry,
  RemoteFileListing,
  NodeSummary,
  PivotListenerDTO,
  SessionSummary,
  SessionDiagnosticsDTO,
  StartBackwardProxyResult,
  StartForwardProxyResult,
  StreamEventDTO,
  StreamHandle,
  TimelineEvent,
  UploadRemoteFileResult,
} from '@/api/types';

type BuiltinTab = 'events' | 'sessions' | 'dtn' | 'control' | 'streams' | 'listeners';
type BottomTab = BuiltinTab | `target:${string}`;
type SideMode = 'networks' | 'listeners';
type MainView = 'table' | 'graph';
type TargetMode = 'overview' | 'shell' | 'files' | 'proxy' | 'ssh' | 'listeners';
type MenuKey = 'stockman' | 'view' | 'operations' | 'listeners' | 'sessions' | 'reports' | 'help';
type ResizePane = 'left' | 'bottom';
type ProxyResult = StartForwardProxyResult | StartBackwardProxyResult;

const conn = useConnectionStore();
const topo = useTopologyStore();
const events = useEventsStore();
const metrics = useMetricsStore();

const bottomTab = ref<BottomTab>('events');
const sideMode = ref<SideMode>('networks');
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
const uploadLocalPath = ref('');
const uploadRemotePath = ref('/tmp/stockman-upload.bin');
const lootExportPath = ref('');
const proxyLocalBind = ref('127.0.0.1:1080');
const proxyRemoteAddr = ref('127.0.0.1:80');
const backwardRemotePort = ref('9001');
const backwardLocalPort = ref('8080');
const socksUsername = ref('');
const socksPassword = ref('');
const sshServerAddr = ref('127.0.0.1:22');
const sshUsername = ref('');
const sshPassword = ref('');
const sshAgentPort = ref('2222');
const sshPrivateKey = ref('');
const sessionReason = ref('operator action');
const pivotProtocol = ref('tcp');
const pivotBind = ref('0.0.0.0:9001');
const pivotMode = ref<'normal' | 'iptables' | 'soreuse'>('normal');
const controllerProtocol = ref('tcp');
const controllerBind = ref('0.0.0.0:50061');
const shellHandles = reactive<Record<string, StreamHandle>>({});
const shellLines = reactive<Record<string, string[]>>({});
const sshHandles = reactive<Record<string, StreamHandle>>({});
const sshLines = reactive<Record<string, string[]>>({});
const sshInput = ref('');
const fileListings = reactive<Record<string, RemoteFileListing>>({});
const fileErrors = reactive<Record<string, string>>({});
const fileLoading = reactive<Record<string, boolean>>({});
const lootItems = reactive<Record<string, LootItem[]>>({});
const proxyResults = reactive<Record<string, ProxyResult[]>>({});
const sessionDiagnostics = reactive<Record<string, SessionDiagnosticsDTO>>({});
const pivotListeners = ref<PivotListenerDTO[]>([]);
const controllerListeners = ref<ControllerListenerDTO[]>([]);
const opsShellRef = ref<HTMLElement | null>(null);
const leftPaneWidth = ref(readLayoutSize('stockman:leftPaneWidth', 250, 190, 460));
const bottomPaneHeight = ref(readLayoutSize('stockman:bottomPaneHeight', 246, 160, 540));
const resizingPane = ref<ResizePane | ''>('');
const resizeStart = reactive({
  x: 0,
  y: 0,
  left: 0,
  bottom: 0,
});
let workspaceRefreshTimer: number | null = null;

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

const layoutStyle = computed(() => ({
  '--left-pane-width': `${leftPaneWidth.value}px`,
  '--bottom-pane-height': `${bottomPaneHeight.value}px`,
}));

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

const activeSshHandle = computed(() => {
  const uuid = activeTargetUUID.value;
  return uuid ? sshHandles[uuid] : undefined;
});

const activeSshLines = computed(() => {
  const uuid = activeTargetUUID.value;
  return uuid ? (sshLines[uuid] ?? []) : [];
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

const activeLootItems = computed(() => {
  const uuid = activeTargetUUID.value;
  return uuid ? (lootItems[uuid] ?? []) : [];
});

const activeSessionDiagnostics = computed(() => {
  const uuid = activeTargetUUID.value;
  return uuid ? sessionDiagnostics[uuid] : undefined;
});

const activePivotListeners = computed(() => {
  const uuid = activeTargetUUID.value;
  return uuid ? pivotListeners.value.filter((l) => l.targetUuid === uuid) : [];
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
  const groups = [
    ...controllerListeners.value.map((l) => ({
      key: `controller:${l.bind}`,
      count: 1,
      nodes: [l.status],
    })),
    ...pivotListeners.value.map((l) => ({
      key: `pivot:${l.bind}`,
      count: 1,
      nodes: [labelForUUID(l.targetUuid || '')],
    })),
  ];
  return groups.sort((a, b) => a.key.localeCompare(b.key));
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

const graphSummary = computed(() => {
  const primary = topo.edges.filter((edge) => !edge.supplemental).length;
  const supplemental = topo.edges.filter((edge) => edge.supplemental).length;
  return { primary, supplemental };
});

onMounted(async () => {
  await refreshAll();
  events.bootstrap();
  metrics.start(3000);
  startWorkspaceRefresh(3000);
  const offStream = onStreamEvent(handleStreamEvent);
  (window as any).__stockmanOffStream = offStream;
});

onBeforeUnmount(() => {
  stopResize();
  stopWorkspaceRefresh();
  metrics.stop();
  events.dispose();
  (window as any).__stockmanOffStream?.();
});

function readLayoutSize(key: string, fallback: number, min: number, max: number): number {
  const raw = window.localStorage.getItem(key);
  const value = raw ? Number.parseInt(raw, 10) : fallback;
  return clamp(Number.isFinite(value) ? value : fallback, min, max);
}

function clamp(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, value));
}

function shellRect() {
  return opsShellRef.value?.getBoundingClientRect();
}

function leftPaneBounds() {
  const width = shellRect()?.width ?? window.innerWidth;
  return {
    min: 190,
    max: Math.min(520, Math.max(220, width - 560)),
  };
}

function bottomPaneBounds() {
  const height = shellRect()?.height ?? window.innerHeight;
  return {
    min: 160,
    max: Math.min(620, Math.max(190, height - 292)),
  };
}

function persistLayoutSize(pane: ResizePane) {
  if (pane === 'left') {
    window.localStorage.setItem('stockman:leftPaneWidth', String(leftPaneWidth.value));
  } else {
    window.localStorage.setItem('stockman:bottomPaneHeight', String(bottomPaneHeight.value));
  }
}

function startResize(pane: ResizePane, ev: PointerEvent) {
  ev.preventDefault();
  ev.stopPropagation();
  closeOverlays();
  if (resizingPane.value) stopResize();
  resizingPane.value = pane;
  resizeStart.x = ev.clientX;
  resizeStart.y = ev.clientY;
  resizeStart.left = leftPaneWidth.value;
  resizeStart.bottom = bottomPaneHeight.value;
  document.body.style.cursor = pane === 'left' ? 'col-resize' : 'row-resize';
  document.body.style.userSelect = 'none';
  try {
    (ev.currentTarget as HTMLElement | null)?.setPointerCapture?.(ev.pointerId);
  } catch {
    // Some embedded webviews do not allow capture after synthetic pointer events.
  }
  window.addEventListener('pointermove', handleResize);
  window.addEventListener('pointerup', stopResize);
  window.addEventListener('pointercancel', stopResize);
  handleResize(ev);
}

function handleResize(ev: PointerEvent) {
  if (resizingPane.value === 'left') {
    const bounds = leftPaneBounds();
    leftPaneWidth.value = clamp(resizeStart.left + ev.clientX - resizeStart.x, bounds.min, bounds.max);
  } else if (resizingPane.value === 'bottom') {
    const bounds = bottomPaneBounds();
    bottomPaneHeight.value = clamp(resizeStart.bottom + resizeStart.y - ev.clientY, bounds.min, bounds.max);
  }
}

function stopResize() {
  if (resizingPane.value) persistLayoutSize(resizingPane.value);
  resizingPane.value = '';
  document.body.style.cursor = '';
  document.body.style.userSelect = '';
  window.removeEventListener('pointermove', handleResize);
  window.removeEventListener('pointerup', stopResize);
  window.removeEventListener('pointercancel', stopResize);
}

function nudgePane(pane: ResizePane, delta: number) {
  if (pane === 'left') {
    const bounds = leftPaneBounds();
    leftPaneWidth.value = clamp(leftPaneWidth.value + delta, bounds.min, bounds.max);
  } else {
    const bounds = bottomPaneBounds();
    bottomPaneHeight.value = clamp(bottomPaneHeight.value + delta, bounds.min, bounds.max);
  }
  persistLayoutSize(pane);
}

function resetPane(pane: ResizePane) {
  if (pane === 'left') {
    leftPaneWidth.value = 250;
  } else {
    bottomPaneHeight.value = 246;
  }
  persistLayoutSize(pane);
}

async function refreshAll() {
  await Promise.all([topo.refresh(), metrics.refresh()]);
  pivotListeners.value = [...(topo.pivotListeners ?? [])];
  controllerListeners.value = [...(topo.controllerListeners ?? [])];
  await refreshListeners();
}

async function refreshWorkspaceSnapshot() {
  await topo.refreshQuietly();
  pivotListeners.value = [...(topo.pivotListeners ?? [])];
  controllerListeners.value = [...(topo.controllerListeners ?? [])];
}

function startWorkspaceRefresh(intervalMs = 3000) {
  stopWorkspaceRefresh();
  workspaceRefreshTimer = window.setInterval(() => {
    refreshWorkspaceSnapshot();
  }, intervalMs);
}

function stopWorkspaceRefresh() {
  if (workspaceRefreshTimer !== null) {
    window.clearInterval(workspaceRefreshTimer);
    workspaceRefreshTimer = null;
  }
}

async function logout() {
  stopWorkspaceRefresh();
  await conn.disconnect();
  topo.clear();
  events.dispose();
  metrics.reset();
}

function labelForNode(node: NodeSummary): string {
  return node.alias || node.uuid.slice(0, 8);
}

function labelForUUID(uuid: string): string {
  if (!uuid) return '-';
  const node = topo.nodeMap.get(uuid);
  return node ? labelForNode(node) : uuid.slice(0, 8);
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

function compactEventTime(timestamp: string): string {
  const value = timestamp.trim();
  if (!value) return '--:--:--';
  const isoTime = value.match(/T(\d{2}:\d{2}:\d{2})(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?$/);
  if (isoTime) return isoTime[1];
  const clockTime = value.match(/\b(\d{2}:\d{2}:\d{2})(?:\.\d+)?\b/);
  if (clockTime) return clockTime[1];
  return value.length > 12 ? value.slice(0, 12) : value;
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
  contextMenu.y = Math.min(ev.clientY, Math.max(12, window.innerHeight - 520));
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
}

async function refreshListeners() {
  try {
    const [pivots, controllers] = await Promise.all([
      listPivotListeners({}),
      listControllerListeners(),
    ]);
    pivotListeners.value = pivots;
    controllerListeners.value = controllers;
  } catch (err: any) {
    action.error = err?.message ?? String(err);
  }
}

function closeTargetTab(uuid: string) {
  targetTabs.value = targetTabs.value.filter((id) => id !== uuid);
  if (bottomTab.value === `target:${uuid}`) {
    bottomTab.value = targetTabs.value.length
      ? `target:${targetTabs.value[targetTabs.value.length - 1]}`
      : 'events';
  }
}

async function chooseTargetAction(kind: 'detail' | 'shell' | 'files' | 'proxy' | 'ssh' | 'listeners' | 'events' | 'sessions' | 'streams' | 'dtn' | 'sleep' | 'refresh' | 'copy' | 'prune' | 'mark-alive' | 'mark-dead' | 'repair' | 'reconnect' | 'terminate' | 'diagnostics') {
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
  } else if (kind === 'ssh') {
    openTargetTab(node.uuid, 'ssh');
  } else if (kind === 'listeners') {
    openTargetTab(node.uuid, 'listeners');
    await refreshListeners();
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
  } else if (kind === 'mark-alive') {
    await runSessionAction('mark-alive', node.uuid);
  } else if (kind === 'mark-dead') {
    await runSessionAction('mark-dead', node.uuid);
  } else if (kind === 'repair') {
    await runSessionAction('repair', node.uuid);
  } else if (kind === 'reconnect') {
    await runSessionAction('reconnect', node.uuid);
  } else if (kind === 'terminate') {
    await runSessionAction('terminate', node.uuid);
  } else if (kind === 'diagnostics') {
    openTargetTab(node.uuid, 'overview');
    await loadSessionDiagnostics(node.uuid);
  } else if (kind === 'copy') {
    await navigator.clipboard?.writeText(node.uuid);
    action.message = `Copied node UUID: ${node.uuid}`;
  } else if (kind === 'prune') {
    await submitPrune();
  }
}

function chooseMenuAction(actionName: 'refresh' | 'table' | 'graph' | 'events' | 'sessions' | 'streams' | 'dtn' | 'sleep' | 'listeners' | 'prune' | 'disconnect') {
  activeMenu.value = '';
  if (actionName === 'refresh') refreshAll();
  else if (actionName === 'table') mainView.value = 'table';
  else if (actionName === 'graph') mainView.value = 'graph';
  else if (actionName === 'events') bottomTab.value = 'events';
  else if (actionName === 'sessions') bottomTab.value = 'sessions';
  else if (actionName === 'streams') bottomTab.value = 'streams';
  else if (actionName === 'dtn') bottomTab.value = 'dtn';
  else if (actionName === 'sleep') bottomTab.value = 'control';
  else if (actionName === 'listeners') {
    bottomTab.value = 'listeners';
    refreshListeners();
  }
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
  const isSsh = ev.kind === 'ssh' || sshHandles[uuid]?.handleId === ev.handleId;
  const lines = isSsh ? sshLines : shellLines;
  if (!lines[uuid]) lines[uuid] = [];
  if (ev.type === 'open') {
    lines[uuid].push(`[stream ${ev.streamId || '-'} opened]`);
    const handles = isSsh ? sshHandles : shellHandles;
    if (handles[uuid] && ev.streamId) handles[uuid].streamId = ev.streamId;
  } else if (ev.type === 'data' && ev.data) {
    lines[uuid].push(...ev.data.replace(/\r/g, '').split('\n').filter(Boolean));
  } else if (ev.type === 'closed') {
    lines[uuid].push('[stream closed]');
  } else if (ev.type === 'error') {
    lines[uuid].push(`[error] ${ev.error || 'stream error'}`);
  }
  lines[uuid] = lines[uuid].slice(-160);
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

async function ensureSsh(uuid = activeTargetUUID.value) {
  if (!uuid) return;
  targetMode.value = 'ssh';
  if (sshHandles[uuid]) return;
  if (!sshLines[uuid]) sshLines[uuid] = [];
  try {
    const handle = await startSshSession({
      target: uuid,
      serverAddr: sshServerAddr.value,
      username: sshUsername.value,
      password: sshPassword.value,
    });
    sshHandles[uuid] = handle;
    sshLines[uuid].push(`[ssh requested] ${handle.sessionId || handle.handleId}`);
  } catch (err: any) {
    action.error = err?.message ?? String(err);
    sshLines[uuid].push(`[error] ${action.error}`);
  }
}

async function submitSshCommand() {
  const uuid = activeTargetUUID.value;
  const cmd = sshInput.value;
  if (!uuid || !cmd.trim()) return;
  await ensureSsh(uuid);
  const handle = sshHandles[uuid];
  sshLines[uuid].push(`$ ${cmd}`);
  sshInput.value = '';
  try {
    await sendStreamData({ handleId: handle.handleId, data: `${cmd}\n` });
  } catch (err: any) {
    action.error = err?.message ?? String(err);
    sshLines[uuid].push(`[error] ${action.error}`);
  }
}

async function closeActiveSsh() {
  const uuid = activeTargetUUID.value;
  const handle = uuid ? sshHandles[uuid] : undefined;
  if (!uuid || !handle) return;
  try {
    await closeInteractiveStream({ handleId: handle.handleId, reason: 'operator closed ssh' });
  } catch (err: any) {
    action.error = err?.message ?? String(err);
  }
  delete sshHandles[uuid];
}

async function submitSshTunnel() {
  const uuid = activeTargetUUID.value;
  if (!uuid) return;
  try {
    await startSshTunnel({
      target: uuid,
      serverAddr: sshServerAddr.value,
      agentPort: sshAgentPort.value,
      authMethod: sshPrivateKey.value.trim() ? 'cert' : 'password',
      username: sshUsername.value,
      password: sshPassword.value,
      privateKey: sshPrivateKey.value,
    });
    action.message = `SSH tunnel requested for ${labelForUUID(uuid)}.`;
  } catch (err: any) {
    action.error = err?.message ?? String(err);
  }
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

async function uploadFileForActive() {
  const uuid = activeTargetUUID.value;
  if (!uuid) return;
  try {
    const result: UploadRemoteFileResult = await uploadRemoteFile({
      target: uuid,
      localPath: uploadLocalPath.value,
      remotePath: uploadRemotePath.value,
    });
    action.message = `Uploaded ${result.remotePath} (${result.size || 0} bytes)`;
    await loadFiles(uuid, filePath.value);
  } catch (err: any) {
    action.error = err?.message ?? String(err);
  }
}

async function loadLootForActive() {
  const uuid = activeTargetUUID.value;
  if (!uuid) return;
  try {
    const result: ListLootResult = await listLoot({ target: uuid, limit: 100 });
    lootItems[uuid] = result.items || [];
  } catch (err: any) {
    action.error = err?.message ?? String(err);
  }
}

async function exportLootItem(item: LootItem) {
  try {
    const result: ExportLootResult = await exportLoot({
      lootId: item.lootId,
      localPath: lootExportPath.value,
    });
    action.message = `Exported ${result.bytes} bytes to ${result.localPath}`;
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

async function startBackwardForActive() {
  const uuid = activeTargetUUID.value;
  if (!uuid) return;
  targetMode.value = 'proxy';
  if (!proxyResults[uuid]) proxyResults[uuid] = [];
  try {
    const result = await startBackwardProxy({
      target: uuid,
      remotePort: backwardRemotePort.value,
      localPort: backwardLocalPort.value,
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

async function stopProxyForActive(proxy: ProxyResult) {
  const uuid = activeTargetUUID.value;
  if (!uuid) return;
  try {
    if (proxy.kind === 'backward') {
      await stopBackwardProxy({ target: uuid, proxyId: proxy.proxyId });
    } else {
      await stopForwardProxy({ target: uuid, proxyId: proxy.proxyId });
    }
  } catch (err: any) {
    action.error = err?.message ?? String(err);
    return;
  }
  proxyResults[uuid] = (proxyResults[uuid] || []).filter((p) => p.proxyId !== proxy.proxyId);
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

async function runSessionAction(
  kind: 'mark-alive' | 'mark-dead' | 'repair' | 'reconnect' | 'terminate',
  uuid = activeTargetUUID.value || topo.selectedUUID,
) {
  if (!uuid) return;
  try {
    if (kind === 'mark-alive') {
      await markSession({ target: uuid, action: 'alive', reason: sessionReason.value });
      action.message = `Marked ${labelForUUID(uuid)} alive.`;
    } else if (kind === 'mark-dead') {
      await markSession({ target: uuid, action: 'dead', reason: sessionReason.value });
      action.message = `Marked ${labelForUUID(uuid)} dead.`;
    } else if (kind === 'repair') {
      await repairSession({ target: uuid, force: true, reason: sessionReason.value });
      action.message = `Repair queued for ${labelForUUID(uuid)}.`;
    } else if (kind === 'reconnect') {
      await reconnectSession({ target: uuid, reason: sessionReason.value });
      action.message = `Reconnect triggered for ${labelForUUID(uuid)}.`;
    } else if (kind === 'terminate') {
      await terminateSession({ target: uuid, reason: sessionReason.value });
      action.message = `Terminate requested for ${labelForUUID(uuid)}.`;
    }
    await refreshAll();
  } catch (err: any) {
    action.error = err?.message ?? String(err);
  }
}

async function loadSessionDiagnostics(uuid = activeTargetUUID.value || topo.selectedUUID) {
  if (!uuid) return;
  try {
    const diag = await getSessionDiagnostics({ target: uuid });
    sessionDiagnostics[uuid] = diag;
    action.message = `Diagnostics loaded for ${labelForUUID(uuid)}.`;
  } catch (err: any) {
    action.error = err?.message ?? String(err);
  }
}

async function createPivotForActive() {
  const uuid = activeTargetUUID.value || topo.selectedUUID;
  if (!uuid) return;
  try {
    await createPivotListener({
      target: uuid,
      protocol: pivotProtocol.value,
      bind: pivotBind.value,
      mode: pivotMode.value,
    });
    action.message = `Pivot listener created on ${labelForUUID(uuid)}.`;
    await refreshListeners();
  } catch (err: any) {
    action.error = err?.message ?? String(err);
  }
}

async function setPivotStatus(listener: PivotListenerDTO, desiredStatus: 'resume' | 'pause') {
  try {
    await updatePivotListener({
      listenerId: listener.listenerId,
      desiredStatus,
      includeSpec: false,
    });
    await refreshListeners();
  } catch (err: any) {
    action.error = err?.message ?? String(err);
  }
}

async function removePivot(listener: PivotListenerDTO) {
  try {
    await deletePivotListener({ listenerId: listener.listenerId });
    await refreshListeners();
  } catch (err: any) {
    action.error = err?.message ?? String(err);
  }
}

async function createController() {
  try {
    await createControllerListener({
      protocol: controllerProtocol.value,
      bind: controllerBind.value,
    });
    action.message = 'Controller listener created.';
    await refreshListeners();
  } catch (err: any) {
    action.error = err?.message ?? String(err);
  }
}

async function setControllerStatus(listener: ControllerListenerDTO, desiredStatus: 'running' | 'stopped') {
  try {
    await updateControllerListener({
      listenerId: listener.listenerId,
      desiredStatus,
      includeSpec: false,
    });
    await refreshListeners();
  } catch (err: any) {
    action.error = err?.message ?? String(err);
  }
}

async function removeController(listener: ControllerListenerDTO) {
  try {
    await deleteControllerListener({ listenerId: listener.listenerId });
    await refreshListeners();
  } catch (err: any) {
    action.error = err?.message ?? String(err);
  }
}

function proxyDisplay(proxy: ProxyResult) {
  if (proxy.kind === 'backward') {
    const back = proxy as StartBackwardProxyResult;
    return `${back.remotePort} -> 127.0.0.1:${back.localPort}`;
  }
  const fwd = proxy as StartForwardProxyResult;
  return `${fwd.bind} -> ${fwd.remoteAddr}`;
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
  <section
    ref="opsShellRef"
    class="ops-shell"
    :class="{
      'is-resizing-left': resizingPane === 'left',
      'is-resizing-bottom': resizingPane === 'bottom',
    }"
    :style="layoutStyle"
    @click="closeOverlays"
    @keydown.esc="closeOverlays"
  >
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
            <button @click="chooseMenuAction('listeners')"><span>Listener Manager</span><kbd>⌘L</kbd></button>
            <button @click="sideMode = 'listeners'; activeMenu = ''"><span>Show Listener Groups</span><kbd>⌘⇧L</kbd></button>
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
      <button data-tip="Open listener manager" @click="chooseMenuAction('listeners')">⌁</button>
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
          <button :class="{ active: sideMode === 'networks' }" @click="sideMode = 'networks'">Networks</button>
          <button :class="{ active: sideMode === 'listeners' }" @click="sideMode = 'listeners'">Listeners</button>
        </div>

        <section v-if="sideMode === 'networks'" class="side-groups">
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

      <button
        type="button"
        class="vertical-splitter"
        role="separator"
        aria-orientation="vertical"
        aria-label="Resize side pane"
        :aria-valuenow="leftPaneWidth"
        aria-valuemin="190"
        aria-valuemax="520"
        tabindex="0"
        title="Drag to resize side pane"
        @click.stop
        @dblclick.stop="resetPane('left')"
        @pointerdown="startResize('left', $event)"
        @keydown.left.prevent="nudgePane('left', -18)"
        @keydown.right.prevent="nudgePane('left', 18)"
      ></button>

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
          <ForceGraph
            v-if="orderedNodes.length"
            :nodes="topo.nodes"
            :edges="topo.edges"
            :selected="topo.selectedUUID"
            @select="selectNode"
            @open="openTargetTab"
            @target-menu="openTargetMenu"
          />
          <div v-if="orderedNodes.length" class="graph-legend">
            <span><i class="legend-line primary"></i>Primary {{ graphSummary.primary }}</span>
            <span><i class="legend-line supplemental"></i>Supplemental {{ graphSummary.supplemental }}</span>
            <span><i class="legend-dot online"></i>Online {{ stats.online }}</span>
          </div>
          <p v-if="!orderedNodes.length" class="empty table-empty">
            Waiting for graph data.
          </p>
        </div>
      </section>
    </main>

    <button
      type="button"
      class="horizontal-splitter"
      role="separator"
      aria-orientation="horizontal"
      aria-label="Resize bottom workspace"
      :aria-valuenow="bottomPaneHeight"
      aria-valuemin="160"
      aria-valuemax="620"
      tabindex="0"
      title="Drag to resize bottom workspace"
      @click.stop
      @dblclick.stop="resetPane('bottom')"
      @pointerdown="startResize('bottom', $event)"
      @keydown.up.prevent="nudgePane('bottom', 18)"
      @keydown.down.prevent="nudgePane('bottom', -18)"
    ></button>

    <section class="bottom-pane">
      <nav class="bottom-tabs">
        <button data-tip="Chronological operator and node event log" :class="{ active: bottomTab === 'events' }" @click="bottomTab = 'events'">Event Log</button>
        <button data-tip="Session status, remote address, and errors" :class="{ active: bottomTab === 'sessions' }" @click="bottomTab = 'sessions'">Sessions</button>
        <button data-tip="Queue store-carry-forward payloads" :class="{ active: bottomTab === 'dtn' }" @click="bottomTab = 'dtn'">DTN Queue</button>
        <button data-tip="Active stream diagnostics and flow control" :class="{ active: bottomTab === 'streams' }" @click="bottomTab = 'streams'">Streams</button>
        <button data-tip="Controller and pivot listener manager" :class="{ active: bottomTab === 'listeners' }" @click="chooseMenuAction('listeners')">Listeners</button>
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
            <span class="time" :title="ev.timestamp">[{{ compactEventTime(ev.timestamp) }}]</span>
            <span class="kind" :title="ev.kind">{{ ev.kind }}</span>
            <span class="action" :title="ev.action">{{ ev.action }}</span>
            <span class="summary" :title="ev.summary">{{ ev.summary }}</span>
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
            <button @click="openTargetTab(session.targetUuid, 'overview')">Inspect</button>
            <button @click="loadSessionDiagnostics(session.targetUuid)">Diag</button>
            <button @click="runSessionAction('repair', session.targetUuid)">Repair</button>
            <button @click="runSessionAction('reconnect', session.targetUuid)">Reconnect</button>
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

        <section v-else-if="bottomTab === 'listeners'" class="listener-manager">
          <div class="listener-create">
            <section>
              <h3>Controller Listener</h3>
              <label><span>Protocol</span><input v-model="controllerProtocol" /></label>
              <label><span>Bind</span><input v-model="controllerBind" class="sf-mono" /></label>
              <button @click="createController">Create</button>
            </section>
            <section>
              <h3>Pivot Listener</h3>
              <label><span>Target</span><input :value="activeTargetUUID || topo.selectedUUID" class="sf-mono" readonly /></label>
              <label><span>Protocol</span><input v-model="pivotProtocol" /></label>
              <label><span>Bind</span><input v-model="pivotBind" class="sf-mono" /></label>
              <label>
                <span>Mode</span>
                <select v-model="pivotMode">
                  <option value="normal">normal</option>
                  <option value="iptables">iptables</option>
                  <option value="soreuse">soreuse</option>
                </select>
              </label>
              <button :disabled="!(activeTargetUUID || topo.selectedUUID)" @click="createPivotForActive">Create</button>
            </section>
          </div>
          <div class="listener-table">
            <h3>Controller</h3>
            <p v-for="listener in controllerListeners" :key="listener.listenerId" class="listener-row">
              <strong>{{ listener.bind }}</strong>
              <span>{{ listener.protocol }}</span>
              <span>{{ listener.status }}</span>
              <small>{{ listener.lastError || listener.listenerId }}</small>
              <button @click="setControllerStatus(listener, 'running')">Start</button>
              <button @click="setControllerStatus(listener, 'stopped')">Stop</button>
              <button @click="removeController(listener)">Delete</button>
            </p>
            <p v-if="!controllerListeners.length" class="empty">No controller listeners.</p>
            <h3>Pivot</h3>
            <p v-for="listener in pivotListeners" :key="listener.listenerId" class="listener-row">
              <strong>{{ listener.bind }}</strong>
              <span>{{ labelForUUID(listener.targetUuid || '') }}</span>
              <span>{{ listener.status }}</span>
              <small>{{ listener.lastError || listener.listenerId }}</small>
              <button @click="setPivotStatus(listener, 'resume')">Resume</button>
              <button @click="setPivotStatus(listener, 'pause')">Pause</button>
              <button @click="removePivot(listener)">Delete</button>
            </p>
            <p v-if="!pivotListeners.length" class="empty">No pivot listeners.</p>
          </div>
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
              <button @click="targetMode = 'ssh'">SSH</button>
              <button @click="targetMode = 'listeners'; refreshListeners()">Listeners</button>
              <button @click="bottomTab = 'dtn'">Queue DTN</button>
              <button @click="submitSleep">Apply Sleep</button>
            </div>
          </header>
          <nav class="target-mode-tabs">
            <button :class="{ active: targetMode === 'overview' }" @click="targetMode = 'overview'">Overview</button>
            <button :class="{ active: targetMode === 'shell' }" @click="ensureShell()">Shell</button>
            <button :class="{ active: targetMode === 'files' }" @click="loadFiles()">Files</button>
            <button :class="{ active: targetMode === 'proxy' }" @click="targetMode = 'proxy'">Proxy</button>
            <button :class="{ active: targetMode === 'ssh' }" @click="targetMode = 'ssh'">SSH</button>
            <button :class="{ active: targetMode === 'listeners' }" @click="targetMode = 'listeners'; refreshListeners()">Listeners</button>
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
            <section class="target-panel session-tools">
              <h3>Session Actions</h3>
              <label><span>Reason</span><input v-model="sessionReason" /></label>
              <div class="button-grid">
                <button @click="runSessionAction('mark-alive')">Mark Alive</button>
                <button @click="runSessionAction('mark-dead')">Mark Dead</button>
                <button @click="runSessionAction('repair')">Repair</button>
                <button @click="runSessionAction('reconnect')">Reconnect</button>
                <button @click="runSessionAction('terminate')">Terminate</button>
                <button @click="loadSessionDiagnostics()">Diagnostics</button>
              </div>
              <div v-if="activeSessionDiagnostics" class="diag-box">
                <strong>Diagnostics</strong>
                <p v-for="metric in activeSessionDiagnostics.metrics || []" :key="metric.name">
                  <span>{{ metric.name }}</span><small>{{ metric.value }}</small>
                </p>
                <p v-for="issue in activeSessionDiagnostics.issues || []" :key="issue.code" class="error-text">
                  <span>{{ issue.code }}</span><small>{{ issue.message }}</small>
                </p>
              </div>
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
            <div class="file-transfer-row">
              <input v-model="uploadLocalPath" class="sf-mono" placeholder="/local/path/payload.bin" />
              <input v-model="uploadRemotePath" class="sf-mono" placeholder="/tmp/payload.bin" />
              <button @click="uploadFileForActive">Upload</button>
              <button @click="loadLootForActive">Loot</button>
            </div>
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
            <div class="loot-table">
              <div class="loot-export">
                <input v-model="lootExportPath" class="sf-mono" placeholder="/local/export/path (optional)" />
              </div>
              <p v-for="item in activeLootItems" :key="item.lootId" class="loot-row">
                <strong>{{ item.name || item.lootId }}</strong>
                <span>{{ item.originPath || '-' }}</span>
                <small>{{ item.size || 0 }} bytes</small>
                <button @click="exportLootItem(item)">Export</button>
              </p>
              <p v-if="!activeLootItems.length" class="empty">No loot loaded for this target.</p>
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
              <h3>Backward Proxy</h3>
              <label><span>Agent remote port</span><input v-model="backwardRemotePort" class="sf-mono" /></label>
              <label><span>Kelpie local port</span><input v-model="backwardLocalPort" class="sf-mono" /></label>
              <button @click="startBackwardForActive">Start Backward</button>
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
                <span>{{ proxy.kind || 'forward' }}</span>
                <span>{{ proxyDisplay(proxy) }}</span>
                <button @click="stopProxyForActive(proxy)">Stop</button>
              </p>
              <p v-if="!activeProxyResults.length" class="empty">No proxies for this target.</p>
            </section>
          </div>
          <div v-else-if="targetMode === 'ssh'" class="ssh-workspace">
            <section class="ssh-config">
              <label><span>SSH server</span><input v-model="sshServerAddr" class="sf-mono" /></label>
              <label><span>Username</span><input v-model="sshUsername" /></label>
              <label><span>Password</span><input v-model="sshPassword" type="password" /></label>
              <label><span>Agent tunnel port</span><input v-model="sshAgentPort" class="sf-mono" /></label>
              <button @click="ensureSsh()">Start Session</button>
              <button @click="submitSshTunnel">Start Tunnel</button>
              <button @click="closeActiveSsh">Close Session</button>
            </section>
            <textarea v-model="sshPrivateKey" class="ssh-key sf-mono" placeholder="private key for cert auth tunnel"></textarea>
            <div class="shell-head">
              <span>{{ activeSshHandle ? activeSshHandle.status : 'not started' }}</span>
              <span class="sf-mono">{{ activeSshHandle?.sessionId || activeTargetUUID }}</span>
            </div>
            <div class="shell-output">
              <p v-for="(line, idx) in activeSshLines" :key="idx">{{ line }}</p>
              <p v-if="!activeSshLines.length" class="empty">No SSH output yet.</p>
            </div>
            <form class="shell-input" @submit.prevent="submitSshCommand">
              <span>$</span>
              <input v-model="sshInput" class="sf-mono" placeholder="hostname" />
              <button>Send</button>
            </form>
          </div>
          <div v-else-if="targetMode === 'listeners'" class="target-listener-workspace">
            <section class="listener-create compact">
              <section>
                <h3>Pivot Listener</h3>
                <label><span>Protocol</span><input v-model="pivotProtocol" /></label>
                <label><span>Bind</span><input v-model="pivotBind" class="sf-mono" /></label>
                <label>
                  <span>Mode</span>
                  <select v-model="pivotMode">
                    <option value="normal">normal</option>
                    <option value="iptables">iptables</option>
                    <option value="soreuse">soreuse</option>
                  </select>
                </label>
                <button @click="createPivotForActive">Create Pivot</button>
              </section>
            </section>
            <section class="listener-table">
              <h3>Target Pivot Listeners</h3>
              <p v-for="listener in activePivotListeners" :key="listener.listenerId" class="listener-row">
                <strong>{{ listener.bind }}</strong>
                <span>{{ listener.protocol }}</span>
                <span>{{ listener.status }}</span>
                <small>{{ listener.lastError || listener.listenerId }}</small>
                <button @click="setPivotStatus(listener, 'resume')">Resume</button>
                <button @click="setPivotStatus(listener, 'pause')">Pause</button>
                <button @click="removePivot(listener)">Delete</button>
              </p>
              <p v-if="!activePivotListeners.length" class="empty">No pivot listener on this target.</p>
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
      <button @click="chooseTargetAction('ssh')">
        <span>SSH Session / Tunnel</span>
        <kbd>SSH</kbd>
      </button>
      <button @click="chooseTargetAction('listeners')">
        <span>Pivot Listener</span>
        <kbd>L</kbd>
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
      <button @click="chooseTargetAction('diagnostics')">
        <span>Diagnostics</span>
        <kbd>D</kbd>
      </button>
      <button @click="chooseTargetAction('mark-alive')">
        <span>Mark Alive</span>
        <kbd>A</kbd>
      </button>
      <button @click="chooseTargetAction('mark-dead')">
        <span>Mark Dead</span>
        <kbd>X</kbd>
      </button>
      <button @click="chooseTargetAction('repair')">
        <span>Repair Session</span>
        <kbd>R</kbd>
      </button>
      <button @click="chooseTargetAction('reconnect')">
        <span>Reconnect</span>
        <kbd>C</kbd>
      </button>
      <button @click="chooseTargetAction('terminate')">
        <span>Terminate</span>
        <kbd>T</kbd>
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
  grid-template-rows: 36px 42px minmax(180px, 1fr) 8px var(--bottom-pane-height) 28px;
  height: 100vh;
  width: 100vw;
  overflow: hidden;
  background: var(--ops-bg);
  color: var(--ops-text);
  font-family: var(--sf-font-sans);
}

.ops-shell.is-resizing-left,
.ops-shell.is-resizing-left * {
  cursor: col-resize !important;
  user-select: none;
}

.ops-shell.is-resizing-bottom,
.ops-shell.is-resizing-bottom * {
  cursor: row-resize !important;
  user-select: none;
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
  grid-template-columns: var(--left-pane-width) 8px minmax(0, 1fr);
  min-height: 0;
}

.left-pane,
.center-pane {
  min-height: 0;
  overflow: hidden;
  background: var(--ops-panel);
}

.left-pane {
  border-right: 0;
}

.vertical-splitter,
.horizontal-splitter {
  position: relative;
  z-index: 12;
  display: block;
  width: 100%;
  height: 100%;
  padding: 0;
  border: 0;
  background: #151a20;
  outline: none;
  appearance: none;
  touch-action: none;
}

.vertical-splitter {
  cursor: col-resize;
  border-left: 1px solid var(--ops-line-soft);
  border-right: 1px solid var(--ops-line-soft);
}

.horizontal-splitter {
  cursor: row-resize;
  border-top: 1px solid var(--ops-line-soft);
  border-bottom: 1px solid var(--ops-line-soft);
}

.vertical-splitter::before,
.horizontal-splitter::before {
  content: '';
  position: absolute;
  background: var(--ops-line);
}

.vertical-splitter::before {
  top: 0;
  bottom: 0;
  left: 50%;
  width: 1px;
  transform: translateX(-50%);
}

.horizontal-splitter::before {
  left: 0;
  right: 0;
  top: 50%;
  height: 1px;
  transform: translateY(-50%);
}

.vertical-splitter:hover,
.vertical-splitter:focus-visible,
.horizontal-splitter:hover,
.horizontal-splitter:focus-visible,
.ops-shell.is-resizing-left .vertical-splitter,
.ops-shell.is-resizing-bottom .horizontal-splitter {
  background: #1f2830;
}

.vertical-splitter:hover::before,
.vertical-splitter:focus-visible::before,
.horizontal-splitter:hover::before,
.horizontal-splitter:focus-visible::before,
.ops-shell.is-resizing-left .vertical-splitter::before,
.ops-shell.is-resizing-bottom .horizontal-splitter::before {
  background: var(--ops-accent);
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

.side-groups {
  height: 100%;
  overflow: auto;
  padding: 8px;
}

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

.group-row:hover {
  background: var(--ops-panel-2);
  border-color: var(--ops-line);
  color: var(--ops-text);
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
  position: relative;
  min-height: 0;
  overflow: hidden;
  background-color: #101419;
}

.graph-legend {
  position: absolute;
  left: 14px;
  bottom: 12px;
  z-index: 2;
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
  padding: 6px 8px;
  border: 1px solid rgba(133, 151, 170, 0.34);
  background: rgba(16, 20, 25, 0.82);
  color: #cad6e2;
  font-size: 0.72rem;
}

.graph-legend span {
  display: inline-flex;
  align-items: center;
  gap: 6px;
}

.legend-line {
  width: 26px;
  height: 0;
  border-top: 2px solid rgba(205, 215, 226, 0.72);
}

.legend-line.supplemental {
  border-top-color: #67d6ff;
  border-top-style: dashed;
}

.legend-dot {
  width: 8px;
  height: 8px;
  background: #1863dc;
  border: 1px solid #ffffff;
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
  padding: 6px 0;
  font-family: var(--sf-font-mono);
  font-size: 0.76rem;
  line-height: 20px;
}

.event-line {
  display: grid;
  grid-template-columns: 92px 82px minmax(220px, 32%) minmax(280px, 1fr);
  gap: 12px;
  margin: 0;
  min-height: 22px;
  padding: 0 10px;
  align-items: center;
  color: var(--ops-muted);
  border-bottom: 1px solid rgba(42, 50, 59, 0.38);
}

.event-line > span {
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
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

.event-line .time {
  font-variant-numeric: tabular-nums;
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
  grid-template-columns: auto 110px 120px 150px minmax(0, 1fr) repeat(4, auto);
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

.data-row button {
  height: 26px;
  padding: 0 8px;
  border: 1px solid var(--ops-line);
  background: var(--ops-panel-2);
  color: var(--ops-muted);
  cursor: pointer;
}

.data-row button:hover {
  border-color: var(--ops-accent);
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
.session-tools input,
.shell-head button,
.shell-input button,
.shell-input input,
.file-path input,
.file-path button,
.file-transfer-row input,
.file-transfer-row button,
.loot-export input,
.loot-row button,
.ssh-config input,
.ssh-config button,
.ssh-key,
.listener-create input,
.listener-create select,
.listener-create button,
.listener-row button,
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
.file-transfer-row button,
.loot-row button,
.ssh-config button,
.listener-create button,
.listener-row button,
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
.file-transfer-row button:hover,
.loot-row button:hover,
.ssh-config button:hover,
.listener-create button:hover,
.listener-row button:hover,
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

.session-tools label,
.listener-create label,
.ssh-config label {
  display: grid;
  gap: 5px;
  margin-bottom: 8px;
  color: var(--ops-faint);
  font-size: 0.72rem;
}

.session-tools input,
.listener-create input,
.listener-create select,
.ssh-config input,
.ssh-key {
  min-height: 28px;
  padding: 0 8px;
}

.button-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 6px;
}

.diag-box {
  margin-top: 10px;
  padding-top: 8px;
  border-top: 1px solid var(--ops-line-soft);
}

.diag-box p {
  grid-template-columns: minmax(0, 120px) minmax(0, 1fr);
}

.sleep-editor input {
  height: 28px;
  padding: 0 8px;
}

.shell-workspace,
.files-workspace,
.proxy-workspace,
.ssh-workspace,
.target-listener-workspace {
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
  grid-template-rows: 32px 32px minmax(130px, 1fr) minmax(90px, 0.6fr);
}

.file-path {
  grid-template-columns: minmax(0, 1fr) 80px 70px;
}

.file-transfer-row {
  display: grid;
  grid-template-columns: minmax(0, 1.1fr) minmax(0, 1.1fr) 86px 74px;
  gap: 8px;
}

.file-transfer-row input,
.loot-export input {
  height: 30px;
  padding: 0 8px;
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

.loot-table {
  min-height: 0;
  overflow: auto;
  border: 1px solid var(--ops-line);
}

.loot-export {
  padding: 8px;
  border-bottom: 1px solid var(--ops-line-soft);
}

.loot-export input {
  width: 100%;
}

.loot-row {
  min-height: 30px;
  display: grid;
  grid-template-columns: minmax(0, 1fr) minmax(0, 1.4fr) 110px auto;
  align-items: center;
  gap: 10px;
  margin: 0;
  padding: 0 10px;
  border-bottom: 1px solid var(--ops-line-soft);
  color: var(--ops-muted);
}

.loot-row strong,
.loot-row span,
.loot-row small {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.proxy-workspace {
  grid-template-columns: repeat(4, minmax(210px, 1fr));
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
  grid-template-columns: minmax(0, 1fr) 82px minmax(0, 1.2fr) auto;
  gap: 8px;
  align-items: center;
  margin: 0 0 8px;
  color: var(--ops-muted);
}

.ssh-workspace {
  grid-template-rows: auto 70px 30px minmax(0, 1fr) 32px;
}

.ssh-config {
  display: grid;
  grid-template-columns: repeat(4, minmax(150px, 1fr)) repeat(3, auto);
  align-items: end;
  gap: 8px;
}

.ssh-key {
  width: 100%;
  resize: vertical;
  min-height: 64px;
  background: #070a0d;
}

.listener-manager,
.target-listener-workspace {
  min-height: 0;
  display: grid;
  grid-template-columns: minmax(260px, 0.8fr) minmax(0, 1.2fr);
  gap: 12px;
  padding: 12px;
}

.target-listener-workspace {
  padding: 0;
}

.listener-create {
  display: grid;
  gap: 10px;
  align-content: start;
}

.listener-create > section,
.listener-table {
  border: 1px solid var(--ops-line);
  background: rgba(255, 255, 255, 0.015);
  padding: 10px;
}

.listener-create h3,
.listener-table h3 {
  margin: 0 0 10px;
  font-size: 0.86rem;
  color: var(--ops-text);
}

.listener-row {
  display: grid;
  grid-template-columns: minmax(0, 1fr) 100px 90px minmax(0, 1.2fr) repeat(3, auto);
  gap: 8px;
  align-items: center;
  min-height: 32px;
  margin: 0 0 6px;
  color: var(--ops-muted);
}

.listener-row strong,
.listener-row span,
.listener-row small {
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
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
  max-height: calc(100vh - 24px);
  overflow: auto;
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
    grid-template-columns: minmax(190px, var(--left-pane-width)) 8px minmax(0, 1fr);
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
