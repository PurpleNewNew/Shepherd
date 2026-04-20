<script setup lang="ts">
import { computed, reactive, ref } from 'vue';
import { useTopologyStore } from '@/stores/topology';
import {
  enqueueDTN,
  pruneOffline,
  updateSleep,
} from '@/api/bindings';
import type { EnqueueDTNResult } from '@/api/types';

const topo = useTopologyStore();

const tab = ref<'dtn' | 'sleep' | 'prune'>('dtn');

interface DTNState {
  target: string;
  payload: string;
  priority: 'low' | 'normal' | 'high';
  ttlSeconds: number;
  busy: boolean;
  lastBundleId: string;
  error: string;
}
const dtn = reactive<DTNState>({
  target: '',
  payload: 'demo: hello from Stockman',
  priority: 'normal',
  ttlSeconds: 600,
  busy: false,
  lastBundleId: '',
  error: '',
});

interface SleepState {
  target: string;
  sleepSeconds: number;
  workSeconds: number;
  jitter: number;
  busy: boolean;
  message: string;
  error: string;
}
const sleep = reactive<SleepState>({
  target: '',
  sleepSeconds: 15,
  workSeconds: 5,
  jitter: 10,
  busy: false,
  message: '',
  error: '',
});

interface PruneState {
  busy: boolean;
  removed: number;
  message: string;
  error: string;
}
const prune = reactive<PruneState>({
  busy: false,
  removed: 0,
  message: '',
  error: '',
});

const nodeOptions = computed(() =>
  topo.nodes.map((n) => ({
    uuid: n.uuid,
    label: `${n.alias || n.uuid.slice(0, 8)} · ${n.status ?? '?'}`,
  })),
);

function ensureTarget(source: string): boolean {
  if (source) return true;
  if (topo.selectedUUID) return true;
  return false;
}

/*
 * Flock 的 applyDTNPayload (internal/flock/process/router.go) 只认四类前缀
 * 的 payload："memo:"/"log:"/"stream:"/"proto:"。任何其他 payload 都会被回
 * DTN_ACK error="unsupported payload"，而 Kelpie 的 onDTNAck 目前会把非
 * 致命错误无限 Requeue —— 直接触发 Gossip 路径上的 session 抖动。
 * 这里把用户输入兜住：如果没显式带保留前缀，就按 memo 投递（默认最安全
 * 的语义：设置 target 节点的 memo 字段）。
 */
const DTN_RESERVED_PREFIXES = ['memo:', 'log:', 'stream:', 'proto:'];

function normalizeDTNPayload(raw: string): string {
  const trimmed = raw.trim();
  const lower = trimmed.toLowerCase();
  for (const p of DTN_RESERVED_PREFIXES) {
    if (lower.startsWith(p)) return trimmed;
  }
  return `memo:${trimmed}`;
}

async function submitDTN() {
  dtn.error = '';
  const target = dtn.target || topo.selectedUUID;
  if (!target) {
    dtn.error = '请先选择目标节点（或在拓扑页选中一个）';
    return;
  }
  if (!dtn.payload.trim()) {
    dtn.error = 'payload 不能为空';
    return;
  }
  dtn.busy = true;
  try {
    const res: EnqueueDTNResult = await enqueueDTN({
      target,
      payload: normalizeDTNPayload(dtn.payload),
      priority: dtn.priority,
      ttlSeconds: dtn.ttlSeconds,
    });
    dtn.lastBundleId = res.bundleId;
  } catch (err: any) {
    dtn.error = err?.message ?? String(err);
  } finally {
    dtn.busy = false;
  }
}

async function submitSleep() {
  sleep.error = '';
  sleep.message = '';
  const target = sleep.target || topo.selectedUUID;
  if (!target) {
    sleep.error = '请先选择目标节点';
    return;
  }
  sleep.busy = true;
  try {
    await updateSleep({
      target,
      sleepSeconds: sleep.sleepSeconds,
      workSeconds: sleep.workSeconds,
      jitter: sleep.jitter,
    });
    sleep.message = `已发送 sleep 更新到 ${target.slice(0, 8)}…`;
  } catch (err: any) {
    sleep.error = err?.message ?? String(err);
  } finally {
    sleep.busy = false;
  }
}

async function submitPrune() {
  prune.error = '';
  prune.message = '';
  prune.busy = true;
  try {
    const r = await pruneOffline();
    prune.removed = r.removed;
    prune.message = `已请求清理：共移除 ${r.removed} 个离线节点`;
    topo.refresh();
  } catch (err: any) {
    prune.error = err?.message ?? String(err);
  } finally {
    prune.busy = false;
  }
}
</script>

<template>
  <section class="console-wrap">
    <aside class="side sf-panel">
      <p class="sf-label sf-label--strong side-head">Actions</p>
      <button
        :class="['side-tab', { active: tab === 'dtn' }]"
        @click="tab = 'dtn'"
      >
        <span class="tab-code sf-label">A · DTN</span>
        <span class="tab-label">Enqueue a bundle</span>
        <span class="tab-hint">投递一条 DTN payload</span>
      </button>
      <button
        :class="['side-tab', { active: tab === 'sleep' }]"
        @click="tab = 'sleep'"
      >
        <span class="tab-code sf-label">B · Sleep</span>
        <span class="tab-label">Adjust duty-cycle</span>
        <span class="tab-hint">调整节点 sleep/work/jitter</span>
      </button>
      <button
        :class="['side-tab', { active: tab === 'prune' }]"
        @click="tab = 'prune'"
      >
        <span class="tab-code sf-label">C · Prune</span>
        <span class="tab-label">Drop offline nodes</span>
        <span class="tab-hint">清理长时间离线的节点</span>
      </button>
      <div class="side-help">
        答辩控制台。每次动作都会在 Event Timeline 产生对应事件，便于讲解
        Gossip、DTN、sleep-aware 这条主线如何串起来。
      </div>
    </aside>

    <section class="panel sf-panel">
      <!-- DTN -->
      <div v-if="tab === 'dtn'" class="form">
        <header class="form-head">
          <p class="sf-label sf-label--strong">A · DTN Enqueue</p>
          <h3 class="form-title">Send a bundle through the store-carry-forward queue</h3>
          <p class="form-lede">
            选一个目标，发送一条 DTN payload。若目标当前离线或在 sleep window
            内，Kelpie 会先把 bundle 保留在 held 队列里，等接触窗口到来再投递。
          </p>
        </header>
        <div class="grid">
          <label class="field">
            <span class="sf-label sf-label--strong">Target</span>
            <select class="sf-select sf-mono" v-model="dtn.target">
              <option value="">
                使用当前选中：{{
                  topo.selectedUUID?.slice(0, 8) || '未选中'
                }}
              </option>
              <option v-for="n in nodeOptions" :key="n.uuid" :value="n.uuid">
                {{ n.label }}
              </option>
            </select>
          </label>
          <label class="field">
            <span class="sf-label sf-label--strong">Priority</span>
            <select class="sf-select" v-model="dtn.priority">
              <option value="low">low</option>
              <option value="normal">normal</option>
              <option value="high">high</option>
            </select>
          </label>
          <label class="field">
            <span class="sf-label sf-label--strong">TTL · seconds</span>
            <input
              class="sf-input sf-mono"
              type="number"
              min="5"
              :value="dtn.ttlSeconds"
              @input="(e: any) => (dtn.ttlSeconds = Number(e.target.value) || 0)"
            />
          </label>
          <label class="field fullspan">
            <span class="sf-label sf-label--strong">Payload</span>
            <textarea
              class="sf-input sf-mono"
              rows="3"
              v-model="dtn.payload"
              placeholder="memo:hello / log:... / stream:ping / proto:<hex>;&#10;或直接写文本，默认会按 memo 投递"
            />
          </label>
        </div>
        <footer class="form-foot">
          <button
            class="sf-btn primary"
            :disabled="dtn.busy"
            @click="submitDTN"
          >
            {{ dtn.busy ? 'Enqueuing…' : 'Enqueue bundle →' }}
          </button>
          <span v-if="dtn.lastBundleId" class="sf-chip ok">
            bundle
            <span class="sf-mono">
              {{ dtn.lastBundleId.slice(0, 10) }}…
            </span>
          </span>
          <span v-if="dtn.error" class="sf-chip danger">{{ dtn.error }}</span>
        </footer>
      </div>

      <!-- Sleep -->
      <div v-else-if="tab === 'sleep'" class="form">
        <header class="form-head">
          <p class="sf-label sf-label--strong">B · Duty-cycle</p>
          <h3 class="form-title">
            Push a new sleep/work profile to a node
          </h3>
          <p class="form-lede">
            调整节点的 sleep/work/jitter。用来在答辩里演示 duty-cycling
            如何让 DTN 交付时延变化。
          </p>
        </header>
        <div class="grid">
          <label class="field">
            <span class="sf-label sf-label--strong">Target</span>
            <select class="sf-select sf-mono" v-model="sleep.target">
              <option value="">
                使用当前选中：{{
                  topo.selectedUUID?.slice(0, 8) || '未选中'
                }}
              </option>
              <option v-for="n in nodeOptions" :key="n.uuid" :value="n.uuid">
                {{ n.label }}
              </option>
            </select>
          </label>
          <label class="field">
            <span class="sf-label sf-label--strong">sleep_seconds</span>
            <div class="slider-row">
              <input
                type="range"
                min="0"
                max="60"
                v-model.number="sleep.sleepSeconds"
              />
              <span class="slider-value sf-mono">{{ sleep.sleepSeconds }}s</span>
            </div>
          </label>
          <label class="field">
            <span class="sf-label sf-label--strong">work_seconds</span>
            <div class="slider-row">
              <input
                type="range"
                min="0"
                max="30"
                v-model.number="sleep.workSeconds"
              />
              <span class="slider-value sf-mono">{{ sleep.workSeconds }}s</span>
            </div>
          </label>
          <label class="field">
            <span class="sf-label sf-label--strong">jitter %</span>
            <div class="slider-row">
              <input
                type="range"
                min="0"
                max="60"
                v-model.number="sleep.jitter"
              />
              <span class="slider-value sf-mono">{{ sleep.jitter }}%</span>
            </div>
          </label>
        </div>
        <footer class="form-foot">
          <button
            class="sf-btn primary"
            :disabled="sleep.busy"
            @click="submitSleep"
          >
            {{ sleep.busy ? 'Pushing…' : 'Apply to node →' }}
          </button>
          <span v-if="sleep.message" class="sf-chip ok">
            {{ sleep.message }}
          </span>
          <span v-if="sleep.error" class="sf-chip danger">
            {{ sleep.error }}
          </span>
        </footer>
      </div>

      <!-- Prune -->
      <div v-else class="form">
        <header class="form-head">
          <p class="sf-label sf-label--strong">C · Housekeeping</p>
          <h3 class="form-title">Drop nodes that have been offline too long</h3>
          <p class="form-lede">
            一次性清理拓扑中长时间未上线的节点。常用于演示结束或视图复位；被清理的节点再次上线会重新加入拓扑。
          </p>
        </header>
        <footer class="form-foot">
          <button
            class="sf-btn primary"
            :disabled="prune.busy"
            @click="submitPrune"
          >
            {{ prune.busy ? 'Pruning…' : 'Run PruneOffline →' }}
          </button>
          <span v-if="prune.message" class="sf-chip ok">
            {{ prune.message }}
          </span>
          <span v-if="prune.error" class="sf-chip danger">
            {{ prune.error }}
          </span>
        </footer>
      </div>
    </section>
  </section>
</template>

<style scoped>
/*
 * DemoConsole：左列 3 项 action 导航 + 右列具体表单。
 * Cohere 化要点：side-tab 采用 code/label/hint 三层结构，active 用左黑竖条；
 * form-title 用 display serif 的 h3 风格承担表单的语义重量。
 */

.console-wrap {
  display: grid;
  grid-template-columns: 280px 1fr;
  gap: 16px;
  flex: 1 1 auto;
  min-height: 0;
}

/* ---------- 左侧 actions 导航 ---------- */
.side {
  padding: 18px 16px 16px;
  display: flex;
  flex-direction: column;
  gap: 6px;
}
.side-head {
  margin: 4px 4px 8px;
  color: var(--sf-fg-2);
}
.side-tab {
  display: grid;
  gap: 2px;
  padding: 10px 12px;
  border-radius: var(--sf-r-sm);
  color: var(--sf-fg-1);
  text-align: left;
  background: transparent;
  border: 1px solid transparent;
  position: relative;
  transition: background var(--sf-dur-fast) var(--sf-ease),
    border-color var(--sf-dur-fast) var(--sf-ease),
    color var(--sf-dur-fast) var(--sf-ease);
}
.side-tab:hover {
  background: var(--sf-bg-0);
  border-color: var(--sf-border-1);
  color: var(--sf-fg-0);
}
.side-tab.active {
  background: var(--sf-bg-0);
  border-color: var(--sf-border-2);
  color: var(--sf-fg-0);
}
.side-tab.active::before {
  content: '';
  position: absolute;
  left: -16px;
  top: 8px;
  bottom: 8px;
  width: 2px;
  background: var(--sf-fg-0);
  border-radius: 2px;
}
.side-tab .tab-code {
  color: var(--sf-fg-3);
}
.side-tab.active .tab-code {
  color: var(--sf-accent);
}
.side-tab .tab-label {
  font-weight: 500;
  font-size: 0.95rem;
  color: inherit;
  letter-spacing: -0.1px;
}
.side-tab .tab-hint {
  font-size: 0.75rem;
  color: var(--sf-fg-3);
  line-height: 1.35;
}

.side-help {
  margin-top: auto;
  padding: 14px 16px;
  background: var(--sf-bg-1);
  border-radius: var(--sf-r-md);
  border: 1px solid var(--sf-border-0);
  font-size: 0.82rem;
  line-height: 1.55;
  color: var(--sf-fg-2);
}

/* ---------- 右侧表单主区 ---------- */
.panel {
  padding: 24px 28px 24px;
  min-height: 0;
  overflow: auto;
  display: flex;
  flex-direction: column;
}

.form {
  display: flex;
  flex-direction: column;
  gap: 20px;
}
.form-head {
  display: flex;
  flex-direction: column;
  gap: 6px;
  padding-bottom: 20px;
  border-bottom: 1px solid var(--sf-border-0);
}
.form-title {
  margin: 2px 0 4px;
  font-family: var(--sf-font-display);
  font-weight: 400;
  font-size: 1.75rem;
  line-height: 1.15;
  letter-spacing: -0.4px;
  color: var(--sf-fg-0);
}
.form-lede {
  margin: 0;
  color: var(--sf-fg-2);
  font-size: 0.95rem;
  line-height: 1.55;
  max-width: 56ch;
}

.grid {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 18px;
  align-items: start;
}
.field {
  display: flex;
  flex-direction: column;
  gap: 8px;
}
.field.fullspan {
  grid-column: 1 / -1;
}

/* slider：range + 右侧数值 mono */
.slider-row {
  display: flex;
  align-items: center;
  gap: 12px;
}
.slider-row input[type='range'] {
  flex: 1;
  accent-color: var(--sf-fg-0);
}
.slider-value {
  min-width: 42px;
  text-align: right;
  font-size: 0.85rem;
  color: var(--sf-fg-0);
  font-variant-numeric: tabular-nums;
}

.form-foot {
  display: flex;
  align-items: center;
  gap: 14px;
  flex-wrap: wrap;
  padding-top: 18px;
  border-top: 1px solid var(--sf-border-0);
}

@media (max-width: 1040px) {
  .console-wrap {
    grid-template-columns: 1fr;
  }
  .grid {
    grid-template-columns: 1fr;
  }
}
</style>
