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
      payload: dtn.payload,
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
      <button
        :class="['side-tab', { active: tab === 'dtn' }]"
        @click="tab = 'dtn'"
      >
        <span class="k">DTN</span>
        <span class="v sf-muted">投递一条离线消息</span>
      </button>
      <button
        :class="['side-tab', { active: tab === 'sleep' }]"
        @click="tab = 'sleep'"
      >
        <span class="k">Sleep</span>
        <span class="v sf-muted">调整节点 duty-cycle</span>
      </button>
      <button
        :class="['side-tab', { active: tab === 'prune' }]"
        @click="tab = 'prune'"
      >
        <span class="k">Prune</span>
        <span class="v sf-muted">清理长时间离线节点</span>
      </button>
      <div class="side-help sf-muted">
        演示控制台用于答辩现场触发最核心的三类动作。所有动作都会在事件时间线里产生对应事件。
      </div>
    </aside>

    <section class="panel sf-panel">
      <!-- DTN -->
      <div v-if="tab === 'dtn'" class="form">
        <h3>DTN 投递</h3>
        <p class="sf-muted">
          选择一个目标，发送一条 DTN payload。若目标当前离线，消息会进入 HoldUntil 队列，等下次接触窗口再投递。
        </p>
        <div class="grid">
          <label class="field">
            <span>目标节点</span>
            <select class="sf-select sf-mono" v-model="dtn.target">
              <option value="">（使用当前选中：{{ topo.selectedUUID?.slice(0, 8) || '未选中' }}）</option>
              <option v-for="n in nodeOptions" :key="n.uuid" :value="n.uuid">
                {{ n.label }}
              </option>
            </select>
          </label>
          <label class="field">
            <span>优先级</span>
            <select class="sf-select" v-model="dtn.priority">
              <option value="low">low</option>
              <option value="normal">normal</option>
              <option value="high">high</option>
            </select>
          </label>
          <label class="field">
            <span>TTL (秒)</span>
            <input class="sf-input sf-mono" type="number" min="5"
              :value="dtn.ttlSeconds"
              @input="(e: any) => (dtn.ttlSeconds = Number(e.target.value) || 0)"
            />
          </label>
          <label class="field fullspan">
            <span>Payload</span>
            <textarea class="sf-input sf-mono" rows="3"
              v-model="dtn.payload"
              placeholder="一段文本，会被 UTF-8 编码后入队"
            />
          </label>
        </div>
        <footer>
          <button class="sf-btn primary" :disabled="dtn.busy" @click="submitDTN">
            {{ dtn.busy ? '投递中…' : '投递 DTN' }}
          </button>
          <span v-if="dtn.lastBundleId" class="sf-chip ok">
            bundle: <span class="sf-mono">{{ dtn.lastBundleId.slice(0, 10) }}…</span>
          </span>
          <span v-if="dtn.error" class="sf-chip danger">{{ dtn.error }}</span>
        </footer>
      </div>

      <!-- Sleep -->
      <div v-else-if="tab === 'sleep'" class="form">
        <h3>Sleep 参数</h3>
        <p class="sf-muted">
          调整节点的 sleep/work/jitter 参数，用于演示 duty-cycling 下 DTN 交付时延如何变化。
        </p>
        <div class="grid">
          <label class="field">
            <span>目标节点</span>
            <select class="sf-select sf-mono" v-model="sleep.target">
              <option value="">（使用当前选中：{{ topo.selectedUUID?.slice(0, 8) || '未选中' }}）</option>
              <option v-for="n in nodeOptions" :key="n.uuid" :value="n.uuid">
                {{ n.label }}
              </option>
            </select>
          </label>
          <label class="field">
            <span>sleep_seconds</span>
            <input type="range" min="0" max="60"
              v-model.number="sleep.sleepSeconds" />
            <span class="sf-mono sf-muted">{{ sleep.sleepSeconds }}s</span>
          </label>
          <label class="field">
            <span>work_seconds</span>
            <input type="range" min="0" max="30"
              v-model.number="sleep.workSeconds" />
            <span class="sf-mono sf-muted">{{ sleep.workSeconds }}s</span>
          </label>
          <label class="field">
            <span>jitter %</span>
            <input type="range" min="0" max="60"
              v-model.number="sleep.jitter" />
            <span class="sf-mono sf-muted">{{ sleep.jitter }}%</span>
          </label>
        </div>
        <footer>
          <button class="sf-btn primary" :disabled="sleep.busy" @click="submitSleep">
            {{ sleep.busy ? '发送中…' : '应用到节点' }}
          </button>
          <span v-if="sleep.message" class="sf-chip ok">{{ sleep.message }}</span>
          <span v-if="sleep.error" class="sf-chip danger">{{ sleep.error }}</span>
        </footer>
      </div>

      <!-- Prune -->
      <div v-else class="form">
        <h3>清理离线节点</h3>
        <p class="sf-muted">
          一次性清理拓扑中长时间未上线的节点。常用于演示结束或视图复位。被清理的节点若再次上线会重新加入拓扑。
        </p>
        <footer>
          <button class="sf-btn primary" :disabled="prune.busy" @click="submitPrune">
            {{ prune.busy ? '清理中…' : '执行 PruneOffline' }}
          </button>
          <span v-if="prune.message" class="sf-chip ok">{{ prune.message }}</span>
          <span v-if="prune.error" class="sf-chip danger">{{ prune.error }}</span>
        </footer>
      </div>
    </section>
  </section>
</template>

<style scoped>
.console-wrap {
  display: grid;
  grid-template-columns: 260px 1fr;
  gap: 14px;
  flex: 1 1 auto;
  min-height: 0;
}
.side {
  padding: 12px;
  display: flex;
  flex-direction: column;
  gap: 6px;
}
.side-tab {
  display: flex;
  flex-direction: column;
  align-items: flex-start;
  gap: 2px;
  padding: 10px 12px;
  border-radius: var(--sf-r-md);
  color: var(--sf-fg-1);
  transition: background var(--sf-dur-fast) var(--sf-ease);
  text-align: left;
}
.side-tab:hover {
  background: var(--sf-bg-hover);
}
.side-tab.active {
  background: var(--sf-accent-dim);
  box-shadow: inset 0 0 0 1px var(--sf-accent-line);
  color: var(--sf-fg-0);
}
.side-tab .k {
  font-weight: 500;
}
.side-tab .v {
  font-size: 0.78rem;
}
.side-help {
  margin-top: auto;
  padding: 10px;
  background: var(--sf-bg-1);
  border-radius: var(--sf-r-md);
  border: 1px solid var(--sf-border-1);
  font-size: 0.8rem;
  line-height: 1.5;
}

.panel {
  padding: 18px 22px 18px;
  min-height: 0;
  overflow: auto;
  display: flex;
  flex-direction: column;
}
.form h3 {
  margin: 0 0 6px;
  font-size: 1.1rem;
}
.form p {
  margin: 0 0 18px;
  font-size: 0.9rem;
  line-height: 1.55;
}
.grid {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 14px;
  align-items: start;
}
.field {
  display: flex;
  flex-direction: column;
  gap: 6px;
}
.field.fullspan {
  grid-column: 1 / -1;
}
.field > span {
  font-size: 0.78rem;
  color: var(--sf-fg-2);
  text-transform: uppercase;
  letter-spacing: 0.3px;
}
.field input[type='range'] {
  width: 100%;
  accent-color: var(--sf-accent);
}
.form footer {
  margin-top: 18px;
  display: flex;
  align-items: center;
  gap: 12px;
  flex-wrap: wrap;
  padding-top: 10px;
  border-top: 1px solid var(--sf-border-1);
}

@media (max-width: 900px) {
  .console-wrap {
    grid-template-columns: 1fr;
  }
  .grid {
    grid-template-columns: 1fr;
  }
}
</style>
