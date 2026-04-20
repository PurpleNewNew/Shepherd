<script setup lang="ts">
import { computed, ref } from 'vue';
import { useTopologyStore } from '@/stores/topology';

const emit = defineEmits<{ (e: 'jump', tab: string): void }>();

const topo = useTopologyStore();

const filter = ref<string>('');
const visibleNodes = computed(() => {
  const q = filter.value.trim().toLowerCase();
  if (!q) return topo.nodes;
  return topo.nodes.filter((n) =>
    (n.uuid + ' ' + (n.alias ?? '') + ' ' + (n.memo ?? '')).toLowerCase().includes(q),
  );
});

const detail = computed(() => topo.detail);

function selectUUID(uuid: string) {
  topo.select(uuid);
}
</script>

<template>
  <div class="node-detail-wrap">
    <aside class="node-picker sf-panel">
      <header>
        <p class="sf-label sf-label--strong picker-label">Nodes</p>
        <input
          v-model="filter"
          class="sf-input"
          placeholder="搜索 UUID / 别名 / 备注"
        />
      </header>
      <ul>
        <li
          v-for="n in visibleNodes"
          :key="n.uuid"
          :class="['item', { active: topo.selectedUUID === n.uuid }]"
          @click="selectUUID(n.uuid)"
        >
          <div class="item-top">
            <span class="name">{{ n.alias || n.uuid.slice(0, 8) }}</span>
            <span
              class="sf-chip"
              :class="{
                ok: String(n.status ?? '').toLowerCase().includes('online'),
                warn: String(n.status ?? '').toLowerCase().includes('sleep'),
                danger:
                  String(n.status ?? '').toLowerCase().includes('offline') ||
                  String(n.status ?? '').toLowerCase().includes('fail'),
              }"
            >{{ n.status || 'unknown' }}</span>
          </div>
          <div class="item-sub sf-mono sf-muted">{{ n.uuid }}</div>
          <div class="item-tags">
            <span class="sf-chip info" v-if="n.network">net:{{ n.network }}</span>
            <span class="sf-chip" v-if="n.sleep">sleep:{{ n.sleep }}</span>
            <span class="sf-chip" v-if="n.activeStreams">streams:{{ n.activeStreams }}</span>
          </div>
        </li>
      </ul>
    </aside>

    <section class="detail sf-panel">
      <div v-if="!detail && !topo.selectedUUID" class="empty">
        <p class="sf-label">No selection</p>
        <p class="empty-lede">从左侧选择一个节点查看详情</p>
      </div>
      <div v-else-if="topo.detailLoading" class="empty">
        <span class="spinner" />
        <span class="sf-label">Loading node detail…</span>
      </div>
      <div v-else-if="topo.detailError" class="empty sf-chip danger">
        {{ topo.detailError }}
      </div>
      <div v-else-if="detail" class="detail-body">
        <header class="detail-head">
          <div class="title-group">
            <h2>{{ detail.node.alias || detail.node.uuid }}</h2>
            <code class="sf-mono sf-muted">{{ detail.node.uuid }}</code>
          </div>
          <div class="head-chips">
            <span class="sf-chip ok" v-if="String(detail.node.status ?? '').toLowerCase().includes('online')">
              {{ detail.node.status }}
            </span>
            <span class="sf-chip warn" v-else-if="String(detail.node.status ?? '').toLowerCase().includes('sleep')">
              {{ detail.node.status }}
            </span>
            <span class="sf-chip danger" v-else>
              {{ detail.node.status || 'unknown' }}
            </span>
            <span class="sf-chip info" v-if="detail.node.network">net:{{ detail.node.network }}</span>
            <span class="sf-chip" v-if="detail.node.depth !== undefined">depth:{{ detail.node.depth }}</span>
            <span class="sf-chip" v-if="detail.node.memo">memo:{{ detail.node.memo }}</span>
          </div>
        </header>

        <section class="subgrid">
          <div class="card">
            <p class="sf-label sf-label--strong">Sleep profile</p>
            <div v-if="detail.sleep" class="kv">
              <span>sleep_seconds</span><b class="sf-mono">{{ detail.sleep.sleepSeconds ?? '—' }}</b>
              <span>work_seconds</span><b class="sf-mono">{{ detail.sleep.workSeconds ?? '—' }}</b>
              <span>jitter%</span><b class="sf-mono">{{ detail.sleep.jitter ?? '—' }}</b>
              <span>profile</span><b>{{ detail.sleep.profile || '—' }}</b>
              <span>last updated</span><b class="sf-mono">{{ detail.sleep.lastUpdated || '—' }}</b>
              <span>next wake</span><b class="sf-mono">{{ detail.sleep.nextWakeAt || '—' }}</b>
            </div>
            <div v-else class="sf-muted empty-sm">尚未上报 sleep profile</div>
          </div>

          <div class="card">
            <p class="sf-label sf-label--strong">Sessions</p>
            <ul v-if="detail.sessions?.length" class="list">
              <li v-for="(s, i) in detail.sessions" :key="i">
                <div class="row">
                  <span class="sf-chip"
                    :class="{
                      ok: s.status?.toLowerCase().includes('active'),
                      warn: s.status?.toLowerCase().includes('degraded'),
                      danger: s.status?.toLowerCase().includes('fail'),
                    }"
                  >{{ s.status }}</span>
                  <span class="sf-mono">{{ s.remoteAddr || '—' }}</span>
                </div>
                <div class="row sf-muted sf-mono">
                  {{ s.upstream || '—' }} / {{ s.downstream || '—' }} · {{ s.networkId || '—' }}
                </div>
                <div v-if="s.lastError" class="row danger sf-mono">
                  !! {{ s.lastError }}
                </div>
              </li>
            </ul>
            <div v-else class="sf-muted empty-sm">无活跃会话</div>
          </div>

          <div class="card">
            <p class="sf-label sf-label--strong">Streams</p>
            <ul v-if="detail.streams?.length" class="list mono">
              <li v-for="s in detail.streams" :key="s.streamId">
                <span>#{{ s.streamId }}</span>
                <span>{{ s.kind }}{{ s.outbound ? '→' : '←' }}</span>
                <span>seq={{ s.seq }}/ack={{ s.ack }}</span>
                <span>win={{ s.window }}</span>
                <span>pend={{ s.pending }}</span>
              </li>
            </ul>
            <div v-else class="sf-muted empty-sm">暂无活跃流</div>
          </div>

          <div class="card">
            <p class="sf-label sf-label--strong">Pivot listeners</p>
            <ul v-if="detail.pivotListeners?.length" class="list mono">
              <li v-for="l in detail.pivotListeners" :key="l.listenerId">
                <span class="sf-chip info">{{ l.protocol || 'tcp' }}</span>
                <span>{{ l.bind }}</span>
                <span class="sf-muted">{{ l.status }}</span>
              </li>
            </ul>
            <div v-else class="sf-muted empty-sm">此节点未挂接 pivot listener</div>
          </div>
        </section>

        <footer class="detail-foot">
          <button
            class="sf-btn ghost"
            @click="topo.loadDetail(detail.node.uuid)"
          >
            ↻ Re-fetch
          </button>
          <button class="sf-btn" @click="emit('jump', 'timeline')">
            View events for this node →
          </button>
        </footer>
      </div>
    </section>
  </div>
</template>

<style scoped>
/*
 * NodeDetail：左列可搜索节点列表 + 右列主详情卡。
 * Cohere 化的要点：22px sf-panel 容器、白底 card、uppercase 小分区标签、
 * detail 主标题用 display serif 类（Cohere 报纸头条感）。
 */

.node-detail-wrap {
  display: grid;
  grid-template-columns: 300px 1fr;
  gap: 16px;
  flex: 1 1 auto;
  min-height: 0;
}

/* ---------- 左列：节点 picker ---------- */
.node-picker {
  display: flex;
  flex-direction: column;
  min-height: 0;
  padding: 18px 16px 14px;
  gap: 12px;
}
.node-picker header {
  display: flex;
  flex-direction: column;
  gap: 10px;
}
.picker-label {
  margin: 0;
  color: var(--sf-fg-2);
}
.node-picker ul {
  flex: 1 1 auto;
  min-height: 0;
  overflow: auto;
  list-style: none;
  padding: 0;
  margin: 0;
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.item {
  border-radius: var(--sf-r-md);
  padding: 10px 12px;
  border: 1px solid var(--sf-border-0);
  background: var(--sf-bg-0);
  cursor: pointer;
  transition: background var(--sf-dur-fast) var(--sf-ease),
    border-color var(--sf-dur-fast) var(--sf-ease);
}
.item:hover {
  border-color: var(--sf-border-2);
  background: var(--sf-bg-1);
}
.item.active {
  border-color: var(--sf-fg-0);
  background: var(--sf-bg-1);
}
.item-top {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 8px;
}
.item-top .name {
  font-weight: 500;
  color: var(--sf-fg-0);
}
.item-sub {
  font-family: var(--sf-font-mono);
  font-size: 0.72rem;
  margin-top: 4px;
  color: var(--sf-fg-3);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.item-tags {
  margin-top: 6px;
  display: flex;
  gap: 4px;
  flex-wrap: wrap;
}

/* ---------- 右列：主详情 ---------- */
.detail {
  padding: 22px 24px 20px;
  min-height: 0;
  display: flex;
  flex-direction: column;
}
.detail-body {
  display: flex;
  flex-direction: column;
  gap: 20px;
  overflow: auto;
}
.detail-head {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  gap: 20px;
  flex-wrap: wrap;
  padding-bottom: 16px;
  border-bottom: 1px solid var(--sf-border-0);
}
.title-group {
  display: flex;
  flex-direction: column;
  gap: 4px;
}
.title-group h2 {
  margin: 0;
  font-family: var(--sf-font-display);
  font-weight: 400;
  font-size: 1.875rem;
  line-height: 1.1;
  letter-spacing: -0.48px;
  color: var(--sf-fg-0);
}
.title-group code {
  font-size: 0.78rem;
  color: var(--sf-fg-3);
}
.head-chips {
  display: flex;
  gap: 6px;
  flex-wrap: wrap;
  align-items: center;
}

.subgrid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
  gap: 14px;
}
.card {
  padding: 16px 18px;
  background: var(--sf-bg-0);
  border: 1px solid var(--sf-border-1);
  border-radius: var(--sf-r-md);
  display: flex;
  flex-direction: column;
  gap: 10px;
}

.kv {
  display: grid;
  grid-template-columns: 120px 1fr;
  row-gap: 8px;
  column-gap: 12px;
  font-size: 0.85rem;
}
.kv span {
  color: var(--sf-fg-3);
  font-family: var(--sf-font-mono);
  font-size: 0.78rem;
  text-transform: lowercase;
  letter-spacing: 0;
}
.kv b {
  color: var(--sf-fg-0);
  font-weight: 500;
}

.list {
  list-style: none;
  padding: 0;
  margin: 0;
  display: flex;
  flex-direction: column;
  gap: 6px;
  font-size: 0.85rem;
}
.list.mono {
  font-family: var(--sf-font-mono);
  font-size: 0.8rem;
}
.list li {
  padding: 8px 10px;
  border-radius: var(--sf-r-sm);
  background: var(--sf-bg-1);
  border: 1px solid var(--sf-border-0);
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
  color: var(--sf-fg-1);
}
.row {
  display: flex;
  gap: 6px;
  align-items: center;
  width: 100%;
}
.row.danger {
  color: var(--sf-danger);
}

.detail-foot {
  display: flex;
  gap: 10px;
  margin-top: auto;
  padding-top: 14px;
  border-top: 1px solid var(--sf-border-0);
}

/* 占位态 */
.empty,
.empty-sm {
  display: flex;
  align-items: center;
  justify-content: center;
  flex-direction: column;
  gap: 8px;
  padding: 24px;
  color: var(--sf-fg-2);
  height: 100%;
}
.empty-lede {
  margin: 0;
  color: var(--sf-fg-3);
  font-size: 0.9rem;
}
.empty-sm {
  padding: 6px 0;
  height: auto;
  justify-content: flex-start;
  color: var(--sf-fg-3);
  font-size: 0.85rem;
  flex-direction: row;
}

.spinner {
  width: 14px;
  height: 14px;
  border-radius: 50%;
  border: 2px solid var(--sf-border-2);
  border-top-color: var(--sf-accent);
  animation: sp 0.9s linear infinite;
}
@keyframes sp {
  to {
    transform: rotate(360deg);
  }
}

@media (max-width: 1040px) {
  .node-detail-wrap {
    grid-template-columns: 1fr;
  }
}
</style>
