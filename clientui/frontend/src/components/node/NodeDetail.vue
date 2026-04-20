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
        <h3>节点列表</h3>
        <input v-model="filter" class="sf-input" placeholder="搜索 UUID / 别名 / 备注" />
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
      <div v-if="!detail && !topo.selectedUUID" class="empty sf-muted">
        从左侧选择一个节点查看详情
      </div>
      <div v-else-if="topo.detailLoading" class="empty">
        <span class="spinner" /> 正在加载节点详情…
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
            <h4>Sleep Profile</h4>
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
            <h4>Sessions</h4>
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
            <h4>Streams</h4>
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
            <h4>Pivot Listeners</h4>
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
          <button class="sf-btn ghost" @click="topo.loadDetail(detail.node.uuid)">重新拉取</button>
          <button class="sf-btn" @click="emit('jump', 'timeline')">查看此节点相关事件</button>
        </footer>
      </div>
    </section>
  </div>
</template>

<style scoped>
.node-detail-wrap {
  display: grid;
  grid-template-columns: 280px 1fr;
  gap: 14px;
  flex: 1 1 auto;
  min-height: 0;
}

.node-picker {
  display: flex;
  flex-direction: column;
  min-height: 0;
  padding: 12px;
  gap: 10px;
}
.node-picker header {
  display: flex;
  flex-direction: column;
  gap: 8px;
}
.node-picker h3 {
  margin: 0;
  font-size: 1rem;
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
  padding: 8px 10px;
  border: 1px solid var(--sf-border-1);
  background: var(--sf-bg-1);
  cursor: pointer;
  transition: background var(--sf-dur-fast) var(--sf-ease),
    border-color var(--sf-dur-fast) var(--sf-ease);
}
.item:hover {
  border-color: var(--sf-accent-line);
  background: var(--sf-bg-2);
}
.item.active {
  border-color: var(--sf-accent);
  box-shadow: 0 0 0 2px var(--sf-accent-dim);
}
.item-top {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 8px;
}
.item-top .name {
  font-weight: 500;
}
.item-sub {
  font-size: 0.74rem;
  margin-top: 3px;
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

.detail {
  padding: 16px 18px 18px;
  min-height: 0;
  display: flex;
  flex-direction: column;
}
.detail-body {
  display: flex;
  flex-direction: column;
  gap: 14px;
  overflow: auto;
}
.detail-head {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  gap: 20px;
  flex-wrap: wrap;
  padding-bottom: 10px;
  border-bottom: 1px solid var(--sf-border-1);
}
.title-group h2 {
  margin: 0;
  font-size: 1.25rem;
}
.title-group code {
  font-size: 0.78rem;
}
.head-chips {
  display: flex;
  gap: 6px;
  flex-wrap: wrap;
  align-items: center;
}
.subgrid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
  gap: 12px;
}
.card {
  padding: 12px 14px;
  background: var(--sf-bg-1);
  border: 1px solid var(--sf-border-1);
  border-radius: var(--sf-r-md);
}
.card h4 {
  margin: 0 0 8px;
  font-size: 0.85rem;
  letter-spacing: 0.3px;
  color: var(--sf-fg-2);
  text-transform: uppercase;
}
.kv {
  display: grid;
  grid-template-columns: 128px 1fr;
  row-gap: 6px;
  column-gap: 10px;
  font-size: 0.85rem;
}
.kv span {
  color: var(--sf-fg-3);
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
  font-size: 0.82rem;
}
.list li {
  padding: 6px 8px;
  border-radius: var(--sf-r-sm);
  background: var(--sf-bg-2);
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
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
  padding-top: 10px;
  border-top: 1px solid var(--sf-border-1);
}

.empty,
.empty-sm {
  display: flex;
  align-items: center;
  justify-content: center;
  height: 100%;
  color: var(--sf-fg-2);
  gap: 8px;
  padding: 14px;
}
.empty-sm {
  padding: 6px;
  height: auto;
  justify-content: flex-start;
  font-size: 0.85rem;
}
.spinner {
  width: 14px;
  height: 14px;
  border-radius: 50%;
  border: 2px solid rgba(255, 255, 255, 0.2);
  border-top-color: var(--sf-accent);
  animation: sp 0.9s linear infinite;
}
@keyframes sp {
  to {
    transform: rotate(360deg);
  }
}

@media (max-width: 960px) {
  .node-detail-wrap {
    grid-template-columns: 1fr;
  }
}
</style>
