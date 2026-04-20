<script setup lang="ts">
import { computed, ref } from 'vue';
import { useTopologyStore } from '@/stores/topology';
import ForceGraph from './ForceGraph.vue';
import TreeGraph from './TreeGraph.vue';

type ViewMode = 'force' | 'tree';

const topo = useTopologyStore();
const mode = ref<ViewMode>('force');

const summary = computed(() => {
  const online = topo.nodes.filter((n) =>
    String(n.status ?? '')
      .toLowerCase()
      .includes('online'),
  ).length;
  const sleeping = topo.nodes.filter((n) =>
    String(n.status ?? '')
      .toLowerCase()
      .includes('sleep'),
  ).length;
  const offline = topo.nodes.length - online - sleeping;
  const suppEdges = topo.edges.filter((e) => e.supplemental).length;
  return { online, sleeping, offline, suppEdges };
});

function select(uuid: string) {
  topo.select(uuid);
}
</script>

<template>
  <section class="topology-wrap sf-panel">
    <header class="bar">
      <div class="modes">
        <button
          :class="['mode', { active: mode === 'force' }]"
          @click="mode = 'force'"
        >
          力导向视图
        </button>
        <button
          :class="['mode', { active: mode === 'tree' }]"
          @click="mode = 'tree'"
        >
          树状视图
        </button>
      </div>
      <div class="legend">
        <span class="lg ok">● 在线 {{ summary.online }}</span>
        <span class="lg warn">● 睡眠 {{ summary.sleeping }}</span>
        <span class="lg danger">● 离线 {{ summary.offline }}</span>
        <span class="lg info">╌ 补链 {{ summary.suppEdges }}</span>
      </div>
      <div class="actions">
        <button class="sf-btn ghost" @click="topo.refresh()">刷新</button>
      </div>
    </header>

    <div class="graph-area">
      <Transition name="sf-fade-up" mode="out-in">
        <ForceGraph
          v-if="mode === 'force'"
          key="force"
          :nodes="topo.nodes"
          :edges="topo.edges"
          :selected="topo.selectedUUID"
          @select="select"
        />
        <TreeGraph
          v-else
          key="tree"
          :nodes="topo.nodes"
          :edges="topo.edges"
          :selected="topo.selectedUUID"
          @select="select"
        />
      </Transition>

      <div v-if="topo.loading" class="overlay">
        <span class="spinner" />
        <span>加载拓扑快照…</span>
      </div>
      <div v-else-if="!topo.nodes.length" class="overlay sf-muted">
        暂无节点。启动 Flock 或等待 gossip 收敛后再点刷新。
      </div>
      <div v-if="topo.error" class="error sf-chip danger">
        {{ topo.error }}
      </div>
    </div>
  </section>
</template>

<style scoped>
.topology-wrap {
  flex: 1 1 auto;
  display: flex;
  flex-direction: column;
  min-height: 0;
  overflow: hidden;
  padding: 12px 14px 14px;
}
.bar {
  display: flex;
  gap: 12px;
  align-items: center;
  flex-wrap: wrap;
  padding-bottom: 10px;
  border-bottom: 1px solid var(--sf-border-1);
}
.modes {
  display: flex;
  gap: 4px;
  padding: 3px;
  background: var(--sf-bg-1);
  border: 1px solid var(--sf-border-1);
  border-radius: 999px;
}
.mode {
  padding: 6px 14px;
  border-radius: 999px;
  color: var(--sf-fg-2);
  font-size: 0.85rem;
  transition: background var(--sf-dur-fast) var(--sf-ease),
    color var(--sf-dur-fast) var(--sf-ease);
}
.mode:hover {
  color: var(--sf-fg-0);
}
.mode.active {
  background: var(--sf-accent-dim);
  color: var(--sf-accent);
  box-shadow: inset 0 0 0 1px var(--sf-accent-line);
}

.legend {
  display: flex;
  gap: 10px;
  flex-wrap: wrap;
}
.lg {
  font-size: 0.78rem;
  color: var(--sf-fg-2);
}
.lg.ok {
  color: var(--sf-ok);
}
.lg.warn {
  color: var(--sf-warn);
}
.lg.danger {
  color: var(--sf-danger);
}
.lg.info {
  color: var(--sf-info);
}

.actions {
  margin-left: auto;
}

.graph-area {
  position: relative;
  flex: 1 1 auto;
  min-height: 0;
  margin-top: 10px;
  border-radius: var(--sf-r-md);
  background: linear-gradient(
    180deg,
    rgba(8, 10, 14, 0.8),
    rgba(13, 16, 22, 0.65)
  );
  overflow: hidden;
}

.overlay {
  position: absolute;
  inset: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 10px;
  color: var(--sf-fg-1);
  pointer-events: none;
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

.error {
  position: absolute;
  top: 10px;
  right: 10px;
}
</style>
