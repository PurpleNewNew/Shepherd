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
      <div class="modes" role="tablist" aria-label="Topology view mode">
        <button
          :class="['mode', { active: mode === 'force' }]"
          role="tab"
          :aria-selected="mode === 'force'"
          @click="mode = 'force'"
        >
          Force
        </button>
        <button
          :class="['mode', { active: mode === 'tree' }]"
          role="tab"
          :aria-selected="mode === 'tree'"
          @click="mode = 'tree'"
        >
          Tree
        </button>
      </div>

      <div class="legend" aria-label="Legend">
        <span class="lg"><span class="dot online" />Online · {{ summary.online }}</span>
        <span class="lg"><span class="dot sleep" />Sleep · {{ summary.sleeping }}</span>
        <span class="lg"><span class="dot off" />Offline · {{ summary.offline }}</span>
        <span class="lg"><span class="dash" />Supplemental · {{ summary.suppEdges }}</span>
      </div>

      <div class="actions">
        <button class="sf-btn ghost" @click="topo.refresh()">↻ Refresh</button>
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
        <span class="sf-label">Loading snapshot…</span>
      </div>
      <div v-else-if="!topo.nodes.length" class="overlay empty">
        <span class="sf-label sf-label--strong">No nodes yet</span>
        <span class="empty-lede">
          启动 Flock 或等待 gossip 收敛后再点 Refresh。
        </span>
      </div>
      <div v-if="topo.error" class="error sf-chip danger">
        {{ topo.error }}
      </div>
    </div>
  </section>
</template>

<style scoped>
/*
 * TopologyPanel：Cohere 风格白底卡。
 *   - 22px 签名圆角容器；
 *   - 工具条 = pill segmented + legend dots + ghost refresh；
 *   - 图表区域 = Snow 底（#fafafa），比白更浅一层以和白卡分层，不用深色 gradient。
 */

.topology-wrap {
  flex: 1 1 auto;
  display: flex;
  flex-direction: column;
  min-height: 0;
  overflow: hidden;
  padding: 20px 24px 24px;
}

.bar {
  display: flex;
  gap: 16px;
  align-items: center;
  flex-wrap: wrap;
  padding-bottom: 16px;
  border-bottom: 1px solid var(--sf-border-0);
}

/* segmented control */
.modes {
  display: flex;
  gap: 2px;
  padding: 3px;
  background: var(--sf-bg-1);
  border: 1px solid var(--sf-border-1);
  border-radius: var(--sf-r-pill);
}
.mode {
  padding: 6px 18px;
  border-radius: var(--sf-r-pill);
  color: var(--sf-fg-2);
  font-size: 0.85rem;
  font-weight: 500;
  letter-spacing: 0;
  transition: background var(--sf-dur-fast) var(--sf-ease),
    color var(--sf-dur-fast) var(--sf-ease);
}
.mode:hover {
  color: var(--sf-fg-0);
}
.mode.active {
  background: var(--sf-bg-0);
  color: var(--sf-fg-0);
  box-shadow: inset 0 0 0 1px var(--sf-border-2);
}

/* legend */
.legend {
  display: flex;
  gap: 18px;
  flex-wrap: wrap;
  align-items: center;
}
.lg {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  font-family: var(--sf-font-mono);
  font-size: 0.72rem;
  letter-spacing: 0.16px;
  text-transform: uppercase;
  color: var(--sf-fg-2);
}
.dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background: var(--sf-fg-3);
  display: inline-block;
}
.dot.online {
  background: #1863dc;
}
.dot.sleep {
  background: #b5760f;
}
.dot.off {
  background: #93939f;
}
.dash {
  width: 14px;
  height: 2px;
  border-top: 1.5px dashed var(--sf-focus-purple);
  display: inline-block;
}

.actions {
  margin-left: auto;
}

/* graph area：使用 Snow 背景区分拓扑画布与卡片主面。 */
.graph-area {
  position: relative;
  flex: 1 1 auto;
  min-height: 0;
  margin-top: 16px;
  border-radius: var(--sf-r-md);
  background:
    radial-gradient(
      1200px 600px at 50% 0%,
      rgba(24, 99, 220, 0.04),
      transparent 60%
    ),
    var(--sf-bg-1);
  border: 1px solid var(--sf-border-0);
  overflow: hidden;
}

.overlay {
  position: absolute;
  inset: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  flex-direction: column;
  gap: 10px;
  color: var(--sf-fg-2);
  pointer-events: none;
}
.overlay.empty {
  color: var(--sf-fg-2);
}
.empty-lede {
  font-size: 0.875rem;
  color: var(--sf-fg-3);
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

.error {
  position: absolute;
  top: 14px;
  right: 14px;
}
</style>
