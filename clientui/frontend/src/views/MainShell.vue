<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref } from 'vue';
import { useConnectionStore } from '@/stores/connection';
import { useTopologyStore } from '@/stores/topology';
import { useEventsStore } from '@/stores/events';
import { useMetricsStore } from '@/stores/metrics';
import TopologyPanel from '@/components/topology/TopologyPanel.vue';
import NodeDetail from '@/components/node/NodeDetail.vue';
import EventTimeline from '@/components/timeline/EventTimeline.vue';
import DemoConsole from '@/components/console/DemoConsole.vue';
import StatCard from '@/components/base/StatCard.vue';

type TabKey = 'topology' | 'node' | 'timeline' | 'console';

const conn = useConnectionStore();
const topo = useTopologyStore();
const events = useEventsStore();
const metrics = useMetricsStore();

const tab = ref<TabKey>('topology');

const tabs: { key: TabKey; label: string; hint: string }[] = [
  { key: 'topology', label: '拓扑总览', hint: 'Gossip + 补链实时视图' },
  { key: 'node', label: '节点详情', hint: '单节点 session/stream/sleep' },
  { key: 'timeline', label: '事件时间线', hint: 'Kelpie WatchEvents 流' },
  { key: 'console', label: '演示控制台', hint: 'DTN / Sleep / Prune 动作' },
];

const connectedDisplay = computed(() => {
  if (!conn.isConnected) return '';
  const prefix = conn.useTLS ? 'TLS ' : '';
  return `${prefix}${conn.endpoint}`;
});

const stats = computed(() => {
  const dtn = metrics.bundle.dtn;
  const supp = metrics.bundle.supplemental;
  return {
    nodes: topo.nodes.length,
    edges: topo.edges.length,
    suppEdges: topo.edges.filter((e) => e.supplemental).length,
    dtnEnqueued: dtn.enqueued,
    dtnDelivered: dtn.delivered,
    dtnFailed: dtn.failed,
    dtnHeld: dtn.global.held,
    suppActive: supp.activeLinks,
    suppDispatched: supp.dispatched,
  };
});

onMounted(async () => {
  await topo.refresh();
  events.bootstrap();
  metrics.start(3000);
});

onBeforeUnmount(() => {
  metrics.stop();
  events.dispose();
});

async function refreshAll() {
  await Promise.all([topo.refresh(), metrics.refresh()]);
}

async function logout() {
  await conn.disconnect();
  topo.clear();
  events.dispose();
  metrics.reset();
}
</script>

<template>
  <section class="main-shell">
    <aside class="sidebar">
      <div class="brand">
        <div class="mark">S</div>
        <div>
          <div class="bname">Stockman</div>
          <div class="bsub sf-muted sf-mono">{{ connectedDisplay }}</div>
        </div>
      </div>
      <nav class="tabs">
        <button
          v-for="t in tabs"
          :key="t.key"
          :class="['tab', { active: tab === t.key }]"
          @click="tab = t.key"
        >
          <span class="tab-label">{{ t.label }}</span>
          <span class="tab-hint sf-muted">{{ t.hint }}</span>
        </button>
      </nav>
      <div class="sidebar-foot">
        <button class="sf-btn ghost wide" @click="refreshAll">
          手动刷新
        </button>
        <button class="sf-btn danger wide" @click="logout">断开连接</button>
      </div>
    </aside>

    <main class="workspace">
      <header class="workspace-head">
        <div class="crumbs">
          <span class="sf-chip ok">在线</span>
          <h2>{{ tabs.find((t) => t.key === tab)?.label }}</h2>
          <span class="sf-muted">
            {{ tabs.find((t) => t.key === tab)?.hint }}
          </span>
        </div>
        <div class="stat-strip">
          <StatCard
            label="节点"
            :value="stats.nodes"
            :trend="stats.suppActive > 0 ? 'up' : 'flat'"
            :sub="`${stats.edges} 条边（${stats.suppEdges} 补链）`"
          />
          <StatCard
            label="DTN 投递"
            :value="stats.dtnDelivered"
            accent="ok"
            :sub="`入队 ${stats.dtnEnqueued} · 失败 ${stats.dtnFailed}`"
          />
          <StatCard
            label="Held"
            :value="stats.dtnHeld"
            accent="warn"
            sub="等待 HoldUntil 的 bundle"
          />
          <StatCard
            label="补链"
            :value="stats.suppDispatched"
            accent="info"
            :sub="`active=${stats.suppActive}`"
          />
        </div>
      </header>

      <Transition name="sf-fade-up" mode="out-in">
        <section class="tab-body" :key="tab">
          <TopologyPanel v-if="tab === 'topology'" />
          <NodeDetail
            v-else-if="tab === 'node'"
            @jump="(k: string) => (tab = k as TabKey)"
          />
          <EventTimeline v-else-if="tab === 'timeline'" />
          <DemoConsole v-else-if="tab === 'console'" />
        </section>
      </Transition>
    </main>
  </section>
</template>

<style scoped>
.main-shell {
  display: grid;
  grid-template-columns: 240px 1fr;
  height: 100vh;
  width: 100vw;
  overflow: hidden;
}

.sidebar {
  display: flex;
  flex-direction: column;
  gap: 18px;
  padding: 18px 14px 18px;
  border-right: 1px solid var(--sf-border-1);
  background: rgba(12, 15, 20, 0.6);
  backdrop-filter: blur(20px);
}
.brand {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 0 6px 10px;
  border-bottom: 1px solid var(--sf-border-1);
}
.brand .mark {
  width: 34px;
  height: 34px;
  border-radius: 10px;
  background: linear-gradient(135deg, #3d7bff 0%, #7ab7ff 60%, #c1a1ff 100%);
  display: grid;
  place-items: center;
  color: #0b0d11;
  font-weight: 700;
}
.bname {
  font-weight: 600;
  letter-spacing: 0.3px;
}
.bsub {
  font-size: 0.72rem;
  margin-top: 2px;
}

.tabs {
  display: flex;
  flex-direction: column;
  gap: 4px;
}
.tab {
  display: flex;
  flex-direction: column;
  align-items: flex-start;
  gap: 2px;
  padding: 10px 12px;
  border-radius: var(--sf-r-md);
  text-align: left;
  color: var(--sf-fg-1);
  transition: background var(--sf-dur-fast) var(--sf-ease),
    color var(--sf-dur-fast) var(--sf-ease);
}
.tab:hover {
  background: var(--sf-bg-hover);
  color: var(--sf-fg-0);
}
.tab.active {
  background: var(--sf-accent-dim);
  color: var(--sf-fg-0);
  box-shadow: inset 0 0 0 1px var(--sf-accent-line);
}
.tab-label {
  font-weight: 500;
}
.tab-hint {
  font-size: 0.74rem;
}

.sidebar-foot {
  margin-top: auto;
  display: flex;
  flex-direction: column;
  gap: 8px;
}
.sf-btn.wide {
  width: 100%;
  justify-content: center;
}

.workspace {
  display: flex;
  flex-direction: column;
  min-width: 0;
  min-height: 0;
  padding: 18px 22px 20px;
  gap: 14px;
  overflow: hidden;
}
.workspace-head {
  display: flex;
  flex-direction: column;
  gap: 10px;
}
.crumbs {
  display: flex;
  align-items: center;
  gap: 10px;
}
.crumbs h2 {
  margin: 0;
  font-size: 1.1rem;
  letter-spacing: 0.3px;
}
.stat-strip {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
  gap: 10px;
}

.tab-body {
  flex: 1 1 auto;
  min-height: 0;
  display: flex;
  flex-direction: column;
  overflow: hidden;
}
</style>
