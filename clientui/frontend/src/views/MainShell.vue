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

interface TabDef {
  key: TabKey;
  code: string;        // 侧栏的 uppercase code 标签，如 "01 · TOPO"
  label: string;       // 中英对照的主标签
  hint: string;        // caption
}

const tabs: TabDef[] = [
  {
    key: 'topology',
    code: '01 · Topology',
    label: 'Topology',
    hint: 'Gossip 视图 · 力导向 / 树状双展示',
  },
  {
    key: 'node',
    code: '02 · Node',
    label: 'Node Detail',
    hint: '单节点 session / stream / sleep 状态',
  },
  {
    key: 'timeline',
    code: '03 · Timeline',
    label: 'Event Timeline',
    hint: 'Kelpie WatchEvents 实时事件流',
  },
  {
    key: 'console',
    code: '04 · Console',
    label: 'Demo Console',
    hint: '触发 DTN / Sleep / Prune 等答辩动作',
  },
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
        <div class="brand-mark" aria-hidden="true">
          <span class="brand-serif">S</span>
        </div>
        <div class="brand-text">
          <span class="sf-label sf-label--strong">Stockman</span>
          <span class="brand-endpoint sf-mono">{{ connectedDisplay }}</span>
        </div>
      </div>

      <p class="sf-label sidebar-section-label">Views</p>
      <nav class="tabs">
        <button
          v-for="t in tabs"
          :key="t.key"
          :class="['tab', { active: tab === t.key }]"
          @click="tab = t.key"
        >
          <span class="tab-code sf-label">{{ t.code }}</span>
          <span class="tab-label">{{ t.label }}</span>
          <span class="tab-hint">{{ t.hint }}</span>
        </button>
      </nav>

      <div class="sidebar-foot">
        <button class="sf-btn ghost wide" @click="refreshAll">
          ↻ Refresh all
        </button>
        <button class="sf-btn wide" @click="logout">Disconnect</button>
      </div>
    </aside>

    <main class="workspace">
      <header class="workspace-head">
        <div class="crumb-row">
          <span class="sf-chip ok">● Connected</span>
          <span class="sf-label crumb-code">
            {{ tabs.find((t) => t.key === tab)?.code }}
          </span>
          <h2 class="crumb-title">
            {{ tabs.find((t) => t.key === tab)?.label }}
          </h2>
        </div>
        <p class="crumb-hint sf-body-lg">
          {{ tabs.find((t) => t.key === tab)?.hint }}
        </p>

        <div class="stat-strip">
          <StatCard
            label="Nodes"
            :value="stats.nodes"
            :trend="stats.suppActive > 0 ? 'up' : 'flat'"
            :sub="`${stats.edges} edges · ${stats.suppEdges} supplemental`"
          />
          <StatCard
            label="DTN Delivered"
            :value="stats.dtnDelivered"
            accent="ok"
            :sub="`${stats.dtnEnqueued} enqueued · ${stats.dtnFailed} failed`"
          />
          <StatCard
            label="Held bundles"
            :value="stats.dtnHeld"
            accent="warn"
            sub="Waiting for HoldUntil"
          />
          <StatCard
            label="Supplemental"
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
/*
 * MainShell：Cohere enterprise console 风格
 *   - 侧栏：纯白 / Snow 底，uppercase 分区标签 + 无边框 tab 列
 *   - 主区：白底 + stat strip 横排卡片 + tab 主体区
 *   - 无 shadow、无霓虹，靠 typography 与 border 做层次。
 */

.main-shell {
  display: grid;
  grid-template-columns: 260px 1fr;
  height: 100vh;
  width: 100vw;
  overflow: hidden;
  background: var(--sf-bg-0);
  color: var(--sf-fg-1);
}

/* ---------- 侧栏 ---------- */
.sidebar {
  display: flex;
  flex-direction: column;
  gap: 20px;
  padding: 24px 20px 20px;
  background: var(--sf-bg-1);
  border-right: 1px solid var(--sf-border-1);
  min-width: 0;
}

.brand {
  display: flex;
  align-items: center;
  gap: 12px;
  padding-bottom: 20px;
  border-bottom: 1px solid var(--sf-border-1);
}
.brand-mark {
  width: 40px;
  height: 40px;
  border-radius: var(--sf-r-xl); /* 签名 22px */
  background: var(--sf-fg-0);
  display: grid;
  place-items: center;
  flex-shrink: 0;
}
.brand-serif {
  font-family: var(--sf-font-display);
  font-size: 22px;
  color: var(--sf-bg-0);
  line-height: 1;
  transform: translateY(-1px);
  letter-spacing: -0.5px;
}
.brand-text {
  display: flex;
  flex-direction: column;
  gap: 3px;
  min-width: 0;
}
.brand-endpoint {
  font-size: 0.72rem;
  color: var(--sf-fg-3);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.sidebar-section-label {
  margin: 4px 4px 0;
}

/* 侧栏 tab 列：三层结构 code / label / hint。active 态用左边黑竖条 + 加粗 */
.tabs {
  display: flex;
  flex-direction: column;
  gap: 2px;
}
.tab {
  display: grid;
  grid-template-columns: auto;
  grid-auto-rows: auto;
  gap: 2px;
  padding: 10px 12px;
  border-radius: var(--sf-r-sm);
  text-align: left;
  color: var(--sf-fg-1);
  background: transparent;
  border: 1px solid transparent;
  position: relative;
  transition: background var(--sf-dur-fast) var(--sf-ease),
    color var(--sf-dur-fast) var(--sf-ease),
    border-color var(--sf-dur-fast) var(--sf-ease);
}
.tab:hover {
  background: var(--sf-bg-0);
  border-color: var(--sf-border-1);
  color: var(--sf-fg-0);
}
.tab.active {
  background: var(--sf-bg-0);
  border-color: var(--sf-border-2);
  color: var(--sf-fg-0);
}
.tab.active::before {
  content: '';
  position: absolute;
  left: -20px;
  top: 8px;
  bottom: 8px;
  width: 2px;
  background: var(--sf-fg-0);
  border-radius: 2px;
}
.tab-code {
  color: var(--sf-fg-3);
}
.tab.active .tab-code {
  color: var(--sf-accent);
}
.tab-label {
  font-weight: 500;
  font-size: 0.95rem;
  color: inherit;
  letter-spacing: -0.1px;
}
.tab-hint {
  font-size: 0.75rem;
  color: var(--sf-fg-3);
  line-height: 1.35;
}

/* 侧栏底部按钮 */
.sidebar-foot {
  margin-top: auto;
  display: flex;
  flex-direction: column;
  gap: 8px;
  padding-top: 16px;
  border-top: 1px solid var(--sf-border-1);
}
.sf-btn.wide {
  width: 100%;
  justify-content: center;
}

/* ---------- 主区 ---------- */
.workspace {
  display: flex;
  flex-direction: column;
  min-width: 0;
  min-height: 0;
  padding: 32px 40px 24px;
  gap: 24px;
  overflow: hidden;
}

.workspace-head {
  display: flex;
  flex-direction: column;
  gap: 8px;
  padding-bottom: 20px;
  border-bottom: 1px solid var(--sf-border-1);
}
.crumb-row {
  display: flex;
  align-items: center;
  gap: 14px;
  flex-wrap: wrap;
}
.crumb-code {
  color: var(--sf-accent);
}
.crumb-title {
  margin: 0;
  font-family: var(--sf-font-sans);
  font-weight: 400;
  font-size: 2rem;
  line-height: 1.15;
  letter-spacing: -0.32px;
  color: var(--sf-fg-0);
}
.crumb-hint {
  margin: 0;
  color: var(--sf-fg-2);
  font-size: 1rem;
}

.stat-strip {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
  gap: 14px;
  margin-top: 14px;
}

/* tab 主体区 —— 让其内部自己决定布局，这里只占位 */
.tab-body {
  flex: 1 1 auto;
  min-height: 0;
  display: flex;
  flex-direction: column;
  overflow: hidden;
}

/* ---------- 响应式（演示窗口下行） ---------- */
@media (max-width: 1040px) {
  .main-shell {
    grid-template-columns: 220px 1fr;
  }
  .workspace {
    padding: 24px 24px 20px;
  }
  .sidebar {
    padding: 18px 16px 16px;
  }
}
</style>
