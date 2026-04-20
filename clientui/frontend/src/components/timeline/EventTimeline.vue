<script setup lang="ts">
import { computed, ref } from 'vue';
import { useEventsStore } from '@/stores/events';
import { useTopologyStore } from '@/stores/topology';
import type { EventKind, TimelineEvent } from '@/api/types';

const events = useEventsStore();
const topo = useTopologyStore();

interface KindFilter { key: EventKind | 'all'; label: string; }
const kinds: KindFilter[] = [
  { key: 'all', label: '全部' },
  { key: 'node', label: '节点' },
  { key: 'session', label: '会话' },
  { key: 'supplemental', label: '补链' },
  { key: 'sleep', label: '睡眠' },
  { key: 'stream', label: '流' },
  { key: 'log', label: '日志' },
  { key: 'dial', label: '拨号' },
  { key: 'listener', label: '监听' },
];

const activeKind = ref<'all' | EventKind>('all');
const keyword = ref<string>('');
const selectedOnly = ref<boolean>(false);

const filtered = computed<TimelineEvent[]>(() => {
  const kw = keyword.value.trim().toLowerCase();
  return events.events.filter((ev) => {
    if (activeKind.value !== 'all' && ev.kind !== activeKind.value) return false;
    if (selectedOnly.value && topo.selectedUUID) {
      const sel = topo.selectedUUID;
      if (ev.target !== sel && ev.source !== sel) return false;
    }
    if (kw) {
      const hay = `${ev.summary} ${ev.target ?? ''} ${ev.source ?? ''} ${ev.reason ?? ''}`.toLowerCase();
      if (!hay.includes(kw)) return false;
    }
    return true;
  });
});

function fmtTime(ts: string): string {
  if (!ts) return '';
  const d = new Date(ts);
  if (Number.isNaN(d.getTime())) return ts;
  const hh = String(d.getHours()).padStart(2, '0');
  const mm = String(d.getMinutes()).padStart(2, '0');
  const ss = String(d.getSeconds()).padStart(2, '0');
  const ms = String(d.getMilliseconds()).padStart(3, '0');
  return `${hh}:${mm}:${ss}.${ms}`;
}

function kindTone(k: EventKind): string {
  switch (k) {
    case 'node':
      return 'ok';
    case 'session':
      return 'info';
    case 'supplemental':
      return 'info';
    case 'sleep':
      return 'warn';
    case 'stream':
      return 'info';
    case 'log':
      return '';
    case 'dial':
      return 'info';
    case 'listener':
      return '';
    case 'audit':
      return 'warn';
    default:
      return '';
  }
}

function alias(uuid?: string): string {
  if (!uuid) return '';
  const node = topo.nodeMap.get(uuid);
  return node?.alias || uuid.slice(0, 8);
}
</script>

<template>
  <div class="timeline-wrap sf-panel">
    <header class="top">
      <div class="filter-chips">
        <button
          v-for="k in kinds"
          :key="k.key"
          :class="['chip-btn', { active: activeKind === k.key }]"
          @click="activeKind = k.key"
        >
          {{ k.label }}
        </button>
      </div>
      <div class="controls">
        <input
          class="sf-input keyword"
          v-model="keyword"
          placeholder="搜索 summary / uuid"
        />
        <label class="toggle">
          <input type="checkbox" v-model="selectedOnly" />
          仅看选中节点
        </label>
        <button class="sf-btn ghost" @click="events.togglePause()">
          {{ events.paused ? '继续订阅' : '暂停订阅' }}
        </button>
        <button class="sf-btn ghost" @click="events.clear()">清空</button>
      </div>
    </header>

    <ul class="timeline">
      <TransitionGroup name="sf-fade-up">
        <li
          v-for="ev in filtered"
          :key="ev.seq"
          :class="['event', 'kind-' + ev.kind]"
        >
          <div class="dot" :class="kindTone(ev.kind)"></div>
          <div class="body">
            <div class="head">
              <span class="time sf-mono">{{ fmtTime(ev.timestamp) }}</span>
              <span class="sf-chip" :class="kindTone(ev.kind)">
                {{ ev.kind }} · {{ ev.action || '—' }}
              </span>
              <span v-if="ev.target" class="target sf-mono">
                → {{ alias(ev.target) }}
              </span>
              <span v-if="ev.source" class="source sf-mono sf-muted">
                from {{ alias(ev.source) }}
              </span>
            </div>
            <div class="summary">{{ ev.summary || '(空)' }}</div>
            <div v-if="ev.reason" class="reason sf-muted sf-mono">reason: {{ ev.reason }}</div>
            <div v-if="ev.extras && Object.keys(ev.extras).length" class="extras sf-muted">
              <span v-for="(v, k) in ev.extras" :key="k" class="tag">{{ k }}={{ v }}</span>
            </div>
          </div>
        </li>
      </TransitionGroup>
      <li v-if="!filtered.length" class="empty sf-muted">
        没有匹配的事件。可尝试调整过滤条件或等待节点产生新事件。
      </li>
    </ul>
  </div>
</template>

<style scoped>
.timeline-wrap {
  flex: 1 1 auto;
  display: flex;
  flex-direction: column;
  min-height: 0;
  padding: 12px 16px 16px;
}
.top {
  display: flex;
  gap: 12px;
  align-items: center;
  flex-wrap: wrap;
  padding-bottom: 10px;
  border-bottom: 1px solid var(--sf-border-1);
}
.filter-chips {
  display: flex;
  gap: 4px;
  padding: 3px;
  background: var(--sf-bg-1);
  border: 1px solid var(--sf-border-1);
  border-radius: 999px;
  flex-wrap: wrap;
}
.chip-btn {
  padding: 5px 11px;
  border-radius: 999px;
  font-size: 0.82rem;
  color: var(--sf-fg-2);
  transition: background var(--sf-dur-fast), color var(--sf-dur-fast);
}
.chip-btn:hover {
  color: var(--sf-fg-0);
}
.chip-btn.active {
  background: var(--sf-accent-dim);
  color: var(--sf-accent);
  box-shadow: inset 0 0 0 1px var(--sf-accent-line);
}
.controls {
  display: flex;
  gap: 8px;
  margin-left: auto;
  align-items: center;
  flex-wrap: wrap;
}
.keyword {
  width: 240px;
}
.toggle {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  font-size: 0.84rem;
  color: var(--sf-fg-1);
}
.toggle input {
  accent-color: var(--sf-accent);
}

.timeline {
  list-style: none;
  margin: 10px 0 0;
  padding: 0 4px 6px;
  flex: 1 1 auto;
  overflow: auto;
  display: flex;
  flex-direction: column;
  gap: 4px;
}
.event {
  display: grid;
  grid-template-columns: 12px 1fr;
  gap: 12px;
  padding: 8px 10px;
  border-radius: var(--sf-r-md);
  background: var(--sf-bg-1);
  border: 1px solid transparent;
  transition: background var(--sf-dur-fast) var(--sf-ease),
    border-color var(--sf-dur-fast) var(--sf-ease);
}
.event:hover {
  background: var(--sf-bg-2);
  border-color: var(--sf-border-1);
}
.event .dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  margin-top: 8px;
  background: var(--sf-fg-3);
}
.dot.ok {
  background: var(--sf-ok);
}
.dot.warn {
  background: var(--sf-warn);
}
.dot.info {
  background: var(--sf-info);
}
.dot.danger {
  background: var(--sf-danger);
}
.head {
  display: flex;
  gap: 8px;
  align-items: center;
  flex-wrap: wrap;
}
.time {
  font-size: 0.78rem;
  color: var(--sf-fg-2);
}
.target,
.source {
  font-size: 0.82rem;
}
.summary {
  margin-top: 3px;
  color: var(--sf-fg-0);
}
.reason,
.extras {
  margin-top: 3px;
  font-size: 0.78rem;
}
.tag {
  display: inline-block;
  margin-right: 6px;
}
.empty {
  padding: 20px;
  text-align: center;
  background: var(--sf-bg-1);
  border-radius: var(--sf-r-md);
}
</style>
