<script setup lang="ts">
import { computed, ref } from 'vue';
import { useEventsStore } from '@/stores/events';
import { useTopologyStore } from '@/stores/topology';
import type { EventKind, TimelineEvent } from '@/api/types';

const events = useEventsStore();
const topo = useTopologyStore();

interface KindFilter { key: EventKind | 'all'; label: string; }
const kinds: KindFilter[] = [
  { key: 'all', label: 'All' },
  { key: 'node', label: 'Node' },
  { key: 'session', label: 'Session' },
  { key: 'supplemental', label: 'Supp' },
  { key: 'sleep', label: 'Sleep' },
  { key: 'stream', label: 'Stream' },
  { key: 'log', label: 'Log' },
  { key: 'dial', label: 'Dial' },
  { key: 'listener', label: 'Listener' },
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
      <div class="filter-chips" role="tablist" aria-label="Event kind filter">
        <button
          v-for="k in kinds"
          :key="k.key"
          :class="['chip-btn', { active: activeKind === k.key }]"
          role="tab"
          :aria-selected="activeKind === k.key"
          @click="activeKind = k.key"
        >
          {{ k.label }}
        </button>
      </div>
      <div class="controls">
        <input
          class="sf-input keyword"
          v-model="keyword"
          placeholder="Search summary / uuid"
        />
        <label class="toggle">
          <input type="checkbox" v-model="selectedOnly" />
          <span>Selected only</span>
        </label>
        <button class="sf-btn ghost" @click="events.togglePause()">
          {{ events.paused ? '▶ Resume' : '⏸ Pause' }}
        </button>
        <button class="sf-btn ghost" @click="events.clear()">Clear</button>
      </div>
    </header>

    <ul class="timeline">
      <TransitionGroup name="sf-fade-up">
        <li
          v-for="ev in filtered"
          :key="ev.seq"
          :class="['event', 'kind-' + ev.kind, kindTone(ev.kind)]"
        >
          <div class="rail" />
          <div class="body">
            <div class="head">
              <span class="time sf-mono">{{ fmtTime(ev.timestamp) }}</span>
              <span class="sf-chip" :class="kindTone(ev.kind)">
                {{ ev.kind }} · {{ ev.action || '—' }}
              </span>
              <span v-if="ev.target" class="target sf-mono">
                → {{ alias(ev.target) }}
              </span>
              <span v-if="ev.source" class="source sf-mono">
                from {{ alias(ev.source) }}
              </span>
            </div>
            <div class="summary">{{ ev.summary || '—' }}</div>
            <div v-if="ev.reason" class="reason sf-mono">
              reason: {{ ev.reason }}
            </div>
            <div
              v-if="ev.extras && Object.keys(ev.extras).length"
              class="extras"
            >
              <span v-for="(v, k) in ev.extras" :key="k" class="tag">
                <b>{{ k }}</b>={{ v }}
              </span>
            </div>
          </div>
        </li>
      </TransitionGroup>
      <li v-if="!filtered.length" class="empty">
        <p class="sf-label sf-label--strong">No matching events</p>
        <p class="empty-lede">
          调整过滤条件，或等待节点产生新事件；用 Demo Console 可以手工触发一个。
        </p>
      </li>
    </ul>
  </div>
</template>

<style scoped>
/*
 * EventTimeline：白底 22px 卡，内部事件行走"左侧颜色 rail + body"布局。
 * rail 颜色是语义色（info/ok/warn/danger），是整条记录的唯一彩色元素；
 * body 所有文字走黑灰梯度，避免信息拥挤时色彩过载。
 */

.timeline-wrap {
  flex: 1 1 auto;
  display: flex;
  flex-direction: column;
  min-height: 0;
  padding: 20px 24px 24px;
}

.top {
  display: flex;
  gap: 16px;
  align-items: center;
  flex-wrap: wrap;
  padding-bottom: 16px;
  border-bottom: 1px solid var(--sf-border-0);
}

/* 过滤器 segmented pill */
.filter-chips {
  display: flex;
  gap: 2px;
  padding: 3px;
  background: var(--sf-bg-1);
  border: 1px solid var(--sf-border-1);
  border-radius: var(--sf-r-pill);
  flex-wrap: wrap;
}
.chip-btn {
  padding: 6px 14px;
  border-radius: var(--sf-r-pill);
  font-size: 0.78rem;
  font-weight: 500;
  color: var(--sf-fg-2);
  transition: background var(--sf-dur-fast), color var(--sf-dur-fast);
  letter-spacing: 0;
}
.chip-btn:hover {
  color: var(--sf-fg-0);
}
.chip-btn.active {
  background: var(--sf-fg-0);
  color: var(--sf-bg-0);
}

.controls {
  display: flex;
  gap: 10px;
  margin-left: auto;
  align-items: center;
  flex-wrap: wrap;
}
.keyword {
  width: 260px;
}
.toggle {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  font-size: 0.85rem;
  color: var(--sf-fg-1);
}
.toggle input {
  accent-color: var(--sf-fg-0);
  width: 14px;
  height: 14px;
}

/* timeline 主体 */
.timeline {
  list-style: none;
  margin: 16px 0 0;
  padding: 0 4px 6px;
  flex: 1 1 auto;
  overflow: auto;
  display: flex;
  flex-direction: column;
  gap: 8px;
}

/* 单条事件：[left rail] + [body] */
.event {
  display: grid;
  grid-template-columns: 3px 1fr;
  gap: 14px;
  padding: 12px 14px 12px 10px;
  border-radius: var(--sf-r-md);
  background: var(--sf-bg-0);
  border: 1px solid var(--sf-border-0);
  transition: background var(--sf-dur-fast) var(--sf-ease),
    border-color var(--sf-dur-fast) var(--sf-ease);
}
.event:hover {
  background: var(--sf-bg-1);
  border-color: var(--sf-border-1);
}
.rail {
  width: 3px;
  border-radius: 2px;
  background: var(--sf-border-2);
}
.event.ok .rail {
  background: var(--sf-ok);
}
.event.warn .rail {
  background: var(--sf-warn);
}
.event.info .rail {
  background: var(--sf-accent);
}
.event.danger .rail {
  background: var(--sf-danger);
}

.head {
  display: flex;
  gap: 10px;
  align-items: center;
  flex-wrap: wrap;
}
.time {
  font-family: var(--sf-font-mono);
  font-size: 0.72rem;
  letter-spacing: 0.16px;
  color: var(--sf-fg-3);
  text-transform: uppercase;
}
.target,
.source {
  font-family: var(--sf-font-mono);
  font-size: 0.78rem;
  color: var(--sf-fg-1);
}
.source {
  color: var(--sf-fg-3);
}
.summary {
  margin-top: 4px;
  color: var(--sf-fg-0);
  font-size: 0.9rem;
  line-height: 1.5;
}
.reason {
  margin-top: 4px;
  font-size: 0.75rem;
  color: var(--sf-fg-3);
}
.extras {
  margin-top: 6px;
  font-family: var(--sf-font-mono);
  font-size: 0.72rem;
  color: var(--sf-fg-3);
  display: flex;
  gap: 10px;
  flex-wrap: wrap;
}
.extras .tag b {
  color: var(--sf-fg-1);
  font-weight: 500;
}

.empty {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: 4px;
  padding: 40px 24px;
  background: var(--sf-bg-1);
  border-radius: var(--sf-r-md);
  border: 1px dashed var(--sf-border-2);
  color: var(--sf-fg-2);
  text-align: center;
}
.empty-lede {
  margin: 0;
  font-size: 0.9rem;
  color: var(--sf-fg-3);
}
</style>
