<script setup lang="ts">
defineProps<{
  label: string;
  value: string | number;
  sub?: string;
  accent?: 'info' | 'ok' | 'warn' | 'danger';
  trend?: 'up' | 'down' | 'flat';
}>();
</script>

<template>
  <div :class="['stat', accent]">
    <div class="top">
      <span class="label">{{ label }}</span>
      <span v-if="trend" :class="['trend', trend]">{{
        trend === 'up' ? '▲' : trend === 'down' ? '▼' : '—'
      }}</span>
    </div>
    <div class="value sf-mono">{{ value }}</div>
    <div v-if="sub" class="sub sf-muted">{{ sub }}</div>
  </div>
</template>

<style scoped>
.stat {
  padding: 10px 14px;
  border-radius: var(--sf-r-md);
  background: var(--sf-bg-2);
  border: 1px solid var(--sf-border-1);
  min-width: 0;
  transition: border-color var(--sf-dur-fast) var(--sf-ease),
    background var(--sf-dur-fast) var(--sf-ease);
}
.stat:hover {
  border-color: var(--sf-border-2);
}
.top {
  display: flex;
  justify-content: space-between;
  align-items: center;
}
.label {
  font-size: 0.72rem;
  letter-spacing: 0.3px;
  color: var(--sf-fg-2);
  text-transform: uppercase;
}
.value {
  font-size: 1.5rem;
  line-height: 1.15;
  margin-top: 4px;
  letter-spacing: 0.4px;
  color: var(--sf-fg-0);
}
.sub {
  font-size: 0.78rem;
  margin-top: 4px;
}
.stat.ok .value {
  color: var(--sf-ok);
}
.stat.warn .value {
  color: var(--sf-warn);
}
.stat.info .value {
  color: var(--sf-info);
}
.stat.danger .value {
  color: var(--sf-danger);
}

.trend {
  font-size: 0.7rem;
  padding: 1px 6px;
  border-radius: 999px;
  background: var(--sf-bg-3);
  color: var(--sf-fg-2);
}
.trend.up {
  color: var(--sf-ok);
}
.trend.down {
  color: var(--sf-danger);
}
</style>
