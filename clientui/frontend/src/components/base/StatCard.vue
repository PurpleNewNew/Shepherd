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
      <span class="sf-label sf-label--strong stat-label">{{ label }}</span>
      <span v-if="trend" :class="['trend', trend]" aria-hidden="true">
        {{ trend === 'up' ? '↗' : trend === 'down' ? '↘' : '—' }}
      </span>
    </div>
    <div class="value">{{ value }}</div>
    <div v-if="sub" class="sub sf-small sf-muted">{{ sub }}</div>
  </div>
</template>

<style scoped>
/*
 * StatCard：Cohere 式白底 22px 卡片。
 *   - 数字使用 display serif 变体（IBM Plex Serif 400）营造"报纸头条感"；
 *   - 数字保持黑色（fg-0），语义色仅通过 accent bar 体现；
 *   - hover 时黑边强调，没有 shadow。
 */

.stat {
  position: relative;
  padding: 18px 20px 16px;
  border-radius: var(--sf-r-xl); /* 签名 22px */
  background: var(--sf-bg-2);
  border: 1px solid var(--sf-border-1);
  min-width: 0;
  display: flex;
  flex-direction: column;
  gap: 6px;
  transition: border-color var(--sf-dur-fast) var(--sf-ease),
    transform var(--sf-dur-fast) var(--sf-ease);
  overflow: hidden;
}
.stat:hover {
  border-color: var(--sf-fg-0);
}

/* 顶部左侧的 accent 立方块：4px 宽，代表语义色。 */
.stat::before {
  content: '';
  position: absolute;
  left: 0;
  top: 18px;
  bottom: 18px;
  width: 3px;
  border-radius: 0 3px 3px 0;
  background: transparent;
  transition: background var(--sf-dur-fast) var(--sf-ease);
}
.stat.info::before {
  background: var(--sf-accent);
}
.stat.ok::before {
  background: var(--sf-ok);
}
.stat.warn::before {
  background: var(--sf-warn);
}
.stat.danger::before {
  background: var(--sf-danger);
}

.top {
  display: flex;
  justify-content: space-between;
  align-items: center;
}
.stat-label {
  color: var(--sf-fg-2);
}
.value {
  font-family: var(--sf-font-display);
  font-weight: 400;
  font-size: 2.25rem;       /* 36px，display serif */
  line-height: 1.05;
  letter-spacing: -0.8px;
  color: var(--sf-fg-0);
}
.sub {
  margin-top: 2px;
  font-family: var(--sf-font-sans);
  letter-spacing: 0;
  text-transform: none;
}

.trend {
  font-family: var(--sf-font-mono);
  font-size: 0.72rem;
  padding: 2px 8px;
  border-radius: var(--sf-r-pill);
  background: var(--sf-bg-1);
  border: 1px solid var(--sf-border-1);
  color: var(--sf-fg-3);
  line-height: 1;
}
.trend.up {
  color: var(--sf-ok);
  border-color: rgba(42, 127, 85, 0.24);
}
.trend.down {
  color: var(--sf-danger);
  border-color: rgba(179, 0, 0, 0.24);
}
</style>
