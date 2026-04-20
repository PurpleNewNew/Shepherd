<script setup lang="ts">
import { onBeforeUnmount, ref, shallowRef, watch } from 'vue';
import {
  forceCenter,
  forceCollide,
  forceLink,
  forceManyBody,
  forceSimulation,
  type Simulation,
  type SimulationNodeDatum,
  type SimulationLinkDatum,
} from 'd3-force';
import { select } from 'd3-selection';
import { zoom, zoomIdentity, type ZoomBehavior } from 'd3-zoom';
import type { EdgeSummary, NodeSummary } from '@/api/types';
import {
  aliasOf,
  nodeRadius,
  statusCategory,
  statusColor,
} from './graphUtils';

interface Props {
  nodes: NodeSummary[];
  edges: EdgeSummary[];
  selected: string;
}
const props = defineProps<Props>();
const emit = defineEmits<{
  (e: 'select', uuid: string): void;
}>();

interface ForceNode extends SimulationNodeDatum {
  uuid: string;
  data: NodeSummary;
}
interface ForceLink extends SimulationLinkDatum<ForceNode> {
  source: string | ForceNode;
  target: string | ForceNode;
  supplemental: boolean;
}

const containerRef = ref<HTMLElement | null>(null);
const svgRef = ref<SVGSVGElement | null>(null);
const groupRef = ref<SVGGElement | null>(null);
const simulation = shallowRef<Simulation<ForceNode, ForceLink> | null>(null);
// 节点/边仍然通过 Vue 的 v-for 负责 enter/exit，但位置属性 x/y/x1/x2...
// 由 simulation.on('tick') 直接 setAttribute 更新，避免 Vue diff 开销。
const forceNodes = shallowRef<ForceNode[]>([]);
const forceLinks = shallowRef<ForceLink[]>([]);
const zoomBehavior = shallowRef<ZoomBehavior<SVGSVGElement, unknown> | null>(
  null,
);
const transform = ref<{ x: number; y: number; k: number }>({
  x: 0,
  y: 0,
  k: 1,
});
const dimensions = ref<{ w: number; h: number }>({ w: 800, h: 600 });

/**
 * 每一帧由 d3 simulation 触发，直接 DOM 操作节点和边的坐标，
 * 绕过 Vue 的响应式 diff。rAF 节流避免 tick 超过显示刷新率做无用功。
 */
let rafHandle = 0;
function scheduleTickRender() {
  if (rafHandle) return;
  rafHandle = requestAnimationFrame(() => {
    rafHandle = 0;
    tickRender();
  });
}
function tickRender() {
  const root = groupRef.value;
  if (!root) return;

  // 用 data-uuid / data-idx 作为身份索引，无需 d3.data().join()。
  const nodeByUuid = new Map<string, ForceNode>();
  for (const n of forceNodes.value) nodeByUuid.set(n.uuid, n);

  const gs = root.querySelectorAll<SVGGElement>('g.node');
  for (let i = 0; i < gs.length; i++) {
    const el = gs[i];
    const uuid = el.dataset.uuid;
    if (!uuid) continue;
    const n = nodeByUuid.get(uuid);
    if (!n) continue;
    const x = n.x ?? 0;
    const y = n.y ?? 0;
    // 用 transform attr 而不是 translate：SVG 的 transform 属性对位置变更很便宜，
    // 不会触发 style recalc。
    el.setAttribute('transform', `translate(${x},${y})`);
  }

  const lines = root.querySelectorAll<SVGLineElement>('line.edge');
  const links = forceLinks.value;
  for (let i = 0; i < lines.length; i++) {
    const el = lines[i];
    const idx = Number(el.dataset.idx);
    const link = links[idx];
    if (!link) continue;
    const s = typeof link.source === 'string' ? null : link.source;
    const t = typeof link.target === 'string' ? null : link.target;
    el.setAttribute('x1', String(s?.x ?? 0));
    el.setAttribute('y1', String(s?.y ?? 0));
    el.setAttribute('x2', String(t?.x ?? 0));
    el.setAttribute('y2', String(t?.y ?? 0));
  }
}

function rebuildSimulation() {
  const container = containerRef.value;
  if (!container) return;
  const w = container.clientWidth || 800;
  const h = container.clientHeight || 600;
  dimensions.value = { w, h };

  // 保持已有节点位置，避免重布局闪烁。
  const prev = new Map(forceNodes.value.map((n) => [n.uuid, n]));
  const nextNodes: ForceNode[] = props.nodes.map((n) => {
    const existing = prev.get(n.uuid);
    if (existing) {
      existing.data = n;
      return existing;
    }
    return {
      uuid: n.uuid,
      data: n,
      x: Math.random() * w,
      y: Math.random() * h,
    };
  });
  const nodeSet = new Set(nextNodes.map((n) => n.uuid));
  const nextLinks: ForceLink[] = props.edges
    .filter((e) => nodeSet.has(e.parentUuid) && nodeSet.has(e.childUuid))
    .map((e) => ({
      source: e.parentUuid,
      target: e.childUuid,
      supplemental: e.supplemental,
    }));

  forceNodes.value = nextNodes;
  forceLinks.value = nextLinks;

  if (simulation.value) simulation.value.stop();

  const sim = forceSimulation<ForceNode>(nextNodes)
    .force(
      'link',
      forceLink<ForceNode, ForceLink>(nextLinks)
        .id((d) => d.uuid)
        .distance((l) => (l.supplemental ? 150 : 110))
        .strength((l) => (l.supplemental ? 0.12 : 0.7)),
    )
    .force('charge', forceManyBody<ForceNode>().strength(-320))
    .force('center', forceCenter(w / 2, h / 2).strength(0.08))
    .force('collide', forceCollide<ForceNode>().radius(26))
    .alpha(0.9)
    .alphaDecay(0.05)           // 略加快收敛，减少后台无谓计算
    .velocityDecay(0.4)         // 标准阻尼
    .on('tick', scheduleTickRender);
  simulation.value = sim;

  // Vue 首次渲染完 DOM 后，初始 tick 一次把位置填上，避免首帧在 (0,0)。
  queueMicrotask(() => tickRender());
}

function setupZoom() {
  if (!svgRef.value || !groupRef.value) return;
  const svg = select<SVGSVGElement, unknown>(svgRef.value);
  const behavior = zoom<SVGSVGElement, unknown>()
    .scaleExtent([0.3, 3])
    .on('zoom', (event) => {
      transform.value = {
        x: event.transform.x,
        y: event.transform.y,
        k: event.transform.k,
      };
    });
  zoomBehavior.value = behavior;
  svg.call(behavior).on('dblclick.zoom', null);
}

function resetZoom() {
  if (!svgRef.value || !zoomBehavior.value) return;
  // 不引入 d3-transition 依赖；直接把 transform 置回 identity，CSS 不负责插值，
  // 但 SVG 本身会被重绘到新坐标，视觉上是"瞬时回正"，对演示足够。
  const sel = select(svgRef.value);
  zoomBehavior.value.transform(sel, zoomIdentity);
}

function onNodeClick(uuid: string) {
  emit('select', uuid);
}

/*
 * 节点拖拽：
 *   - 使用 Pointer Events + setPointerCapture，保证拖出 SVG 边界也能收到 move。
 *   - 拖拽期间给 node 设 fx/fy（固定位置），alphaTarget 维持低温，simulation
 *     自己会调度 tick；我们不直接调 setAttribute，全部走 tickRender。
 *   - 不做 rAF 节流：d3.simulation 的 tick 本身就是每帧一次，pointermove
 *     高频到来只是更新 fx/fy 的两个字段，极廉价。
 */
function onPointerDown(event: PointerEvent, node: ForceNode) {
  event.preventDefault();
  event.stopPropagation();
  if (!simulation.value || !svgRef.value) return;
  const target = event.currentTarget as SVGGElement | null;
  target?.setPointerCapture?.(event.pointerId);

  const sim = simulation.value;
  sim.alphaTarget(0.25).restart();
  const svg = svgRef.value;
  const rect = svg.getBoundingClientRect();

  const toLocal = (clientX: number, clientY: number) => {
    const t = transform.value;
    return {
      x: (clientX - rect.left - t.x) / t.k,
      y: (clientY - rect.top - t.y) / t.k,
    };
  };

  const start = toLocal(event.clientX, event.clientY);
  node.fx = start.x;
  node.fy = start.y;

  const move = (ev: PointerEvent) => {
    const p = toLocal(ev.clientX, ev.clientY);
    node.fx = p.x;
    node.fy = p.y;
  };
  const up = (ev: PointerEvent) => {
    sim.alphaTarget(0);
    target?.releasePointerCapture?.(ev.pointerId);
    target?.removeEventListener('pointermove', move);
    target?.removeEventListener('pointerup', up);
    target?.removeEventListener('pointercancel', up);
  };
  target?.addEventListener('pointermove', move);
  target?.addEventListener('pointerup', up);
  target?.addEventListener('pointercancel', up);
}

function onNodeDouble(node: ForceNode) {
  node.fx = null;
  node.fy = null;
  simulation.value?.alpha(0.3).restart();
}

// observer：容器大小变化时重算 simulation center
let ro: ResizeObserver | null = null;
function observeResize() {
  if (!containerRef.value) return;
  ro?.disconnect();
  ro = new ResizeObserver(() => {
    if (!containerRef.value) return;
    const w = containerRef.value.clientWidth;
    const h = containerRef.value.clientHeight;
    dimensions.value = { w, h };
    const center = simulation.value?.force('center');
    if (center && 'x' in (center as any)) {
      (center as any).x(w / 2).y(h / 2);
      simulation.value?.alpha(0.2).restart();
    }
  });
  ro.observe(containerRef.value);
}

// ---- lifecycle ----
watch(
  () => [props.nodes, props.edges],
  () => {
    rebuildSimulation();
  },
  { deep: true, immediate: false },
);

// Vue 首次 mount 后：SVG ref 就绪再设置 zoom + simulation。
const onMountedOnce = () => {
  rebuildSimulation();
  setupZoom();
  observeResize();
};
// 因为 svg 在 template 里，直接通过 ref 的 watch 即可
watch(
  [svgRef, containerRef],
  ([svg, container]) => {
    if (svg && container && !simulation.value) {
      onMountedOnce();
    }
  },
  { immediate: false },
);

onBeforeUnmount(() => {
  simulation.value?.on('tick', null);
  simulation.value?.stop();
  if (rafHandle) cancelAnimationFrame(rafHandle);
  rafHandle = 0;
  ro?.disconnect();
});
</script>

<template>
  <div class="force-graph" ref="containerRef">
    <svg
      ref="svgRef"
      :viewBox="`0 0 ${dimensions.w} ${dimensions.h}`"
      :width="dimensions.w"
      :height="dimensions.h"
    >
      <!--
        Cohere 亮色主题：去掉暗主题的 glow filter，改用"实心圆 + selected halo"
        的极简表达。节点/边的颜色来自 graphUtils 里的 Interaction Blue / 冷灰
        系列，所有连线走 #17171c 冷灰，只有补链才用 Focus Purple 虚线强调。
      -->
      <g
        ref="groupRef"
        :transform="`translate(${transform.x},${transform.y}) scale(${transform.k})`"
      >
        <!--
          line 的 x1/y1/x2/y2 和 g.node 的 transform 都不使用响应式绑定，
          由 tickRender 直接 setAttribute 更新；data-idx / data-uuid 作为身份索引。
        -->
        <line
          v-for="(link, i) in forceLinks"
          :key="`l-${i}`"
          :class="['edge', { supp: link.supplemental }]"
          :data-idx="i"
          x1="0"
          y1="0"
          x2="0"
          y2="0"
        />

        <g
          v-for="node in forceNodes"
          :key="node.uuid"
          :class="['node', { selected: selected === node.uuid }]"
          :data-uuid="node.uuid"
          transform="translate(0,0)"
          @pointerdown="onPointerDown($event, node)"
          @click.stop="onNodeClick(node.uuid)"
          @dblclick.stop="onNodeDouble(node)"
        >
          <!-- halo：仅 selected / hover 时出现（hover 由 CSS :hover 驱动，零 JS 开销） -->
          <circle
            class="halo"
            :r="nodeRadius(node.data) + 8"
            :fill="statusColor(statusCategory(node.data))"
            fill-opacity="0.12"
          />
          <!-- 主节点：实心圆 + 白描边 -->
          <circle
            class="dot"
            :r="nodeRadius(node.data)"
            :fill="statusColor(statusCategory(node.data))"
            stroke="#ffffff"
            stroke-width="1.5"
          />
          <text class="node-label" :y="nodeRadius(node.data) + 16">
            {{ aliasOf(node.data) }}
          </text>
        </g>
      </g>
    </svg>

    <div class="ctrls">
      <button class="sf-btn ghost" @click="resetZoom" title="Reset view">
        ⟲ Reset
      </button>
    </div>
  </div>
</template>

<style scoped>
.force-graph {
  position: relative;
  width: 100%;
  height: 100%;
  min-height: 0;
}
svg {
  width: 100%;
  height: 100%;
  display: block;
  cursor: grab;
}
svg:active {
  cursor: grabbing;
}

/* 边：默认 Muted Slate 冷灰极细线，补链 = Focus Purple 虚线。 */
.edge {
  stroke: rgba(17, 17, 28, 0.22);
  stroke-width: 1.2;
  pointer-events: none; /* 不响应鼠标，避免拖拽时命中 */
}
.edge.supp {
  stroke: var(--sf-focus-purple);
  stroke-opacity: 0.55;
  stroke-dasharray: 6 4;
  stroke-width: 1.4;
}

.node {
  cursor: grab;
  touch-action: none; /* 关闭浏览器对触摸手势的默认处理，让拖拽更跟手 */
}
.node:active {
  cursor: grabbing;
}

/* halo：默认不可见，selected/hover 时出来 */
.halo {
  opacity: 0;
  transition: opacity var(--sf-dur-fast) var(--sf-ease);
}
.node:hover .halo,
.node.selected .halo {
  opacity: 1;
}

/* 选中态：主圆外加深色描边，让"被选"非常明确 */
.node.selected .dot {
  stroke: var(--sf-fg-0);
  stroke-width: 2;
}

.node-label {
  fill: var(--sf-fg-1);
  font-size: 10px;
  text-anchor: middle;
  pointer-events: none;
  font-family: var(--sf-font-mono);
  letter-spacing: 0.2px;
  paint-order: stroke;
  stroke: #ffffff;      /* 给 label 一圈白描边，避免和 edge 冲突难读 */
  stroke-width: 3;
}
.node.selected .node-label,
.node:hover .node-label {
  fill: var(--sf-fg-0);
}

.ctrls {
  position: absolute;
  bottom: 14px;
  right: 14px;
  display: flex;
  gap: 6px;
}
</style>
