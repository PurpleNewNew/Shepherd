<script setup lang="ts">
import { computed, onBeforeUnmount, ref, shallowRef, watch } from 'vue';
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
const hovered = ref<string>('');
const simulation = shallowRef<Simulation<ForceNode, ForceLink> | null>(null);
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

const hoveredNeighbors = computed(() => {
  const id = hovered.value || props.selected;
  if (!id) return new Set<string>();
  const set = new Set<string>([id]);
  for (const link of forceLinks.value) {
    const s = typeof link.source === 'string' ? link.source : link.source.uuid;
    const t = typeof link.target === 'string' ? link.target : link.target.uuid;
    if (s === id) set.add(t);
    if (t === id) set.add(s);
  }
  return set;
});

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
    .alphaDecay(0.035);
  simulation.value = sim;
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

// 简易拖拽：鼠标按下节点时固定位置。
function onMouseDown(
  event: MouseEvent,
  node: ForceNode,
) {
  event.preventDefault();
  event.stopPropagation();
  if (!simulation.value || !svgRef.value) return;
  const sim = simulation.value;
  sim.alphaTarget(0.3).restart();
  const svg = svgRef.value;
  const rect = svg.getBoundingClientRect();
  const t = transform.value;
  const startX = (event.clientX - rect.left - t.x) / t.k;
  const startY = (event.clientY - rect.top - t.y) / t.k;
  node.fx = startX;
  node.fy = startY;

  const move = (ev: MouseEvent) => {
    const x = (ev.clientX - rect.left - t.x) / t.k;
    const y = (ev.clientY - rect.top - t.y) / t.k;
    node.fx = x;
    node.fy = y;
  };
  const up = () => {
    sim.alphaTarget(0);
    // 双击还原由 onNodeDouble 处理；这里保持固定位置让用户手工布局。
    window.removeEventListener('mousemove', move);
    window.removeEventListener('mouseup', up);
  };
  window.addEventListener('mousemove', move);
  window.addEventListener('mouseup', up);
}

function onNodeDouble(node: ForceNode) {
  node.fx = null;
  node.fy = null;
  simulation.value?.alpha(0.3).restart();
}

function linkCoords(link: ForceLink) {
  const s = typeof link.source === 'string' ? null : link.source;
  const t = typeof link.target === 'string' ? null : link.target;
  return {
    x1: s?.x ?? 0,
    y1: s?.y ?? 0,
    x2: t?.x ?? 0,
    y2: t?.y ?? 0,
  };
}

function linkDim(link: ForceLink): boolean {
  const s = typeof link.source === 'string' ? link.source : link.source.uuid;
  const t = typeof link.target === 'string' ? link.target : link.target.uuid;
  const id = hovered.value || props.selected;
  if (!id) return false;
  return !(s === id || t === id);
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
  simulation.value?.stop();
  ro?.disconnect();
});
</script>

<template>
  <div class="force-graph" ref="containerRef">
    <svg ref="svgRef" :viewBox="`0 0 ${dimensions.w} ${dimensions.h}`"
      :width="dimensions.w" :height="dimensions.h">
      <defs>
        <filter id="sf-glow" x="-50%" y="-50%" width="200%" height="200%">
          <feGaussianBlur stdDeviation="3" result="blur" />
          <feMerge>
            <feMergeNode in="blur" />
            <feMergeNode in="SourceGraphic" />
          </feMerge>
        </filter>
      </defs>
      <g ref="groupRef"
        :transform="`translate(${transform.x},${transform.y}) scale(${transform.k})`">
        <line
          v-for="(link, i) in forceLinks"
          :key="`l-${i}`"
          :class="[
            'edge',
            { supp: link.supplemental },
            { dim: linkDim(link) },
            { highlight:
              !linkDim(link) && (hovered || selected) },
          ]"
          :x1="linkCoords(link).x1"
          :y1="linkCoords(link).y1"
          :x2="linkCoords(link).x2"
          :y2="linkCoords(link).y2"
        />

        <g
          v-for="node in forceNodes"
          :key="node.uuid"
          :class="[
            'node',
            {
              selected: selected === node.uuid,
              dim: (hovered || selected)
                && !hoveredNeighbors.has(node.uuid),
            },
          ]"
          :transform="`translate(${node.x ?? 0},${node.y ?? 0})`"
          @mousedown="onMouseDown($event, node)"
          @click.stop="onNodeClick(node.uuid)"
          @dblclick.stop="onNodeDouble(node)"
          @mouseenter="hovered = node.uuid"
          @mouseleave="hovered = ''"
        >
          <circle
            :r="nodeRadius(node.data)"
            :fill="statusColor(statusCategory(node.data))"
            :opacity="0.85"
            filter="url(#sf-glow)"
          />
          <circle
            :r="nodeRadius(node.data) + 4"
            fill="none"
            :stroke="statusColor(statusCategory(node.data))"
            stroke-opacity="0.35"
            stroke-width="1.4"
          />
          <text
            class="node-label"
            :y="nodeRadius(node.data) + 14"
          >{{ aliasOf(node.data) }}</text>
        </g>
      </g>
    </svg>

    <div class="ctrls">
      <button class="sf-btn ghost" @click="resetZoom" title="重置视图">⟲</button>
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

.edge {
  stroke: rgba(255, 255, 255, 0.2);
  stroke-width: 1.4;
  transition: stroke var(--sf-dur-fast) var(--sf-ease),
    stroke-opacity var(--sf-dur-fast) var(--sf-ease);
}
.edge.supp {
  stroke: var(--sf-info);
  stroke-opacity: 0.45;
  stroke-dasharray: 6 4;
}
.edge.highlight {
  stroke: var(--sf-accent);
  stroke-opacity: 0.85;
}
.edge.dim {
  stroke-opacity: 0.05;
}

.node {
  cursor: pointer;
  transition: transform var(--sf-dur-fast) var(--sf-ease),
    opacity var(--sf-dur-fast) var(--sf-ease);
}
.node.dim {
  opacity: 0.25;
}
.node.selected circle:first-child {
  stroke: #ffffff;
  stroke-width: 2;
}
.node:hover circle:first-child {
  filter: brightness(1.2);
}

.node-label {
  fill: var(--sf-fg-1);
  font-size: 10px;
  text-anchor: middle;
  pointer-events: none;
  font-family: var(--sf-font-mono);
  letter-spacing: 0.2px;
}

.ctrls {
  position: absolute;
  bottom: 12px;
  right: 12px;
  display: flex;
  gap: 6px;
}
</style>
