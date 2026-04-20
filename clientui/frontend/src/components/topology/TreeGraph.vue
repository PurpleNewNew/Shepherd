<script setup lang="ts">
import { computed, ref, onBeforeUnmount, onMounted, watch } from 'vue';
import { hierarchy, tree, type HierarchyPointNode } from 'd3-hierarchy';
import { linkVertical } from 'd3-shape';
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
const emit = defineEmits<{ (e: 'select', uuid: string): void }>();

const containerRef = ref<HTMLElement | null>(null);
const width = ref(800);
const height = ref(520);

// 树状布局：根据 edges 中的 tree edge 构建父子关系；supplemental 单独渲染为虚线。
const layout = computed(() => {
  const nodes = props.nodes;
  if (!nodes.length) {
    return {
      positioned: new Map<string, HierarchyPointNode<NodeSummary>>(),
      links: [] as { d: string; supplemental: boolean; source: string; target: string }[],
      width: width.value,
      height: height.value,
    };
  }

  const byUuid = new Map(nodes.map((n) => [n.uuid, n]));
  const parentByChild = new Map<string, string>();
  for (const e of props.edges) {
    if (!e.supplemental) parentByChild.set(e.childUuid, e.parentUuid);
  }
  const childrenByParent = new Map<string, NodeSummary[]>();
  for (const n of nodes) {
    const p = parentByChild.get(n.uuid);
    if (p && byUuid.has(p)) {
      const arr = childrenByParent.get(p) ?? [];
      arr.push(n);
      childrenByParent.set(p, arr);
    }
  }
  const assigned = new Set(parentByChild.keys());
  const roots = nodes.filter((n) => !assigned.has(n.uuid));
  if (!roots.length && nodes.length) {
    roots.push(nodes[0]);
  }

  // 虚拟根，把多个 root 汇聚到一起显示。
  const virtualRoot: NodeSummary = {
    uuid: '__root__',
    alias: 'Kelpie',
    status: 'root',
    depth: -1,
    activeStreams: 0,
  };
  const root = hierarchy<NodeSummary>(virtualRoot, (d) => {
    if (d.uuid === '__root__') return roots;
    return childrenByParent.get(d.uuid) ?? [];
  });

  const w = width.value;
  const h = height.value;
  const layoutFn = tree<NodeSummary>()
    .nodeSize([110, 110])
    .separation((a, b) => (a.parent === b.parent ? 1 : 1.3));
  layoutFn(root);

  // 求实际范围，做一次居中 + 缩放。
  let minX = Infinity;
  let maxX = -Infinity;
  let minY = Infinity;
  let maxY = -Infinity;
  const positioned = new Map<string, HierarchyPointNode<NodeSummary>>();
  root.each((node) => {
    positioned.set(node.data.uuid, node as HierarchyPointNode<NodeSummary>);
    if (node.data.uuid === '__root__') return;
    const nx = (node as HierarchyPointNode<NodeSummary>).x;
    const ny = (node as HierarchyPointNode<NodeSummary>).y;
    minX = Math.min(minX, nx);
    maxX = Math.max(maxX, nx);
    minY = Math.min(minY, ny);
    maxY = Math.max(maxY, ny);
  });
  if (!isFinite(minX)) {
    minX = 0;
    maxX = 0;
    minY = 0;
    maxY = 0;
  }
  const contentW = Math.max(maxX - minX + 160, 200);
  const contentH = Math.max(maxY - minY + 160, 200);
  const scale = Math.min(w / contentW, h / contentH, 1);
  const offsetX = (w - (maxX - minX) * scale) / 2 - minX * scale;
  const offsetY = (h - (maxY - minY) * scale) / 2 - minY * scale + 20;
  const project = (nx: number, ny: number) => ({
    x: nx * scale + offsetX,
    y: ny * scale + offsetY,
  });
  positioned.forEach((node) => {
    const p = project((node as any).x, (node as any).y);
    (node as any).__x = p.x;
    (node as any).__y = p.y;
  });

  const linkGen = linkVertical<
    { source: { x: number; y: number }; target: { x: number; y: number } },
    { x: number; y: number }
  >()
    .x((d) => d.x)
    .y((d) => d.y);

  const links: { d: string; supplemental: boolean; source: string; target: string }[] = [];
  for (const e of props.edges) {
    const s = positioned.get(e.parentUuid);
    const t = positioned.get(e.childUuid);
    if (!s || !t) continue;
    const src = { x: (s as any).__x, y: (s as any).__y };
    const tgt = { x: (t as any).__x, y: (t as any).__y };
    const d = linkGen({ source: src, target: tgt }) ?? '';
    links.push({ d, supplemental: e.supplemental, source: e.parentUuid, target: e.childUuid });
  }
  return { positioned, links, width: w, height: h };
});

const linkHighlight = computed(() => {
  const id = props.selected;
  if (!id) return () => false;
  return (link: { source: string; target: string }) =>
    link.source === id || link.target === id;
});

function onClick(uuid: string) {
  emit('select', uuid);
}

let ro: ResizeObserver | null = null;
function observe() {
  if (!containerRef.value) return;
  ro?.disconnect();
  ro = new ResizeObserver(() => {
    if (!containerRef.value) return;
    width.value = containerRef.value.clientWidth;
    height.value = containerRef.value.clientHeight;
  });
  ro.observe(containerRef.value);
}

onMounted(() => observe());
onBeforeUnmount(() => ro?.disconnect());
watch([containerRef], ([el]) => {
  if (el) {
    width.value = el.clientWidth;
    height.value = el.clientHeight;
  }
});
</script>

<template>
  <div class="tree-graph" ref="containerRef">
    <svg :viewBox="`0 0 ${layout.width} ${layout.height}`">
      <g>
        <path
          v-for="(l, i) in layout.links"
          :key="`p-${i}`"
          :class="['edge', { supp: l.supplemental }, { highlight: linkHighlight(l) }]"
          :d="l.d"
        />
        <g
          v-for="[uuid, node] in layout.positioned"
          :key="uuid"
          :transform="`translate(${(node as any).__x},${(node as any).__y})`"
          :class="[
            'node',
            { virtual: uuid === '__root__' },
            { selected: selected === uuid },
          ]"
          @click="uuid !== '__root__' && onClick(uuid)"
        >
          <template v-if="uuid === '__root__'">
            <rect x="-28" y="-14" width="56" height="28" rx="10"
              fill="rgba(122,183,255,0.16)" stroke="rgba(122,183,255,0.45)" />
            <text class="label" dy="5">Kelpie</text>
          </template>
          <template v-else>
            <circle
              :r="nodeRadius(node.data)"
              :fill="statusColor(statusCategory(node.data))"
              opacity="0.88"
            />
            <circle
              :r="nodeRadius(node.data) + 4"
              fill="none"
              :stroke="statusColor(statusCategory(node.data))"
              stroke-opacity="0.3"
              stroke-width="1.4"
            />
            <text class="label" :y="nodeRadius(node.data) + 14">
              {{ aliasOf(node.data) }}
            </text>
          </template>
        </g>
      </g>
    </svg>
  </div>
</template>

<style scoped>
.tree-graph {
  width: 100%;
  height: 100%;
}
svg {
  width: 100%;
  height: 100%;
  display: block;
}
.edge {
  fill: none;
  stroke: rgba(255, 255, 255, 0.16);
  stroke-width: 1.4;
  transition: stroke var(--sf-dur-fast) var(--sf-ease),
    stroke-opacity var(--sf-dur-fast) var(--sf-ease);
}
.edge.supp {
  stroke: var(--sf-info);
  stroke-opacity: 0.5;
  stroke-dasharray: 6 4;
}
.edge.highlight {
  stroke: var(--sf-accent);
  stroke-opacity: 0.9;
}
.node {
  cursor: pointer;
  transition: transform var(--sf-dur-fast) var(--sf-ease);
}
.node.virtual {
  cursor: default;
}
.node.selected circle:first-child {
  stroke: #ffffff;
  stroke-width: 2;
}
.label {
  fill: var(--sf-fg-1);
  font-size: 10.5px;
  text-anchor: middle;
  font-family: var(--sf-font-mono);
  pointer-events: none;
}
</style>
