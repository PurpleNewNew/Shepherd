import type { NodeSummary } from '@/api/types';

export type StatusCategory = 'online' | 'sleeping' | 'offline' | 'unknown';

export function statusCategory(n: NodeSummary | undefined | null): StatusCategory {
  const raw = String(n?.status ?? '').toLowerCase();
  if (!raw) return 'unknown';
  if (raw.includes('offline') || raw.includes('dead') || raw.includes('fail')) {
    return 'offline';
  }
  if (raw.includes('sleep') || raw.includes('pending')) {
    return 'sleeping';
  }
  if (raw.includes('online') || raw.includes('active') || raw.includes('ok')) {
    return 'online';
  }
  return 'unknown';
}

export function statusColor(category: StatusCategory): string {
  switch (category) {
    case 'online':
      return '#6be19d';
    case 'sleeping':
      return '#ffcd6a';
    case 'offline':
      return '#ff7a93';
    default:
      return '#89e0ff';
  }
}

export function nodeRadius(n: NodeSummary | undefined | null): number {
  if (!n) return 10;
  const base = 10;
  const depthReduce = Math.min(3, Math.max(0, n.depth ?? 0));
  return base + 4 - depthReduce * 0.4;
}

export function aliasOf(n: NodeSummary | undefined | null): string {
  if (!n) return '';
  return n.alias && n.alias.trim() ? n.alias : (n.uuid ?? '').slice(0, 6);
}
