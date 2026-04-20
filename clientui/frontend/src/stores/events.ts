import { defineStore } from 'pinia';
import { ref } from 'vue';
import type { TimelineEvent } from '@/api/types';
import { onKelpieEvent, recentEvents } from '@/api/bindings';

const MAX_EVENTS = 400;

export const useEventsStore = defineStore('events', () => {
  const events = ref<TimelineEvent[]>([]);
  const paused = ref(false);
  let unsub: (() => void) | null = null;

  function push(ev: TimelineEvent) {
    if (paused.value) return;
    events.value.unshift(ev);
    if (events.value.length > MAX_EVENTS) events.value.length = MAX_EVENTS;
  }

  async function bootstrap() {
    try {
      const seed = await recentEvents(MAX_EVENTS);
      // seed 可能按时间正序，反转为倒序显示（最新在上）
      if (seed && seed.length) {
        const reversed = [...seed].reverse();
        events.value = reversed;
      }
    } catch (err) {
      // ignore
    }
    unsub?.();
    unsub = onKelpieEvent((ev) => push(ev));
  }

  function dispose() {
    unsub?.();
    unsub = null;
    events.value = [];
  }

  function togglePause() {
    paused.value = !paused.value;
  }

  function clear() {
    events.value = [];
  }

  return { events, paused, push, bootstrap, dispose, togglePause, clear };
});
