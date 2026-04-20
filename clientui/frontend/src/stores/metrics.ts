import { defineStore } from 'pinia';
import { ref } from 'vue';
import type { MetricsBundle } from '@/api/types';
import { getMetrics } from '@/api/bindings';

const DEFAULT_BUNDLE: MetricsBundle = {
  dtn: {
    enqueued: 0,
    delivered: 0,
    failed: 0,
    retried: 0,
    global: {
      total: 0,
      ready: 0,
      held: 0,
      capacity: 0,
      highWatermark: 0,
      averageWait: '',
      droppedTotal: 0,
      expiredTotal: 0,
    },
  },
  reconnect: { attempts: 0, success: 0, failures: 0 },
  supplemental: {
    enabled: false,
    queueLength: 0,
    pendingActions: 0,
    activeLinks: 0,
    dispatched: 0,
    success: 0,
    failures: 0,
    dropped: 0,
    recycled: 0,
    queueHigh: 0,
  },
  capturedAt: '',
};

export const useMetricsStore = defineStore('metrics', () => {
  const bundle = ref<MetricsBundle>(DEFAULT_BUNDLE);
  const loading = ref(false);
  const error = ref<string>('');
  let timer: number | null = null;

  async function refresh() {
    loading.value = true;
    error.value = '';
    try {
      bundle.value = await getMetrics();
    } catch (err: any) {
      error.value = err?.message ?? String(err);
    } finally {
      loading.value = false;
    }
  }

  function start(intervalMs = 3000) {
    stop();
    refresh();
    timer = window.setInterval(() => {
      refresh();
    }, intervalMs);
  }

  function stop() {
    if (timer !== null) {
      clearInterval(timer);
      timer = null;
    }
  }

  function reset() {
    stop();
    bundle.value = DEFAULT_BUNDLE;
    error.value = '';
  }

  return { bundle, loading, error, refresh, start, stop, reset };
});
