<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted } from 'vue';
import { useConnectionStore } from '@/stores/connection';
import { useTopologyStore } from '@/stores/topology';
import { useEventsStore } from '@/stores/events';
import { useMetricsStore } from '@/stores/metrics';
import ConnectView from '@/views/ConnectView.vue';
import MainShell from '@/views/MainShell.vue';

const conn = useConnectionStore();
const topo = useTopologyStore();
const events = useEventsStore();
const metrics = useMetricsStore();
const previewMode = new URLSearchParams(window.location.search).has('preview');

const stage = computed<'connect' | 'main'>(() =>
  conn.isConnected || previewMode ? 'main' : 'connect',
);

onMounted(() => {
  if (previewMode) {
    seedPreviewIfRequested();
  } else {
    conn.bootstrap();
  }
});

onBeforeUnmount(() => {
  conn.dispose();
  events.dispose();
  metrics.stop();
  topo.clear();
});

function seedPreviewIfRequested() {
  if (!(import.meta as any).env?.DEV) return;
  if (!new URLSearchParams(window.location.search).has('preview')) return;

  conn.phase = 'connected';
  conn.endpoint = '127.0.0.1:50051';
  conn.useTLS = true;
  conn.connectedAt = new Date().toISOString();

  const now = new Date().toISOString();
  topo.applySnapshot({
    fetchedAt: now,
    nodes: [
      {
        uuid: 'root-0001-controller',
        alias: 'kelpie-root',
        status: 'connected',
        network: 'control',
        sleep: 'active',
        depth: 0,
        tags: ['controller'],
        activeStreams: 2,
        workProfile: 'grpc/tls',
      },
      {
        uuid: 'edge-1057-laptop',
        alias: 'design-laptop',
        parentUuid: 'root-0001-controller',
        status: 'online',
        network: 'campus',
        sleep: '15s/5s +10%',
        memo: 'demo target',
        depth: 1,
        tags: ['macos'],
        activeStreams: 1,
        workProfile: 'direct',
      },
      {
        uuid: 'relay-7782-pivot',
        alias: 'pivot-relay',
        parentUuid: 'root-0001-controller',
        status: 'sleeping',
        network: 'lab',
        sleep: '45s/8s +20%',
        memo: 'store-carry-forward',
        depth: 1,
        tags: ['pivot'],
        activeStreams: 0,
        workProfile: 'dtn',
      },
      {
        uuid: 'leaf-4419-sensor',
        alias: 'leaf-sensor',
        parentUuid: 'relay-7782-pivot',
        status: 'held',
        network: 'lab',
        sleep: '120s/10s +15%',
        depth: 2,
        tags: ['intermittent'],
        activeStreams: 0,
        workProfile: 'dtn',
      },
    ],
    edges: [
      { parentUuid: 'root-0001-controller', childUuid: 'edge-1057-laptop', supplemental: false },
      { parentUuid: 'root-0001-controller', childUuid: 'relay-7782-pivot', supplemental: false },
      { parentUuid: 'relay-7782-pivot', childUuid: 'leaf-4419-sensor', supplemental: false },
      { parentUuid: 'edge-1057-laptop', childUuid: 'leaf-4419-sensor', supplemental: true },
    ],
    streams: [
      {
        streamId: 7,
        targetUuid: 'edge-1057-laptop',
        kind: 'shell',
        outbound: true,
        pending: 0,
        inFlight: 1,
        window: 8,
        seq: 42,
        ack: 41,
        rto: '1.2s',
        lastActivity: now,
        sessionId: 'sess-edge',
      },
      {
        streamId: 9,
        targetUuid: 'root-0001-controller',
        kind: 'socks',
        outbound: true,
        pending: 2,
        inFlight: 3,
        window: 12,
        seq: 88,
        ack: 85,
        rto: '900ms',
        lastActivity: now,
        sessionId: 'sess-root',
      },
    ],
    sessions: [
      {
        targetUuid: 'edge-1057-laptop',
        status: 'active',
        active: true,
        connected: true,
        remoteAddr: '10.18.2.57',
        upstream: 'root-0001-controller',
        downstream: 'direct',
        networkId: 'campus',
        lastSeen: now,
        sleepSeconds: 15,
        workSeconds: 5,
        jitter: 10,
      },
      {
        targetUuid: 'relay-7782-pivot',
        status: 'sleeping',
        active: false,
        connected: true,
        remoteAddr: '10.18.8.12',
        upstream: 'root-0001-controller',
        downstream: 'leaf-4419-sensor',
        networkId: 'lab',
        lastSeen: now,
        sleepSeconds: 45,
        workSeconds: 8,
        jitter: 20,
      },
    ],
  });
  topo.select('edge-1057-laptop');

  metrics.bundle = {
    dtn: {
      enqueued: 18,
      delivered: 15,
      failed: 1,
      retried: 4,
      global: {
        total: 5,
        ready: 2,
        held: 3,
        capacity: 128,
        highWatermark: 21,
        averageWait: '3.1s',
        droppedTotal: 0,
        expiredTotal: 1,
      },
    },
    reconnect: { attempts: 4, success: 3, failures: 1, lastError: 'transient timeout' },
    supplemental: {
      enabled: true,
      queueLength: 2,
      pendingActions: 1,
      activeLinks: 1,
      dispatched: 22,
      success: 20,
      failures: 1,
      dropped: 0,
      recycled: 2,
      queueHigh: 7,
    },
    capturedAt: now,
  };

  events.events = [
    {
      seq: 103,
      kind: 'session',
      action: 'connected',
      timestamp: now,
      summary: 'design-laptop joined via direct transport',
      target: 'edge-1057-laptop',
    },
    {
      seq: 102,
      kind: 'log',
      action: 'held',
      timestamp: now,
      summary: 'leaf-sensor bundle held until next work window',
      target: 'leaf-4419-sensor',
    },
    {
      seq: 101,
      kind: 'supplemental',
      action: 'repair',
      timestamp: now,
      summary: 'supplemental edge promoted after primary timeout',
      source: 'edge-1057-laptop',
      target: 'leaf-4419-sensor',
    },
  ];
}
</script>

<template>
  <main class="stockman-app">
    <Transition name="sf-stage" mode="out-in">
      <ConnectView v-if="stage === 'connect'" key="connect" />
      <MainShell v-else key="main" />
    </Transition>
  </main>
</template>

<style scoped>
.stockman-app {
  height: 100vh;
  width: 100vw;
  display: flex;
  overflow: hidden;
  position: relative;
}
</style>
