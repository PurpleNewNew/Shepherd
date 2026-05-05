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

const stage = computed<'connect' | 'main'>(() =>
  conn.isConnected ? 'main' : 'connect',
);

onMounted(() => {
  conn.bootstrap();
});

onBeforeUnmount(() => {
  conn.dispose();
  events.dispose();
  metrics.stop();
  topo.clear();
});
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
