<script setup lang="ts">
import { computed, reactive, ref, watch } from 'vue';
import { useConnectionStore } from '@/stores/connection';
import type { RecentConnection } from '@/api/types';

const conn = useConnectionStore();

interface FormState {
  endpoint: string;
  token: string;
  useTLS: boolean;
  label: string;
  remember: boolean;
}

const form = reactive<FormState>({
  endpoint: '127.0.0.1:50061',
  token: '',
  useTLS: false,
  label: '',
  remember: true,
});

const activeRecentId = ref<string>('');
const submitting = ref(false);
const showMismatch = ref(false);

const canSubmit = computed(
  () => form.endpoint.trim().length > 0 && !submitting.value,
);

const fingerprintDisplay = computed(() =>
  formatFingerprint(conn.pendingFingerprint),
);

function formatFingerprint(raw?: string): string {
  if (!raw) return '';
  const s = raw.replace(/[^0-9a-fA-F]/g, '').toUpperCase();
  return s.match(/.{1,2}/g)?.join(':') ?? s;
}

function pickRecent(r: RecentConnection) {
  activeRecentId.value = r.id;
  form.endpoint = r.endpoint;
  form.useTLS = r.useTLS;
  form.label = r.label ?? '';
  form.remember = true;
  form.token = '';
}

async function removeRecent(id: string) {
  await conn.remove(id);
  if (activeRecentId.value === id) activeRecentId.value = '';
}

async function submit() {
  submitting.value = true;
  try {
    const res = await conn.connect({
      endpoint: form.endpoint.trim(),
      token: form.token.trim(),
      useTLS: form.useTLS,
      label: form.label.trim(),
      remember: form.remember,
      forceAccept: false,
    });
    if (res.mismatchExpected) {
      showMismatch.value = true;
    }
  } catch (_err) {
    // 错误会写回 conn.lastError，不单独处理。
  } finally {
    submitting.value = false;
  }
}

async function trustAndRetry() {
  submitting.value = true;
  try {
    await conn.connect({
      endpoint: form.endpoint.trim(),
      token: form.token.trim(),
      useTLS: form.useTLS,
      label: form.label.trim(),
      remember: form.remember,
      forceAccept: true,
    });
  } finally {
    submitting.value = false;
  }
}

function rejectTrust() {
  conn.disconnect();
}

async function forgetAndRetry() {
  await conn.forget(form.endpoint.trim());
  showMismatch.value = false;
  submit();
}

watch(
  () => conn.bindingsReady,
  (ready) => {
    if (ready) conn.refreshRecent();
  },
  { immediate: true },
);
</script>

<template>
  <section class="connect-view">
    <form class="connect-dialog" @submit.prevent="submit">
      <header class="dialog-titlebar">
        <div>
          <span class="eyebrow">TEAM SERVER</span>
          <h1>Connect</h1>
        </div>
        <span class="build-tag">Wails v3</span>
      </header>

      <div class="form-grid">
        <label class="field span-2">
          <span>Host</span>
          <input
            v-model="form.endpoint"
            class="mono"
            autocomplete="off"
            placeholder="127.0.0.1:50061"
            :disabled="submitting"
          />
        </label>

        <label class="field span-2">
          <span>Token</span>
          <input
            v-model="form.token"
            class="mono"
            type="password"
            autocomplete="off"
            placeholder="-ui-grpc-token"
            :disabled="submitting"
          />
        </label>

        <label class="field span-2">
          <span>Alias</span>
          <input
            v-model="form.label"
            autocomplete="off"
            placeholder="thesis-demo"
            :disabled="submitting"
          />
        </label>

        <label class="check-field">
          <input type="checkbox" v-model="form.useTLS" :disabled="submitting" />
          <span>TLS</span>
        </label>

        <label class="check-field">
          <input
            type="checkbox"
            v-model="form.remember"
            :disabled="submitting"
          />
          <span>Remember</span>
        </label>
      </div>

      <section class="recent-panel">
        <header>
          <span>Recent</span>
          <small>{{ conn.recent.length }}</small>
        </header>
        <div class="recent-list" v-if="conn.recent.length">
          <button
            v-for="r in conn.recent"
            :key="r.id"
            type="button"
            :class="['recent-row', { active: activeRecentId === r.id }]"
            @click="pickRecent(r)"
          >
            <span class="recent-main">
              <b>{{ r.label || r.endpoint }}</b>
              <em>{{ r.endpoint }}</em>
            </span>
            <span class="recent-mode">{{ r.useTLS ? 'TLS' : 'PLAIN' }}</span>
            <span
              class="remove-recent"
              title="Remove"
              @click.stop="removeRecent(r.id)"
            >
              ×
            </span>
          </button>
        </div>
        <div v-else class="empty-recent">No saved endpoints</div>
      </section>

      <p v-if="conn.lastError" class="error-text">
        {{ conn.lastError }}
      </p>

      <footer class="actions">
        <span class="status-text">
          {{ submitting ? 'CONNECTING' : conn.phase.toUpperCase() }}
        </span>
        <button class="connect-btn" :disabled="!canSubmit" type="submit">
          {{ submitting ? 'Connecting...' : 'Connect' }}
        </button>
      </footer>
    </form>

    <Teleport to="body">
      <Transition name="modal-pop">
        <div v-if="conn.awaitingTrust" class="modal-mask">
          <div class="modal">
            <header>
              <span class="eyebrow">TOFU PIN</span>
              <h2>Confirm fingerprint</h2>
            </header>
            <pre class="fingerprint mono">{{ fingerprintDisplay }}</pre>
            <div class="modal-actions">
              <button type="button" @click="rejectTrust">Cancel</button>
              <button type="button" :disabled="submitting" @click="trustAndRetry">
                Trust
              </button>
            </div>
          </div>
        </div>
      </Transition>

      <Transition name="modal-pop">
        <div v-if="showMismatch && conn.pendingMismatch" class="modal-mask">
          <div class="modal danger">
            <header>
              <span class="eyebrow">IDENTITY</span>
              <h2>Fingerprint changed</h2>
            </header>
            <p>
              Pinned
              <code class="mono">{{ formatFingerprint(conn.pendingMismatch) }}</code>
              but got
              <code class="mono">{{ formatFingerprint(conn.pendingFingerprint) }}</code>.
            </p>
            <div class="modal-actions">
              <button type="button" @click="showMismatch = false">Close</button>
              <button type="button" @click="forgetAndRetry">Forget & retry</button>
            </div>
          </div>
        </div>
      </Transition>
    </Teleport>
  </section>
</template>

<style scoped>
.connect-view {
  min-height: 100vh;
  width: 100vw;
  display: grid;
  place-items: stretch;
  background: #121821;
  color: #d8e0ea;
  font-family:
    'Inter',
    -apple-system,
    BlinkMacSystemFont,
    'Segoe UI',
    'PingFang SC',
    sans-serif;
}

.connect-dialog {
  height: 100vh;
  display: grid;
  grid-template-rows: auto auto minmax(74px, 1fr) auto auto;
  gap: 10px;
  padding: 14px;
  border: 1px solid #263343;
  background:
    linear-gradient(180deg, rgba(255, 255, 255, 0.03), transparent 38%),
    #141b25;
}

.dialog-titlebar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding-bottom: 10px;
  border-bottom: 1px solid #2b394a;
}

.eyebrow,
.build-tag,
.status-text,
.recent-panel header,
.field span,
.check-field,
.recent-mode {
  font-size: 10px;
  font-weight: 700;
  line-height: 1;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: #8ea0b6;
}

h1,
h2 {
  margin: 3px 0 0;
  color: #f4f7fb;
  font-size: 18px;
  font-weight: 650;
  letter-spacing: 0;
}

.build-tag {
  color: #6fd2a1;
}

.form-grid {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 8px 10px;
}

.span-2 {
  grid-column: 1 / -1;
}

.field {
  display: grid;
  grid-template-columns: 64px minmax(0, 1fr);
  align-items: center;
  gap: 8px;
}

.field input {
  min-width: 0;
  height: 28px;
  padding: 0 8px;
  border: 1px solid #334358;
  border-radius: 0;
  outline: none;
  background: #0d1219;
  color: #edf3fb;
  font-size: 12px;
}

.field input:focus {
  border-color: #68b7ff;
  box-shadow: inset 0 0 0 1px rgba(104, 183, 255, 0.25);
}

.field input:disabled {
  opacity: 0.55;
}

.mono {
  font-family:
    'IBM Plex Mono',
    'SF Mono',
    Menlo,
    Consolas,
    monospace;
}

.check-field {
  height: 24px;
  display: inline-flex;
  align-items: center;
  gap: 7px;
  color: #c1cad6;
}

.check-field input {
  width: 13px;
  height: 13px;
  margin: 0;
  accent-color: #6fd2a1;
}

.recent-panel {
  min-height: 0;
  border: 1px solid #283646;
  background: #101720;
  display: grid;
  grid-template-rows: auto 1fr;
}

.recent-panel header {
  display: flex;
  justify-content: space-between;
  padding: 7px 8px;
  border-bottom: 1px solid #283646;
  background: #172130;
}

.recent-panel small {
  color: #6fd2a1;
}

.recent-list {
  min-height: 0;
  overflow: auto;
}

.recent-row {
  width: 100%;
  height: 38px;
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto 22px;
  align-items: center;
  gap: 8px;
  padding: 0 0 0 8px;
  border: 0;
  border-bottom: 1px solid #202c3b;
  border-radius: 0;
  background: transparent;
  color: #d7e0ec;
  text-align: left;
  cursor: default;
}

.recent-row:hover,
.recent-row.active {
  background: #1c2d40;
}

.recent-main {
  min-width: 0;
  display: flex;
  flex-direction: column;
  gap: 2px;
}

.recent-main b,
.recent-main em {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  font-style: normal;
}

.recent-main b {
  color: #eef5ff;
  font-size: 12px;
  font-weight: 600;
}

.recent-main em {
  color: #7e8fa4;
  font-size: 10px;
}

.recent-mode {
  color: #6fd2a1;
}

.remove-recent {
  height: 100%;
  display: grid;
  place-items: center;
  color: #8394aa;
  font-size: 16px;
}

.remove-recent:hover {
  background: #42202a;
  color: #ff9aa8;
}

.empty-recent {
  display: grid;
  place-items: center;
  color: #63758b;
  font-size: 12px;
}

.error-text {
  min-height: 18px;
  margin: 0;
  color: #ff9aa8;
  font-size: 11px;
  line-height: 1.35;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.actions {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding-top: 2px;
}

.connect-btn,
.modal-actions button {
  min-width: 92px;
  height: 30px;
  border: 1px solid #50647c;
  border-radius: 0;
  background: #26364a;
  color: #f3f8ff;
  font-size: 12px;
  font-weight: 650;
}

.connect-btn:hover:not(:disabled),
.modal-actions button:hover:not(:disabled) {
  border-color: #68b7ff;
  background: #30506c;
}

.connect-btn:disabled,
.modal-actions button:disabled {
  opacity: 0.45;
}

.modal-mask {
  position: fixed;
  inset: 0;
  display: grid;
  place-items: center;
  padding: 18px;
  background: rgba(5, 8, 12, 0.72);
  z-index: 10;
}

.modal {
  width: min(380px, calc(100vw - 36px));
  padding: 14px;
  border: 1px solid #40536b;
  border-radius: 0;
  background: #151e2b;
  color: #dce5f0;
}

.modal.danger {
  border-color: #8d3d48;
}

.modal p {
  margin: 10px 0 0;
  color: #b5c2d1;
  font-size: 12px;
  line-height: 1.5;
}

.fingerprint {
  white-space: pre-wrap;
  word-break: break-all;
  margin: 12px 0 0;
  padding: 10px;
  border: 1px solid #2b3b4f;
  background: #0c1118;
  color: #9ce6bd;
  font-size: 11px;
  line-height: 1.5;
}

.modal-actions {
  display: flex;
  justify-content: flex-end;
  gap: 8px;
  margin-top: 14px;
}

.modal-pop-enter-active,
.modal-pop-leave-active {
  transition: opacity 140ms ease;
}

.modal-pop-enter-from,
.modal-pop-leave-to {
  opacity: 0;
}
</style>
