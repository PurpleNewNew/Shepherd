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
  endpoint: '127.0.0.1:9090',
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
    // 错误会写回 conn.lastError，不单独处理
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
  // 用户不信任，直接关闭提示并断开。
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
    <div class="connect-bg" />
    <div class="connect-shell sf-panel">
      <header class="brand">
        <div class="mark">
          <span class="dot" />
          <span class="bar" />
        </div>
        <div class="title">
          <h1>Stockman</h1>
          <p class="sf-muted">Shepherd 控制面 · 答辩演示客户端</p>
        </div>
      </header>

      <form class="form" @submit.prevent="submit">
        <label class="field">
          <span>Kelpie gRPC 地址</span>
          <input
            v-model="form.endpoint"
            class="sf-input sf-mono"
            autocomplete="off"
            placeholder="127.0.0.1:9090"
            :disabled="submitting"
          />
        </label>

        <label class="field">
          <span>Teamserver Token</span>
          <input
            v-model="form.token"
            class="sf-input sf-mono"
            type="password"
            autocomplete="off"
            placeholder="-ui-auth-token 指定的值"
            :disabled="submitting"
          />
        </label>

        <label class="field">
          <span>别名（可选）</span>
          <input
            v-model="form.label"
            class="sf-input"
            placeholder="例如：Star-6 本机实验"
            :disabled="submitting"
          />
        </label>

        <div class="options-row">
          <label class="toggle">
            <input type="checkbox" v-model="form.useTLS" :disabled="submitting" />
            <span>启用 TLS（TOFU 校验证书指纹）</span>
          </label>
          <label class="toggle">
            <input type="checkbox" v-model="form.remember" :disabled="submitting" />
            <span>记住此连接</span>
          </label>
        </div>

        <button
          class="sf-btn primary submit"
          :disabled="!canSubmit"
          type="submit"
        >
          <span v-if="submitting">连接中…</span>
          <span v-else>连接到 Kelpie</span>
        </button>

        <p v-if="conn.lastError" class="error-text">
          连接失败：{{ conn.lastError }}
        </p>
      </form>

      <section class="recent" v-if="conn.recent.length">
        <header class="recent-head">
          <h2>最近连接</h2>
          <span class="sf-muted">{{ conn.recent.length }} 条</span>
        </header>
        <ul>
          <li
            v-for="r in conn.recent"
            :key="r.id"
            :class="['recent-item', { active: activeRecentId === r.id }]"
            @click="pickRecent(r)"
          >
            <div class="recent-title">
              <span class="label">{{ r.label || r.endpoint }}</span>
              <span class="sf-chip" :class="r.useTLS ? 'info' : ''">
                {{ r.useTLS ? 'TLS' : '明文' }}
              </span>
            </div>
            <div class="recent-meta sf-muted sf-mono">{{ r.endpoint }}</div>
            <button
              class="remove-btn"
              @click.stop="removeRecent(r.id)"
              title="删除"
            >
              ×
            </button>
          </li>
        </ul>
      </section>
    </div>

    <!-- TOFU 首次指纹确认 -->
    <Teleport to="body">
      <Transition name="sf-fade-up">
        <div v-if="conn.awaitingTrust" class="modal-mask">
          <div class="modal sf-panel">
            <h3>首次连接：请确认证书指纹</h3>
            <p class="sf-muted">
              你正在 TLS 连接 <b class="sf-mono">{{ form.endpoint }}</b>。
              本地尚未记录过该服务的证书。请对照 Kelpie 启动日志中打印的 SHA256 指纹，确认一致后再信任。
            </p>
            <pre class="fingerprint sf-mono">{{ fingerprintDisplay }}</pre>
            <div class="modal-actions">
              <button class="sf-btn ghost" @click="rejectTrust">取消</button>
              <button class="sf-btn primary" :disabled="submitting" @click="trustAndRetry">
                {{ submitting ? '正在信任…' : '信任并连接' }}
              </button>
            </div>
          </div>
        </div>
      </Transition>

      <Transition name="sf-fade-up">
        <div v-if="showMismatch && conn.pendingMismatch" class="modal-mask">
          <div class="modal sf-panel danger">
            <h3>指纹不匹配</h3>
            <p>
              本地记录的指纹为
              <code class="sf-mono">{{ formatFingerprint(conn.pendingMismatch) }}</code>，
              实际得到
              <code class="sf-mono">{{ formatFingerprint(conn.pendingFingerprint) }}</code>。
            </p>
            <p class="sf-muted">
              如果这是你重置了 Kelpie 或者更换了服务端证书，可以选择"忘记旧指纹后重试"。
              若并非预期行为，请终止连接并排查中间人。
            </p>
            <div class="modal-actions">
              <button class="sf-btn ghost" @click="showMismatch = false">关闭</button>
              <button class="sf-btn danger" @click="forgetAndRetry">忘记旧指纹并重试</button>
            </div>
          </div>
        </div>
      </Transition>
    </Teleport>
  </section>
</template>

<style scoped>
.connect-view {
  flex: 1;
  min-height: 100vh;
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 48px 24px;
  position: relative;
}
.connect-bg {
  position: absolute;
  inset: 0;
  pointer-events: none;
  background:
    radial-gradient(
      650px 450px at 20% 30%,
      rgba(122, 183, 255, 0.14),
      transparent 65%
    ),
    radial-gradient(
      500px 500px at 85% 80%,
      rgba(193, 161, 255, 0.1),
      transparent 60%
    );
  animation: ambient-drift 18s ease-in-out infinite alternate;
}
@keyframes ambient-drift {
  from { transform: translate3d(0, 0, 0); }
  to   { transform: translate3d(4%, -2%, 0); }
}

.connect-shell {
  position: relative;
  width: min(920px, 100%);
  padding: 36px 40px 32px;
  display: grid;
  grid-template-columns: 1.15fr 1fr;
  gap: 40px;
  backdrop-filter: blur(18px);
  animation: sf-fade-up var(--sf-dur-slow) var(--sf-ease);
}

.brand {
  grid-column: 1 / -1;
  display: flex;
  align-items: center;
  gap: 16px;
  padding-bottom: 18px;
  border-bottom: 1px solid var(--sf-border-1);
}
.mark {
  width: 44px;
  height: 44px;
  border-radius: 12px;
  background: linear-gradient(135deg, #3d7bff 0%, #7ab7ff 60%, #c1a1ff 100%);
  display: grid;
  place-items: center;
  box-shadow: 0 6px 18px rgba(122, 183, 255, 0.35);
}
.mark .dot {
  width: 10px;
  height: 10px;
  border-radius: 50%;
  background: #0b0d11;
}
.mark .bar {
  width: 16px;
  height: 2px;
  background: #0b0d11;
  margin-top: 5px;
}
.title h1 {
  font-size: 1.35rem;
  margin: 0;
  letter-spacing: 0.5px;
}
.title p {
  margin: 4px 0 0;
  font-size: 0.85rem;
}

.form {
  display: flex;
  flex-direction: column;
  gap: 14px;
}
.field {
  display: flex;
  flex-direction: column;
  gap: 6px;
}
.field span {
  font-size: 0.78rem;
  color: var(--sf-fg-2);
  letter-spacing: 0.3px;
  text-transform: uppercase;
}
.options-row {
  display: flex;
  gap: 18px;
  flex-wrap: wrap;
  font-size: 0.88rem;
  color: var(--sf-fg-1);
}
.toggle {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  cursor: pointer;
}
.toggle input {
  accent-color: var(--sf-accent);
}
.submit {
  margin-top: 12px;
  padding: 12px 18px;
  font-size: 0.95rem;
}
.error-text {
  margin: 0;
  color: var(--sf-danger);
  font-size: 0.85rem;
}

.recent {
  display: flex;
  flex-direction: column;
  min-width: 0;
}
.recent-head {
  display: flex;
  justify-content: space-between;
  align-items: baseline;
  padding-bottom: 10px;
  border-bottom: 1px solid var(--sf-border-1);
}
.recent-head h2 {
  margin: 0;
  font-size: 1.02rem;
}
.recent ul {
  list-style: none;
  padding: 0;
  margin: 10px 0 0;
  display: flex;
  flex-direction: column;
  gap: 8px;
  max-height: 340px;
  overflow: auto;
}
.recent-item {
  position: relative;
  padding: 10px 12px;
  border-radius: var(--sf-r-md);
  border: 1px solid var(--sf-border-1);
  background: var(--sf-bg-1);
  cursor: pointer;
  transition: border-color var(--sf-dur-fast) var(--sf-ease),
    background var(--sf-dur-fast) var(--sf-ease);
}
.recent-item:hover {
  border-color: var(--sf-accent-line);
  background: var(--sf-bg-2);
}
.recent-item.active {
  border-color: var(--sf-accent);
  box-shadow: 0 0 0 2px var(--sf-accent-dim);
}
.recent-title {
  display: flex;
  align-items: center;
  gap: 8px;
}
.recent-title .label {
  font-weight: 500;
  color: var(--sf-fg-0);
}
.recent-meta {
  margin-top: 4px;
  font-size: 0.82rem;
}
.remove-btn {
  position: absolute;
  right: 8px;
  top: 8px;
  width: 22px;
  height: 22px;
  border-radius: 50%;
  background: transparent;
  color: var(--sf-fg-3);
  font-size: 1.1rem;
  line-height: 1;
  padding: 0;
  opacity: 0;
  transition: opacity var(--sf-dur-fast) var(--sf-ease),
    background var(--sf-dur-fast) var(--sf-ease);
}
.recent-item:hover .remove-btn {
  opacity: 1;
}
.remove-btn:hover {
  background: rgba(255, 122, 147, 0.2);
  color: var(--sf-danger);
}

/* 模态 */
.modal-mask {
  position: fixed;
  inset: 0;
  background: rgba(6, 8, 12, 0.55);
  backdrop-filter: blur(10px);
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 24px;
  z-index: 1000;
}
.modal {
  max-width: 520px;
  width: 100%;
  padding: 24px 28px;
  animation: sf-fade-up var(--sf-dur) var(--sf-ease);
}
.modal.danger {
  border-color: rgba(255, 122, 147, 0.35);
}
.modal h3 {
  margin: 0 0 10px;
}
.modal p {
  color: var(--sf-fg-1);
  font-size: 0.9rem;
  line-height: 1.55;
}
.modal pre.fingerprint {
  padding: 12px 14px;
  background: var(--sf-bg-1);
  border-radius: var(--sf-r-md);
  border: 1px solid var(--sf-border-2);
  color: var(--sf-teal);
  font-size: 0.8rem;
  word-break: break-all;
  white-space: pre-wrap;
  margin: 10px 0 18px;
}
.modal-actions {
  display: flex;
  gap: 10px;
  justify-content: flex-end;
  margin-top: 10px;
}

@media (max-width: 780px) {
  .connect-shell {
    grid-template-columns: 1fr;
    padding: 28px 22px;
  }
}
</style>
