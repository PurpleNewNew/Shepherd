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

// hero 左栏底部的 "Session" 时间戳：仅在组件首次挂载时求一次，无需实时。
const sessionStamp = computed(() => {
  const d = new Date();
  const pad = (n: number) => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} · ${pad(d.getHours())}:${pad(d.getMinutes())}`;
});

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
    <!-- 左侧 hero：深紫渐变带，Cohere "enterprise command deck" 的戏剧入口。 -->
    <aside class="hero sf-hero-violet">
      <div class="hero-top">
        <div class="brand-row">
          <div class="brand-mark" aria-hidden="true">
            <span class="brand-serif">S</span>
          </div>
          <div class="brand-wordmark">
            <span class="sf-label">Shepherd · Stockman</span>
            <span class="brand-caption">
              Defense-grade control plane companion
            </span>
          </div>
        </div>
      </div>

      <div class="hero-center">
        <p class="sf-label hero-eyebrow">
          Control plane for constrained networks
        </p>
        <h1 class="hero-title">
          One admin console.<br />
          <em>Many unreachable edges.</em>
        </h1>
        <p class="hero-lede">
          Stockman 是 Shepherd 的毕业设计演示客户端——面向答辩的平静、排印化的
          Kelpie 控制面视口。它不是生产运维台；它只做一件事：把 Gossip、补链、
          DTN 和 sleep-aware 这些机制，讲清楚。
        </p>
      </div>

      <footer class="hero-bottom">
        <div class="hero-meta">
          <span class="sf-label">Build</span>
          <span class="hero-meta-value">Wails v2 · Vue 3 · Vite · Go</span>
        </div>
        <div class="hero-meta">
          <span class="sf-label">Session</span>
          <span class="hero-meta-value">{{ sessionStamp }}</span>
        </div>
      </footer>
    </aside>

    <!-- 右侧：白底连接表单 + 最近连接 -->
    <div class="panel">
      <div class="panel-head">
        <p class="sf-label sf-label--strong">Step 01 — Connect</p>
        <h2 class="sf-h2 panel-title">Sign in to a Kelpie control plane</h2>
        <p class="sf-body-lg panel-lede">
          填入 Kelpie 的 gRPC UI 端点与 <code class="sf-mono">-ui-auth-token</code>。
          TLS 为可选；若启用，首次连接会要求你确认 SHA-256 证书指纹（TOFU）。
        </p>
      </div>

      <form class="form" @submit.prevent="submit">
        <label class="field">
          <span class="sf-label sf-label--strong">gRPC Endpoint</span>
          <input
            v-model="form.endpoint"
            class="sf-input sf-mono"
            autocomplete="off"
            placeholder="127.0.0.1:9090"
            :disabled="submitting"
          />
        </label>

        <label class="field">
          <span class="sf-label sf-label--strong">Teamserver Token</span>
          <input
            v-model="form.token"
            class="sf-input sf-mono"
            type="password"
            autocomplete="off"
            placeholder="-ui-auth-token"
            :disabled="submitting"
          />
        </label>

        <label class="field">
          <span class="sf-label sf-label--strong">Label · Optional</span>
          <input
            v-model="form.label"
            class="sf-input"
            placeholder="e.g. star-6 · thesis-demo"
            :disabled="submitting"
          />
        </label>

        <div class="options-row">
          <label class="toggle">
            <input
              type="checkbox"
              v-model="form.useTLS"
              :disabled="submitting"
            />
            <span>启用 TLS（TOFU pin）</span>
          </label>
          <label class="toggle">
            <input
              type="checkbox"
              v-model="form.remember"
              :disabled="submitting"
            />
            <span>记住此端点</span>
          </label>
        </div>

        <button
          class="sf-btn primary submit"
          :disabled="!canSubmit"
          type="submit"
        >
          <span v-if="submitting">Connecting…</span>
          <span v-else>Connect to Kelpie →</span>
        </button>

        <p v-if="conn.lastError" class="error-text">
          {{ conn.lastError }}
        </p>
      </form>

      <section class="recent" v-if="conn.recent.length">
        <header class="recent-head">
          <p class="sf-label sf-label--strong">Recent endpoints</p>
          <span class="sf-small sf-muted">{{ conn.recent.length }} saved</span>
        </header>
        <ul>
          <li
            v-for="r in conn.recent"
            :key="r.id"
            :class="['recent-item', { active: activeRecentId === r.id }]"
            @click="pickRecent(r)"
          >
            <div class="recent-title">
              <span class="recent-label">{{ r.label || r.endpoint }}</span>
              <span class="sf-chip" :class="r.useTLS ? 'info' : ''">
                {{ r.useTLS ? 'TLS' : 'Plain' }}
              </span>
            </div>
            <div class="recent-meta sf-mono">{{ r.endpoint }}</div>
            <button
              class="remove-btn"
              @click.stop="removeRecent(r.id)"
              title="Remove"
              aria-label="Remove this endpoint"
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
            <p class="sf-label sf-label--strong">Trust on first use</p>
            <h3 class="sf-h3 modal-title">Confirm server fingerprint</h3>
            <p class="modal-lede">
              You are connecting to
              <b class="sf-mono">{{ form.endpoint }}</b> over TLS for the first
              time. 请对照 Kelpie 启动日志中打印的 SHA-256 指纹，确认一致后再信任。
            </p>
            <pre class="fingerprint sf-mono">{{ fingerprintDisplay }}</pre>
            <div class="modal-actions">
              <button class="sf-btn ghost" @click="rejectTrust">Cancel</button>
              <button
                class="sf-btn primary"
                :disabled="submitting"
                @click="trustAndRetry"
              >
                {{ submitting ? 'Trusting…' : 'Trust & connect' }}
              </button>
            </div>
          </div>
        </div>
      </Transition>

      <Transition name="sf-fade-up">
        <div v-if="showMismatch && conn.pendingMismatch" class="modal-mask">
          <div class="modal sf-panel danger">
            <p class="sf-label sf-label--strong danger-label">
              Fingerprint mismatch
            </p>
            <h3 class="sf-h3 modal-title">The server identity changed</h3>
            <p class="modal-lede">
              Pinned
              <code class="sf-mono">{{
                formatFingerprint(conn.pendingMismatch)
              }}</code>，但实际得到
              <code class="sf-mono">{{
                formatFingerprint(conn.pendingFingerprint)
              }}</code>。
            </p>
            <p class="sf-small sf-muted">
              只有在你主动轮换了 Kelpie 证书时才选择“忘记旧指纹并重试”。
              否则请终止连接并排查中间人。
            </p>
            <div class="modal-actions">
              <button class="sf-btn ghost" @click="showMismatch = false">
                Close
              </button>
              <button class="sf-btn danger" @click="forgetAndRetry">
                Forget old pin & retry
              </button>
            </div>
          </div>
        </div>
      </Transition>
    </Teleport>
  </section>
</template>

<style scoped>
/*
 * ConnectView：Cohere 式的"左紫右白"分屏入口。
 *   - 左：深紫 hero band（sf-hero-violet），承载品牌 + display serif 标题；
 *   - 右：纯白画布 + 表单 + 最近连接卡片。
 * 主窗口无外边距、无圆角，让两端直接贴到 Wails 窗口边缘，营造 "page" 而非 "card".
 */

.connect-view {
  flex: 1;
  min-height: 100vh;
  width: 100%;
  display: grid;
  grid-template-columns: minmax(360px, 0.85fr) minmax(420px, 1fr);
  background: var(--sf-bg-0);
}

/* ---------- Hero（左栏） ---------- */
.hero {
  position: relative;
  display: flex;
  flex-direction: column;
  justify-content: space-between;
  padding: 56px 64px 48px;
  overflow: hidden;
  min-height: 100vh;
}

.hero::after {
  /* 在 hero 底部加一道极细的深紫→透明分割，营造"section 收尾" */
  content: '';
  position: absolute;
  right: 0;
  top: 0;
  bottom: 0;
  width: 1px;
  background: linear-gradient(
    180deg,
    rgba(255, 255, 255, 0.08) 0%,
    rgba(255, 255, 255, 0.02) 100%
  );
}

/* 品牌：22px 圆角的 dark solid 方块 + uppercase wordmark */
.brand-row {
  display: inline-flex;
  align-items: center;
  gap: 14px;
}
.brand-mark {
  width: 48px;
  height: 48px;
  border-radius: var(--sf-r-xl); /* 签名 22px */
  background: rgba(255, 255, 255, 0.08);
  border: 1px solid rgba(255, 255, 255, 0.14);
  display: grid;
  place-items: center;
  backdrop-filter: blur(6px);
}
.brand-serif {
  font-family: var(--sf-font-display);
  font-size: 26px;
  line-height: 1;
  color: var(--sf-fg-on-violet);
  letter-spacing: -0.5px;
  transform: translateY(-1px);
}
.brand-wordmark {
  display: flex;
  flex-direction: column;
  gap: 2px;
}
.brand-wordmark .sf-label {
  color: var(--sf-fg-on-violet);
}
.brand-caption {
  font-size: 0.8rem;
  color: var(--sf-fg-on-violet-dim);
}

/* 居中区块：display serif hero */
.hero-center {
  display: flex;
  flex-direction: column;
  gap: 20px;
  max-width: 520px;
}
.hero-eyebrow {
  color: var(--sf-fg-on-violet-dim);
}
.hero-title {
  margin: 0;
  font-family: var(--sf-font-display);
  font-weight: 400;
  font-size: clamp(44px, 5.2vw, 64px);
  line-height: 1.02;
  letter-spacing: -1.2px;
  color: var(--sf-fg-on-violet);
}
.hero-title em {
  font-style: italic;
  color: rgba(255, 255, 255, 0.72);
}
.hero-lede {
  margin: 0;
  font-family: var(--sf-font-sans);
  font-size: 1rem;
  line-height: 1.55;
  color: var(--sf-fg-on-violet-dim);
  max-width: 460px;
}

/* 底部 meta：uppercase label + body value */
.hero-bottom {
  display: flex;
  gap: 48px;
  padding-top: 22px;
  border-top: 1px solid rgba(255, 255, 255, 0.1);
}
.hero-meta {
  display: flex;
  flex-direction: column;
  gap: 4px;
}
.hero-meta-value {
  font-family: var(--sf-font-mono);
  font-size: 0.85rem;
  color: var(--sf-fg-on-violet);
  letter-spacing: 0.1px;
}

/* ---------- Panel（右栏） ---------- */
.panel {
  display: flex;
  flex-direction: column;
  gap: 28px;
  padding: 72px 64px 48px;
  overflow-y: auto;
  min-height: 100vh;
}

.panel-head {
  display: flex;
  flex-direction: column;
  gap: 10px;
}
.panel-title {
  margin: 0;
}
.panel-lede {
  margin: 4px 0 0;
  max-width: 46ch;
  color: var(--sf-fg-2);
}
.panel-lede code {
  background: var(--sf-bg-3);
  padding: 1px 6px;
  border-radius: var(--sf-r-xs);
  font-size: 0.88em;
}

/* 表单 */
.form {
  display: flex;
  flex-direction: column;
  gap: 16px;
}
.field {
  display: flex;
  flex-direction: column;
  gap: 6px;
}
.options-row {
  display: flex;
  gap: 24px;
  flex-wrap: wrap;
  padding: 4px 0;
  font-size: 0.875rem;
  color: var(--sf-fg-1);
}
.toggle {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  cursor: pointer;
}
.toggle input {
  accent-color: var(--sf-fg-0);
  width: 16px;
  height: 16px;
}
.submit {
  margin-top: 6px;
  padding: 14px 22px;
  font-size: 0.95rem;
  letter-spacing: 0.1px;
  align-self: flex-start;
}
.error-text {
  margin: 4px 0 0;
  padding: 10px 14px;
  background: var(--sf-danger-bg);
  border: 1px solid rgba(179, 0, 0, 0.18);
  border-radius: var(--sf-r-sm);
  color: var(--sf-danger);
  font-size: 0.85rem;
  line-height: 1.5;
}

/* 最近连接 */
.recent {
  display: flex;
  flex-direction: column;
  gap: 12px;
  min-width: 0;
  padding-top: 28px;
  border-top: 1px solid var(--sf-border-1);
}
.recent-head {
  display: flex;
  justify-content: space-between;
  align-items: center;
}
.recent ul {
  list-style: none;
  padding: 0;
  margin: 0;
  display: flex;
  flex-direction: column;
  gap: 8px;
  max-height: 320px;
  overflow: auto;
}
.recent-item {
  position: relative;
  padding: 14px 16px;
  border-radius: var(--sf-r-md);
  border: 1px solid var(--sf-border-1);
  background: var(--sf-bg-0);
  cursor: pointer;
  transition: border-color var(--sf-dur-fast) var(--sf-ease),
    background var(--sf-dur-fast) var(--sf-ease);
}
.recent-item:hover {
  border-color: var(--sf-fg-0);
  background: var(--sf-bg-1);
}
.recent-item.active {
  border-color: var(--sf-fg-0);
  background: var(--sf-bg-1);
}
.recent-title {
  display: flex;
  align-items: center;
  gap: 8px;
}
.recent-title .recent-label {
  font-weight: 500;
  color: var(--sf-fg-0);
  font-size: 0.95rem;
}
.recent-meta {
  margin-top: 4px;
  font-size: 0.8rem;
  color: var(--sf-fg-3);
}
.remove-btn {
  position: absolute;
  right: 8px;
  top: 8px;
  width: 24px;
  height: 24px;
  border-radius: var(--sf-r-pill);
  background: transparent;
  color: var(--sf-fg-3);
  font-size: 1.15rem;
  line-height: 1;
  padding: 0;
  opacity: 0;
  transition: opacity var(--sf-dur-fast) var(--sf-ease),
    background var(--sf-dur-fast) var(--sf-ease),
    color var(--sf-dur-fast) var(--sf-ease);
}
.recent-item:hover .remove-btn,
.recent-item.active .remove-btn {
  opacity: 1;
}
.remove-btn:hover {
  background: var(--sf-danger-bg);
  color: var(--sf-danger);
}

/* ---------- 模态（dialog radius 8px，不用签名 22px） ---------- */
.modal-mask {
  position: fixed;
  inset: 0;
  background: rgba(17, 17, 28, 0.28);
  backdrop-filter: blur(4px);
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 24px;
  z-index: 1000;
}
.modal {
  max-width: 540px;
  width: 100%;
  padding: 28px 32px;
  border-radius: var(--sf-r-sm); /* dialog = 8px */
  box-shadow: var(--sf-shadow-2);
  animation: sf-fade-up var(--sf-dur) var(--sf-ease);
  display: flex;
  flex-direction: column;
  gap: 8px;
}
.modal.danger {
  border-color: rgba(179, 0, 0, 0.32);
}
.modal-title {
  margin: 2px 0 4px;
}
.modal-lede {
  margin: 4px 0 0;
  color: var(--sf-fg-1);
  font-size: 0.95rem;
  line-height: 1.55;
}
.modal pre.fingerprint {
  padding: 14px 16px;
  margin: 14px 0 10px;
  background: var(--sf-bg-1);
  border-radius: var(--sf-r-sm);
  border: 1px solid var(--sf-border-2);
  color: var(--sf-fg-0);
  font-size: 0.85rem;
  line-height: 1.5;
  word-break: break-all;
  white-space: pre-wrap;
  letter-spacing: 0.5px;
}
.modal-actions {
  display: flex;
  gap: 10px;
  justify-content: flex-end;
  margin-top: 12px;
}
.danger-label {
  color: var(--sf-danger) !important;
}

/* ---------- 响应式 ---------- */
@media (max-width: 960px) {
  .connect-view {
    grid-template-columns: 1fr;
  }
  .hero {
    min-height: 340px;
    padding: 40px 32px 32px;
  }
  .hero-title {
    font-size: 44px;
  }
  .panel {
    padding: 36px 32px 48px;
    min-height: auto;
  }
}
</style>
