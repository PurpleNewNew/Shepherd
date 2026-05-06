declare module '*.vue' {
  import type { DefineComponent } from 'vue';
  const component: DefineComponent<{}, {}, any>;
  export default component;
}

declare global {
  interface Window {
    _wails?: {
      environment?: Record<string, unknown>;
      invoke?: (msg: unknown) => void;
    };
  }
}

export {};
