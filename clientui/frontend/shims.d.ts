declare module '*.vue' {
  import type { DefineComponent } from 'vue';
  const component: DefineComponent<{}, {}, any>;
  export default component;
}

// Wails runtime 在构建期不一定存在，此处声明全局以便 TS 放行。
declare global {
  interface Window {
    go?: Record<string, unknown>;
    runtime?: {
      EventsOn: (name: string, cb: (data: unknown) => void) => () => void;
      EventsOff: (name: string) => void;
      EventsEmit: (name: string, ...data: unknown[]) => void;
      LogInfo: (msg: string) => void;
    };
  }
}

export {};
