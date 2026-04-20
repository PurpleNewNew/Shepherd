package main

import (
	"context"
	"log"

	"codeberg.org/agnoie/shepherd/clientui/backend/config"
	"codeberg.org/agnoie/shepherd/clientui/backend/service"
)

// App 聚合 Wails 应用的生命周期回调与 facade API。
// Wails 会把我们在 Bind 里传入的对象方法生成 TS bindings，供前端通过
// window.go.<package>.<Struct>.<Method>(...) 调用。
type App struct {
	api   *service.API
	store *config.Store
}

// NewApp 初始化配置存储并创建 facade；失败直接 fatal（窗口没法打开就没必要继续）。
func NewApp() *App {
	store, err := config.New()
	if err != nil {
		log.Fatalf("stockman: init config store: %v", err)
	}
	return &App{
		store: store,
		api:   service.New(store),
	}
}

// Startup 由 Wails runtime 在 UI 就绪前调用。
func (a *App) Startup(ctx context.Context) {
	a.api.BindContext(ctx)
}

// Shutdown 由 Wails runtime 在窗口关闭前调用。
func (a *App) Shutdown(ctx context.Context) {
	a.api.Shutdown(ctx)
}

// API 暴露 facade 给 Wails.Bind。
func (a *App) API() *service.API { return a.api }
