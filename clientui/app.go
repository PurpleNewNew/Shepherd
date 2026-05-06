package main

import (
	"log"

	"codeberg.org/agnoie/shepherd/clientui/backend/config"
	"codeberg.org/agnoie/shepherd/clientui/backend/service"
)

// App 聚合 Stockman 的配置存储与 facade API。
type App struct {
	api   *service.API
	store *config.Store
}

// NewApp 初始化配置存储并创建 facade；失败直接 fatal（窗口没法打开就没必要继续）。
func NewApp(hooks service.Hooks) *App {
	store, err := config.New()
	if err != nil {
		log.Fatalf("stockman: init config store: %v", err)
	}
	return &App{
		store: store,
		api:   service.New(store, hooks),
	}
}

// Shutdown 由 Wails v3 在应用退出前调用。
func (a *App) Shutdown() {
	_ = a.api.ServiceShutdown()
}

// API 暴露 facade 给 Wails v3 service binding。
func (a *App) API() *service.API { return a.api }
