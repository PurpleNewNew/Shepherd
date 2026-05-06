package main

import (
	"embed"
	"io/fs"
	"log"
	"sync/atomic"
	"time"

	"github.com/wailsapp/wails/v3/pkg/application"
	"github.com/wailsapp/wails/v3/pkg/events"

	"codeberg.org/agnoie/shepherd/clientui/backend/service"
)

//go:embed all:frontend/dist
var assets embed.FS

func main() {
	var wailsApp *application.App
	var connectWindow application.Window
	var mainWindow application.Window
	var mainWindowActive atomic.Bool

	showMainWindow := func() {
		mainWindowActive.Store(true)
		if mainWindow != nil {
			mainWindow.Show()
			mainWindow.Focus()
		}
		if connectWindow != nil {
			connectWindow.Hide()
		}
	}

	showConnectWindow := func() {
		mainWindowActive.Store(false)
		if connectWindow != nil {
			connectWindow.Show()
			connectWindow.Focus()
		}
		if mainWindow != nil {
			mainWindow.Hide()
		}
	}

	stockman := NewApp(service.Hooks{
		Emit: func(topic string, payload any) {
			if wailsApp != nil {
				wailsApp.Event.Emit(topic, payload)
			}
		},
		ShowMainWindow:    showMainWindow,
		ShowConnectWindow: showConnectWindow,
	})
	assetFS, err := fs.Sub(assets, "frontend/dist")
	if err != nil {
		log.Fatalf("stockman: prepare frontend assets: %v", err)
	}

	wailsApp = application.New(application.Options{
		Name:        "Stockman",
		Description: "Shepherd Stockman desktop console",
		Assets: application.AssetOptions{
			Handler: application.AssetFileServerFS(assetFS),
		},
		Services: []application.Service{
			application.NewService(stockman.API()),
		},
		Mac: application.MacOptions{
			ActivationPolicy: application.ActivationPolicyRegular,
		},
		OnShutdown: stockman.Shutdown,
	})

	connectWindow = wailsApp.Window.NewWithOptions(application.WebviewWindowOptions{
		Name:            "connect",
		Title:           "Stockman Connect",
		Width:           460,
		Height:          390,
		MinWidth:        460,
		MinHeight:       390,
		MaxWidth:        520,
		MaxHeight:       460,
		DisableResize:   false,
		URL:             "/?window=connect",
		InitialPosition: application.WindowCentered,
		BackgroundType:  application.BackgroundTypeSolid,
		BackgroundColour: application.RGBA{
			Red: 18, Green: 24, Blue: 33, Alpha: 255,
		},
		Mac: application.MacWindow{
			Appearance: application.NSAppearanceNameDarkAqua,
			TitleBar:   application.MacTitleBarDefault,
		},
	})
	connectWindow.OnWindowEvent(events.Mac.WebViewDidFinishNavigation, func(*application.WindowEvent) {
		showConnectWindow()
	})

	mainWindow = wailsApp.Window.NewWithOptions(application.WebviewWindowOptions{
		Name:            "main",
		Title:           "Stockman · Shepherd 控制台",
		Width:           1320,
		Height:          860,
		MinWidth:        1080,
		MinHeight:       680,
		Hidden:          true,
		URL:             "/?window=main",
		InitialPosition: application.WindowCentered,
		BackgroundType:  application.BackgroundTypeSolid,
		BackgroundColour: application.RGBA{
			Red: 255, Green: 255, Blue: 255, Alpha: 255,
		},
		Mac: application.MacWindow{
			Appearance: application.NSAppearanceNameAqua,
			TitleBar:   application.MacTitleBarHiddenInset,
		},
	})

	go func() {
		for range 12 {
			time.Sleep(250 * time.Millisecond)
			if mainWindowActive.Load() {
				return
			}
			showConnectWindow()
		}
	}()

	if err := wailsApp.Run(); err != nil {
		log.Fatalf("stockman: wails run: %v", err)
	}
}
