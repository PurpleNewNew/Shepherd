package printer

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/fatih/color"
)

type Hook func(level, message string)

var (
	warningPrinter func(string, ...interface{})
	successPrinter func(string, ...interface{})
	failPrinter    func(string, ...interface{})
	printerMu      sync.RWMutex
	hookMu         sync.RWMutex
	hooks          map[uint64]Hook
	hookSeq        atomic.Uint64
)

func InitPrinter() {
	printerMu.Lock()
	defer printerMu.Unlock()
	warningPrinter = color.New(color.FgYellow).PrintfFunc()
	successPrinter = color.New(color.FgGreen).PrintfFunc()
	failPrinter = color.New(color.FgRed).PrintfFunc()
}

func Warning(format string, a ...interface{}) {
	printerMu.RLock()
	printer := warningPrinter
	printerMu.RUnlock()
	logWithPrinter("warning", printer, format, a...)
}

func Success(format string, a ...interface{}) {
	printerMu.RLock()
	printer := successPrinter
	printerMu.RUnlock()
	logWithPrinter("success", printer, format, a...)
}

func Fail(format string, a ...interface{}) {
	printerMu.RLock()
	printer := failPrinter
	printerMu.RUnlock()
	logWithPrinter("fail", printer, format, a...)
}

func logWithPrinter(level string, printerFunc func(string, ...interface{}), format string, a ...interface{}) {
	msg := fmt.Sprintf(format, a...)
	if printerFunc != nil {
		printerFunc(format, a...)
	} else {
		fmt.Print(msg)
	}
	dispatchHooks(level, msg)
}

func dispatchHooks(level, msg string) {
	hookMu.RLock()
	defer hookMu.RUnlock()
	for _, hook := range hooks {
		if hook != nil {
			hook(level, msg)
		}
	}
}

func RegisterHook(h Hook) func() {
	if h == nil {
		return func() {}
	}
	id := hookSeq.Add(1)
	hookMu.Lock()
	if hooks == nil {
		hooks = make(map[uint64]Hook)
	}
	hooks[id] = h
	hookMu.Unlock()
	return func() {
		hookMu.Lock()
		delete(hooks, id)
		hookMu.Unlock()
	}
}

func TestOnlyReplaceFailPrinter(fn func(string, ...interface{})) func() {
	return replacePrinter(&failPrinter, fn)
}

func replacePrinter(target *func(string, ...interface{}), fn func(string, ...interface{})) func() {
	printerMu.Lock()
	prev := *target
	*target = fn
	printerMu.Unlock()
	return func() {
		printerMu.Lock()
		*target = prev
		printerMu.Unlock()
	}
}
