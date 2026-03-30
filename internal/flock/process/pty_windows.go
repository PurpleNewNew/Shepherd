//go:build windows

package process

import (
	"errors"
	"os"
	"os/exec"
	"syscall"

	"github.com/ActiveState/termtest/conpty"
	"golang.org/x/sys/windows"
)

type conptyControl struct {
	con *conpty.ConPty
}

func (c *conptyControl) Resize(cols, rows uint16) error {
	if c == nil || c.con == nil {
		return nil
	}
	return c.con.Resize(cols, rows)
}

func (c *conptyControl) Close() error {
	if c == nil || c.con == nil {
		return nil
	}
	return c.con.Close()
}

func startPTYCommand(cmd *exec.Cmd) (*ptyStartResult, error) {
	const (
		defaultCols = 120
		defaultRows = 30
	)

	con, err := conpty.New(int16(defaultCols), int16(defaultRows))
	if err != nil {
		if isConPTYUnsupported(err) {
			return nil, errPTYUnsupported
		}
		return nil, err
	}

	argv0 := cmd.Path
	if argv0 == "" && len(cmd.Args) > 0 {
		argv0 = cmd.Args[0]
	}
	if argv0 == "" {
		_ = con.Close()
		return nil, errors.New("missing command path")
	}

	args := append([]string(nil), cmd.Args...)
	if len(args) > 0 {
		args = args[1:]
	}

	attr := &syscall.ProcAttr{
		Dir: cmd.Dir,
		Env: cmd.Env,
	}
	if len(attr.Env) == 0 {
		attr.Env = os.Environ()
	}

	pd, handle, err := con.Spawn(argv0, args, attr)
	if err != nil {
		_ = con.Close()
		if isConPTYUnsupported(err) {
			return nil, errPTYUnsupported
		}
		return nil, err
	}

	process, err := os.FindProcess(pd)
	if err != nil {
		windows.CloseHandle(windows.Handle(handle))
		_ = con.Close()
		return nil, err
	}

	// FindProcess 会复制进程句柄，关闭原句柄以避免泄漏。
	windows.CloseHandle(windows.Handle(handle))

	return &ptyStartResult{
		in:      con.InPipe(),
		out:     con.OutPipe(),
		ctrl:    &conptyControl{con: con},
		process: process,
	}, nil
}

func isConPTYUnsupported(err error) bool {
	return errors.Is(err, syscall.ERROR_INVALID_PARAMETER) ||
		errors.Is(err, syscall.ERROR_PROC_NOT_FOUND) ||
		errors.Is(err, syscall.ERROR_NOT_SUPPORTED) ||
		errors.Is(err, syscall.ERROR_CALL_NOT_IMPLEMENTED)
}
