//go:build !windows

package process

import (
	"os"
	"os/exec"

	"github.com/creack/pty"
)

type unixPTYControl struct {
	file *os.File
}

func (c *unixPTYControl) Resize(cols, rows uint16) error {
	if c == nil || c.file == nil {
		return nil
	}
	return pty.Setsize(c.file, &pty.Winsize{Cols: cols, Rows: rows})
}

func (c *unixPTYControl) Close() error {
	if c == nil || c.file == nil {
		return nil
	}
	return c.file.Close()
}

func startPTYCommand(cmd *exec.Cmd) (*ptyStartResult, error) {
	ptmx, err := pty.Start(cmd)
	if err != nil {
		return nil, err
	}
	return &ptyStartResult{
		file:    ptmx,
		in:      ptmx,
		out:     ptmx,
		ctrl:    &unixPTYControl{file: ptmx},
		cmd:     cmd,
		process: cmd.Process,
	}, nil
}
