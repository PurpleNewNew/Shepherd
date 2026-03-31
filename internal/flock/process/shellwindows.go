//go:build windows

package process

import (
	"os/exec"
	"syscall"
)

func prepareShellCommand(path string, args []string) *exec.Cmd {
	cmd := exec.Command(path, args...)

	cmd.SysProcAttr = &syscall.SysProcAttr{
		HideWindow:    true,
		CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP | syscall.CREATE_UNICODE_ENVIRONMENT,
	}

	return cmd
}
