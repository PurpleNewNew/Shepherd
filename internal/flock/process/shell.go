package process

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"syscall"
	"time"

	"codeberg.org/agnoie/shepherd/internal/flock/initial"
	"codeberg.org/agnoie/shepherd/internal/flock/manager"
	"codeberg.org/agnoie/shepherd/pkg/utils"
	"codeberg.org/agnoie/shepherd/protocol"
)

const (
	shellReasonEOF         = "process-exited"
	shellReasonIdleTimeout = "idle-timeout"
)

type shellCandidate struct {
	Path string
	Args []string
}

// shellSessionFactory allows tests to override session creation.
var shellSessionFactory = launchShellSession

// TestOnlySetShellSessionFactory allows tests to override how shell sessions are created.
// Passing nil restores the default launcher.
func TestOnlySetShellSessionFactory(factory func(*initial.Options, string, uint16) (*manager.ShellSession, error)) {
	if factory == nil {
		shellSessionFactory = launchShellSession
		return
	}
	shellSessionFactory = factory
}

var (
	errPTYUnsupported = errors.New("pty unsupported")
)

// Shell 会话通过 STREAM_OPEN(kind=shell) 发起。
type ShellReqWithStream struct {
	SessionID string
	Mode      uint16
	Resume    bool
}

// ShellCommandMsg 作为本地消息类型，通过 ShellManager 传递命令文本。
type ShellCommandMsg struct {
	SessionID string
	Command   string
}

type ptyStartResult struct {
	file    *os.File
	in      io.WriteCloser
	out     io.ReadCloser
	ctrl    manager.PTYControl
	cmd     *exec.Cmd
	process *os.Process
}

func DispatchShellMess(ctx context.Context, mgr *manager.Manager, options *initial.Options) {
	for {
		var message interface{}
		select {
		case <-ctx.Done():
			return
		case message = <-mgr.ShellManager.ShellMessChan:
		}

		switch mess := message.(type) {
		case *ShellReqWithStream:
			sessionID := normalizeShellSessionID(mgr, mess.SessionID)
			requestedMode := normalizeShellMode(mess.Mode)
			logger.Infof("shell(stream) request: node=%s sessionID=%s resume=%t mode=%d",
				activeNodeUUID(mgr), sessionID, mess.Resume, requestedMode)
			session, created, err := ensureShellSession(mgr, options, sessionID, mess.Resume, requestedMode)
			if err != nil {
				agentWarnRuntime(mgr, "AGENT_SHELL_SESSION", false, err, "failed to ensure shell session %s", sessionID)
			}
			if err == nil && created && session != nil {
				go streamShellOutput(mgr, session)
				go monitorShellSession(mgr, session)
			}
			continue
		case *ShellCommandMsg:
			logger.Infof("shell command received: sessionID=%s size=%d preview=%q", mess.SessionID, len(mess.Command), truncateForLog(mess.Command, 80))
			if err := writeShellCommand(mgr, mess.SessionID, mess.Command); err != nil {
				agentWarnRuntime(mgr, "AGENT_SHELL_WRITE_STDIN", true, err, "failed to write shell command")
			}
		}
	}
}

func normalizeShellSessionID(mgr *manager.Manager, requested string) string {
	id := strings.TrimSpace(requested)
	if id != "" {
		return id
	}
	if mgr != nil {
		if uuid := mgr.ActiveUUID(); uuid != "" {
			return uuid
		}
	}
	return utils.GenerateUUID()
}

func normalizeShellMode(mode uint16) uint16 {
	if mode == protocol.ShellModePTY {
		return protocol.ShellModePTY
	}
	return protocol.ShellModePipe
}

func ensureShellSession(mgr *manager.Manager, options *initial.Options, sessionID string, resume bool, mode uint16) (*manager.ShellSession, bool, error) {
	if mgr == nil || mgr.ShellManager == nil {
		return nil, false, errors.New("shell manager not initialized")
	}

	if existing, ok := mgr.ShellManager.GetSession(sessionID); ok && sessionAlive(existing) {
		updateShellActivity(existing)
		return existing, false, nil
	}

	if resume {
		return nil, false, fmt.Errorf("no existing session %s to resume", sessionID)
	}

	session, err := shellSessionFactory(options, sessionID, mode)
	if err != nil {
		return nil, false, err
	}

	mgr.ShellManager.SetSession(sessionID, session)
	updateShellActivity(session)
	return session, true, nil
}

func launchShellSession(options *initial.Options, sessionID string, mode uint16) (*manager.ShellSession, error) {
	usePTY := mode == protocol.ShellModePTY
	charset := "utf-8"
	if options != nil && options.Charset != "" {
		charset = options.Charset
	}

	candidates := availableShellCandidates()
	var lastErr error

	for _, candidate := range candidates {
		path, err := exec.LookPath(candidate.Path)
		if err != nil {
			lastErr = err
			continue
		}

		cmd := prepareShellCommand(path, candidate.Args)

		if usePTY {
			result, err := startPTYCommand(cmd)
			if err != nil {
				lastErr = err
				continue
			}
			now := time.Now()
			session := &manager.ShellSession{
				ID:         sessionID,
				Cmd:        result.cmd,
				Process:    result.process,
				PTY:        result.file,
				PTYIn:      result.in,
				PTYOut:     result.out,
				PTYControl: result.ctrl,
				Mode:       protocol.ShellModePTY,
				Charset:    charset,
				CreatedAt:  now,
				Done:       make(chan struct{}),
			}
			session.SetLastActivity(now)
			return session, nil
		}

		stdout, err := cmd.StdoutPipe()
		if err != nil {
			lastErr = err
			continue
		}
		stdin, err := cmd.StdinPipe()
		if err != nil {
			_ = stdout.Close()
			lastErr = err
			continue
		}
		cmd.Stderr = cmd.Stdout

		if err := cmd.Start(); err != nil {
			_ = stdin.Close()
			_ = stdout.Close()
			lastErr = err
			continue
		}

		now := time.Now()
		session := &manager.ShellSession{
			ID:        sessionID,
			Cmd:       cmd,
			Process:   cmd.Process,
			Stdin:     stdin,
			Stdout:    stdout,
			Mode:      protocol.ShellModePipe,
			Charset:   charset,
			CreatedAt: now,
			Done:      make(chan struct{}),
		}
		session.SetLastActivity(now)
		return session, nil
	}

	if lastErr == nil {
		lastErr = errors.New("no available shell candidates")
	}
	return nil, lastErr
}

func HandleShellResize(mgr *manager.Manager, sessionID string, rows, cols uint16) {
	if mgr == nil || mgr.ShellManager == nil {
		return
	}
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" || rows == 0 || cols == 0 {
		return
	}
	session, ok := mgr.ShellManager.GetSession(sessionID)
	if !ok || session == nil || !sessionAlive(session) {
		return
	}
	if session.Mode != protocol.ShellModePTY {
		return
	}
	session.WinRows = rows
	session.WinCols = cols
	updateShellActivity(session)
	if session.PTYControl != nil {
		_ = session.PTYControl.Resize(cols, rows)
	}
}

func availableShellCandidates() []shellCandidate {
	switch utils.CheckSystem() {
	case 0x01:
		return []shellCandidate{
			{Path: "cmd.exe", Args: []string{"/K"}},
			{Path: "powershell.exe", Args: []string{"-NoExit"}},
		}
	default:
		if runtime.GOARCH == "386" || runtime.GOARCH == "amd64" {
			return []shellCandidate{
				{Path: "/bin/bash", Args: []string{"-i"}},
				{Path: "/bin/sh", Args: []string{"-i"}},
				{Path: "/bin/ash", Args: []string{"-i"}},
			}
		}
		return []shellCandidate{
			{Path: "/bin/sh", Args: []string{"-i"}},
			{Path: "/bin/ash", Args: []string{"-i"}},
		}
	}
}

func sessionAlive(session *manager.ShellSession) bool {
	if session == nil || session.Closed {
		return false
	}
	if session.Cmd == nil {
		return session.Process != nil
	}
	if ps := session.Cmd.ProcessState; ps != nil && ps.Exited() {
		return false
	}
	return true
}

func updateShellActivity(session *manager.ShellSession) {
	if session == nil {
		return
	}
	session.TouchActivity()
}

func writeShellCommand(mgr *manager.Manager, sessionID string, command string) error {
	if mgr == nil || mgr.ShellManager == nil {
		return errors.New("shell manager not ready")
	}
	id := normalizeShellSessionID(mgr, sessionID)
	session, ok := mgr.ShellManager.GetSession(id)
	if !ok || !sessionAlive(session) {
		return fmt.Errorf("shell session %s not active", id)
	}

	updateShellActivity(session)
	payload := command
	if strings.EqualFold(session.Charset, "gbk") {
		payload = utils.ConvertStr2GBK(command)
	}

	var writer io.Writer
	if session.Mode == protocol.ShellModePTY {
		if session.PTYIn != nil {
			writer = session.PTYIn
		} else {
			writer = session.PTY
		}
	} else {
		writer = session.Stdin
	}
	if writer == nil {
		return errors.New("stdin not available")
	}

	if session.Mode != protocol.ShellModePTY && runtime.GOOS == "windows" {
		payload = strings.ReplaceAll(payload, "\r\n", "\n")
		payload = strings.ReplaceAll(payload, "\n", "\r\n")
	}

	_, err := writer.Write([]byte(payload))
	return err
}

func streamShellOutput(mgr *manager.Manager, session *manager.ShellSession) {
	defer closeShellSession(mgr, session)

	var reader io.Reader
	if session.Mode == protocol.ShellModePTY {
		if session.PTYOut != nil {
			reader = session.PTYOut
		} else {
			reader = session.PTY
		}
	} else {
		reader = session.Stdout
	}
	if reader == nil {
		return
	}

	buffer := make([]byte, 4096)
	// 若该 session 绑定了 streamID，则输出走 STREAM_DATA。
	var streamID uint32
	if mgr != nil && mgr.ShellManager != nil {
		streamID = mgr.ShellManager.StreamForSession(session.ID)
	}
	for {
		count, err := reader.Read(buffer)
		if count > 0 {
			result := string(buffer[:count])
			if strings.EqualFold(session.Charset, "gbk") {
				result = utils.ConvertGBK2Str(result)
			}
			if streamID != 0 {
				// 发送按序自增的帧
				seq := uint32(0)
				if mgr != nil && mgr.ShellManager != nil {
					seq = mgr.ShellManager.NextSeqForSession(session.ID)
				}
				sendStreamData(mgr, streamID, seq, []byte(result))
			} else {
				sendShellResult(mgr, session.ID, result)
			}
			updateShellActivity(session)
		}
		if err != nil {
			// Treat PTY EOF (EIO on Linux /dev/ptmx) as normal closure
			if errors.Is(err, io.EOF) || isPTYEOF(err) {
				if session.CloseReason == "" {
					session.CloseReason = shellReasonEOF
				}
			} else {
				session.CloseReason = err.Error()
			}
			if streamID != 0 {
				sendStreamClose(mgr, streamID, 0, "ok")
				if mgr != nil && mgr.ShellManager != nil {
					mgr.ShellManager.ClearStreamForSession(session.ID)
				}
			}
			return
		}
	}
}

// isPTYEOF returns true when the error represents a normal PTY end-of-file
// condition (for instance, EIO from /dev/ptmx after the slave is closed).
func isPTYEOF(err error) bool {
	if err == nil {
		return false
	}
	// Direct EIO
	if errors.Is(err, syscall.EIO) {
		return true
	}
	// Unwrap os.PathError -> errno
	var perr *os.PathError
	if errors.As(err, &perr) {
		if errors.Is(perr.Err, syscall.EIO) {
			return true
		}
	}
	return false
}

func monitorShellSession(mgr *manager.Manager, session *manager.ShellSession) {
	if mgr == nil || session == nil {
		return
	}
	timeout := mgr.ShellManager.IdleTimeout()
	if timeout <= 0 {
		return
	}
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-session.Done:
			return
		case <-ticker.C:
			if session.Closed {
				return
			}
			if time.Since(session.LastActivity()) >= timeout {
				session.CloseReason = shellReasonIdleTimeout
				terminateShellProcess(session)
				return
			}
		}
	}
}

func closeShellSession(mgr *manager.Manager, session *manager.ShellSession) {
	if session == nil {
		return
	}

	if session.Cmd != nil {
		_ = session.Cmd.Wait()
	} else if session.Process != nil {
		_, _ = session.Process.Wait()
	}

	if session.PTYControl != nil {
		_ = session.PTYControl.Close()
	}
	if session.PTY != nil {
		_ = session.PTY.Close()
	}
	if session.PTYOut != nil && session.PTYOut != session.PTY {
		_ = session.PTYOut.Close()
	}
	if session.PTYIn != nil && session.PTYIn != session.PTY {
		_ = session.PTYIn.Close()
	}
	mgr.ShellManager.DeleteSession(session.ID, session.CloseReason)
	session.Closed = true
	if session.Stdout != nil {
		_ = session.Stdout.Close()
	}
	if session.Done != nil {
		close(session.Done)
	}
	if session.Stdin != nil {
		_ = session.Stdin.Close()
	}

	reason := session.CloseReason
	if reason == "" {
		reason = shellReasonEOF
	}
	sendShellExit(mgr, session.ID, reason)
}

func terminateShellProcess(session *manager.ShellSession) {
	if session == nil {
		return
	}
	if session.Stdin != nil {
		_ = session.Stdin.Close()
	}
	if session.Stdout != nil {
		_ = session.Stdout.Close()
	}
	if session.PTY != nil {
		_ = session.PTY.Close()
	}
	if session.PTYControl != nil {
		_ = session.PTYControl.Close()
	}
	if session.PTYOut != nil && session.PTYOut != session.PTY {
		_ = session.PTYOut.Close()
	}
	if session.PTYIn != nil && session.PTYIn != session.PTY {
		_ = session.PTYIn.Close()
	}
	if session.Cmd != nil && session.Cmd.Process != nil {
		_ = session.Cmd.Process.Kill()
	} else if session.Process != nil {
		_ = session.Process.Kill()
	}
}

func sendShellResult(mgr *manager.Manager, sessionID string, result string) {
	_ = mgr
	_ = sessionID
	_ = result
}

func sendShellExit(mgr *manager.Manager, sessionID string, reason string) {
	_ = mgr
	_ = sessionID
	_ = reason
}

func activeNodeUUID(mgr *manager.Manager) string {
	if mgr == nil {
		return ""
	}
	return mgr.ActiveUUID()
}

func truncateForLog(s string, limit int) string {
	if limit <= 0 || len(s) <= limit {
		return s
	}
	return s[:limit] + "..."
}
