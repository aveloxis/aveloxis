// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package pidfile manages PID files for aveloxis background processes.
// Each component (serve, web, api) writes its PID to a file at startup
// and removes it on shutdown. The start/stop commands use these files
// to reliably identify and manage background processes.
package pidfile

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

// Dir returns the directory for PID and log files.
// Uses $HOME/.aveloxis/ — created if it doesn't exist.
func Dir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		home = "/tmp"
	}
	dir := filepath.Join(home, ".aveloxis")
	// Best-effort: if this fails, the subsequent Write surfaces a clear
	// error against the missing directory.
	_ = os.MkdirAll(dir, 0o755)
	return dir
}

// Path returns the PID file path for a component (serve, web, api).
func Path(component string) string {
	return filepath.Join(Dir(), "aveloxis-"+component+".pid")
}

// LogPath returns the log file path for a component.
// serve → aveloxis.log (the main log), web → web.log, api → api.log.
func LogPath(component string) string {
	switch component {
	case "serve":
		return filepath.Join(Dir(), "aveloxis.log")
	default:
		return filepath.Join(Dir(), component+".log")
	}
}

// Write creates a PID file with the given process ID.
func Write(path string, pid int) error {
	return os.WriteFile(path, []byte(strconv.Itoa(pid)), 0o644)
}

// Read returns the PID from a PID file.
func Read(path string) (int, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return 0, fmt.Errorf("invalid PID in %s: %w", path, err)
	}
	return pid, nil
}

// Remove deletes a PID file. Best-effort by contract: a stale PID file
// is handled by the liveness check on the next start.
func Remove(path string) {
	_ = os.Remove(path)
}

// IsRunning checks if the process with the given PID is still alive.
//
// v0.27.5 bug fix: the previous implementation called proc.Signal(nil),
// which the os package rejects with "unsupported signal type" for EVERY
// pid — IsRunning reported every process as dead since the function was
// introduced. Consequences before the fix: `aveloxis start` could
// double-start an already-running component (its "already running"
// guard never fired), and `aveloxis stop` always logged "stale PID
// file" and fell through to the pgrep fallback (which masked the bug).
// The documented intent was always "send signal 0"; this makes the code
// do that.
func IsRunning(pid int) bool {
	proc, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	// On Unix, FindProcess always succeeds. Send signal 0 to check if
	// alive: nil error = alive; EPERM = alive but owned by another user
	// (it EXISTS, which is what liveness means here); anything else
	// (ESRCH, ErrProcessDone) = dead.
	err = proc.Signal(syscall.Signal(0))
	if err == nil {
		return true
	}
	return errors.Is(err, syscall.EPERM)
}
