// Package pidfile manages PID files for aveloxis background processes.
// Each component (serve, web, api) writes its PID to a file at startup
// and removes it on shutdown. The start/stop commands use these files
// to reliably identify and manage background processes.
package pidfile

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Dir returns the directory for PID and log files.
// Uses $HOME/.aveloxis/ — created if it doesn't exist.
func Dir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		home = "/tmp"
	}
	dir := filepath.Join(home, ".aveloxis")
	os.MkdirAll(dir, 0o755)
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

// Remove deletes a PID file. No error if it doesn't exist.
func Remove(path string) {
	os.Remove(path)
}

// IsRunning checks if the process with the given PID is still alive.
func IsRunning(pid int) bool {
	proc, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	// On Unix, FindProcess always succeeds. Send signal 0 to check if alive.
	return proc.Signal(nil) == nil
}
