// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package pidfile

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// TestIsRunning_LiveProcess pins the v0.27.5 fix: the pre-fix
// implementation called proc.Signal(nil), which errors for EVERY pid
// ("unsupported signal type"), so IsRunning reported every process —
// including this test's own — as dead. `aveloxis start`'s
// already-running guard never fired and `aveloxis stop` always logged
// "stale PID file". Caught by run-scorecard's serve-guard behavioral
// test on first run.
func TestIsRunning_LiveProcess(t *testing.T) {
	if !IsRunning(os.Getpid()) {
		t.Error("IsRunning(own pid) = false — a live process must report as running " +
			"(pre-v0.27.5 proc.Signal(nil) bug: every process reported dead)")
	}
}

func TestIsRunning_DeadProcess(t *testing.T) {
	// Spawn a process that exits immediately, then check its pid.
	cmd := exec.Command("true")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	pid := cmd.Process.Pid
	if err := cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	if IsRunning(pid) {
		t.Errorf("IsRunning(%d) = true for an exited (reaped) process", pid)
	}
}

func TestWrite_CreatesFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.pid")

	if err := Write(path, 12345); err != nil {
		t.Fatalf("Write: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(data) != "12345" {
		t.Errorf("content = %q, want 12345", data)
	}
}

func TestRead_ValidPID(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.pid")
	os.WriteFile(path, []byte("99999"), 0o644)

	pid, err := Read(path)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if pid != 99999 {
		t.Errorf("pid = %d, want 99999", pid)
	}
}

func TestRead_MissingFile(t *testing.T) {
	pid, err := Read("/tmp/nonexistent-aveloxis-test.pid")
	if err == nil {
		t.Error("expected error for missing file")
	}
	if pid != 0 {
		t.Errorf("pid = %d, want 0", pid)
	}
}

func TestRead_InvalidContent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.pid")
	os.WriteFile(path, []byte("not-a-number"), 0o644)

	_, err := Read(path)
	if err == nil {
		t.Error("expected error for invalid content")
	}
}

func TestRemove(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.pid")
	os.WriteFile(path, []byte("12345"), 0o644)

	Remove(path)

	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Error("file should be removed")
	}
}

func TestRemove_NonexistentIsNoOp(t *testing.T) {
	// Should not panic or error.
	Remove("/tmp/nonexistent-aveloxis-test.pid")
}

func TestPath_DefaultDir(t *testing.T) {
	p := Path("serve")
	if p == "" {
		t.Error("Path should not be empty")
	}
	if filepath.Base(p) != "aveloxis-serve.pid" {
		t.Errorf("filename = %q, want aveloxis-serve.pid", filepath.Base(p))
	}
}

func TestPath_Components(t *testing.T) {
	tests := []struct {
		component string
		wantFile  string
	}{
		{"serve", "aveloxis-serve.pid"},
		{"web", "aveloxis-web.pid"},
		{"api", "aveloxis-api.pid"},
	}
	for _, tt := range tests {
		got := filepath.Base(Path(tt.component))
		if got != tt.wantFile {
			t.Errorf("Path(%q) file = %q, want %q", tt.component, got, tt.wantFile)
		}
	}
}

func TestLogPath_Components(t *testing.T) {
	tests := []struct {
		component string
		wantFile  string
	}{
		{"serve", "aveloxis.log"},
		{"web", "web.log"},
		{"api", "api.log"},
	}
	for _, tt := range tests {
		got := filepath.Base(LogPath(tt.component))
		if got != tt.wantFile {
			t.Errorf("LogPath(%q) file = %q, want %q", tt.component, got, tt.wantFile)
		}
	}
}
