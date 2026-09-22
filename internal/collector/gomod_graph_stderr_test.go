// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestGoModGraphOneLogsToolchainStderr — v0.29.58 (2026-09-22 log
// review, finding 2): 147 "go toolchain invocation failed" warnings
// carried only "exit status 1". cmd.Output() keeps the toolchain's stderr
// on the ExitError; the warning must print it, or the operator cannot
// tell a missing module from a malformed go.mod from a network refusal.
func TestGoModGraphOneLogsToolchainStderr(t *testing.T) {
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Skip("go toolchain not installed")
	}
	work := t.TempDir()
	mod := filepath.Join(work, "broken")
	if err := os.MkdirAll(mod, 0o755); err != nil {
		t.Fatal(err)
	}
	// A go.mod the toolchain rejects at parse time: the failure is local,
	// deterministic and needs no network.
	if err := os.WriteFile(filepath.Join(mod, "go.mod"), []byte("module example.com/broken\n\ngo banana\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	var logBuf bytes.Buffer
	ac := &AnalysisCollector{logger: slog.New(slog.NewTextHandler(&logBuf, nil))}
	_, _, ok := ac.goModGraphOne(context.Background(), goBin, work, mod, map[string]bool{}, nil, nil)
	if ok {
		t.Fatal("goModGraphOne reported success on a go.mod the toolchain rejects")
	}
	logs := logBuf.String()
	if !strings.Contains(logs, "go toolchain invocation failed") {
		t.Fatalf("expected the toolchain warning, got:\n%s", logs)
	}
	if !strings.Contains(logs, "stderr=") || !strings.Contains(logs, "banana") {
		t.Fatalf("warning does not carry the toolchain's stderr (want the parse diagnostic naming 'banana'):\n%s", logs)
	}
}
