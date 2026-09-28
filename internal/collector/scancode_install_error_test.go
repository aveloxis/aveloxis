// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// fakeTools puts executable shell scripts named after tools on a PATH that
// holds nothing else.
func fakeTools(t *testing.T, scripts map[string]string) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("shell-script fakes")
	}
	dir := t.TempDir()
	for name, body := range scripts {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("#!/bin/sh\n"+body+"\n"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("PATH", dir)
}

// TestScancodeFreshInstallReportsTheRealFailure (final whole-tree review F2,
// 2026-09-28): a failed or timed-out pipx install fell through to pip under
// the same (possibly expired) context, and the fall-through ended in a fixed
// "neither pipx nor pip found" — install-tools then told the operator to
// install Python 3.10+ for a PyPI stall, and a real pip failure was hidden.
func TestScancodeFreshInstallReportsTheRealFailure(t *testing.T) {
	t.Run("a timeout is a timeout", func(t *testing.T) {
		fakeTools(t, map[string]string{"pipx": "sleep 5", "pip3": "exit 0"})
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		err := ensureScancodeCurrent(ctx, false)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("err = %v; want the per-tool deadline in the chain", err)
		}
	})
	t.Run("a pip failure is reported as one", func(t *testing.T) {
		fakeTools(t, map[string]string{"pipx": "exit 3", "pip3": "exit 4"})
		err := ensureScancodeCurrent(context.Background(), false)
		if err == nil || strings.Contains(err.Error(), "neither pipx nor pip found") {
			t.Fatalf("err = %v; want the pipx and pip failures, not 'not found'", err)
		}
		if !strings.Contains(err.Error(), "exit status 3") || !strings.Contains(err.Error(), "exit status 4") {
			t.Errorf("err = %v; want both installers' failures", err)
		}
	})
	t.Run("no installer is still 'not found'", func(t *testing.T) {
		fakeTools(t, nil)
		err := ensureScancodeCurrent(context.Background(), false)
		if err == nil || !strings.Contains(err.Error(), "neither pipx nor pip found") {
			t.Errorf("err = %v; want the not-found advice", err)
		}
	})
}
