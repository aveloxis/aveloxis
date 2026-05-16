// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

//go:build !windows

package main

import (
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"
	"time"
)

// installGoroutineDumpHandler registers a SIGUSR1 handler that writes
// runtime.Stack(buf, true) to a timestamped file under dumpDir and
// returns. v0.22.4 item 8 — non-destructive alternative to SIGQUIT
// (which kills the process and loses in-flight collection state).
//
// Operator workflow:
//
//	kill -USR1 $(cat ~/.aveloxis/aveloxis-serve.pid)
//	ls -lh ~/.aveloxis/serve-goroutines-*.txt
//
// The returned function uninstalls the handler — exposed for tests
// and graceful shutdown. The handler goroutine consumes signals in a
// loop so multiple SIGUSR1 sends in quick succession each produce
// their own dump file (operators sometimes want to take two snapshots
// 30 seconds apart to see what changed).
//
// Buffer is 1 MB. runtime.Stack truncates silently when the buffer
// is short, which is acceptable here — the primary use case is
// finding which goroutine is stalled, and 1 MB covers thousands of
// frames at typical stack widths.
func installGoroutineDumpHandler(logger *slog.Logger, dumpDir string) func() {
	ch := make(chan os.Signal, 4)
	signal.Notify(ch, syscall.SIGUSR1)
	done := make(chan struct{})

	go func() {
		for {
			select {
			case <-done:
				return
			case <-ch:
				if err := writeGoroutineDump(dumpDir); err != nil {
					if logger != nil {
						logger.Warn("SIGUSR1 goroutine dump failed", "error", err)
					}
					continue
				}
				if logger != nil {
					logger.Info("goroutine dump written via SIGUSR1", "dir", dumpDir)
				}
			}
		}
	}()

	return func() {
		signal.Stop(ch)
		close(done)
	}
}

// writeGoroutineDump captures every goroutine's stack and writes the
// result to dumpDir/serve-goroutines-<UTC-timestamp>.txt. Returns the
// path on success.
func writeGoroutineDump(dumpDir string) error {
	if err := os.MkdirAll(dumpDir, 0o755); err != nil {
		return err
	}
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	ts := time.Now().UTC().Format("20060102T150405Z")
	path := filepath.Join(dumpDir, "serve-goroutines-"+ts+".txt")
	return os.WriteFile(path, buf[:n], 0o644)
}

// defaultGoroutineDumpDir returns ~/.aveloxis when the home directory
// is available, falling back to os.TempDir() otherwise. The directory
// is created lazily by writeGoroutineDump.
func defaultGoroutineDumpDir() string {
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		return filepath.Join(home, ".aveloxis")
	}
	return os.TempDir()
}
