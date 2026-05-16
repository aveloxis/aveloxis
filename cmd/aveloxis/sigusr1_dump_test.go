// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

//go:build !windows

package main

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestSIGUSR1HandlerRegistered — v0.22.4 item 8.
//
// Pin that runServe wires a SIGUSR1 handler that calls
// installGoroutineDumpHandler (or equivalent named symbol). When the
// operator sends `kill -USR1 $(pidof aveloxis)`, the binary writes
// runtime.Stack(buf, true) to a timestamped file without exiting.
//
// Non-destructive alternative to SIGQUIT — SIGQUIT kills the process
// and forces every in-flight collection to lose progress.
func TestSIGUSR1HandlerRegistered(t *testing.T) {
	mainSrc, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	main := string(mainSrc)

	// runServe must invoke the handler-installer.
	idx := strings.Index(main, "func runServe(")
	if idx < 0 {
		t.Fatal("runServe not found in main.go")
	}
	end := idx + 6000
	if end > len(main) {
		end = len(main)
	}
	fn := main[idx:end]

	if !strings.Contains(fn, "installGoroutineDumpHandler") {
		t.Error("runServe must call installGoroutineDumpHandler so SIGUSR1 produces a non-destructive goroutine dump (alternative to SIGQUIT which kills the process)")
	}

	// The handler implementation may live in main.go or a sibling file.
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		b, _ := os.ReadFile(e.Name())
		s := string(b)
		if strings.Contains(s, "func installGoroutineDumpHandler(") &&
			strings.Contains(s, "syscall.SIGUSR1") &&
			strings.Contains(s, "runtime.Stack(") {
			found = true
			break
		}
	}
	if !found {
		t.Error("installGoroutineDumpHandler must exist, register syscall.SIGUSR1, and call runtime.Stack(buf, true)")
	}
}

// TestInstallGoroutineDumpHandlerWritesFile — behavioral.
//
// Direct invocation of the dump-writer helper (the part that runs
// when the signal fires). Sends a signal to the registered handler
// via a real signal.Notify channel, then asserts a dump file
// appeared under the configured directory containing a real Go
// goroutine stack header.
func TestInstallGoroutineDumpHandlerWritesFile(t *testing.T) {
	dir := t.TempDir()

	// Spawn a goroutine the dump will surely capture so we can assert
	// the stack content is meaningful, not just any-bytes-on-disk.
	stop := make(chan struct{})
	go func() {
		<-stop
	}()
	defer close(stop)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	uninstall := installGoroutineDumpHandler(logger, dir)
	defer uninstall()

	// Fire SIGUSR1 at ourselves.
	if err := syscall.Kill(syscall.Getpid(), syscall.SIGUSR1); err != nil {
		t.Fatalf("kill SIGUSR1: %v", err)
	}

	// Wait for the file to appear.
	deadline := time.Now().Add(2 * time.Second)
	var dumpPath string
	for time.Now().Before(deadline) {
		entries, err := os.ReadDir(dir)
		if err == nil {
			for _, e := range entries {
				if strings.HasPrefix(e.Name(), "serve-goroutines-") {
					dumpPath = filepath.Join(dir, e.Name())
					break
				}
			}
		}
		if dumpPath != "" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if dumpPath == "" {
		t.Fatal("no serve-goroutines-*.txt file written within deadline")
	}
	data, err := os.ReadFile(dumpPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "goroutine ") {
		t.Errorf("dump does not look like runtime.Stack output (no 'goroutine ' header):\n%s", data[:min(len(data), 200)])
	}
	// Sanity: file is at least one goroutine's worth of bytes.
	if len(data) < 50 {
		t.Errorf("dump suspiciously short (%d bytes)", len(data))
	}
	_ = runtime.NumGoroutine() // keep this import used
}
