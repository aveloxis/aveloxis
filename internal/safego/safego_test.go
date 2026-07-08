// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package safego

import (
	"bytes"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

// A panic inside the wrapped function must not crash the process, must
// be logged at ERROR with the goroutine name, and must include a stack
// trace so operators can diagnose without a core dump.
func TestGoRecoversAndLogsPanic(t *testing.T) {
	var buf bytes.Buffer
	var mu sync.Mutex
	logger := slog.New(slog.NewTextHandler(&lockedWriter{w: &buf, mu: &mu}, nil))

	Go(logger, "exploding-task", func() {
		panic("boom: nil map write")
	})

	// fn's defers run BEFORE safego's recover+log during unwind, so a
	// done-channel closed inside fn races the log write — poll instead.
	var out string
	for range 200 {
		mu.Lock()
		out = buf.String()
		mu.Unlock()
		if strings.Contains(out, "stack=") {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	for _, needle := range []string{"goroutine panic recovered", "exploding-task", "boom: nil map write", "safego"} {
		if !strings.Contains(out, needle) {
			t.Errorf("panic log missing %q; got:\n%s", needle, out)
		}
	}
	if !strings.Contains(out, "stack=") {
		t.Errorf("panic log must carry the stack trace; got:\n%s", out)
	}
}

// The wrapped function's own defers must still run during panic unwind
// (worker pools rely on deferred wg.Done for slot accounting).
func TestGoRunsInnerDefersBeforeRecovering(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	deferRan := make(chan struct{})
	Go(logger, "defer-check", func() {
		defer close(deferRan)
		panic("unwind")
	})
	<-deferRan // hangs (test timeout) if the inner defer was skipped
}

// Non-panicking functions run to completion with zero log output.
func TestGoHappyPathIsSilent(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	done := make(chan int, 1)
	Go(logger, "quiet", func() { done <- 42 })
	if got := <-done; got != 42 {
		t.Fatalf("fn did not run: %d", got)
	}
	if buf.Len() != 0 {
		t.Errorf("happy path must not log; got %q", buf.String())
	}
}

// A nil logger must not itself panic (defensive: some call sites are
// constructed before logging is wired).
func TestGoToleratesNilLogger(t *testing.T) {
	done := make(chan struct{})
	Go(nil, "nil-logger", func() {
		defer close(done)
		panic("still recovered")
	})
	<-done
}

type lockedWriter struct {
	w  *bytes.Buffer
	mu *sync.Mutex
}

func (l *lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}
