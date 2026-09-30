// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"errors"
	"os/exec"
	"sync"
	"testing"
	"time"
)

var (
	sigkilledOnce sync.Once
	sigkilledErr  error
)

// sigkilledExitError is a real *exec.ExitError for a child SIGKILLed by its
// context — the error cmd.Wait returns when a scan's wall-clock bound fires
// (the child is this test's own `sleep`; no other process is signalled).
func sigkilledExitError(t *testing.T) error {
	t.Helper()
	sigkilledOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		sigkilledErr = exec.CommandContext(ctx, "sleep", "5").Run()
	})
	var ee *exec.ExitError
	if !errors.As(sigkilledErr, &ee) {
		t.Fatalf("could not produce a SIGKILLed child: %v", sigkilledErr)
	}
	return sigkilledErr
}

// TestKilledBySIGKILLReadsTheWaitStatus — v0.29.71 (summary/43 item 8): the
// scancode classifier decided "timed out" by comparing the error TEXT with
// "signal: killed"; it reads the exit's wait status now. Real subprocess
// errors both ways, and the bare text is not a kill.
func TestKilledBySIGKILLReadsTheWaitStatus(t *testing.T) {
	if !killedBySIGKILL(sigkilledExitError(t)) {
		t.Error("a child SIGKILLed by its context must read as killed")
	}
	if !killedBySIGKILL(errors.Join(errors.New("scan"), sigkilledExitError(t))) {
		t.Error("a wrapped kill must read as killed")
	}
	termErr := exec.Command("sh", "-c", "kill -TERM $$").Run()
	exitErr := exec.Command("sh", "-c", "exit 1").Run()
	for name, err := range map[string]error{
		"SIGTERM":   termErr,
		"exit 1":    exitErr,
		"the text":  errors.New("signal: killed"),
		"no error":  nil,
		"a context": context.DeadlineExceeded,
	} {
		if killedBySIGKILL(err) {
			t.Errorf("%s must not read as a SIGKILL (%v)", name, err)
		}
	}
}
