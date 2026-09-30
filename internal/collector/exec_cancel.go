// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"errors"
	"os/exec"
	"syscall"
)

// execErr maps a subprocess failure under a canceled context to the
// context's own error. exec.CommandContext kills the child on cancel
// and Run/Wait then report `signal: killed` — never context.Canceled —
// so every caller classifying `errors.Is(err, context.Canceled)` read a
// shutdown that landed mid-git/scc/scorecard as a failure (pass 35,
// v0.28.18: the scheduler's classifications on the exec-backed phases
// were decorative for exactly the phases a `stop serve` usually lands
// in). The owning layer decides (SR-18): when ctx is done, the context
// is the cause and the kill is its symptom — return ctx.Err() for the
// call site's %w wrap. A failure under a live ctx passes through.
func execErr(ctx context.Context, err error) error {
	if err != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

// killedBySIGKILL reports whether err is a subprocess exit by SIGKILL —
// what cmd.Wait returns when a wall-clock context fires cmd.Cancel. It
// reads the exit's wait status (v0.29.71); the scancode classifier used to
// compare the error TEXT with "signal: killed" (the error-text class).
func killedBySIGKILL(err error) bool {
	var ee *exec.ExitError
	if !errors.As(err, &ee) {
		return false
	}
	ws, ok := ee.Sys().(syscall.WaitStatus)
	return ok && ws.Signaled() && ws.Signal() == syscall.SIGKILL
}
