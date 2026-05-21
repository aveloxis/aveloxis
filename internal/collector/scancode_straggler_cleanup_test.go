// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// v0.23.7 — deferred straggler kill in runOne + RunScorecard.
//
// The v0.23.3 pgid cleanup only fires when ctx cancels while
// cmd.Wait() is blocked. If the lead subprocess exits NORMALLY
// before ctx cancels, cmd.Cancel never runs — any straggler
// children (Python multiprocessing workers from scancode --processes,
// git-lfs / git-remote-https subprocesses from clones) become true
// orphans (PPID=1) after the parent goroutine returns.
//
// The v0.23.7 fix: immediately after cmd.Start() succeeds, install
// a deferred best-effort kill of the entire process group. This
// catches the "lead exited cleanly but children survived" case AND
// preserves the v0.23.3 ctx-cancel path. The kill is idempotent —
// syscall.Kill returns ESRCH on an already-dead pgid, which we
// ignore.

func TestScancodeRunOneDefersStragglerKill(t *testing.T) {
	src, err := os.ReadFile("scancode_worker.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	// Pin the deferred syscall.Kill(-pid, SIGKILL) idiom inside
	// runOne. The negative PID is the load-bearing detail —
	// syscall.Kill with a POSITIVE pid only kills the immediate
	// child, leaving the process group orphans alive.
	if !strings.Contains(code, "defer syscall.Kill") {
		t.Error("runOne must install `defer syscall.Kill(-pid, " +
			"syscall.SIGKILL)` immediately after cmd.Start() succeeds. " +
			"Without it, the v0.23.3 cmd.Cancel path only kills " +
			"straggler children when ctx-cancel fires while cmd.Wait " +
			"is blocked. Normal-exit-with-surviving-children leaves " +
			"orphans behind.")
	}
	// Pin the NEGATIVE pid pattern. A positive PID kill = just the
	// immediate child, defeating the purpose.
	negPidPattern := regexp.MustCompile(`syscall\.Kill\(\s*-pid\s*,\s*syscall\.SIGKILL\s*\)`)
	if !negPidPattern.MatchString(code) {
		t.Error("the deferred kill must use a NEGATIVE pid " +
			"(`syscall.Kill(-pid, syscall.SIGKILL)`) to target the " +
			"process group rather than just the immediate child. " +
			"Positive PIDs miss exactly the grandchildren we're " +
			"trying to clean up.")
	}
}

func TestScancodeStragglerKillIsAfterStart(t *testing.T) {
	src, err := os.ReadFile("scancode_worker.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	// The deferred kill must appear AFTER `cmd.Start()` (otherwise
	// cmd.Process.Pid is unset / zero) AND AFTER the v0.23.3
	// RecordScancodeLockState block (otherwise the abort path would
	// race the deferred kill on the still-spinning subprocess).
	// Pin the relative order textually inside the runOne function.
	startCallIdx := strings.Index(code, "if err := cmd.Start()")
	if startCallIdx < 0 {
		t.Fatal("cmd.Start() call missing — runOne shape changed")
	}
	deferKillIdx := strings.Index(code[startCallIdx:], "defer syscall.Kill")
	if deferKillIdx < 0 {
		t.Error("deferred straggler kill must appear AFTER cmd.Start() " +
			"in runOne — before Start, cmd.Process is nil and the PID " +
			"is undefined")
	}
}

func TestScorecardDefersStragglerKill(t *testing.T) {
	src, err := os.ReadFile("scorecard.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	if !strings.Contains(code, "defer syscall.Kill") {
		t.Error("RunScorecard must install `defer syscall.Kill(-pid, " +
			"syscall.SIGKILL)` so any leftover children (the git " +
			"clone subprocess in remote mode, git-lfs, scorecard's " +
			"check-probe spawns) die when the function returns. " +
			"v0.23.3's cmd.Cancel path doesn't catch normal-exit " +
			"orphans.")
	}
	// Scorecard pre-v0.23.7 used cmd.Run() (synchronous). The fix
	// requires splitting into cmd.Start() + cmd.Wait() so we have a
	// PID to defer-kill against.
	if !strings.Contains(code, "cmd.Start()") {
		t.Error("RunScorecard must split cmd.Run into cmd.Start + " +
			"cmd.Wait so the PID is available for the deferred " +
			"straggler kill")
	}
}
