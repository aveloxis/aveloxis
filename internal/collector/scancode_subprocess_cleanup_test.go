// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// v0.23.3 — four coupled fixes prompted by the 2026-05-21 diagnostic
// showing scancode wedged for 2 days on a 2-worker pool, both slots
// stuck on subprocesses that never returned.
//
// 1. Subprocess cleanup on ctx cancel — process groups
//    (Setpgid + cmd.Cancel + WaitDelay) so the entire subprocess
//    tree (scancode + its Python worker pool; git clone + git-lfs)
//    dies on aveloxis stop, not just the immediate child.
// 2. Full stderr to file on failure so operators don't have to grep
//    a 4 KB ring buffer that's dominated by libmagic warnings.
// 3. Mid-run orphan recovery — periodic in-loop check that detects
//    locks whose recorded PID isn't alive any more (covers the
//    "subprocess died but cmd.Wait() never returned" wedge).
// 4. RecordScancodeLockState failure aborts the scan instead of
//    proceeding with a NULL-PID lock that no recovery path can
//    distinguish from a legitimate in-flight scan until next
//    startup.

func TestScancodeRunOneSetsProcessGroup(t *testing.T) {
	src, err := os.ReadFile("scancode_worker.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	// Both the git clone and the scancode subprocess must be put in
	// their own process group via SysProcAttr.Setpgid. Without this,
	// killing the immediate child leaves its children (git-lfs,
	// scancode's Python multiprocessing pool) as orphans.
	if !strings.Contains(code, "Setpgid") {
		t.Error("scancode runOne must set cmd.SysProcAttr.Setpgid = true on both " +
			"the git clone command and the scancode subprocess. Without this, " +
			"`syscall.Kill(-pid, ...)` to the process group ID can't kill child " +
			"processes spawned by the subprocess (git-lfs, scancode --processes " +
			"workers). 2026-05-21 diagnostic: 2 worker slots wedged for 6+ hours " +
			"because of this exact gap.")
	}
	// Count occurrences: we expect Setpgid on BOTH commands (clone + scancode).
	if strings.Count(code, "Setpgid") < 2 {
		t.Errorf("Setpgid must be set on both the git clone command AND the " +
			"scancode subprocess command. git clone can hang in git-lfs even with " +
			"GIT_LFS_SKIP_SMUDGE=1 (the smudge filter still tries to spawn the " +
			"check) and needs the same cleanup guarantee.")
	}
}

func TestScancodeRunOneCmdCancelKillsProcessGroup(t *testing.T) {
	src, err := os.ReadFile("scancode_worker.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	// cmd.Cancel callback that signals the whole process group
	// (-pid in syscall.Kill arguments). The negative PID is the
	// signal-to-group syscall convention; without it the kill only
	// hits the immediate child.
	if !strings.Contains(code, "cmd.Cancel") && !strings.Contains(code, ".Cancel = func") {
		t.Error("scancode runOne must override cmd.Cancel with a callback that " +
			"sends SIGKILL to the process group (-pid). The default exec.CommandContext " +
			"behavior only signals the immediate child on ctx cancel — grandchildren " +
			"survive as orphans, which is the 'ghosts consuming CPU and memory' the " +
			"operator reported on 2026-05-21.")
	}
	// Pin the negative-pid pattern. Anything matching syscall.Kill(-...)
	// or syscall.Kill(-cmd.Process.Pid, ...) qualifies.
	if !regexp.MustCompile(`syscall\.Kill\s*\(\s*-`).MatchString(code) {
		t.Error("scancode runOne must call syscall.Kill with a NEGATIVE PID " +
			"(signals the whole process group, not just the leader). syscall.Kill(pid, " +
			"sig) and syscall.Kill(-pid, sig) are different operations — only the " +
			"negative form kills grandchildren.")
	}
	// WaitDelay as the fallback safety net — if cmd.Cancel didn't
	// successfully signal everything, WaitDelay bounds how long
	// Wait() blocks waiting for cleanup.
	if !strings.Contains(code, "WaitDelay") {
		t.Error("scancode runOne must set cmd.WaitDelay so Wait() returns within " +
			"a bounded window even if the subprocess (or its inherited file " +
			"descriptors via grandchildren) refuse to exit. Without WaitDelay, " +
			"Wait() can hang forever if any descendant inherits stderr/stdout " +
			"pipes and doesn't close them — exactly the wedge pattern the " +
			"2026-05-21 diagnostic showed on the chaoss.tv fleet.")
	}
}

func TestScorecardSetsProcessGroupAndCancel(t *testing.T) {
	src, err := os.ReadFile("scorecard.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	if !strings.Contains(code, "Setpgid") {
		t.Error("RunScorecard must set cmd.SysProcAttr.Setpgid = true. Same " +
			"reasoning as scancode: scorecard spawns its own subprocesses (git, " +
			"various check probes) that survive as orphans when only the " +
			"immediate child is killed.")
	}
	if !regexp.MustCompile(`syscall\.Kill\s*\(\s*-`).MatchString(code) {
		t.Error("RunScorecard must signal the process group on ctx cancel (the " +
			"negative-PID form of syscall.Kill). Without this, aveloxis stop leaves " +
			"scorecard subprocess trees running.")
	}
}

func TestScancodeRunOneWritesFullStderrOnFailure(t *testing.T) {
	src, err := os.ReadFile("scancode_worker.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	// The user's explicit ask: on non-zero exit, write the full
	// stderr buffer (NOT the truncated tail) to
	// /tmp/aveloxis-scancode/repo_<id>_stderr.log. Keep the tail
	// in the log line for quick triage.
	if !strings.Contains(code, "_stderr.log") {
		t.Error("scancode runOne must write the full stderr to a per-repo file " +
			"on non-zero exit (path like /tmp/aveloxis-scancode/repo_<id>_stderr.log). " +
			"The bounded tailBuffer is fine for quick triage in the log line, but " +
			"the actual error message is usually OUT OF the 4 KB window because " +
			"libmagic warnings dominate the trailing bytes. Operator-confirmed pain " +
			"point on 2026-05-21.")
	}
	// To capture the FULL stderr we need an unbounded io.Writer
	// (bytes.Buffer or io.MultiWriter with a file). The bounded
	// tailBuffer alone can't satisfy this. Pin at least one
	// bytes.Buffer or io.MultiWriter reference for the failure path.
	if !strings.Contains(code, "bytes.Buffer") && !strings.Contains(code, "io.MultiWriter") {
		t.Error("scancode runOne must capture the FULL stderr via bytes.Buffer or " +
			"io.MultiWriter so the file write isn't truncated to the tailBuffer's " +
			"4 KB cap. tailBuffer alone is too small.")
	}
}

func TestScancodeWorkerHasInFlightOrphanRecovery(t *testing.T) {
	src, err := os.ReadFile("scancode_worker.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	// Per the 2026-05-21 diagnostic, recoverOrphans runs only on
	// worker startup. Once a worker wedges mid-run, no in-flight
	// recovery fires and the lock stays forever. v0.23.3 adds a
	// periodic check that detects locks whose recorded PID isn't
	// alive (kill -0 fails) and clears them so the row re-enters
	// the queue.
	hasInFlightCheck := strings.Contains(code, "inFlightRecovery") ||
		strings.Contains(code, "recoverInFlight") ||
		strings.Contains(code, "checkOwnLocks") ||
		strings.Contains(code, "in-flight orphan")
	if !hasInFlightCheck {
		t.Error("v0.23.3 must add a periodic in-flight orphan recovery loop " +
			"(checkOwnLocks / recoverInFlight / similar) that runs while the " +
			"worker is up. recoverOrphans alone (startup-only) doesn't catch the " +
			"mid-run wedge pattern observed on 2026-05-21 — both worker slots " +
			"stuck for 6+ hours and the worker had no mechanism to unstick itself " +
			"short of an aveloxis restart.")
	}
}

func TestScancodeRunOneAbortsOnLockStateFailure(t *testing.T) {
	src, err := os.ReadFile("scancode_worker.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	// Find the RecordScancodeLockState call site. Pre-v0.23.3 the
	// failure path logged a warning and proceeded; the resulting
	// row had scancode_locked_at set but scancode_locked_pid NULL,
	// which is indistinguishable from "PID never recorded" by any
	// recovery path. The 2026-05-21 production DB had exactly one
	// of these rows (ropensci/neotoma).
	// Anchor on the actual call site, not the docstring mention.
	startIdx := strings.Index(code, "w.store.RecordScancodeLockState(")
	if startIdx < 0 {
		t.Fatal("w.store.RecordScancodeLockState call not found in scancode_worker.go")
	}
	// The failure block can run to ~1.5KB once you include a thorough
	// v0.23.3 comment, an unconditional ROLLBACK-equivalent kill, the
	// reap, and the failure-record. 2000 chars is generous and still
	// keeps the slice anchored on the RecordScancodeLockState call site.
	end := startIdx + 2000
	if end > len(code) {
		end = len(code)
	}
	region := code[startIdx:end]
	// The failure path must kill the subprocess and clear the lock,
	// not just log. Within the failure-branch region we want to see
	// BOTH a kill-style operation AND a failure-record call. Don't
	// constrain the order; comments / log lines can interleave.
	hasKill := regexp.MustCompile(`syscall\.Kill|cmd\.Process\.Kill`).MatchString(region)
	hasFailureRecord := strings.Contains(region, "recordFailureBestEffort") ||
		strings.Contains(region, "ClearScancodeLock")
	hasReturn := strings.Contains(region, "return")
	if !(hasKill && hasFailureRecord && hasReturn) {
		t.Errorf("scancode runOne: when RecordScancodeLockState fails, the path "+
			"must kill the subprocess AND call recordFailureBestEffort (or "+
			"ClearScancodeLock) AND return — NOT proceed with a NULL-PID lock state "+
			"that's only recoverable by restarting aveloxis. 2026-05-21 diagnostic "+
			"showed one production row stuck in this state. "+
			"Found: kill=%v failure_record=%v return=%v",
			hasKill, hasFailureRecord, hasReturn)
	}
	// Negative pin: the misleading "proceeding anyway" comment + " // Don't abort the scan"
	// pattern from v0.21.0 should be gone. The comment isn't load-bearing
	// (it's documentation), but its presence indicates the buggy logic.
	if strings.Contains(region, "Don't abort the scan") {
		t.Error("scancode runOne: remove the 'Don't abort the scan' comment from " +
			"v0.21.0 — the new v0.23.3 behavior IS to abort when lock-state can't " +
			"be written, because the alternative (NULL-PID lock) is unrecoverable " +
			"until next startup.")
	}
}
