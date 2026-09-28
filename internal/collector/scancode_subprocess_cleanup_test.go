// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"os"
	"os/exec"
	"regexp"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// v0.23.3 — four coupled fixes prompted by the 2026-05-21 diagnostic
// showing scancode wedged for 2 days on a 2-worker pool, both slots
// stuck on subprocesses that never returned.
//
// 1. Subprocess cleanup on ctx cancel — process groups
//    (groupKilled: Setpgid + cmd.Cancel + WaitDelay) so the entire subprocess
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

// TestSubprocessGroupKillIsTheSharedHelper: the scancode clone and scan,
// the preflight probe and scorecard each spawn grandchildren (git-lfs, the
// Python multiprocessing pool, scorecard's check probes) that survived a
// kill of the leader alone — the 2026-05-21 wedge: two worker slots held
// for six hours, scorecard ghosts eating CPU after `aveloxis stop`. Each
// site once carried its own Setpgid + Cancel + WaitDelay block; the four
// copies returned a raw errno from Cancel where os/exec wants
// os.ErrProcessDone (v0.29.68 review round 14), so every site calls
// groupKilled on its command and no production file of the package sets
// the three fields inline but groupKilled itself (SR-17). The pins that
// stood here matched the word "Setpgid" anywhere in the file, comments
// included.
func TestSubprocessGroupKillIsTheSharedHelper(t *testing.T) {
	for _, site := range []struct{ file, fn, call string }{
		{"internal/collector/scancode_worker.go", "func (w *ScancodeWorker) prepareClone(", "groupKilled(cloneCmd)"},
		{"internal/collector/scancode_worker.go", "func (w *ScancodeWorker) executeScan(", "groupKilled(cmd)"},
		{"internal/collector/scancode_preflight.go", "func (w *ScancodeWorker) probeScancodeHealth(", "groupKilled(cmd)"},
		{"internal/collector/scorecard.go", "func invokeScorecard(", "groupKilled(cmd)"},
	} {
		body := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, site.file), site.fn))
		if !strings.Contains(body, "exec.CommandContext(") {
			t.Fatalf("%s %s no longer spawns a subprocess — move this pin to where it went", site.file, site.fn)
		}
		if !strings.Contains(body, site.call) {
			t.Errorf("%s %s spawns a subprocess without %s: its grandchildren (git-lfs, the multiprocessing pool, check probes) outlive a cancel — the 2026-05-21 wedge", site.file, site.fn, site.call)
		}
	}
	for name, src := range srctest.PackageFiles(t, "internal/collector", 40) {
		if strings.HasSuffix(name, "/swept_command.go") {
			continue // groupKilled's own body
		}
		code := srctest.StripGoComments(src)
		for _, inline := range []string{"SysProcAttr{", ".Cancel = func", ".WaitDelay = "} {
			if strings.Contains(code, inline) {
				t.Errorf("%s sets %s inline — call groupKilled(cmd): the one shared block returns os.ErrProcessDone for an already-gone group (SR-17)", name, inline)
			}
		}
	}
}

// TestShutdownGraceCoversTheAppliedWaitDelay (review round 15): the
// scheduler waits ScancodeShutdownBookkeepingGrace for the runners' DB
// bookkeeping after a kill, derived from scancodeWaitDelay as the post-kill
// Wait; since round 14 the WaitDelay a scancode subprocess actually carries
// is the one groupKilled sets. A second literal let the two drift with
// every test green (a 3 s sweptWaitDelay passed the suite).
func TestShutdownGraceCoversTheAppliedWaitDelay(t *testing.T) {
	cmd := exec.Command("true")
	groupKilled(cmd)
	if cmd.WaitDelay != scancodeWaitDelay {
		t.Fatalf("groupKilled applies WaitDelay %s; the shutdown grace is derived from scancodeWaitDelay %s", cmd.WaitDelay, scancodeWaitDelay)
	}
	if ScancodeShutdownBookkeepingGrace != cmd.WaitDelay+2*scancodeBestEffortDBTimeout {
		t.Fatalf("ScancodeShutdownBookkeepingGrace %s does not cover the applied post-kill wait %s plus two best-effort writes", ScancodeShutdownBookkeepingGrace, cmd.WaitDelay)
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
