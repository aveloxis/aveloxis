// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/pidfile"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// Round-11 finding 2 (SR-5: a lookup ERROR is not "no"). Through
// v0.29.4 componentAlreadyRunning read:
//
//	pid, err := pidfile.Read(pidfile.Path(component))
//	if err != nil || !pidfile.IsRunning(pid) { return 0, false }
//
// pidfile.Read returns errors for EACCES and EIO as well as for a
// corrupt or truncated file ("invalid PID in %s") — not just ENOENT. So
// a LIVE serve whose pidfile cannot be read reported "not running", and
// two things followed from one collapsed answer: v0.29.4's own gate
// ordering (round-8 finding 6) ran the deploy gate against a live
// primary, and startComponent — the ONLY hard double-start guard, since
// the other-serve probe warns and never blocks — launched a SECOND
// scheduler against the same queue and API keys.
//
// Only ENOENT is a definitive "not running". Everything else is UNKNOWN,
// and every caller that acts on the answer must fail closed.

func withTempPidDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("HOME", dir)
	return filepath.Join(dir, ".aveloxis")
}

func TestComponentAlreadyRunningMissingPidfileIsDefinitivelyNotRunning(t *testing.T) {
	withTempPidDir(t)
	pid, running, err := componentAlreadyRunning("serve")
	if err != nil {
		t.Fatalf("a MISSING pidfile is the ordinary not-running state (ENOENT), not an error: %v", err)
	}
	if running || pid != 0 {
		t.Fatalf("missing pidfile must report not-running, got pid=%d running=%v", pid, running)
	}
}

func TestComponentAlreadyRunningCorruptPidfileIsUnknown(t *testing.T) {
	withTempPidDir(t)
	if err := os.WriteFile(pidfile.Path("serve"), []byte("not-a-pid\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, running, err := componentAlreadyRunning("serve")
	if err == nil {
		t.Fatal("a CORRUPT pidfile is not evidence that serve is stopped — pidfile.Read returns\n" +
			"\"invalid PID in <path>\" and the pre-fix code collapsed that into not-running, which let\n" +
			"startComponent launch a second scheduler beside a live one (round-11 finding 2, SR-5).")
	}
	if running {
		t.Fatal("an unknown liveness state must never report running=true")
	}
}

func TestComponentAlreadyRunningUnreadablePidfileIsUnknown(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root — mode 0000 is still readable")
	}
	withTempPidDir(t)
	path := pidfile.Path("serve")
	if err := os.WriteFile(path, []byte(strconv.Itoa(os.Getpid())), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(path, 0o644) })

	_, running, err := componentAlreadyRunning("serve")
	if err == nil {
		t.Fatal("an UNREADABLE pidfile (EACCES) is not evidence that serve is stopped — this is the\n" +
			"exact shape that lets a second scheduler start beside a live one (round-11 finding 2).")
	}
	if running {
		t.Fatal("an unknown liveness state must never report running=true")
	}
}

func TestComponentAlreadyRunningLivePidfile(t *testing.T) {
	withTempPidDir(t)
	if err := pidfile.Write(pidfile.Path("serve"), os.Getpid()); err != nil {
		t.Fatal(err)
	}
	pid, running, err := componentAlreadyRunning("serve")
	if err != nil {
		t.Fatalf("a readable pidfile naming a live process is not an error: %v", err)
	}
	if !running || pid != os.Getpid() {
		t.Fatalf("want running with our own pid %d, got pid=%d running=%v", os.Getpid(), pid, running)
	}
}

func TestComponentAlreadyRunningStalePidfileIsDefinitivelyNotRunning(t *testing.T) {
	withTempPidDir(t)
	// PID 0x7FFFFFFF is above every plausible pid_max; signal 0 to it
	// returns ESRCH, which IS a definitive "not running".
	if err := pidfile.Write(pidfile.Path("serve"), 0x7FFFFFFF); err != nil {
		t.Fatal(err)
	}
	pid, running, err := componentAlreadyRunning("serve")
	if err != nil {
		t.Fatalf("a STALE pidfile (readable, PID dead) is the ordinary not-running state: %v", err)
	}
	if running || pid != 0 {
		t.Fatalf("stale pidfile must report not-running, got pid=%d running=%v", pid, running)
	}
}

// Every caller that ACTS on the answer must fail closed on the unknown.
// A three-valued predicate whose callers discard the third value is the
// v0.27.107 decorative-gate class.
func TestComponentLivenessCallersFailClosed(t *testing.T) {
	mainSrc := srctest.Read(t, "cmd/aveloxis/main.go")
	scorecardSrc := srctest.Read(t, "cmd/aveloxis/run_scorecard.go")

	sites := []struct {
		src, fn, why string
	}{
		{mainSrc, "func startComponent(", "startComponent is the ONLY hard double-start guard — the other-serve probe warns and never blocks"},
		{scorecardSrc, "func refuseIfServeRunning(", "run-scorecard must not compete with a live serve for the shared GitHub API budget"},
		{scorecardSrc, "func acquireRunScorecardPidfile(", "two bulk passes would double-scan the same backlog"},
	}
	for _, site := range sites {
		body := srctest.StripGoComments(srctest.FuncBody(t, site.src, site.fn))
		idx := strings.Index(body, "componentAlreadyRunning(")
		if idx < 0 {
			t.Errorf("%s must route its liveness test through componentAlreadyRunning (SR-17)", site.fn)
			continue
		}
		// Capture the assignment's left-hand side.
		lineStart := strings.LastIndex(body[:idx], "\n") + 1
		line := body[lineStart:idx]
		if !strings.Contains(line, "err") {
			t.Errorf("%s discards componentAlreadyRunning's error arm — an unreadable or corrupt\n"+
				"pidfile is NOT evidence the component is stopped (%s).\ngot: %s",
				site.fn, site.why, strings.TrimSpace(line))
		}
	}
}

// pidfile.Remove's contract comment claimed "a stale PID file is handled
// by the liveness check on the next start". A CORRUPT file is neither
// stale nor live — the liveness check cannot reach it — so the comment
// described a guarantee the code does not make.
func TestPidfileRemoveContractDoesNotOverclaim(t *testing.T) {
	src := srctest.Read(t, "internal/pidfile/pidfile.go")
	doc := src[:strings.Index(src, "func Remove(")]
	tail := srctest.NormalizeWS(strings.ReplaceAll(doc[strings.LastIndex(doc, "\n\n"):], "//", " "))
	if strings.Contains(tail, "stale PID file is handled by the liveness check") {
		t.Error("pidfile.Remove's contract must not claim the liveness check handles every leftover\n" +
			"pidfile: a CORRUPT or UNREADABLE file never reaches IsRunning, and since round-11\n" +
			"finding 2 that state is reported as UNKNOWN and refuses the start (round-11 finding 2).")
	}
}
