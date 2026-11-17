// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// maintenanceFixture is a bare clone one fetch away from git's automatic
// maintenance: three packs where the limit is one, and one more commit
// upstream. hook is the body of the clone's pre-auto-gc hook, which git
// runs when (and only when) it has decided to repack.
type maintenanceFixture struct {
	bare, upstream string
	// commit adds one commit upstream.
	commit func(content string)
}

func newMaintenanceFixture(t *testing.T, hook string) maintenanceFixture {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not installed")
	}
	tmp := t.TempDir()
	// The host's git configuration must not decide the test (a global
	// maintenance.auto=false would switch the subject off).
	empty := filepath.Join(tmp, "gitconfig")
	if err := os.WriteFile(empty, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GIT_CONFIG_GLOBAL", empty)
	t.Setenv("GIT_CONFIG_NOSYSTEM", "1")
	fx := maintenanceFixture{bare: filepath.Join(tmp, "bare.git"), upstream: filepath.Join(tmp, "upstream")}
	git := func(dir string, args ...string) {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		cmd.Env = append(os.Environ(),
			"GIT_AUTHOR_NAME=Test", "GIT_AUTHOR_EMAIL=test@example.invalid",
			"GIT_COMMITTER_NAME=Test", "GIT_COMMITTER_EMAIL=test@example.invalid")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v in %s: %v\n%s", args, dir, err, out)
		}
	}
	commit := func(content string) {
		if err := os.WriteFile(filepath.Join(fx.upstream, "file.txt"), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		git(fx.upstream, "add", ".")
		git(fx.upstream, "commit", "-m", content)
	}
	if err := os.MkdirAll(fx.upstream, 0o700); err != nil {
		t.Fatal(err)
	}
	fx.commit = commit
	git(fx.upstream, "init", "-b", "main")
	commit("v1")
	git(tmp, "clone", "--bare", fx.upstream, fx.bare)
	// Every fetch keeps its pack (unpackLimit 1); more than one pack is
	// "too many" for git's gc strategy (autoPackLimit 1; gc.auto must be
	// positive for the pack rule to be read at all). Git 2.54 and later
	// maintain with a geometric repack instead, which fires here because
	// packs of equal size break its progression (review round 2, by
	// GIT_TRACE). Either way the premise checks fail loudly if a git stops
	// maintaining this fixture.
	git(fx.bare, "config", "gc.auto", "1")
	git(fx.bare, "config", "gc.autoPackLimit", "1")
	git(fx.bare, "config", "fetch.unpackLimit", "1")
	for _, v := range []string{"v2", "v3"} {
		commit(v)
		git(fx.bare, "-c", "gc.auto=0", "-c", "maintenance.auto=false",
			"fetch", "origin", "+refs/heads/*:refs/heads/*")
	}
	commit("v4")
	if n := fx.packs(t); n < 2 {
		t.Fatalf("fixture: %d packs before the fetch under test, want at least 2 (git changed how a fetch stores objects)", n)
	}
	fx.setHook(t, hook)
	return fx
}

func (fx maintenanceFixture) setHook(t *testing.T, hook string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(fx.bare, "hooks", "pre-auto-gc"), []byte("#!/bin/sh\n"+hook+"\n"), 0o700); err != nil { // #nosec G306 -- a hook must be executable
		t.Fatal(err)
	}
}

// maintenanceLock is the file git holds while a maintenance run is in
// progress. Git never judges it stale: while it exists, every automatic
// maintenance of the clone is skipped, exit status 0 and nothing printed.
func (fx maintenanceFixture) maintenanceLock() string {
	return filepath.Join(fx.bare, "objects", "maintenance.lock")
}

func (fx maintenanceFixture) packs(t *testing.T) int {
	t.Helper()
	m, err := filepath.Glob(filepath.Join(fx.bare, "objects", "pack", "*.pack"))
	if err != nil {
		t.Fatal(err)
	}
	return len(m)
}

// The analysis phase copies the bare clone's pack directory seconds after
// the fetch returns (AnalyzeRepo's local clone, across filesystems). Git's
// automatic maintenance used to be detached: the fetch returned while a
// repack was still renaming and deleting packs, and the copy failed with
// "failed to copy file to …/tmp_pack_…" or "trying to write ref … with
// nonexistent object" (15 times in the first night of 0.29.144, the same
// two messages before it). The fetch returns only after its maintenance.
func TestFetchReturnsOnlyAfterItsMaintenance(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "maintenance-started")
	// The sleep is what makes a detached run observable: without it a
	// background repack of four commits can finish before the assertion.
	fx := newMaintenanceFixture(t, "touch '"+marker+"'\nsleep 1")

	before := fx.packs(t)

	stderr, err := fetchBareClone(context.Background(), fx.bare)
	if err != nil {
		t.Fatalf("fetch: %v\n%s", err, stderr)
	}
	// Read the state the next phase would see, before anything else.
	packs, started, running := fx.packs(t), exists(marker), exists(fx.maintenanceLock())
	if !started {
		t.Fatalf("premise: the fetch returned and git had not started its maintenance (%d packs) — the fixture no longer triggers it, or it ran detached", packs)
	}
	if running {
		t.Error("git's maintenance lock is still held when the fetch returned: the maintenance is running while the next phase would copy the pack directory")
	}
	// The fetch added a pack to `before`; a finished repack leaves fewer
	// than that sum, whichever strategy git's maintenance uses (one pack
	// from gc, possibly more from a geometric repack).
	if packs > before {
		t.Errorf("%d packs when the fetch returned, %d before it: the repack had not run", packs, before)
	}
}

// A stop that lands during the maintenance kills it with the fetch: the
// maintenance holds the fetch's stderr, so a fetch killed alone would keep
// Run waiting for the repack of a kernel-sized clone, and the repack would
// outlive the job that started it.
func TestFetchCancelKillsItsMaintenance(t *testing.T) {
	dir := t.TempDir()
	marker, survived := filepath.Join(dir, "maintenance-started"), filepath.Join(dir, "survived")
	const hookSleep = 3 * time.Second
	fx := newMaintenanceFixture(t, "touch '"+marker+"'\nsleep 3\ntouch '"+survived+"'")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type result struct {
		err error
		at  time.Time
	}
	done := make(chan result, 1)
	go func() {
		_, err := fetchBareClone(ctx, fx.bare)
		done <- result{err, time.Now()}
	}()
	deadline := time.Now().Add(30 * time.Second)
	for !exists(marker) {
		select {
		case r := <-done:
			t.Fatalf("premise: the fetch returned (%v) before its maintenance started", r.err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("premise: the maintenance never started")
		}
		time.Sleep(10 * time.Millisecond)
	}
	canceledAt := time.Now()
	cancel()
	r := <-done
	if r.err == nil {
		t.Fatal("a canceled fetch reported success")
	}
	if !errors.Is(execErr(ctx, r.err), context.Canceled) {
		t.Errorf("a canceled fetch's error does not classify as the shutdown: %v", r.err)
	}
	// Strictly less than the hook's own sleep: a fetch that waited for the
	// hook to finish returns at or after it.
	if waited := r.at.Sub(canceledAt); waited >= hookSleep {
		t.Errorf("the fetch returned %v after the cancel: it waited for the maintenance instead of killing it", waited)
	}
	// Past the moment an unkilled hook would have written its second file.
	time.Sleep(time.Until(canceledAt.Add(hookSleep + time.Second)))
	if exists(survived) {
		t.Error("the maintenance outlived the canceled fetch")
	}

	// The stop must leave the clone able to maintain itself. A maintenance
	// killed outright (SIGKILL) leaves its lock file, and from then on git
	// skips every automatic maintenance of the clone without a word: one
	// more pack per fetch, for ever.
	if exists(fx.maintenanceLock()) {
		t.Error("the canceled fetch left git's maintenance lock behind: git skips the clone's maintenance until someone removes it")
	}
	again := filepath.Join(dir, "maintenance-ran-again")
	fx.setHook(t, "touch '"+again+"'")
	fx.commit("v5")
	if stderr, err := fetchBareClone(context.Background(), fx.bare); err != nil {
		t.Fatalf("the fetch after the canceled one: %v\n%s", err, stderr)
	}
	if !exists(again) {
		t.Error("the fetch after a canceled one did not run git's maintenance")
	}
}

// A maintenance run that fails must not fail the fetch: ensureClone answers
// a failed fetch by deleting the clone and cloning from scratch.
func TestFailedMaintenanceDoesNotFailTheFetch(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions, so the repack cannot be made to fail")
	}
	marker := filepath.Join(t.TempDir(), "maintenance-started")
	fx := newMaintenanceFixture(t, "touch '"+marker+"'\nchmod a-w objects/pack")
	packDir := filepath.Join(fx.bare, "objects", "pack")
	t.Cleanup(func() { _ = os.Chmod(packDir, 0o700) }) // #nosec G302 -- restores a directory for TempDir's removal
	before := fx.packs(t)

	stderr, err := fetchBareClone(context.Background(), fx.bare)
	if !exists(marker) {
		t.Fatal("premise: git did not start its maintenance")
	}
	if fx.packs(t) < before {
		t.Fatalf("premise: the repack succeeded in a read-only pack directory (%d packs, was %d)", fx.packs(t), before)
	}
	if err != nil {
		t.Fatalf("a failed maintenance failed the fetch, which ensureClone answers with a from-scratch clone: %v\n%s", err, stderr)
	}
	// Git exits 0, so the diagnostic is the only sign: ensureClone logs
	// what gitComplaints keeps of it.
	if gitComplaints(stderr) == "" {
		t.Errorf("the maintenance's failure is not among the complaints kept for the log: %q", stderr)
	}
	head := func(dir string) string {
		out, err := exec.Command("git", "-C", dir, "rev-parse", "refs/heads/main").Output()
		if err != nil {
			t.Fatal(err)
		}
		return strings.TrimSpace(string(out))
	}
	if head(fx.bare) != head(fx.upstream) {
		t.Error("the fetch did not advance refs/heads/main")
	}
}

func TestGitComplaints(t *testing.T) {
	long := "error: " + strings.Repeat("é", gitComplaintMaxBytes)
	for _, tc := range []struct {
		name, stderr, want string
	}{
		{"a quiet fetch", "", ""},
		{"a fetch's ordinary chatter", "From https://example.invalid/o/r\n   410d7ba..af2fbc6  main       -> main\n", ""},
		{"a word inside a line is not a complaint", " * [new branch]      error:handling -> error:handling\n", ""},
		{"error and fatal lines, in order, CRLF and indentation trimmed",
			"From x\r\n  fatal: Unable to create temporary file\r\nerror: task 'gc' failed\n", "fatal: Unable to create temporary file; error: task 'gc' failed"},
		{"exactly the line limit is kept whole", strings.Repeat("error: e\n", gitComplaintMaxLines), strings.TrimSuffix(strings.Repeat("error: e; ", gitComplaintMaxLines), "; ")},
		{"one past the limit says so", strings.Repeat("error: e\n", gitComplaintMaxLines+1), strings.Repeat("error: e; ", gitComplaintMaxLines) + "(more)"},
	} {
		if got := gitComplaints(tc.stderr); got != tc.want {
			t.Errorf("%s: gitComplaints = %q, want %q", tc.name, got, tc.want)
		}
	}
	got := gitComplaints(long)
	if !strings.HasSuffix(got, " (truncated)") || len(got) > gitComplaintMaxBytes+len(" (truncated)") || !utf8.ValidString(got) {
		t.Errorf("an over-long complaint is cut at %d bytes on a rune boundary and marked: %d bytes, valid UTF-8 %v", gitComplaintMaxBytes, len(got), utf8.ValidString(got))
	}
}

// ensureClone logs a successful fetch's complaints.
func TestEnsureCloneLogsASuccessfulFetchsComplaints(t *testing.T) {
	ensure := srctest.StripGoComments(srctest.PackageFunc(t, "internal/collector", "func (f *FacadeCollector) ensureClone("))
	fetch, warn, sync := strings.Index(ensure, "fetchBareClone("), strings.Index(ensure, "gitComplaints(stderr)"), strings.Index(ensure, "f.syncDefaultBranch(")
	if fetch < 0 || warn < fetch || sync < warn {
		t.Errorf("ensureClone reads gitComplaints(stderr) between the fetch and the HEAD sync (offsets %d, %d, %d)", fetch, warn, sync)
	}
}

// Something git started can outlive it holding stderr. Git exited 0 and
// the refs advanced: that is not a failed fetch (ensureClone would delete
// the clone), and the returned stderr says what happened.
func TestFetchWhoseChildKeepsStderrIsNotAFailedFetch(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "maintenance-started")
	// Longer than sweptWaitDelay, so Run gives up on the pipe first.
	linger := sweptWaitDelay + 3*time.Second
	fx := newMaintenanceFixture(t, fmt.Sprintf("touch '%s'\nsleep %d &", marker, int(linger.Seconds())))

	start := time.Now()
	stderr, err := fetchBareClone(context.Background(), fx.bare)
	took := time.Since(start)
	if !exists(marker) {
		t.Fatal("premise: git did not start its maintenance")
	}
	if took < sweptWaitDelay {
		t.Fatalf("premise: the fetch returned after %v, before sweptWaitDelay (%v): nothing held its stderr", took, sweptWaitDelay)
	}
	if err != nil {
		t.Fatalf("a fetch that exited 0 is reported failed because a child kept its stderr: %v", err)
	}
	if took >= linger {
		t.Errorf("the fetch waited %v for the lingering child, past sweptWaitDelay (%v)", took, sweptWaitDelay)
	}
	if !strings.Contains(gitComplaints(stderr), "still held its stderr") {
		t.Errorf("the returned stderr does not say a child held it: %q", stderr)
	}
}

// A maintenance lock present before the fetch is reported: git skips the
// clone's maintenance while it exists and says nothing.
func TestLeftoverMaintenanceLockIsReported(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "objects"), 0o700); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	fc := &FacadeCollector{logger: slog.New(slog.NewTextHandler(&buf, nil))}

	fc.warnLeftoverMaintenanceLock(dir)
	if buf.Len() != 0 {
		t.Errorf("a clone without the lock logged: %s", buf.String())
	}
	lock := filepath.Join(dir, gitMaintenanceLock)
	if err := os.WriteFile(lock, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	fc.warnLeftoverMaintenanceLock(dir)
	if out := buf.String(); !strings.Contains(out, "level=WARN") || !strings.Contains(out, "git maintenance lock present before the fetch") || !strings.Contains(out, lock) {
		t.Errorf("the leftover lock was not reported at WARN with its path: %s", out)
	}

	// A check that could not be made is said, not read as "no lock" (SR-5).
	if os.Geteuid() == 0 {
		return // root reads through directory permissions
	}
	buf.Reset()
	objects := filepath.Join(dir, "objects")
	if err := os.Chmod(objects, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(objects, 0o700) }) // #nosec G302 -- restores a directory for TempDir's removal
	fc.warnLeftoverMaintenanceLock(dir)
	if out := buf.String(); !strings.Contains(out, "could not check for a leftover git maintenance lock") {
		t.Errorf("an unreadable objects directory was not reported: %q", out)
	}
}

// ensureClone checks for the lock before the fetch.
func TestEnsureCloneChecksForALeftoverLockBeforeTheFetch(t *testing.T) {
	ensure := srctest.StripGoComments(srctest.PackageFunc(t, "internal/collector", "func (f *FacadeCollector) ensureClone("))
	check, fetch := strings.Index(ensure, "f.warnLeftoverMaintenanceLock(path)"), strings.Index(ensure, "fetchBareClone(")
	if check < 0 || fetch < check {
		t.Errorf("ensureClone calls warnLeftoverMaintenanceLock before fetchBareClone (offsets %d, %d): after it, the fetch's own maintenance holds the lock", check, fetch)
	}
}

// The wiring: ensureClone fetches through fetchBareClone and nowhere else
// runs git fetch, and the fetch carries both settings (git before 2.47
// reads only gc.autoDetach; later ones read maintenance.autoDetach first)
// the clones' environment (no credential prompt) and the group's stop:
// SIGTERM, then a sweep.
func TestTheFetchIsTheOneForegroundMaintenanceFetch(t *testing.T) {
	pkg := srctest.StripGoComments(srctest.PackageSource(t, "internal/collector", 1))
	if n := strings.Count(pkg, `"fetch", "origin"`); n != 1 {
		t.Errorf("%d git fetch commands in internal/collector, want the one in fetchBareClone", n)
	}
	body := srctest.StripGoComments(srctest.PackageFunc(t, "internal/collector", "func fetchBareClone("))
	for _, want := range []string{`"fetch", "origin"`, "gitForegroundMaintenance", "groupTerminated(cmd)", "sweepGroup(cmd)", "cmd.Env = gitCloneEnv()", "gitNoRedirects"} {
		if !strings.Contains(body, want) {
			t.Errorf("fetchBareClone lacks %s", want)
		}
	}
	ensure := srctest.StripGoComments(srctest.PackageFunc(t, "internal/collector", "func (f *FacadeCollector) ensureClone("))
	if all, inEnsure := strings.Count(pkg, "fetchBareClone("), strings.Count(ensure, "fetchBareClone("); inEnsure != 1 || all != 2 { // its declaration and ensureClone's call
		t.Errorf("fetchBareClone appears %d times (%d in ensureClone): ensureClone alone calls it, after resolving the endpoint under the outbound-address rule", all, inEnsure)
	}
	want := []string{"-c", "gc.autoDetach=false", "-c", "maintenance.autoDetach=false"}
	if strings.Join(gitForegroundMaintenance, " ") != strings.Join(want, " ") {
		t.Errorf("gitForegroundMaintenance = %q, want %q", gitForegroundMaintenance, want)
	}
}
