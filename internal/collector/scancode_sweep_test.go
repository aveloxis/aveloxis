// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// v0.27.6 — clone-directory sweeps. runOne's `defer os.RemoveAll`
// covers clean exits only: hard kills (power outage, kill -9, OOM)
// leaked multi-GB clone dirs forever, and recoverOrphans reconciled
// lock ROWS, never the directory. These are behavioral tests against
// a real temp directory.

func sweepTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func mkDir(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(path, 0o755); err != nil {
		t.Fatal(err)
	}
	// A file inside proves RemoveAll (not Remove) semantics.
	if err := os.WriteFile(filepath.Join(path, "f.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
}

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func TestStartupSweepRemovesUnlockedCloneDirsKeepsLocked(t *testing.T) {
	dir := t.TempDir()
	mkDir(t, filepath.Join(dir, "repo_1_111"))             // own-host lock → keep
	mkDir(t, filepath.Join(dir, "repo_2_222"))             // no lock → remove
	mkDir(t, filepath.Join(dir, "repo_2_333"))             // second leak, same repo → remove
	mkDir(t, filepath.Join(dir, "unrelated"))              // not ours → untouched
	mkDir(t, filepath.Join(dir, "scancode-preflight-abc")) // leaked probe dir → remove

	keep := func(repoID int64) bool { return repoID == 1 }
	removedDirs, _ := sweepScancodeDir(sweepTestLogger(), dir, keep, scancodeStderrLogMaxAge, time.Now())

	if !exists(filepath.Join(dir, "repo_1_111")) {
		t.Error("a clone dir with a live own-host lock must be KEPT — a live orphan adopted by recoverOrphans may still be writing into it")
	}
	if exists(filepath.Join(dir, "repo_2_222")) || exists(filepath.Join(dir, "repo_2_333")) {
		t.Error("clone dirs with no matching own-host lock row are hard-kill leaks and must be removed")
	}
	if !exists(filepath.Join(dir, "unrelated")) {
		t.Error("entries that don't match the repo_*/preflight patterns must never be touched")
	}
	if exists(filepath.Join(dir, "scancode-preflight-abc")) {
		t.Error("leaked scancode-preflight-* temp dirs must be removed")
	}
	if removedDirs != 3 {
		t.Errorf("expected 3 removed dirs (repo_2 ×2 + preflight), got %d", removedDirs)
	}
}

func TestStartupSweepAgesOutStderrLogs(t *testing.T) {
	dir := t.TempDir()
	oldLog := filepath.Join(dir, "repo_7_stderr.log")
	freshLog := filepath.Join(dir, "repo_8_stderr.log")
	oldStdout := filepath.Join(dir, "repo_7_stdout.log")
	for _, p := range []string{oldLog, freshLog, oldStdout} {
		if err := os.WriteFile(p, []byte("diag"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	stale := time.Now().Add(-15 * 24 * time.Hour)
	for _, p := range []string{oldLog, oldStdout} {
		if err := os.Chtimes(p, stale, stale); err != nil {
			t.Fatal(err)
		}
	}

	_, removedLogs := sweepScancodeDir(sweepTestLogger(), dir,
		func(int64) bool { return false }, scancodeStderrLogMaxAge, time.Now())

	if exists(oldLog) || exists(oldStdout) {
		t.Error("failure logs older than 14 days must be removed by the startup sweep")
	}
	if !exists(freshLog) {
		t.Error("fresh failure logs are live diagnostics and must be kept")
	}
	if removedLogs != 2 {
		t.Errorf("expected 2 removed logs, got %d", removedLogs)
	}
}

func TestShutdownSweepRemovesAllClonesKeepsDiagnostics(t *testing.T) {
	dir := t.TempDir()
	mkDir(t, filepath.Join(dir, "repo_1_111"))
	mkDir(t, filepath.Join(dir, "repo_2_222"))
	mkDir(t, filepath.Join(dir, "scancode-preflight-xyz"))
	oldLog := filepath.Join(dir, "repo_1_stderr.log")
	if err := os.WriteFile(oldLog, []byte("diag"), 0o644); err != nil {
		t.Fatal(err)
	}
	stale := time.Now().Add(-30 * 24 * time.Hour)
	_ = os.Chtimes(oldLog, stale, stale)

	// Shutdown semantics: keep == nil removes ALL clone dirs;
	// logMaxAge == 0 keeps ALL diagnostics (they age out at the next
	// STARTUP sweep instead — a crash post-shutdown-sweep must not
	// have eaten the operator's failure evidence).
	sweepScancodeDir(sweepTestLogger(), dir, nil, 0, time.Now())

	for _, gone := range []string{"repo_1_111", "repo_2_222", "scancode-preflight-xyz"} {
		if exists(filepath.Join(dir, gone)) {
			t.Errorf("shutdown sweep must remove %s — a clone can't outlive the worker usefully", gone)
		}
	}
	if !exists(oldLog) {
		t.Error("shutdown sweep must KEEP stderr diagnostics regardless of age — they age out via the startup sweep's window")
	}
}

func TestSweepToleratesMissingCloneDir(t *testing.T) {
	d, l := sweepScancodeDir(sweepTestLogger(), filepath.Join(t.TempDir(), "nope"), nil, 0, time.Now())
	if d != 0 || l != 0 {
		t.Errorf("a missing clone dir must sweep nothing, got dirs=%d logs=%d", d, l)
	}
}

// TestRunWiresBothSweeps pins the wiring: startup sweep AFTER
// recoverOrphans (it needs the post-recovery lock rows to build the
// keep set) and the shutdown sweep at the very end of Run.
func TestRunWiresBothSweeps(t *testing.T) {
	src := readScancodeWorkerSource(t)
	body := scancodeMethodBody(t, src, "func (w *ScancodeWorker) Run(")
	recover := strings.Index(body, "w.recoverOrphans(ctx)")
	// v0.27.13: the startup sweep must run in the BACKGROUND —
	// synchronous sweeping blocked dispatch for 20+ min on kate
	// (serial RemoveAll of multi-GB clones on a spinner). Pin the
	// safego.Go form so a refactor can't quietly re-serialize it.
	startup := strings.Index(body, `safego.Go(w.logger, "scancode-startup-sweep", func() { w.sweepCloneDirAtStartup(ctx) })`)
	shutdown := strings.Index(body, "w.sweepCloneDirAtShutdown()")
	if startup < 0 || shutdown < 0 {
		t.Fatal("Run must call both w.sweepCloneDirAtStartup(ctx) and w.sweepCloneDirAtShutdown() — without them hard-kill clone leaks accumulate forever")
	}
	if recover < 0 || startup < recover {
		t.Error("the startup sweep must run AFTER recoverOrphans — recovery may clear lock rows (freeing their dirs for removal) and adopts live orphans (whose dirs must be kept)")
	}
	if shutdown < startup {
		t.Error("the shutdown sweep must be the LAST step of Run")
	}
}

// TestStartupSweepKeepsOwnHostAndLegacyLocks pins the keep-set rule
// on the worker wrapper: own-host locks AND empty-host (pre-v0.27.6)
// locks protect their dirs; cross-host locks do not.
func TestStartupSweepKeepsOwnHostAndLegacyLocks(t *testing.T) {
	src := readScancodeWorkerSource(t)
	body := scancodeMethodBody(t, src, "func (w *ScancodeWorker) sweepCloneDirAtStartup(")
	if !strings.Contains(body, "ListLockedScancodeRows") {
		t.Error("sweepCloneDirAtStartup must build its keep set from the live lock rows")
	}
	if !strings.Contains(body, `r.LockedHost == ""`) || !strings.Contains(body, "r.LockedHost == w.hostname") {
		t.Error("the keep set must include own-host locks AND empty-host (pre-v0.27.6) locks; cross-host locks must NOT protect local dirs — a dir on our disk was created by our workers, and if another host owns the lock, our copy is stale by definition")
	}
}
