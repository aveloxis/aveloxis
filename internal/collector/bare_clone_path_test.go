// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

// bare_clone_path_test.go — v0.29.56. A repository that cannot be fetched
// at all (DMCA takedown, disabled by GitHub, deleted) has no bare clone,
// and every phase that reads one can only fail. In two hours of the
// 2026-09-17 chaoss.tv log 8 repos logged "analysis failed: no bare clone"
// and then spent a remote scorecard run each to be told the repository is
// unreachable. One spelling of the path, used by the facade that creates
// it, analysis that reads it, and the scheduler that skips without it.

import (
	"os"
	"path/filepath"
	"testing"
)

func TestBareClonePathAndPresence(t *testing.T) {
	dir := t.TempDir()
	path := BareClonePath(dir, 150166)
	if want := filepath.Join(dir, "repo_150166"); path != want {
		t.Errorf("BareClonePath = %q, want %q", path, want)
	}
	if HasBareClone(dir, 150166) {
		t.Error("no clone directory yet, but HasBareClone said yes")
	}
	if err := os.MkdirAll(path, 0o755); err != nil {
		t.Fatal(err)
	}
	if HasBareClone(dir, 150166) {
		t.Error("an empty directory is not a clone (a failed clone can leave one)")
	}
	if err := os.WriteFile(filepath.Join(path, "HEAD"), []byte("ref: refs/heads/main\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if !HasBareClone(dir, 150166) {
		t.Error("a clone with a HEAD must be recognized")
	}
}

// TestHasBareCloneDoesNotReadAStatFailureAsNoClone: only a genuine "not
// there" means there is no clone. An unreadable clone directory says
// nothing, and reading it as "no clone" asserts a takedown or deletion that
// did not happen — and skips analysis and scorecard for every repo behind
// that directory (SR-16: a probe needs an error arm).
func TestHasBareCloneDoesNotReadAStatFailureAsNoClone(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root: permission bits do not deny stat")
	}
	dir := t.TempDir()
	locked := filepath.Join(dir, "locked")
	if err := os.MkdirAll(filepath.Join(locked, "repo_1"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(locked, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(locked, 0o755) })

	if !HasBareClone(locked, 1) {
		t.Error("an unreadable clone directory was reported as 'no clone' — that is an assertion the stat never made")
	}
	// A directory that simply has no such repo is still "no clone".
	if HasBareClone(dir, 404) {
		t.Error("a repo with no clone directory must report no clone")
	}
}
