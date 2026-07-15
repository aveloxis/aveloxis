// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

// v0.27.11 — lockfile analysis-walk tests. collectLockfiles is
// filesystem-pure (no DB) so the walk behavior — roster matching,
// skip rules, best-effort parse errors, bun.lockb detect-only — is
// covered without infrastructure. The scanLockfiles wiring (phase
// order, snapshot call) is pinned at source level; the DB round-trip
// lives in the AVELOXIS_TEST_DB tier.

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeTree(t *testing.T, root string, files map[string]string) {
	t.Helper()
	for rel, content := range files {
		full := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCollectLockfilesWalk(t *testing.T) {
	root := t.TempDir()
	poetry := string(readLockFixture(t, "poetry.lock"))
	cargo := string(readLockFixture(t, "Cargo.lock"))
	writeTree(t, root, map[string]string{
		"poetry.lock":                poetry,
		"svc/Cargo.lock":             cargo,
		"requirements.txt":           "flask==2.3.3\n", // NEVER a lockfile
		"node_modules/x/poetry.lock": poetry,           // skipped dir
		"vendor/poetry.lock":         poetry,           // skipped dir
		"broken/pnpm-lock.yaml":      "not: valid: [yaml",
		"README.md":                  "hi",
	})

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	got := collectLockfiles(root, logger)

	paths := map[string]string{}
	for _, pl := range got {
		paths[pl.Path] = pl.Result.Kind
	}
	if len(got) != 2 {
		t.Fatalf("want exactly 2 lockfiles (poetry.lock + svc/Cargo.lock), got %v", paths)
	}
	if paths["poetry.lock"] != "poetry.lock" || paths["svc/Cargo.lock"] != "Cargo.lock" {
		t.Errorf("walk results wrong: %v", paths)
	}
	// requirements.txt excluded (operator ruling), vendored/node_modules
	// trees skipped, malformed lockfile skipped with a WARN — none of
	// them may appear.
	for p := range paths {
		if strings.Contains(p, "requirements.txt") || strings.Contains(p, "vendor") ||
			strings.Contains(p, "node_modules") || strings.Contains(p, "broken") {
			t.Errorf("walk must not include %s", p)
		}
	}
}

func TestCollectLockfilesDetectsBunLockb(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "bun.lockb"),
		[]byte("bun-lockfile-format-v0\x00\x01binary"), 0o644); err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	got := collectLockfiles(root, logger)
	if len(got) != 1 || got[0].Result.Kind != "bun.lockb" || len(got[0].Result.Entries) != 0 {
		t.Fatalf("bun.lockb must be inventoried (kind marker, zero entries): %+v", got)
	}
}

// scanLockfiles wiring pins: Phase 2b runs AFTER the libyear phase
// (the declared-deps filter reads the rows Phase 2 just wrote) and
// persists via the single-transaction snapshot replace.
func TestScanLockfilesWiring(t *testing.T) {
	src, err := os.ReadFile("analysis.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	libyearIdx := strings.Index(s, "ac.scanLibyear(")
	lockfileIdx := strings.Index(s, "ac.scanLockfiles(")
	if lockfileIdx < 0 {
		t.Fatal("AnalyzeRepo must run scanLockfiles (Phase 2b)")
	}
	if libyearIdx < 0 || lockfileIdx < libyearIdx {
		t.Error("scanLockfiles must run AFTER scanLibyear — the declared-deps storage filter reads repo_deps_libyear")
	}

	scan, err := os.ReadFile("lockfile_scan.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, needle := range []string{
		"ReplaceRepoLockfileSnapshot(", // one-tx snapshot replace
		"GetRepoDepsForVulnScan(",      // declared direct deps
		"lockfileMatchKey(",            // ecosystem-aware name matching
	} {
		if !strings.Contains(string(scan), needle) {
			t.Errorf("scanLockfiles must use %s", needle)
		}
	}
}
