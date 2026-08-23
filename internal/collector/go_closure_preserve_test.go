// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// go_closure_preserve_test.go — v0.27.150 (round 29 active #3): the
// Go toolchain expansion is best-effort, but the lockfile snapshot
// REPLACE is not — an incomplete expansion used to wipe the prior Go
// closure, silently shrinking vuln/SBOM output and false-resolving
// findings. This behavioral test drives the exact failure input: a
// repo with a stored Go closure re-scanned while the go toolchain is
// UNAVAILABLE (PATH emptied) — the prior closure must survive the
// snapshot replace.

package collector

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
)

func TestIncompleteGoExpansionPreservesPriorClosure(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := db.NewPostgresStore(ctx, dsn, logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	store.SetMatviewSkip(true)
	testMigrate(ctx, t, store)

	// v0.27.152 (round 31): the repo_id comes from the SEQUENCE via
	// UpsertRepo (keyed on the test-namespaced URL) — a fixed literal
	// PK with ON CONFLICT DO NOTHING could silently reuse an
	// unrelated existing row and DELETE it in cleanup.
	repoID, err := store.UpsertRepo(ctx, &model.Repo{
		Platform: model.PlatformGitHub,
		GitURL:   "https://github.com/_avgoclosure/keep",
		Owner:    "_avgoclosure", Name: "keep",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		for _, q := range []string{
			`DELETE FROM aveloxis_ops.collection_queue WHERE repo_id = $1`,
			`DELETE FROM aveloxis_data.repo_lockfile_edges WHERE repo_id = $1`,
			`DELETE FROM aveloxis_data.repo_lockfile_packages WHERE repo_id = $1`,
			`DELETE FROM aveloxis_data.repo_lockfiles WHERE repo_id = $1`,
			`DELETE FROM aveloxis_data.repos WHERE repo_id = $1`,
		} {
			if _, derr := store.Pool().Exec(cctx, q, repoID); derr != nil {
				t.Logf("cleanup %q: %v", q, derr)
			}
		}
	})

	// The PRIOR snapshot: one Go transitive package + one edge (a
	// healthy earlier run's closure).
	prior := []*db.RepoLockfilePackage{{
		Ecosystem: "go", PackageName: "golang.org/x/text",
		ResolvedVersion: "v0.14.0", LockfilePath: "go.mod", Direct: false,
	}}
	priorEdges := []*db.RepoLockfileEdge{{
		Ecosystem: "go", LockfilePath: "go.mod",
		ParentName: "example.com/app", ParentVersion: "v1.0.0",
		ChildName: "golang.org/x/text", ChildConstraint: "v0.14.0",
	}}
	if err := store.ReplaceRepoLockfileSnapshot(ctx, repoID, nil, prior, priorEdges); err != nil {
		t.Fatal(err)
	}

	// A checkout with a go.mod, scanned while the toolchain is
	// UNREACHABLE — LookPath("go") fails, the expansion reports
	// incomplete, and the preserve branch must carry the prior
	// closure into the replacement.
	workDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(workDir, "go.mod"), []byte("module example.com/app\n\ngo 1.22\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", "")

	ac := NewAnalysisCollector(store, logger, t.TempDir())
	ac.TransitiveLockfiles = true
	if err := ac.scanLockfiles(ctx, repoID, workDir, &AnalysisResult{}); err != nil {
		t.Fatalf("scanLockfiles: %v", err)
	}

	var pkgCount, edgeCount int
	if err := store.Pool().QueryRow(ctx, `SELECT COUNT(*) FROM aveloxis_data.repo_lockfile_packages
		WHERE repo_id = $1 AND ecosystem = 'go' AND package_name = 'golang.org/x/text'`, repoID).Scan(&pkgCount); err != nil {
		t.Fatal(err)
	}
	if err := store.Pool().QueryRow(ctx, `SELECT COUNT(*) FROM aveloxis_data.repo_lockfile_edges
		WHERE repo_id = $1 AND ecosystem = 'go' AND child_name = 'golang.org/x/text'`, repoID).Scan(&edgeCount); err != nil {
		t.Fatal(err)
	}
	if pkgCount != 1 || edgeCount != 1 {
		t.Fatalf("an incomplete Go expansion must PRESERVE the prior closure through the snapshot replace (got %d pkgs / %d edges — pre-fix both were wiped, silently shrinking vuln/SBOM output)", pkgCount, edgeCount)
	}
}

// v0.27.152 (round 31, suppressed): a repo that REMOVED all its Go
// modules must be able to CLEAR its stale closure even on a toolless
// host — zero go.mod files is a definitively complete (empty)
// expansion, so the preserve branch must not fire. Pre-fix, the
// LookPath check ran before module discovery and reported incomplete
// forever, carrying the dead closure on every scan.
func TestGoModuleRemovalClearsStaleClosure(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := db.NewPostgresStore(ctx, dsn, logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	store.SetMatviewSkip(true)
	testMigrate(ctx, t, store)

	repoID, err := store.UpsertRepo(ctx, &model.Repo{
		Platform: model.PlatformGitHub,
		GitURL:   "https://github.com/_avgoclosure/removed",
		Owner:    "_avgoclosure", Name: "removed",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		for _, q := range []string{
			`DELETE FROM aveloxis_ops.collection_queue WHERE repo_id = $1`,
			`DELETE FROM aveloxis_data.repo_lockfile_edges WHERE repo_id = $1`,
			`DELETE FROM aveloxis_data.repo_lockfile_packages WHERE repo_id = $1`,
			`DELETE FROM aveloxis_data.repo_lockfiles WHERE repo_id = $1`,
			`DELETE FROM aveloxis_data.repos WHERE repo_id = $1`,
		} {
			if _, derr := store.Pool().Exec(cctx, q, repoID); derr != nil {
				t.Logf("cleanup %q: %v", q, derr)
			}
		}
	})

	prior := []*db.RepoLockfilePackage{{
		Ecosystem: "go", PackageName: "golang.org/x/dead",
		ResolvedVersion: "v0.1.0", LockfilePath: "go.mod", Direct: false,
	}}
	if err := store.ReplaceRepoLockfileSnapshot(ctx, repoID, nil, prior, nil); err != nil {
		t.Fatal(err)
	}

	// The checkout no longer contains ANY go.mod, and the host has no
	// go toolchain — the finding's exact scenario.
	workDir := t.TempDir()
	t.Setenv("PATH", "")

	ac := NewAnalysisCollector(store, logger, t.TempDir())
	ac.TransitiveLockfiles = true
	if err := ac.scanLockfiles(ctx, repoID, workDir, &AnalysisResult{}); err != nil {
		t.Fatalf("scanLockfiles: %v", err)
	}

	var stale int
	if err := store.Pool().QueryRow(ctx, `SELECT COUNT(*) FROM aveloxis_data.repo_lockfile_packages
		WHERE repo_id = $1 AND ecosystem = 'go'`, repoID).Scan(&stale); err != nil {
		t.Fatal(err)
	}
	if stale != 0 {
		t.Fatalf("a repo with zero go.mod files must CLEAR its stale Go closure (got %d rows) — zero modules is a complete empty expansion, toolchain or not", stale)
	}
}
