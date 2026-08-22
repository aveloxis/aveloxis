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

	repoID := int64(944_150_001)
	if _, err := store.Pool().Exec(ctx, `INSERT INTO aveloxis_data.repos (repo_id, repo_git, repo_owner, repo_name, platform_id)
		VALUES ($1, 'https://github.com/_avgoclosure/keep', '_avgoclosure', 'keep', 1)
		ON CONFLICT (repo_id) DO NOTHING`, repoID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		for _, q := range []string{
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
