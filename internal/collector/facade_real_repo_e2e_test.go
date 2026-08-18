// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/db"
)

// TestFacadeRealRepoEndToEnd is the real-data canary for the v0.27.97 batch
// write path (summary/21 F2): it clones a REAL repository, runs the full
// facade pipeline (git log parse → build → UpsertCommitBatch /
// UpsertCommitMessageBatch → parents), and checks the stored rows against
// git's own ground truth. The mock-only-suites lesson
// ([[feedback_test_against_real_apis]]): synthetic-row e2es prove SQL
// semantics; only a real clone proves the whole path.
//
// Gated on BOTH AVELOXIS_TEST_DB (live scratch Postgres) and
// AVELOXIS_TEST_NETWORK=1 (real clone from GitHub). Rides the weekly
// network-canary workflow tier.
func TestFacadeRealRepoEndToEnd(t *testing.T) {
	if os.Getenv("AVELOXIS_TEST_NETWORK") != "1" {
		t.Skip("network canary: set AVELOXIS_TEST_NETWORK=1 to run")
	}
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set — skipping integration test")
	}
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := db.NewPostgresStore(ctx, dsn, logger)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(store.Close)

	// The project's own public repository: real history, multi-file
	// commits, thousands of commits — exercises chunking (>500 rows) and
	// the message batch on genuine git-log output.
	const gitURL = "https://github.com/aveloxis/aveloxis"

	// Clear any residue from a prior interrupted run, then seed fresh.
	const markerURL = "https://github.com/aveloxis/aveloxis-facade-e2e-marker"
	_, _ = store.Pool().Exec(ctx, `DELETE FROM aveloxis_data.commit_parents WHERE cmt_id IN
		(SELECT cmt_id FROM aveloxis_data.commits WHERE repo_id IN
			(SELECT repo_id FROM aveloxis_data.repos WHERE repo_git = $1))`, markerURL)
	_, _ = store.Pool().Exec(ctx, `DELETE FROM aveloxis_data.commit_messages WHERE repo_id IN
		(SELECT repo_id FROM aveloxis_data.repos WHERE repo_git = $1)`, markerURL)
	_, _ = store.Pool().Exec(ctx, `DELETE FROM aveloxis_data.commits WHERE repo_id IN
		(SELECT repo_id FROM aveloxis_data.repos WHERE repo_git = $1)`, markerURL)
	_, _ = store.Pool().Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_git = $1`, markerURL)

	var repoID int64
	if err := store.Pool().QueryRow(ctx, `
		INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id, repo_group_id)
		VALUES ($1, 'aveloxis', 'aveloxis-facade-e2e', 1, 1)
		RETURNING repo_id`, markerURL).Scan(&repoID); err != nil {
		t.Fatalf("seed repo row: %v", err)
	}
	cleanup := func() {
		_, _ = store.Pool().Exec(ctx, `DELETE FROM aveloxis_data.commit_parents WHERE cmt_id IN
			(SELECT cmt_id FROM aveloxis_data.commits WHERE repo_id = $1)`, repoID)
		_, _ = store.Pool().Exec(ctx, `DELETE FROM aveloxis_data.commit_messages WHERE repo_id = $1`, repoID)
		_, _ = store.Pool().Exec(ctx, `DELETE FROM aveloxis_data.commits WHERE repo_id = $1`, repoID)
		_, _ = store.Pool().Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, repoID)
	}
	t.Cleanup(cleanup)

	repoDir := t.TempDir()
	fc := NewFacadeCollector(store, logger, repoDir)

	result, err := fc.CollectRepo(ctx, repoID, gitURL)
	if err != nil {
		t.Fatalf("facade CollectRepo against real repo: %v", err)
	}
	if result.Commits == 0 {
		t.Fatal("facade collected 0 commits from a real repository")
	}

	// Ground truth from the bare clone the facade itself made.
	clonePath := filepath.Join(repoDir, "repo_"+strconv.FormatInt(repoID, 10))
	out, gitErr := exec.Command("git", "-C", clonePath, "rev-list", "--count", "HEAD").Output()
	if gitErr != nil {
		t.Fatalf("git rev-list ground truth: %v", gitErr)
	}
	want, _ := strconv.Atoi(strings.TrimSpace(string(out)))

	var distinct int
	if err := store.Pool().QueryRow(ctx, `
		SELECT COUNT(DISTINCT cmt_commit_hash) FROM aveloxis_data.commits WHERE repo_id = $1`,
		repoID).Scan(&distinct); err != nil {
		t.Fatalf("count distinct hashes: %v", err)
	}
	if distinct != want {
		t.Errorf("stored distinct commits = %d, git rev-list --count = %d — "+
			"the batch write path lost or duplicated commits", distinct, want)
	}
	if result.Commits != want {
		t.Errorf("FacadeResult.Commits = %d, git ground truth = %d (the "+
			"v0.19.11 distinct-commit contract)", result.Commits, want)
	}

	var msgCount int
	if err := store.Pool().QueryRow(ctx, `
		SELECT COUNT(*) FROM aveloxis_data.commit_messages WHERE repo_id = $1`,
		repoID).Scan(&msgCount); err != nil {
		t.Fatalf("count messages: %v", err)
	}
	if msgCount == 0 {
		t.Error("no commit messages stored from a real repository")
	}
	var parentCount int
	if err := store.Pool().QueryRow(ctx, `
		SELECT COUNT(*) FROM aveloxis_data.commit_parents cp
		JOIN aveloxis_data.commits c ON c.cmt_id = cp.cmt_id
		WHERE c.repo_id = $1`, repoID).Scan(&parentCount); err != nil {
		t.Fatalf("count parents: %v", err)
	}
	if parentCount == 0 {
		t.Error("no commit parents stored — the post-batch parent phase " +
			"(v0.27.97 ordering) is not linking")
	}

	// Idempotent re-run: same clone, same batch path, zero new rows.
	var rowsBefore int
	_ = store.Pool().QueryRow(ctx, `SELECT COUNT(*) FROM aveloxis_data.commits WHERE repo_id = $1`, repoID).Scan(&rowsBefore)
	if _, err := fc.CollectRepo(ctx, repoID, gitURL); err != nil {
		t.Fatalf("second CollectRepo: %v", err)
	}
	var rowsAfter int
	_ = store.Pool().QueryRow(ctx, `SELECT COUNT(*) FROM aveloxis_data.commits WHERE repo_id = $1`, repoID).Scan(&rowsAfter)
	if rowsAfter != rowsBefore {
		t.Errorf("re-collection changed commit rows %d → %d — batch path must be idempotent", rowsBefore, rowsAfter)
	}
}
