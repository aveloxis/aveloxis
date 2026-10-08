// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

// PR #226 Copilot review 5458301284 (High): a walk that inserted NEW commit
// rows but did not record its daily fold (the replace failed, the walk
// erred, or it was stopped) left an already-stamped repository marked
// complete, so the readers kept the old buckets without the new commits.
// SR-3: the stamp may only cover rows proven folded. The facade now clears
// it on exactly that outcome, under the writers' per-repository lock, and
// leaves it alone when the walk inserted nothing new.

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

func TestUnrecordedNewCommitsClearTheDailyStamp(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("integration: set AVELOXIS_TEST_DB to run")
	}
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	ctx := context.Background()
	store, err := db.NewPostgresStore(ctx, dsn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	pool := store.Pool()
	const repoGit = "https://github.com/_av_dailyinval/repo"
	cleanup := func() {
		for _, q := range []string{
			`DELETE FROM aveloxis_data.commit_parents WHERE cmt_id IN (SELECT cmt_id FROM aveloxis_data.commits WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git=$1))`,
			`DELETE FROM aveloxis_data.commit_messages WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git=$1)`,
			`DELETE FROM aveloxis_data.commits WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git=$1)`,
			`DELETE FROM aveloxis_data.repo_commit_daily WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git=$1)`,
			`DELETE FROM aveloxis_data.repos WHERE repo_git=$1`,
		} {
			if _, err := pool.Exec(context.Background(), q, repoGit); err != nil {
				t.Logf("cleanup: %v", err)
			}
		}
	}
	cleanup()
	t.Cleanup(cleanup)
	var repoID int64
	if err := pool.QueryRow(ctx, `INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id)
		VALUES ($1, '_av_dailyinval', 'repo', 1) RETURNING repo_id`, repoGit).Scan(&repoID); err != nil {
		t.Fatal(err)
	}

	work, bare := t.TempDir(), t.TempDir()
	git := func(env []string, args ...string) {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Env = append(os.Environ(), append([]string{"GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1",
			"GIT_AUTHOR_NAME=a", "GIT_AUTHOR_EMAIL=a@example.invalid", "GIT_COMMITTER_NAME=a", "GIT_COMMITTER_EMAIL=a@example.invalid"}, env...)...)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
	commit := func(name, date string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(work, name), []byte(name), 0o644); err != nil {
			t.Fatal(err)
		}
		git(nil, "-C", work, "add", name)
		git([]string{"GIT_AUTHOR_DATE=" + date, "GIT_COMMITTER_DATE=" + date}, "-C", work, "commit", "-q", "-m", name)
	}
	publish := func() {
		t.Helper()
		git(nil, "-C", work, "push", "-q", bare, "main")
		git(nil, "-C", bare, "update-server-info")
	}
	git(nil, "init", "-q", "-b", "main", work)
	commit("a", "2020-05-01T00:00:00Z")
	git(nil, "clone", "-q", "--bare", work, bare)
	git(nil, "-C", bare, "update-server-info")
	srv := httptest.NewServer(http.StripPrefix("/_av_dailyinval/repo.git", http.FileServer(http.Dir(bare))))
	defer srv.Close()
	url := srv.URL + "/_av_dailyinval/repo.git"

	complete := func() bool {
		t.Helper()
		ok, err := store.RepoCommitDailyComplete(ctx, repoID)
		if err != nil {
			t.Fatal(err)
		}
		return ok
	}
	f := NewFacadeCollector(store, slog.New(slog.NewTextHandler(io.Discard, nil)), t.TempDir())
	if _, err := f.CollectRepo(ctx, repoID, url); err != nil {
		t.Fatalf("first collection: %v", err)
	}
	if !complete() {
		t.Fatal("a clean walk must stamp the picture complete")
	}

	failReplace := func(ctx context.Context, repoID int64, rows []db.CommitDailyRow, trim bool) error {
		return errors.New("injected replace failure")
	}

	// Nothing new + a failed replace: the stamped rows still match the
	// table, so the stamp stays (no needless slow path).
	f.replaceCommitDaily = failReplace
	if _, err := f.CollectRepo(ctx, repoID, url); err != nil {
		t.Fatalf("walk with nothing new: %v", err)
	}
	if !complete() {
		t.Error("a walk that inserted nothing new must leave the stamp alone when its replace fails")
	}

	// A new commit + a failed replace: the table holds a commit the rows do
	// not — the stamp must go.
	commit("b", "2021-05-01T00:00:00Z")
	publish()
	if _, err := f.CollectRepo(ctx, repoID, url); err != nil {
		t.Fatalf("walk with a new commit: %v", err)
	}
	if complete() {
		t.Fatal("new commit rows whose fold was not recorded must clear the stamp (the readers would omit them)")
	}

	// A stopped replace counts the same (a canceled context is not a
	// failure to log, but the fold is still unrecorded).
	f.replaceCommitDaily = nil
	if _, err := f.CollectRepo(ctx, repoID, url); err != nil {
		t.Fatalf("clean walk: %v", err)
	}
	if !complete() {
		t.Fatal("the next clean walk must stamp the picture again")
	}
	commit("c", "2022-05-01T00:00:00Z")
	publish()
	f.replaceCommitDaily = func(ctx context.Context, repoID int64, rows []db.CommitDailyRow, trim bool) error {
		return context.Canceled
	}
	if _, err := f.CollectRepo(ctx, repoID, url); err != nil {
		t.Fatalf("walk whose replace was stopped: %v", err)
	}
	if complete() {
		t.Error("a stopped replace after new commit rows must clear the stamp too")
	}

	// And the clean walk after it records every commit.
	f.replaceCommitDaily = nil
	if _, err := f.CollectRepo(ctx, repoID, url); err != nil {
		t.Fatalf("final clean walk: %v", err)
	}
	var total int
	if err := pool.QueryRow(ctx, `SELECT COALESCE(SUM(commits), 0) FROM aveloxis_data.repo_commit_daily WHERE repo_id = $1`, repoID).Scan(&total); err != nil {
		t.Fatal(err)
	}
	if !complete() || total != 3 {
		t.Errorf("after a clean walk: complete=%t commits=%d, want true and 3", complete(), total)
	}
}

// The seam is test-only: production code never assigns it, so the facade
// always writes through the store.
func TestReplaceCommitDailySeamIsTestOnly(t *testing.T) {
	ents, err := os.ReadDir(filepath.Join(srctest.Root(t), "internal", "collector"))
	if err != nil {
		t.Fatal(err)
	}
	seen := 0
	for _, e := range ents {
		name := e.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		seen++
		if strings.Contains(srctest.StripGoComments(srctest.Read(t, "internal/collector/"+name)), ".replaceCommitDaily =") {
			t.Errorf("internal/collector/%s assigns the replaceCommitDaily seam; only tests may", name)
		}
	}
	if seen == 0 {
		t.Fatal("no production files scanned")
	}
}

// L10 round 1 on 0.29.79 (MEDIUM): a walk that swallowed commit writes
// records an UNTRIMMED fold, which keeps the stamp — but a chunk that
// committed new rows before its batch failed, and whose rows then failed
// again in the per-row fallback, is in the commits table and not in the
// fold. recordCommitDaily reports the fold complete only when it was
// recorded AND trimmed (every walked commit proven written and folded),
// so the deferred clear runs for that walk too.
func TestOnlyATrimmedRecordCountsAsComplete(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("integration: set AVELOXIS_TEST_DB to run")
	}
	ctx := context.Background()
	store, err := db.NewPostgresStore(ctx, dsn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	f := NewFacadeCollector(store, slog.New(slog.NewTextHandler(io.Discard, nil)), t.TempDir())
	f.replaceCommitDaily = func(context.Context, int64, []db.CommitDailyRow, bool) error { return nil }
	if !f.recordCommitDaily(ctx, -1, &FacadeResult{}) {
		t.Error("a recorded, trimmed fold is complete")
	}
	if f.recordCommitDaily(ctx, -1, &FacadeResult{CommitWriteFailures: 1}) {
		t.Error("a fold recorded without trimming (swallowed writes) must not count as complete: new rows may be missing from it")
	}
	f.replaceCommitDaily = func(context.Context, int64, []db.CommitDailyRow, bool) error { return errors.New("injected") }
	if f.recordCommitDaily(ctx, -1, &FacadeResult{}) {
		t.Error("a failed replace is not a recorded fold")
	}
}
