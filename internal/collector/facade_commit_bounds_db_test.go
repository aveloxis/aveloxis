// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

// O11 option 2 (operator decision, 2026-09-29): the facade maintains
// repos.first_commit_at / last_commit_at so the repository page never scans
// a giant repository's commit rows. Driven end to end with real git over
// the dumb HTTP protocol and the real store.

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
)

func TestFacadeMaintainsTheCommitBounds(t *testing.T) {
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
	const repoGit = "https://github.com/_av_cmtbounds/repo"
	cleanup := func() {
		for _, q := range []string{
			`DELETE FROM aveloxis_data.commit_parents WHERE cmt_id IN (SELECT cmt_id FROM aveloxis_data.commits WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git=$1))`,
			`DELETE FROM aveloxis_data.commit_messages WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git=$1)`,
			`DELETE FROM aveloxis_data.commits WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git=$1)`,
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
		VALUES ($1, '_av_cmtbounds', 'repo', 1) RETURNING repo_id`, repoGit).Scan(&repoID); err != nil {
		t.Fatal(err)
	}

	// A work repository with three commits whose author dates are out of
	// order (the newest commit is not the latest author date), pushed to a
	// bare repository served over dumb HTTP.
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
	git(nil, "init", "-q", "-b", "main", work)
	commit("a", "2012-05-01T00:00:00Z")
	commit("b", "2030-05-01T00:00:00Z") // a future-dated commit in history
	commit("c", "2018-05-01T00:00:00Z")
	git(nil, "clone", "-q", "--bare", work, bare)
	git(nil, "-C", bare, "update-server-info")
	srv := httptest.NewServer(http.StripPrefix("/_av_cmtbounds/repo.git", http.FileServer(http.Dir(bare))))
	defer srv.Close()
	url := srv.URL + "/_av_cmtbounds/repo.git"

	bounds := func() (first, last *time.Time) {
		t.Helper()
		if err := pool.QueryRow(ctx, `SELECT first_commit_at, last_commit_at FROM aveloxis_data.repos WHERE repo_id = $1`, repoID).
			Scan(&first, &last); err != nil {
			t.Fatal(err)
		}
		return first, last
	}
	at := func(s string) time.Time { v, _ := time.Parse(time.RFC3339, s); return v }
	eq := func(got *time.Time, want string) bool { return got != nil && got.Equal(at(want)) }

	f := NewFacadeCollector(store, slog.New(slog.NewTextHandler(io.Discard, nil)), t.TempDir())
	if _, err := f.CollectRepo(ctx, repoID, url); err != nil {
		t.Fatalf("first collection: %v", err)
	}
	if first, last := bounds(); !eq(first, "2012-05-01T00:00:00Z") || !eq(last, "2030-05-01T00:00:00Z") {
		t.Fatalf("after the first collection: first=%v last=%v; want 2012 and 2030", first, last)
	}

	// A filled row only widens: a run with nothing new keeps what is stored
	// (set wider by hand here, which the table does not support — a rescan
	// would narrow it back).
	if _, err := pool.Exec(ctx, `UPDATE aveloxis_data.repos SET first_commit_at = '2000-01-01Z', last_commit_at = '2040-01-01Z' WHERE repo_id = $1`, repoID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.CollectRepo(ctx, repoID, url); err != nil {
		t.Fatalf("second collection: %v", err)
	}
	if first, last := bounds(); !eq(first, "2000-01-01T00:00:00Z") || !eq(last, "2040-01-01T00:00:00Z") {
		t.Errorf("a run with nothing new narrowed a filled row: first=%v last=%v", first, last)
	}

	// A new commit dated past the stored bound widens it from the run's
	// own rows (no rescan happens on a filled row).
	commit("d", "2041-05-01T00:00:00Z")
	git(nil, "-C", work, "push", "-q", bare, "main")
	git(nil, "-C", bare, "update-server-info")
	if _, err := f.CollectRepo(ctx, repoID, url); err != nil {
		t.Fatalf("third collection: %v", err)
	}
	if _, last := bounds(); !eq(last, "2041-05-01T00:00:00Z") {
		t.Errorf("a new later commit: last=%v; want 2041", last)
	}
}

// TestRefusedCloneStillFillsTheCommitBounds — review round 3 F3: a
// repository whose clone keeps failing never reaches the git log walk, so
// its stored bounds would stay NULL for good and its page would keep the
// full scan. A refused clone fills an unfilled row from the rows already in
// the table.
func TestRefusedCloneStillFillsTheCommitBounds(t *testing.T) {
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
	const repoGit = "https://github.com/_av_cmtbounds/refused"
	cleanup := func() {
		for _, q := range []string{
			`DELETE FROM aveloxis_data.commits WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git=$1)`,
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
		VALUES ($1, '_av_cmtbounds', 'refused', 1) RETURNING repo_id`, repoGit).Scan(&repoID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO aveloxis_data.commits (repo_id, cmt_commit_hash, cmt_filename, cmt_author_timestamp)
		VALUES ($1, $2, 'f', '2017-01-01Z'), ($1, $3, 'f', '2019-01-01Z')`, repoID, strings.Repeat("9", 40), strings.Repeat("0", 40)); err != nil {
		t.Fatal(err)
	}
	refused := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer refused.Close()
	f := NewFacadeCollector(store, slog.New(slog.NewTextHandler(io.Discard, nil)), t.TempDir())
	if _, err := f.CollectRepo(ctx, repoID, refused.URL+"/o/r.git"); err == nil {
		t.Fatal("the refused clone succeeded")
	}
	var first, last *time.Time
	if err := pool.QueryRow(ctx, `SELECT first_commit_at, last_commit_at FROM aveloxis_data.repos WHERE repo_id = $1`, repoID).Scan(&first, &last); err != nil {
		t.Fatal(err)
	}
	if first == nil || last == nil || first.UTC().Year() != 2017 || last.UTC().Year() != 2019 {
		t.Errorf("after a refused clone: first=%v last=%v; want 2017 and 2019 from the table", first, last)
	}
}
