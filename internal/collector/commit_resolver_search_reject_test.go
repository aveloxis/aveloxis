// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/platform"
)

// rejectingSearchClient answers every search the way GitHub answers a
// query it cannot process: 422, wrapped as platform.ErrRequestRejected.
type rejectingSearchClient struct{ calls int }

func (f *rejectingSearchClient) SearchUserByEmail(context.Context, string) (string, int64, error) {
	f.calls++
	return "", 0, fmt.Errorf("unprocessable entity: https://api.github.com/search/users: %w", platform.ErrRequestRejected)
}

func (f *rejectingSearchClient) SearchCommitByAuthorEmail(context.Context, string) (string, int64, error) {
	f.calls++
	return "", 0, fmt.Errorf("unprocessable entity: https://api.github.com/search/commits: %w", platform.ErrRequestRejected)
}

// TestCommitResolverTreatsARejectedSearchAsNoMatch (AVELOXIS_TEST_DB) —
// 2026-09-23 log review: an author email GitHub's search cannot process
// ("m - @ - halle.us") came back 422 on every commit. The resolver counted
// it as an ERROR, did not cache it (so it searched again per commit: 30
// calls against a 30/minute limit), and fed the 422 into Consecutive422 —
// the counter meant for commit SHAs a stale clone does not own, which
// aborts the repo after 50 with a "delete the clone dir" hint. The platform
// already classifies the rejection as a definitive answer
// (IsDefinitiveAnswer; the scheduler's search sweep stamps it): here it is
// a no-match, cached for the run, never an error.
func TestCommitResolverTreatsARejectedSearchAsNoMatch(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("integration: set AVELOXIS_TEST_DB to run")
	}
	ctx := context.Background()
	lg := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	store, err := db.NewPostgresStore(ctx, dsn, lg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)

	const (
		repoGit = "https://github.com/_av_cr_reject/repo"
		email   = "m - @ - halle.us"
	)
	cleanup := func() {
		pool.Exec(ctx, `DELETE FROM aveloxis_data.commits WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git=$1)`, repoGit)
		pool.Exec(ctx, `DELETE FROM aveloxis_data.unresolved_commit_emails WHERE email=$1`, email)
		pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_git=$1`, repoGit)
	}
	cleanup()
	t.Cleanup(cleanup)

	var repoID int64
	if err := pool.QueryRow(ctx, `INSERT INTO aveloxis_data.repos (platform_id, repo_git, repo_owner, repo_name)
		VALUES (1, $1, '_av_cr_reject', 'repo') RETURNING repo_id`, repoGit).Scan(&repoID); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		hash := fmt.Sprintf("%040d", i+1)
		if _, err := pool.Exec(ctx, `INSERT INTO aveloxis_data.commits (repo_id, cmt_commit_hash, cmt_author_raw_email, cmt_author_email)
			VALUES ($1, $2, $3, $3)`, repoID, hash, email); err != nil {
			t.Fatal(err)
		}
	}

	keys := platform.NewKeyPool([]string{"x"}, lg)
	r := NewCommitResolver(store, keys, "", lg)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound) // the Commits API misses: the search step runs
	}))
	defer ts.Close()
	r.http = platform.NewHTTPClient(ts.URL, keys, lg, platform.AuthGitHub)
	search := &rejectingSearchClient{}
	r.searchClient = search

	res, err := r.ResolveCommits(ctx, repoID, "_av_cr_reject", "repo")
	if err != nil {
		t.Fatalf("ResolveCommits: %v", err)
	}
	if res.Errors != 0 {
		t.Errorf("a rejected search is a definitive no-match, not an error: Errors=%d", res.Errors)
	}
	if res.Unresolved != 3 {
		t.Errorf("Unresolved=%d, want all 3 commits (the email has no GitHub user)", res.Unresolved)
	}
	if res.Consecutive422 != 0 {
		t.Errorf("Consecutive422=%d — a search rejection must not feed the stale-clone abort counter", res.Consecutive422)
	}
	if search.calls != 1 {
		t.Errorf("the search ran %d times for one email — the no-match must be cached for the run", search.calls)
	}
}
