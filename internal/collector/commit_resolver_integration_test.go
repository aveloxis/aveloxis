// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestCommitResolverIntegration_Strategy4ViaSharedResolver is the v0.25.x
// regression guard for the 1c migration: resolveOne's Strategy 4 now routes
// through the shared ResolveEmailViaAPI (Search API → global commit-search)
// instead of the removed private githubEmailSearch. It pins that
// ResolveCommits still resolves an author end-to-end, writes the login to the
// commit + creates the contributor, AND that the NEW global-commit-search path
// is wired through (counted as ResolvedCommitSearch) and is idempotent.
//
// Gated on AVELOXIS_TEST_DB (a scratch DB, e.g. aveloxis_cascade_test). Uses a
// test-owned pool for seed/assert + a fake searchClient + an httptest server
// that 404s the Commits API (forcing Strategy 3 to miss → Strategy 4).
func TestCommitResolverIntegration_Strategy4ViaSharedResolver(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("integration: set AVELOXIS_TEST_DB to run")
	}
	ctx := context.Background()
	lg := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	defer pool.Close()
	store, err := db.NewPostgresStore(ctx, dsn, lg)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	defer store.Close()

	const (
		repoGit = "https://github.com/_av_cr_test/repo"
		hash    = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
		email   = "_av_cr_carol@example.com"
		login   = "_av_cr_carol"
		ghID    = int64(778899)
	)
	cleanup := func() {
		// FK-safe order: commits + login_history + identities + aliases →
		// contributors → repo. (contributor_login_history has ON DELETE
		// RESTRICT, so it must go before the contributor.)
		pool.Exec(ctx, `DELETE FROM aveloxis_data.commits WHERE cmt_commit_hash=$1`, hash)
		pool.Exec(ctx, `DELETE FROM aveloxis_data.contributor_login_history WHERE cntrb_id IN (SELECT cntrb_id FROM aveloxis_data.contributors WHERE gh_login=$1 OR cntrb_email=$2)`, login, email)
		pool.Exec(ctx, `DELETE FROM aveloxis_data.contributor_identities WHERE cntrb_id IN (SELECT cntrb_id FROM aveloxis_data.contributors WHERE gh_login=$1 OR cntrb_email=$2)`, login, email)
		pool.Exec(ctx, `DELETE FROM aveloxis_data.contributors_aliases WHERE alias_email=$1 OR cntrb_id IN (SELECT cntrb_id FROM aveloxis_data.contributors WHERE gh_login=$2)`, email, login)
		pool.Exec(ctx, `DELETE FROM aveloxis_data.contributors WHERE gh_login=$1 OR cntrb_email=$2`, login, email)
		pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_git=$1`, repoGit)
	}
	cleanup()
	t.Cleanup(cleanup)

	var repoID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.repos (platform_id, repo_git, repo_owner, repo_name)
		VALUES (1, $1, '_av_cr_test', 'repo') RETURNING repo_id`, repoGit).Scan(&repoID); err != nil {
		t.Fatalf("seed repo: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO aveloxis_data.commits (repo_id, cmt_commit_hash, cmt_author_raw_email, cmt_author_email)
		VALUES ($1, $2, $3, $3)`, repoID, hash, email); err != nil {
		t.Fatalf("seed commit: %v", err)
	}

	// Resolver with Strategy 3 forced to miss (httptest 404) and Strategy 4
	// served by a fake that resolves ONLY via global commit-search.
	keys := platform.NewKeyPool([]string{"x"}, lg)
	r := NewCommitResolver(store, keys, lg)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer ts.Close()
	r.http = platform.NewHTTPClient(ts.URL, keys, lg, platform.AuthGitHub)
	r.searchClient = &fakeSearchClient{commitLogin: login, commitID: ghID} // search miss, commit-search hit

	res, err := r.ResolveCommits(ctx, repoID, "_av_cr_test", "repo")
	if err != nil {
		t.Fatalf("ResolveCommits: %v", err)
	}
	if res.ResolvedCommitSearch < 1 {
		t.Errorf("expected the commit-search path to resolve the author; got %+v", res)
	}

	// The commit got the login.
	var gotLogin string
	pool.QueryRow(ctx, `SELECT cmt_author_platform_username FROM aveloxis_data.commits WHERE cmt_commit_hash=$1`, hash).Scan(&gotLogin)
	if gotLogin != login {
		t.Errorf("commit cmt_author_platform_username = %q, want %q", gotLogin, login)
	}
	// The contributor was created with the resolved login.
	var n int
	pool.QueryRow(ctx, `SELECT count(*) FROM aveloxis_data.contributors WHERE gh_login=$1`, login).Scan(&n)
	if n < 1 {
		t.Errorf("expected a contributor with gh_login=%q to be created", login)
	}

	// Idempotency: the commit is now resolved, so a re-run finds nothing to do.
	res2, err := r.ResolveCommits(ctx, repoID, "_av_cr_test", "repo")
	if err != nil {
		t.Fatalf("ResolveCommits re-run: %v", err)
	}
	if res2.TotalCommits != 0 {
		t.Errorf("re-run should see 0 unresolved commits (already resolved); got %d", res2.TotalCommits)
	}
}
