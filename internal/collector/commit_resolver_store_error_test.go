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
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/platform"
)

type transientSearchClient struct{ calls int }

func (f *transientSearchClient) SearchUserByEmail(context.Context, string) (string, int64, error) {
	f.calls++
	return "", 0, fmt.Errorf("search/users timed out on GitHub's side (incomplete_results): %w", platform.ErrTransient)
}

func (f *transientSearchClient) SearchCommitByAuthorEmail(context.Context, string) (string, int64, error) {
	f.calls++
	return "", 0, fmt.Errorf("search/commits timed out on GitHub's side (incomplete_results): %w", platform.ErrTransient)
}

// TestCommitResolverReturnsTheStoreError pins worklist item 17 at the commit
// resolver, behaviourally (review round 1 replaced a source pin that a
// warn-and-continue arm satisfied): a FindLoginByEmail failure is returned
// before any API strategy runs.
func TestCommitResolverReturnsTheStoreError(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("integration: set AVELOXIS_TEST_DB to run")
	}
	ctx := context.Background()
	lg := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	store, err := db.NewPostgresStore(ctx, dsn, lg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	// The store fails while the context is live (a closed pool), so a
	// warn-and-continue arm would go on to the Commits API and the search;
	// a cancelled context would have stopped those too and proved nothing.
	store.Close()
	keys := platform.NewKeyPool([]string{"x"}, lg)
	r := NewCommitResolver(store, keys, "", lg)
	apiCalls := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		apiCalls++
		w.WriteHeader(http.StatusNotFound)
	}))
	defer ts.Close()
	r.http = platform.NewHTTPClient(ts.URL, keys, lg, platform.AuthGitHub)
	search := &transientSearchClient{}
	r.searchClient = search
	login, _, err := r.resolveOne(ctx, 1, "o", "r", unresolvedCommit{Hash: "0000000000000000000000000000000000000001", Email: "someone@example.invalid"}, &ResolveResult{})
	if err == nil || login != "" {
		t.Errorf("a failed store lookup = (%q, %v); want the error returned", login, err)
	}
	if apiCalls != 0 || search.calls != 0 {
		t.Errorf("the API strategies ran after a store failure (commits API %d, search %d calls)", apiCalls, search.calls)
	}
}

// TestCommitResolverMemoisesATransientSearchFailure pins review round 1's
// finding on item 16: once a search fails without an answer the failure is
// not cached as a miss (right), but without a run-scoped memo every later
// commit by the same author re-spent the search budget and logged a WARN.
// The first commit records the failure; the rest of the run skips the API
// for that email, counted apart (TransientSkipped), nothing stamped.
func TestCommitResolverMemoisesATransientSearchFailure(t *testing.T) {
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
		repoGit = "https://github.com/_av_cr_transient/repo"
		email   = "prolific@example.invalid"
	)
	cleanup := func() {
		pool.Exec(ctx, `DELETE FROM aveloxis_data.commits WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git=$1)`, repoGit)
		pool.Exec(ctx, `DELETE FROM aveloxis_data.unresolved_commit_emails WHERE email=$1`, email)
		pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_git=$1`, repoGit)
		// Commit 3 resolves and creates a contributor + alias; on a rerun in
		// the same database that alias would answer at Strategy 2 and the
		// search would never run (review round 3).
		pool.Exec(ctx, `DELETE FROM aveloxis_data.contributors_aliases WHERE alias_email=$1`, email)
		pool.Exec(ctx, `DELETE FROM aveloxis_data.contributors WHERE gh_login=$1 OR cntrb_login=$1`, "_avcr_octo")
	}
	cleanup()
	t.Cleanup(cleanup)
	var repoID int64
	if err := pool.QueryRow(ctx, `INSERT INTO aveloxis_data.repos (platform_id, repo_git, repo_owner, repo_name)
		VALUES (1, $1, '_av_cr_transient', 'repo') RETURNING repo_id`, repoGit).Scan(&repoID); err != nil {
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
	// Commits resolve in hash order. The Commits API misses for commits 1
	// and 2 (commit 1's search fails without an answer and sets the memo;
	// commit 2 skips the search) and answers for commit 3: the memo covers
	// the SEARCH that failed, not the per-commit SHA lookup, which still runs
	// and can resolve a commit (review round 2, decided as the class).
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if strings.HasSuffix(req.URL.Path, fmt.Sprintf("%040d", 3)) {
			_, _ = w.Write([]byte(`{"sha":"x","author":{"login":"_avcr_octo","id":424242}}`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer ts.Close()
	r.http = platform.NewHTTPClient(ts.URL, keys, lg, platform.AuthGitHub)
	search := &transientSearchClient{}
	r.searchClient = search

	res, err := r.ResolveCommits(ctx, repoID, "_av_cr_transient", "repo")
	if err != nil {
		t.Fatalf("ResolveCommits: %v", err)
	}
	if search.calls != 1 {
		t.Errorf("the search ran %d times for one email whose search failed without an answer — the failure must be memoised for the run", search.calls)
	}
	if res.Errors != 1 || res.TransientSkipped != 1 || res.ResolvedAPI != 1 {
		t.Errorf("Errors=%d TransientSkipped=%d ResolvedAPI=%d; want 1, 1, 1 (one commit records the search failure, one skips the search, one resolves through its SHA regardless)", res.Errors, res.TransientSkipped, res.ResolvedAPI)
	}
	var stamped int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM aveloxis_data.unresolved_commit_emails WHERE email = $1`, email).Scan(&stamped); err != nil {
		t.Fatal(err)
	}
	if stamped != 0 {
		t.Errorf("the email was recorded as unresolved %d times; a failure without an answer is not a miss", stamped)
	}
}

// byEmailSearchClient answers per email: an ErrTransient-wrapped failure for
// transient addresses, the key pool's literal refusal for refused ones.
type byEmailSearchClient struct {
	transient, refused map[string]bool
	calls              int
}

func (f *byEmailSearchClient) SearchUserByEmail(_ context.Context, email string) (string, int64, error) {
	f.calls++
	switch {
	case f.refused[email]:
		// The shape KeyPool.Acquire returns (v0.29.70: typed ErrNoKeys, same text).
		return "", 0, fmt.Errorf("%w — add keys via 'aveloxis add-key' or the database", platform.ErrNoKeys)
	case f.transient[email]:
		return "", 0, fmt.Errorf("search/users timed out: %w", platform.ErrTransient)
	}
	return "", 0, nil
}
func (f *byEmailSearchClient) SearchCommitByAuthorEmail(_ context.Context, email string) (string, int64, error) {
	return f.SearchUserByEmail(context.Background(), email)
}

// TestKeyExhaustionExcludesMemoisedCommits pins review round 2 of item 16:
// the two "remaining" formulas omitted TransientSkipped (and, since long
// before, ResolvedCommitSearch), so a key refusal after a memoised author
// counted the skipped commits as key-exhausted and flipped the run to
// "commit resolution FAILED". One accounting (ResolveResult.accounted).
func TestKeyExhaustionExcludesMemoisedCommits(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("integration: set AVELOXIS_TEST_DB to run")
	}
	ctx := context.Background()
	lg := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError + 1}))
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
	const repoGit = "https://github.com/_av_cr_keyacct/repo"
	emails := []string{"a@example.invalid", "a@example.invalid", "a@example.invalid", "b@example.invalid"}
	cleanup := func() {
		pool.Exec(ctx, `DELETE FROM aveloxis_data.commits WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git=$1)`, repoGit)
		pool.Exec(ctx, `DELETE FROM aveloxis_data.unresolved_commit_emails WHERE email = ANY($1)`, emails)
		pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_git=$1`, repoGit)
	}
	cleanup()
	t.Cleanup(cleanup)
	var repoID int64
	if err := pool.QueryRow(ctx, `INSERT INTO aveloxis_data.repos (platform_id, repo_git, repo_owner, repo_name)
		VALUES (1, $1, '_av_cr_keyacct', 'repo') RETURNING repo_id`, repoGit).Scan(&repoID); err != nil {
		t.Fatal(err)
	}
	for i, email := range emails {
		if _, err := pool.Exec(ctx, `INSERT INTO aveloxis_data.commits (repo_id, cmt_commit_hash, cmt_author_raw_email, cmt_author_email)
			VALUES ($1, $2, $3, $3)`, repoID, fmt.Sprintf("%040d", i+1), email); err != nil {
			t.Fatal(err)
		}
	}
	keys := platform.NewKeyPool([]string{"x"}, lg)
	r := NewCommitResolver(store, keys, "", lg)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNotFound) }))
	defer ts.Close()
	r.http = platform.NewHTTPClient(ts.URL, keys, lg, platform.AuthGitHub)
	r.searchClient = &byEmailSearchClient{transient: map[string]bool{"a@example.invalid": true}, refused: map[string]bool{"b@example.invalid": true}}

	res, err := r.ResolveCommits(ctx, repoID, "_av_cr_keyacct", "repo")
	if err != nil {
		t.Fatalf("ResolveCommits: %v", err)
	}
	// The three "a" commits: one records the failure, two skip. Only "b" was
	// refused for keys.
	if res.KeyExhausted != 1 || res.TransientSkipped != 2 || res.Errors != 1 {
		t.Errorf("KeyExhausted=%d TransientSkipped=%d Errors=%d; want 1, 2, 1", res.KeyExhausted, res.TransientSkipped, res.Errors)
	}
	if !res.IsSuccess() {
		t.Error("the run reads as FAILED (most commits unresolved for keys) although one commit was refused for keys")
	}
}

// TestWriteFailuresAreNotCountedAsResolved pins batch-3 review round 3: a
// commit whose author was found but whose write failed was counted under
// BOTH a Resolved* counter and Errors, so accounted() exceeded the commits
// visited, KeyExhausted went negative, and a refused run read "complete".
// Three noreply commits (resolved without an API call) whose commit UPDATE
// is refused by a trigger, then one commit refused for keys: one commit is
// key-exhausted, and every commit is counted once.
func TestWriteFailuresAreNotCountedAsResolved(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("integration: set AVELOXIS_TEST_DB to run")
	}
	ctx := context.Background()
	lg := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError + 1}))
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
		repoGit = "https://github.com/_av_cr_writefail/repo"
		trigger = "_avtest_cr_writefail"
	)
	emails := []string{"424243+avcrnoreply@users.noreply.github.com", "424243+avcrnoreply@users.noreply.github.com", "424243+avcrnoreply@users.noreply.github.com", "z@example.invalid"}
	cleanup := func() {
		pool.Exec(ctx, `DROP TRIGGER IF EXISTS `+trigger+` ON aveloxis_data.commits`)
		pool.Exec(ctx, `DROP FUNCTION IF EXISTS aveloxis_data.`+trigger+`()`)
		pool.Exec(ctx, `DELETE FROM aveloxis_data.commits WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git=$1)`, repoGit)
		pool.Exec(ctx, `DELETE FROM aveloxis_data.unresolved_commit_emails WHERE email = ANY($1)`, emails)
		pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_git=$1`, repoGit)
		pool.Exec(ctx, `DELETE FROM aveloxis_data.contributors_aliases WHERE alias_email = ANY($1)`, emails)
		pool.Exec(ctx, `DELETE FROM aveloxis_data.contributors WHERE gh_login=$1 OR cntrb_login=$1`, "avcrnoreply")
	}
	cleanup()
	t.Cleanup(cleanup)
	var repoID int64
	if err := pool.QueryRow(ctx, `INSERT INTO aveloxis_data.repos (platform_id, repo_git, repo_owner, repo_name)
		VALUES (1, $1, '_av_cr_writefail', 'repo') RETURNING repo_id`, repoGit).Scan(&repoID); err != nil {
		t.Fatal(err)
	}
	for i, email := range emails {
		if _, err := pool.Exec(ctx, `INSERT INTO aveloxis_data.commits (repo_id, cmt_commit_hash, cmt_author_raw_email, cmt_author_email)
			VALUES ($1, $2, $3, $3)`, repoID, fmt.Sprintf("%040d", i+1), email); err != nil {
			t.Fatal(err)
		}
	}
	// Every author-login UPDATE on this repository's commits is refused.
	if _, err := pool.Exec(ctx, fmt.Sprintf(`CREATE FUNCTION aveloxis_data.`+trigger+`() RETURNS trigger LANGUAGE plpgsql AS $f$
		BEGIN IF NEW.repo_id = %d THEN RAISE EXCEPTION 'injected write failure'; END IF; RETURN NEW; END $f$`, repoID)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `CREATE TRIGGER `+trigger+` BEFORE UPDATE ON aveloxis_data.commits FOR EACH ROW EXECUTE FUNCTION aveloxis_data.`+trigger+`()`); err != nil {
		t.Fatal(err)
	}
	keys := platform.NewKeyPool([]string{"x"}, lg)
	r := NewCommitResolver(store, keys, "", lg)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNotFound) }))
	defer ts.Close()
	r.http = platform.NewHTTPClient(ts.URL, keys, lg, platform.AuthGitHub)
	r.searchClient = &byEmailSearchClient{refused: map[string]bool{"z@example.invalid": true}}

	res, err := r.ResolveCommits(ctx, repoID, "_av_cr_writefail", "repo")
	if err != nil {
		t.Fatalf("ResolveCommits: %v", err)
	}
	if res.WriteFailed != 3 || res.Errors != 3 {
		t.Errorf("WriteFailed=%d Errors=%d; want 3 and 3 (each resolved-then-refused commit is one error)", res.WriteFailed, res.Errors)
	}
	if res.KeyExhausted != 1 {
		t.Errorf("KeyExhausted=%d; want 1 (only the last commit was refused for keys)", res.KeyExhausted)
	}
	// The three write-refused commits are accounted (as errors); the fourth
	// is the remaining one the refusal left, which KeyExhausted names.
	if a := res.accounted(); a != 3 || a+res.KeyExhausted != res.TotalCommits {
		t.Errorf("accounted()=%d + KeyExhausted %d of %d commits; every commit is counted exactly once", a, res.KeyExhausted, res.TotalCommits)
	}
	if !res.IsSuccess() {
		t.Error("the run reads as FAILED although one commit was refused for keys")
	}
}

// TestContributorWriteFailuresAreCountedOnce pins the second WriteFailed
// arm (batch-3 review round 4): the contributor upsert in ensureContributor.
// Only the first commit per author reaches it (the run's email cache answers
// the rest), so three noreply commits and one key-refused commit give
// WriteFailed 1 — and, without the arm, KeyExhausted 0 for a refused commit.
func TestContributorWriteFailuresAreCountedOnce(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("integration: set AVELOXIS_TEST_DB to run")
	}
	ctx := context.Background()
	lg := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError + 1}))
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
		repoGit = "https://github.com/_av_cr_cwritefail/repo"
		trigger = "_avtest_cr_contrib_writefail"
		login   = "avcrcontribfail"
	)
	emails := []string{"424244+" + login + "@users.noreply.github.com", "424244+" + login + "@users.noreply.github.com", "424244+" + login + "@users.noreply.github.com", "z2@example.invalid"}
	cleanup := func() {
		pool.Exec(ctx, `DROP TRIGGER IF EXISTS `+trigger+` ON aveloxis_data.contributors`)
		pool.Exec(ctx, `DROP FUNCTION IF EXISTS aveloxis_data.`+trigger+`()`)
		pool.Exec(ctx, `DELETE FROM aveloxis_data.commits WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git=$1)`, repoGit)
		pool.Exec(ctx, `DELETE FROM aveloxis_data.unresolved_commit_emails WHERE email = ANY($1)`, emails)
		pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_git=$1`, repoGit)
		pool.Exec(ctx, `DELETE FROM aveloxis_data.contributors_aliases WHERE alias_email = ANY($1)`, emails)
		pool.Exec(ctx, `DELETE FROM aveloxis_data.contributors WHERE gh_login=$1 OR cntrb_login=$1`, login)
	}
	cleanup()
	t.Cleanup(cleanup)
	var repoID int64
	if err := pool.QueryRow(ctx, `INSERT INTO aveloxis_data.repos (platform_id, repo_git, repo_owner, repo_name)
		VALUES (1, $1, '_av_cr_cwritefail', 'repo') RETURNING repo_id`, repoGit).Scan(&repoID); err != nil {
		t.Fatal(err)
	}
	for i, email := range emails {
		if _, err := pool.Exec(ctx, `INSERT INTO aveloxis_data.commits (repo_id, cmt_commit_hash, cmt_author_raw_email, cmt_author_email)
			VALUES ($1, $2, $3, $3)`, repoID, fmt.Sprintf("%040d", i+1), email); err != nil {
			t.Fatal(err)
		}
	}
	// The contributor row for this login cannot be written.
	if _, err := pool.Exec(ctx, `CREATE FUNCTION aveloxis_data.`+trigger+`() RETURNS trigger LANGUAGE plpgsql AS $f$
		BEGIN IF NEW.gh_login = '`+login+`' OR NEW.cntrb_login = '`+login+`' THEN RAISE EXCEPTION 'injected contributor write failure'; END IF; RETURN NEW; END $f$`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `CREATE TRIGGER `+trigger+` BEFORE INSERT OR UPDATE ON aveloxis_data.contributors FOR EACH ROW EXECUTE FUNCTION aveloxis_data.`+trigger+`()`); err != nil {
		t.Fatal(err)
	}
	keys := platform.NewKeyPool([]string{"x"}, lg)
	r := NewCommitResolver(store, keys, "", lg)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNotFound) }))
	defer ts.Close()
	r.http = platform.NewHTTPClient(ts.URL, keys, lg, platform.AuthGitHub)
	r.searchClient = &byEmailSearchClient{refused: map[string]bool{"z2@example.invalid": true}}

	res, err := r.ResolveCommits(ctx, repoID, "_av_cr_cwritefail", "repo")
	if err != nil {
		t.Fatalf("ResolveCommits: %v", err)
	}
	if res.WriteFailed != 1 || res.Errors != 1 {
		t.Errorf("WriteFailed=%d Errors=%d; want 1 and 1 (the first commit's contributor write)", res.WriteFailed, res.Errors)
	}
	if res.KeyExhausted != 1 || !res.IsSuccess() {
		t.Errorf("KeyExhausted=%d IsSuccess=%v; want 1 and true", res.KeyExhausted, res.IsSuccess())
	}
	if a := res.accounted(); a+res.KeyExhausted != res.TotalCommits {
		t.Errorf("accounted()=%d + KeyExhausted %d != %d commits", a, res.KeyExhausted, res.TotalCommits)
	}
}
