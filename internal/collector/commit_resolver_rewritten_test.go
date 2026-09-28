// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/platform"
)

// TestCommitResolverSkipsCommitsNoLongerOnTheDefaultBranch (worklist 73,
// the 2026-09-28 kate log): IQSS/dss-workshops' history was rewritten
// upstream; the facade walked 55 commits but 359 unresolved rows remained
// from the old history, every SHA lookup answered 422, and the resolver
// aborted with "commits do not belong to this repo" (and its stale-clone
// hint) at every cycle. On the first 422 the resolver lists the default
// branch from the bare clone once and skips every unresolved commit not on
// it — no API call, no WARN, no abort, counted as NotOnDefaultBranch; the
// rows themselves are left alone. The 50-in-a-row abort stays for its real
// case (a clone of another repository, where the commits ARE on its branch).
func TestCommitResolverSkipsCommitsNoLongerOnTheDefaultBranch(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("integration: set AVELOXIS_TEST_DB to run")
	}
	gitBin, err := exec.LookPath("git")
	if err != nil {
		t.Skip("git not on PATH")
	}
	ctx := context.Background()
	var logs bytes.Buffer
	lg := slog.New(slog.NewTextHandler(&logs, nil))

	// A bare clone with two commits on its default branch.
	work := filepath.Join(t.TempDir(), "work")
	git := func(dir string, args ...string) string {
		t.Helper()
		cmd := exec.Command(gitBin, args...)
		cmd.Dir = dir
		cmd.Env = append(os.Environ(), "GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@example.invalid", "GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@example.invalid", "GIT_CONFIG_NOSYSTEM=1", "HOME="+t.TempDir())
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	if err := os.MkdirAll(work, 0o755); err != nil {
		t.Fatal(err)
	}
	git(work, "init", "-q", "-b", "main")
	var onBranch []string
	for i := 0; i < 2; i++ {
		git(work, "commit", "-q", "--allow-empty", "-m", fmt.Sprintf("c%d", i))
		onBranch = append(onBranch, git(work, "rev-parse", "HEAD"))
	}
	bare := filepath.Join(t.TempDir(), "repo.git")
	git(filepath.Dir(bare), "clone", "-q", "--bare", work, bare)

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
	const repoGit = "https://github.com/_av_cr_rewritten/repo"
	cleanup := func() {
		pool.Exec(context.Background(), `DELETE FROM aveloxis_data.commits WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git=$1)`, repoGit)
		pool.Exec(context.Background(), `DELETE FROM aveloxis_data.unresolved_commit_emails WHERE email LIKE '%@rewritten.invalid'`)
		pool.Exec(context.Background(), `DELETE FROM aveloxis_data.repos WHERE repo_git=$1`, repoGit)
		// The noreply author's rows, children first.
		for _, q := range []string{
			`DELETE FROM aveloxis_data.contributors_aliases WHERE alias_email = '4242424+rewritten-noreply@users.noreply.github.com'`,
			`DELETE FROM aveloxis_data.contributor_identities WHERE cntrb_id IN (SELECT cntrb_id FROM aveloxis_data.contributors WHERE cntrb_login = 'rewritten-noreply')`,
			`DELETE FROM aveloxis_data.contributors WHERE cntrb_login = 'rewritten-noreply'`,
		} {
			if _, err := pool.Exec(context.Background(), q); err != nil {
				t.Logf("cleanup %q: %v", q, err)
			}
		}
	}
	cleanup()
	t.Cleanup(cleanup)
	var repoID int64
	if err := pool.QueryRow(ctx, `INSERT INTO aveloxis_data.repos (platform_id, repo_git, repo_owner, repo_name)
		VALUES (1, $1, '_av_cr_rewritten', 'repo') RETURNING repo_id`, repoGit).Scan(&repoID); err != nil {
		t.Fatal(err)
	}
	const stale = 60 // more than the 50-in-a-row abort threshold
	hashes := append([]string(nil), onBranch...)
	for i := 0; i < stale; i++ {
		hashes = append(hashes, fmt.Sprintf("%040x", 0xdead0000+i))
	}
	// The last stale commit's author is a noreply address: the free
	// strategies (caches, noreply, the store) still resolve a commit that
	// left the branch — only the API lookups are skipped (final review
	// F1 of v0.29.69: the skip ran before every strategy).
	const noreply = "4242424+rewritten-noreply@users.noreply.github.com"
	for i, h := range hashes {
		email := fmt.Sprintf("a%d@rewritten.invalid", i)
		if i == len(hashes)-1 {
			email = noreply
		}
		if _, err := pool.Exec(ctx, `INSERT INTO aveloxis_data.commits (repo_id, cmt_commit_hash, cmt_author_raw_email, cmt_author_email)
			VALUES ($1, $2, $3, $3)`, repoID, h, email); err != nil {
			t.Fatal(err)
		}
	}
	lookups := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		lookups++
		w.WriteHeader(http.StatusUnprocessableEntity) // "No commit found for SHA"
		_, _ = w.Write([]byte(`{"message":"No commit found for SHA"}`))
	}))
	defer ts.Close()
	keys := platform.NewKeyPool([]string{"x"}, lg)
	r := NewCommitResolver(store, keys, "", lg).WithBareClone(bare)
	r.http = platform.NewHTTPClient(ts.URL, keys, lg, platform.AuthGitHub)
	r.searchClient = &transientSearchClient{}

	res, err := r.ResolveCommits(ctx, repoID, "_av_cr_rewritten", "repo")
	if err != nil {
		t.Fatalf("ResolveCommits: %v", err)
	}
	if res.NotOnDefaultBranch != stale-1 {
		t.Errorf("NotOnDefaultBranch = %d; want the %d API-needing commits that are not on the clone's default branch", res.NotOnDefaultBranch, stale-1)
	}
	if res.ResolvedNoreply != 1 {
		t.Errorf("ResolvedNoreply = %d; want the off-branch commit with a noreply author resolved for free", res.ResolvedNoreply)
	}
	if res.ShouldAbort422() || strings.Contains(logs.String(), "commit resolution aborted") {
		t.Errorf("the run aborted on rewritten history (consecutive_422=%d):\n%s", res.Consecutive422, logs.String())
	}
	if lookups > len(onBranch)+1 {
		t.Errorf("%d SHA lookups; want at most one per on-branch commit plus the first stale one that triggered the listing", lookups)
	}
	if !strings.Contains(logs.String(), "no longer on the default branch") {
		t.Errorf("the skip must be logged once as such:\n%s", logs.String())
	}
}

// TestLoadDefaultBranchStopIsNotAWarning (final review F4 of v0.29.69): a
// stop while `git rev-list HEAD` runs is a shutdown, not a failed listing —
// no WARN; the loop's ctx check ends the run.
func TestLoadDefaultBranchStopIsNotAWarning(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not on PATH")
	}
	var logs bytes.Buffer
	r := &CommitResolver{logger: slog.New(slog.NewTextHandler(&logs, nil)), bareClone: t.TempDir()}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if r.loadDefaultBranch(ctx, 1) {
		t.Fatal("a cancelled listing reported a branch list")
	}
	if strings.Contains(logs.String(), "level=WARN") {
		t.Errorf("a stop during the listing was logged as a failure:\n%s", logs.String())
	}
}
