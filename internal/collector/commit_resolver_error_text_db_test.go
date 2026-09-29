// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

// v0.29.70 whole-branch review (reproduced by the reviewer): the resolver
// decided "no API keys" on the ERROR TEXT — strings.Contains(errMsg,
// "invalidated") — and an ordinary lookup error carries the request URL,
// so a repository named with "invalidated" aborted the whole run as a false
// key exhaustion (SR-5). The decision is now errors.Is on the pool's typed
// errors: ErrNoKeys (empty pool) and ErrAllKeysInvalidated.

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/platform"
)

func TestResolverDoesNotReadKeyExhaustionFromTheRepositoryName(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("integration: set AVELOXIS_TEST_DB to run")
	}
	ctx := context.Background()
	var logs strings.Builder
	lg := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo}))
	store, err := db.NewPostgresStore(ctx, dsn, lg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	pool := store.Pool()
	const repoGit = "https://github.com/_av_cr_textprobe/cache-invalidated"
	cleanup := func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM aveloxis_data.commits WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git=$1)`, repoGit)
		_, _ = pool.Exec(context.Background(), `DELETE FROM aveloxis_data.repos WHERE repo_git=$1`, repoGit)
	}
	cleanup()
	t.Cleanup(cleanup)
	var repoID int64
	if err := pool.QueryRow(ctx, `INSERT INTO aveloxis_data.repos (platform_id, repo_git, repo_owner, repo_name)
		VALUES (1, $1, '_av_cr_textprobe', 'cache-invalidated') RETURNING repo_id`, repoGit).Scan(&repoID); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := pool.Exec(ctx, `INSERT INTO aveloxis_data.commits (repo_id, cmt_commit_hash, cmt_author_raw_email, cmt_author_email)
			VALUES ($1, $2, $3, $3)`, repoID, fmt.Sprintf("%040d", 700+i), fmt.Sprintf("textprobe%d@example.invalid", i)); err != nil {
			t.Fatal(err)
		}
	}
	keys := platform.NewKeyPool([]string{"x"}, lg)
	r := NewCommitResolver(store, keys, "", lg)
	// An ordinary, non-definitive error for every commit lookup: its text
	// carries the URL, and so the repository name.
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusConflict) }))
	defer ts.Close()
	r.http = platform.NewHTTPClient(ts.URL, keys, lg, platform.AuthGitHub)
	r.searchClient = &rejectingSearchClient{}

	res, err := r.ResolveCommits(ctx, repoID, "_av_cr_textprobe", "cache-invalidated")
	if err != nil {
		t.Fatalf("ResolveCommits: %v", err)
	}
	if res.KeyExhausted != 0 || strings.Contains(logs.String(), "no API keys available") {
		t.Errorf("a repository named with \"invalidated\" was read as key exhaustion: key_exhausted=%d\n%s", res.KeyExhausted, logs.String())
	}
}

// TestResolverKeyExhaustionIsTyped: the pool's two key-exhaustion errors are
// typed, and the text the operator sees is unchanged.
func TestResolverKeyExhaustionIsTyped(t *testing.T) {
	_, _, err := platform.NewKeyPool(nil, slog.Default()).Acquire(context.Background(), platform.ResourceCore)
	if !isKeyExhaustion(err) || !strings.Contains(err.Error(), "no API keys configured") {
		t.Errorf("empty pool: err=%v, want ErrNoKeys with the same text", err)
	}
	if !isKeyExhaustion(fmt.Errorf("lookup: %w", platform.ErrAllKeysInvalidated)) {
		t.Error("an invalidated pool is key exhaustion")
	}
	for _, e := range []error{
		fmt.Errorf("conflict: https://api.github.com/repos/o/cache-invalidated/commits/x: %w", platform.ErrConflict),
		fmt.Errorf("exhausted 10 retries for https://x/no%%20API%%20keys%%20configured: %w", platform.ErrTransient),
	} {
		if isKeyExhaustion(e) {
			t.Errorf("%v: text is not a key exhaustion", e)
		}
	}
}
