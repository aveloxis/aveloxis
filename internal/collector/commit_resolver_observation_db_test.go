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
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/platform"
)

// TestCommitResolutionLinesCarryTheirCost (AVELOXIS_TEST_DB) — worklist
// items 75 and 76 (Stage 1, observation only): the unresolved-commit query
// and the author-ID backfill ran up to 1,146 s and 4,034 s on kate with no
// duration or repo on any line, and the completion line did not say how
// many email searches a repository cost or how long they took (the ~7 h
// kernel-fork runs). The start line carries the unresolved query's time;
// the completion line carries repo_id, the run's duration, the backfill's
// duration, the searches attempted and the time spent in them.
func TestCommitResolutionLinesCarryTheirCost(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("integration: set AVELOXIS_TEST_DB to run")
	}
	ctx := context.Background()
	var logs strings.Builder
	lg := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo}))
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
		repoGit = "https://github.com/_av_cr_observe/repo"
		email   = "observe-probe@example.invalid"
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
		VALUES (1, $1, '_av_cr_observe', 'repo') RETURNING repo_id`, repoGit).Scan(&repoID); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, err := pool.Exec(ctx, `INSERT INTO aveloxis_data.commits (repo_id, cmt_commit_hash, cmt_author_raw_email, cmt_author_email)
			VALUES ($1, $2, $3, $3)`, repoID, fmt.Sprintf("%040d", 900+i), email); err != nil {
			t.Fatal(err)
		}
	}
	keys := platform.NewKeyPool([]string{"x"}, lg)
	r := NewCommitResolver(store, keys, "", lg)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNotFound) }))
	defer ts.Close()
	r.http = platform.NewHTTPClient(ts.URL, keys, lg, platform.AuthGitHub)
	r.searchClient = &rejectingSearchClient{}

	if _, err := r.ResolveCommits(ctx, repoID, "_av_cr_observe", "repo"); err != nil {
		t.Fatalf("ResolveCommits: %v", err)
	}
	var start, done string
	for _, l := range strings.Split(logs.String(), "\n") {
		if strings.Contains(l, `msg="resolving commit authors"`) {
			start = l
		}
		if strings.Contains(l, `msg="commit resolution complete"`) {
			done = l
		}
	}
	if !strings.Contains(start, "unresolved_query=") {
		t.Errorf("the start line must carry the unresolved query's duration:\n%s", start)
	}
	for _, want := range []string{"repo_id=" + strconv.FormatInt(repoID, 10), "duration=", "backfill_duration=", "search_attempts=1", "search_time="} {
		if !strings.Contains(done, want) {
			t.Errorf("the completion line must carry %q:\n%s", want, done)
		}
	}
}
