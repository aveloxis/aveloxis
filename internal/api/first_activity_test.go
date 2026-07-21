// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

// v0.27.24 — per-entity first-activity floor. Charts previously
// densified every series back to the requested window start (default
// trailing 3 years), so young repos showed years of fabricated zeros
// — which also biased the GUI's OLS trend fits and project_velocity's
// z-mean. The floor clamps each ENTITY's window start to its first
// activity; a metric that starts later than the entity (issues a year
// after commits) keeps its REAL flat-zero head.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
)

// TestHandleCompareUsesEntityFloor is the source-contract pin: the
// per-entity loop must consult entityFirstActivity and feed the
// CLAMPED start into metricSeriesAndParts. A refactor that quietly
// reverts to the raw `since` brings the phantom zero-head back.
func TestHandleCompareUsesEntityFloor(t *testing.T) {
	src := mustReadFile(t, "analytics.go")
	body := extractFuncBody(t, src, "handleCompare")
	for _, needle := range []string{"entityFirstActivity(", "entitySince", "metricSeriesAndParts(r, ids, metric, bucket, entitySince"} {
		if !strings.Contains(body, needle) {
			t.Errorf("handleCompare must clamp each entity's window via %q — young repos otherwise chart fabricated zeros back to the window start", needle)
		}
	}
}

// weekStartUTC mirrors truncBucket's ISO-Monday alignment so the test
// computes expected buckets the same way the server does.
func weekStartUTC(t time.Time) time.Time {
	t = t.UTC()
	d := (int(t.Weekday()) + 6) % 7
	day := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
	return day.AddDate(0, 0, -d)
}

func TestFirstActivityFloorEndToEnd(t *testing.T) {
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
	if err := store.Migrate(ctx); err != nil { // fresh-DB-gate lesson
		t.Fatalf("migrate: %v", err)
	}
	pool := store.Pool()
	suffix := time.Now().UnixNano()
	owner := fmt.Sprintf("_avfloor%d", suffix)

	seedRepo := func(name string) int64 {
		t.Helper()
		var id int64
		if err := pool.QueryRow(ctx, `
			INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id)
			VALUES ($1, $2, $3, 1) RETURNING repo_id`,
			fmt.Sprintf("https://github.com/%s/%s", owner, name), owner, name).Scan(&id); err != nil {
			t.Fatal(err)
		}
		return id
	}
	young := seedRepo("young")
	old := seedRepo("old")
	empty := seedRepo("empty")
	allRepos := []int64{young, old, empty}

	now := time.Now().UTC()
	firstCommit := now.AddDate(0, 0, -40*7) // 40 weeks ago
	issueAt := now.AddDate(0, 0, -12*7)     // 12 weeks ago

	seedCommit := func(repoID int64, hash string, at time.Time) {
		t.Helper()
		if _, err := pool.Exec(ctx, `
			INSERT INTO aveloxis_data.commits (repo_id, cmt_commit_hash, cmt_filename, cmt_author_timestamp)
			VALUES ($1, $2, 'f.go', $3)`, repoID, hash, at); err != nil {
			t.Fatal(err)
		}
	}
	// Young repo: weekly commits from 40 weeks ago; ONE issue at 12
	// weeks — the operator's exact scenario (issues lag commits; that
	// lag must render as real flat zeros, not be trimmed away).
	for i := 0; i < 5; i++ {
		seedCommit(young, fmt.Sprintf("y%dhash%d", i, suffix), firstCommit.AddDate(0, 0, i*7))
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO aveloxis_data.issues (repo_id, platform_issue_id, issue_number, created_at)
		VALUES ($1, $2, 1, $3)`, young, suffix%1000000, issueAt); err != nil {
		t.Fatal(err)
	}
	// Old repo: history older than the default 3-year window — the
	// clamp must be a NO-OP (byte-identical to pre-v0.27.24).
	seedCommit(old, fmt.Sprintf("old1%d", suffix), now.AddDate(-4, 0, 0))
	seedCommit(old, fmt.Sprintf("old2%d", suffix), now.AddDate(0, 0, -7))

	// Admin user (unscoped) with a Bearer token.
	var userID int
	if err := pool.QueryRow(ctx, `
		INSERT INTO aveloxis_ops.users (login_name, oauth_provider, email, admin)
		VALUES ($1, 'github', '', TRUE) RETURNING user_id`,
		fmt.Sprintf("_avfloor_%d", suffix)).Scan(&userID); err != nil {
		t.Fatal(err)
	}
	tok, err := store.CreateSessionToken(ctx, userID, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_session_tokens WHERE user_id = $1`, userID)
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE user_id = $1`, userID)
		for _, id := range allRepos {
			_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_data.issues WHERE repo_id = $1`, id)
			_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_data.commits WHERE repo_id = $1`, id)
			_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, id)
			_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, id)
		}
	})

	// ── Store level ─────────────────────────────────────────────
	fa, ok, err := store.FirstActivityAt(ctx, []int64{young})
	if err != nil || !ok {
		t.Fatalf("FirstActivityAt(young): ok=%v err=%v", ok, err)
	}
	if d := fa.Sub(firstCommit); d < -time.Minute || d > time.Minute {
		t.Errorf("floor = %v, want first commit %v (commits precede the issue)", fa, firstCommit)
	}
	if _, ok, err := store.FirstActivityAt(ctx, []int64{empty}); err != nil || ok {
		t.Errorf("empty repo: want ok=false (no dateable activity), got ok=%v err=%v", ok, err)
	}

	// ── HTTP level ──────────────────────────────────────────────
	srv, err := NewWithOptions(store, logger, Options{ExemptCIDRs: DefaultExemptCIDRs})
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	type point struct {
		Bucket string  `json:"bucket"`
		Value  float64 `json:"value"`
	}
	type serie struct {
		Points    []point `json:"points"`
		DataStart string  `json:"data_start"`
	}
	fetch := func(repoID int64, metric string) serie {
		t.Helper()
		req, _ := http.NewRequest("GET",
			fmt.Sprintf("%s/api/v1/compare?entities=repo:%d&metric=%s", ts.URL, repoID, metric), nil)
		req.Header.Set("Authorization", "Bearer "+tok)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			b, _ := io.ReadAll(resp.Body)
			t.Fatalf("compare %s: status %d: %s", metric, resp.StatusCode, b)
		}
		var payload struct {
			Series []serie `json:"series"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
			t.Fatal(err)
		}
		if len(payload.Series) != 1 {
			t.Fatalf("want 1 series, got %d", len(payload.Series))
		}
		return payload.Series[0]
	}
	bucketDay := func(iso string) string { // points carry RFC3339; compare on the date
		return iso[:10]
	}

	wantFloor := weekStartUTC(firstCommit).Format("2006-01-02")

	// 1. Young repo, commits: series starts at the floor bucket, not
	//    the 3-year window start (~156 weekly buckets).
	commits := fetch(young, "code_change_commits")
	if len(commits.Points) == 0 {
		t.Fatal("commits series empty")
	}
	if got := bucketDay(commits.Points[0].Bucket); got != wantFloor {
		t.Errorf("commits series starts %s, want floor bucket %s", got, wantFloor)
	}
	if len(commits.Points) > 60 {
		t.Errorf("commits series has %d buckets — looks densified back to the window start (~156), not clamped to ~41", len(commits.Points))
	}
	if commits.DataStart == "" {
		t.Error("data_start missing on a repo with known first activity")
	}

	// 2. THE SEMANTIC PIN — young repo, issues: starts at the ENTITY
	//    floor (commit time), NOT the first issue; the 28 weeks of
	//    pre-issue existence are real flat zeros.
	issues := fetch(young, "issues")
	if got := bucketDay(issues.Points[0].Bucket); got != wantFloor {
		t.Errorf("issues series starts %s, want the ENTITY floor %s — the flat-zero head between first commit and first issue is real data and must be kept", got, wantFloor)
	}
	issueBucket := weekStartUTC(issueAt).Format("2006-01-02")
	var sawIssue bool
	for _, p := range issues.Points {
		day := bucketDay(p.Bucket)
		switch {
		case day < issueBucket && p.Value != 0:
			t.Errorf("bucket %s has value %v before the first issue — want flat zero", day, p.Value)
		case day == issueBucket:
			sawIssue = true
			if p.Value != 1 {
				t.Errorf("issue bucket %s = %v, want 1", day, p.Value)
			}
		}
	}
	if !sawIssue {
		t.Errorf("issue bucket %s absent from the series", issueBucket)
	}

	// 3. Old repo: floor precedes the window → clamp is a no-op, the
	//    full ~3-year grid renders exactly as before v0.27.24.
	oldSeries := fetch(old, "code_change_commits")
	if len(oldSeries.Points) < 150 {
		t.Errorf("old repo series has %d buckets, want the full ~156-week window (clamp must be a no-op when history predates the window)", len(oldSeries.Points))
	}
}
