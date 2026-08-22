// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// v0.27.16 — contributor_retention (drive-by vs repeat, 8Knot port).
//
// Unit tier: negative tripwire pinning that the retention query
// functions NEVER touch the explorer_contributor_actions matview
// (operator-decided architecture: 171M rows / 51 GB on production
// with no repo-leading index — the metric computes LIVE from base
// tables scoped to the entity repo set).
//
// Integration tier (AVELOXIS_TEST_DB): behavioral coverage of the
// retention SQL — bucket splits by month of FIRST contribution,
// threshold boundary (repeat = >= threshold, matching 8Knot), commit
// hash dedup across the per-file commits table, comment counting via
// the bridge tables, bot + soft-deleted exclusions, and window
// filtering on the first-contribution bucket.

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
)

// TestRetentionStoreNeverTouchesExplorerMatview is the operator-
// mandated negative tripwire: explorer_contributor_actions is
// OFF-LIMITS for this metric (fleet-scale matview, no repo-leading
// index). If a future refactor "helpfully" reuses it, this fails the
// build before merge. Go line comments are stripped first (shared
// stripLineComments helper from repo_labor_history_test.go — the
// v0.21.5 lesson) so only CODE (the SQL itself) is pinned.
func TestRetentionStoreNeverTouchesExplorerMatview(t *testing.T) {
	body, err := os.ReadFile("retention_store.go")
	if err != nil {
		t.Fatalf("read retention_store.go: %v", err)
	}
	if strings.Contains(stripLineComments(string(body)), "explorer_contributor_actions") {
		t.Error("retention_store.go must NEVER reference explorer_contributor_actions — " +
			"the metric is computed live from base tables (operator decision, v0.27.16: " +
			"171M rows / 51 GB matview with no repo-leading index)")
	}
}

// TestRetentionSQLCountsActionsFromBaseTables pins the action sources
// mirroring explorer_contributor_actions' DEFINITION (not the matview
// itself): distinct commits, issues opened, PRs opened, PR reviews,
// and conversation comments via BOTH bridge tables.
func TestRetentionSQLCountsActionsFromBaseTables(t *testing.T) {
	body, err := os.ReadFile("retention_store.go")
	if err != nil {
		t.Fatalf("read retention_store.go: %v", err)
	}
	src := string(body)
	for _, needle := range []string{
		"aveloxis_data.commits",
		"cmt_ght_author_id",
		"cmt_commit_hash", // one action per DISTINCT hash — the table is per-file
		"aveloxis_data.issues",
		"reporter_id",
		"aveloxis_data.pull_requests",
		"author_id",
		"aveloxis_data.pull_request_reviews",
		"aveloxis_data.issue_message_ref",
		"aveloxis_data.pull_request_message_ref",
		"COALESCE(c.cntrb_deleted, 0) = 0", // v0.20.2 soft-merge losers excluded
		"'Bot'",                            // gh_type bot exclusion
		"[bot]",                            // login-suffix bot exclusion
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("retention_store.go must contain %q (action-source / exclusion contract)", needle)
		}
	}
}

// ============================================================
// Integration tier — live Postgres, gated on AVELOXIS_TEST_DB
// ============================================================

func retentionConnect(t *testing.T) (*PostgresStore, context.Context) {
	t.Helper()
	conn := os.Getenv("AVELOXIS_TEST_DB")
	if conn == "" {
		t.Skip("AVELOXIS_TEST_DB not set — skipping integration test")
	}
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := NewPostgresStore(ctx, conn, logger)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	// House rule (2026-07): every integration test migrates BEFORE
	// seeding — three fresh-DB races were fixed the week of 2026-07-08
	// from tests that assumed the schema was already present.
	testMigrate(ctx, t, store)
	return store, ctx
}

// retentionSeed owns one seeded repo + helpers to add actions.
type retentionSeed struct {
	t      *testing.T
	ctx    context.Context
	store  *PostgresStore
	repoID int64
	seq    int64
}

func newRetentionSeed(ctx context.Context, t *testing.T, store *PostgresStore) *retentionSeed {
	t.Helper()
	slug := fmt.Sprintf("_avret-%d", time.Now().UnixNano())
	id, err := store.UpsertRepo(ctx, &model.Repo{
		Owner:    "_avret",
		Name:     slug,
		GitURL:   fmt.Sprintf("https://github.com/_avret/%s", slug),
		Platform: model.PlatformGitHub,
	})
	if err != nil {
		t.Fatalf("UpsertRepo: %v", err)
	}
	return &retentionSeed{t: t, ctx: ctx, store: store, repoID: id, seq: time.Now().UnixNano()}
}

func (s *retentionSeed) next() int64 {
	s.seq++
	return s.seq
}

// contributor inserts a contributor row and returns its cntrb_id.
func (s *retentionSeed) contributor(login, ghType string, deleted int) string {
	s.t.Helper()
	var id string
	err := s.store.pool.QueryRow(s.ctx, `
		INSERT INTO aveloxis_data.contributors (cntrb_login, gh_login, gh_type, cntrb_deleted)
		VALUES ($1, $1, $2, $3)
		ON CONFLICT (cntrb_login) WHERE cntrb_login != ''
		DO UPDATE SET gh_type = EXCLUDED.gh_type, cntrb_deleted = EXCLUDED.cntrb_deleted
		RETURNING cntrb_id::text`, login, ghType, deleted).Scan(&id)
	if err != nil {
		s.t.Fatalf("seed contributor %s: %v", login, err)
	}
	return id
}

func (s *retentionSeed) issue(cntrb string, when time.Time) {
	s.t.Helper()
	n := s.next()
	_, err := s.store.pool.Exec(s.ctx, `
		INSERT INTO aveloxis_data.issues (repo_id, platform_issue_id, issue_number, reporter_id, created_at)
		VALUES ($1, $2, $3, $4::uuid, $5)`, s.repoID, n, n%1000000, cntrb, when)
	if err != nil {
		s.t.Fatalf("seed issue: %v", err)
	}
}

func (s *retentionSeed) pr(cntrb string, when time.Time) int64 {
	s.t.Helper()
	n := s.next()
	var id int64
	err := s.store.pool.QueryRow(s.ctx, `
		INSERT INTO aveloxis_data.pull_requests (repo_id, platform_pr_id, pr_number, author_id, created_at)
		VALUES ($1, $2, $3, $4::uuid, $5) RETURNING pull_request_id`,
		s.repoID, n, n%1000000, cntrb, when).Scan(&id)
	if err != nil {
		s.t.Fatalf("seed pr: %v", err)
	}
	return id
}

func (s *retentionSeed) review(cntrb string, prID int64, when time.Time) {
	s.t.Helper()
	_, err := s.store.pool.Exec(s.ctx, `
		INSERT INTO aveloxis_data.pull_request_reviews
			(pull_request_id, repo_id, cntrb_id, platform_id, platform_review_id, submitted_at)
		VALUES ($1, $2, $3::uuid, 1, $4, $5)`, prID, s.repoID, cntrb, s.next(), when)
	if err != nil {
		s.t.Fatalf("seed review: %v", err)
	}
}

// commit inserts N per-file rows for ONE commit hash — the retention
// query must count this as a SINGLE action (hash dedup).
func (s *retentionSeed) commit(cntrb string, when time.Time, files int) {
	s.t.Helper()
	hash := fmt.Sprintf("%040d", s.next())
	for i := 0; i < files; i++ {
		_, err := s.store.pool.Exec(s.ctx, `
			INSERT INTO aveloxis_data.commits
				(repo_id, cmt_commit_hash, cmt_filename, cmt_author_timestamp, cmt_ght_author_id)
			VALUES ($1, $2, $3, $4, $5::uuid)`,
			s.repoID, hash, fmt.Sprintf("file%d.go", i), when, cntrb)
		if err != nil {
			s.t.Fatalf("seed commit file %d: %v", i, err)
		}
	}
}

// issueComment inserts a message bridged via issue_message_ref.
func (s *retentionSeed) issueComment(cntrb string, when time.Time) {
	s.t.Helper()
	n := s.next()
	var issueID, msgID int64
	err := s.store.pool.QueryRow(s.ctx, `
		INSERT INTO aveloxis_data.issues (repo_id, platform_issue_id, issue_number, created_at)
		VALUES ($1, $2, $3, $4) RETURNING issue_id`, s.repoID, n, n%1000000, when).Scan(&issueID)
	if err != nil {
		s.t.Fatalf("seed comment parent issue: %v", err)
	}
	err = s.store.pool.QueryRow(s.ctx, `
		INSERT INTO aveloxis_data.messages (repo_id, platform_msg_id, platform_id, msg_timestamp, cntrb_id)
		VALUES ($1, $2, 1, $3, $4::uuid) RETURNING msg_id`, s.repoID, s.next(), when, cntrb).Scan(&msgID)
	if err != nil {
		s.t.Fatalf("seed message: %v", err)
	}
	_, err = s.store.pool.Exec(s.ctx, `
		INSERT INTO aveloxis_data.issue_message_ref (issue_id, repo_id, msg_id)
		VALUES ($1, $2, $3)`, issueID, s.repoID, msgID)
	if err != nil {
		s.t.Fatalf("seed issue_message_ref: %v", err)
	}
}

// monthValue returns the value at the bucket matching the given
// year/month, or 0 when absent.
func monthValue(points []WeeklyPoint, year int, month time.Month) float64 {
	for _, p := range points {
		b := p.Bucket.UTC()
		if b.Year() == year && b.Month() == month {
			return p.Value
		}
	}
	return 0
}

func TestContributorRetentionSeriesEndToEnd(t *testing.T) {
	store, ctx := retentionConnect(t)
	t.Cleanup(store.Close)
	seed := newRetentionSeed(ctx, t, store)
	uniq := time.Now().UnixNano()

	m1 := time.Date(2025, 2, 10, 12, 0, 0, 0, time.UTC) // bucket 2025-02
	m2 := time.Date(2025, 5, 20, 12, 0, 0, 0, time.UTC) // bucket 2025-05

	// alice: 5 actions across all action types, first in m1 → repeat.
	// Her one commit has 3 per-file rows — must count as ONE action.
	alice := seed.contributor(fmt.Sprintf("_avret_alice_%d", uniq), "User", 0)
	seed.commit(alice, m1, 3)
	seed.issue(alice, m1.Add(24*time.Hour))
	prID := seed.pr(alice, m1.Add(48*time.Hour))
	seed.review(alice, prID, m1.Add(72*time.Hour))
	seed.commit(alice, m2, 1)

	// bob: 1 issue, first in m2 → drive-by.
	bob := seed.contributor(fmt.Sprintf("_avret_bob_%d", uniq), "User", 0)
	seed.issue(bob, m2)

	// carol: EXACTLY 4 issues (the threshold), first in m1 → repeat
	// (8Knot classifies repeat as >= threshold — boundary must be repeat).
	carol := seed.contributor(fmt.Sprintf("_avret_carol_%d", uniq), "User", 0)
	for i := 0; i < 4; i++ {
		seed.issue(carol, m1.Add(time.Duration(i)*time.Hour))
	}

	// dana: 3 actions (2 issues + 1 issue comment via the bridge),
	// first in m2 → drive-by. Proves comments count as actions.
	dana := seed.contributor(fmt.Sprintf("_avret_dana_%d", uniq), "User", 0)
	seed.issue(dana, m2)
	seed.issue(dana, m2.Add(time.Hour))
	seed.issueComment(dana, m2.Add(2*time.Hour))

	// Excluded cohorts — none may appear in any bucket:
	bot := seed.contributor(fmt.Sprintf("_avret_bot_%d", uniq), "Bot", 0)
	for i := 0; i < 6; i++ {
		seed.issue(bot, m1.Add(time.Duration(i)*time.Hour))
	}
	bracket := seed.contributor(fmt.Sprintf("_avret_dep_%d[bot]", uniq), "User", 0)
	for i := 0; i < 6; i++ {
		seed.issue(bracket, m1.Add(time.Duration(i)*time.Hour))
	}
	ghost := seed.contributor(fmt.Sprintf("_avret_ghost_%d", uniq), "User", 1)
	for i := 0; i < 6; i++ {
		seed.issue(ghost, m1.Add(time.Duration(i)*time.Hour))
	}

	// eve: first contribution BEFORE the window → must not appear in
	// any displayed bucket even though later actions fall inside it.
	eve := seed.contributor(fmt.Sprintf("_avret_eve_%d", uniq), "User", 0)
	seed.issue(eve, time.Date(2020, 3, 1, 0, 0, 0, 0, time.UTC))
	seed.issue(eve, m1)

	since := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	until := time.Date(2025, 8, 1, 0, 0, 0, 0, time.UTC)
	ids := []int64{seed.repoID}

	driveBy, repeat, err := store.ContributorRetentionSeries(ctx, ids, "month", since, until, 4)
	if err != nil {
		t.Fatalf("ContributorRetentionSeries: %v", err)
	}

	// m1 (2025-02): alice (5 ≥ 4) + carol (4 ≥ 4) repeat; no drive-by.
	if got := monthValue(repeat, 2025, time.February); got != 2 {
		t.Errorf("2025-02 repeat = %v, want 2 (alice + carol; carol sits ON the ≥ boundary)", got)
	}
	if got := monthValue(driveBy, 2025, time.February); got != 0 {
		t.Errorf("2025-02 drive_by = %v, want 0 (bot/bracket/ghost/eve must be excluded)", got)
	}
	// m2 (2025-05): bob (1) + dana (3) drive-by; no repeat. alice's m2
	// commit must NOT re-bucket her (first contribution wins).
	if got := monthValue(driveBy, 2025, time.May); got != 2 {
		t.Errorf("2025-05 drive_by = %v, want 2 (bob + dana incl. bridge-table comment)", got)
	}
	if got := monthValue(repeat, 2025, time.May); got != 0 {
		t.Errorf("2025-05 repeat = %v, want 0 (alice buckets at her FIRST contribution)", got)
	}

	// Commit-hash dedup: alice has exactly 5 actions. At threshold 6
	// she must flip to drive-by — if the per-file rows were counted
	// raw she'd have 7 and stay repeat.
	db6, rp6, err := store.ContributorRetentionSeries(ctx, ids, "month", since, until, 6)
	if err != nil {
		t.Fatalf("threshold 6: %v", err)
	}
	if got := monthValue(rp6, 2025, time.February); got != 0 {
		t.Errorf("threshold 6: 2025-02 repeat = %v, want 0 (alice has 5 deduped actions, not 7)", got)
	}
	if got := monthValue(db6, 2025, time.February); got != 2 {
		t.Errorf("threshold 6: 2025-02 drive_by = %v, want 2 (alice + carol flip below threshold)", got)
	}

	// Bucket whitelist: injection strings must be rejected.
	if _, _, err := store.ContributorRetentionSeries(ctx, ids, "week'); DROP TABLE x;--", since, until, 4); err == nil {
		t.Error("bucket whitelist must reject arbitrary strings")
	}
	// Threshold floor.
	if _, _, err := store.ContributorRetentionSeries(ctx, ids, "month", since, until, 0); err == nil {
		t.Error("threshold < 1 must be rejected")
	}
	// Week bucketing must execute too (compare supports both).
	if _, _, err := store.ContributorRetentionSeries(ctx, ids, "week", since, until, 4); err != nil {
		t.Errorf("week bucket: %v", err)
	}
}
