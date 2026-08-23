// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

// v0.28.1 (A6) — the distinct "gone" state. Incident: the
// department-of-veterans-affairs org privatized 477 collected repos;
// prelim's 404 sideline archived + dequeued them, and the GUI
// misread the queueless state as "queued for first collection" over
// real data. These tests pin the marker's contract.

func TestSchemaDeclaresRepoGoneAt(t *testing.T) {
	schema := readSourceFile(t, "schema.sql")
	if !strings.Contains(schema, "repo_gone_at            TIMESTAMPTZ") {
		t.Error("schema.sql must declare repos.repo_gone_at TIMESTAMPTZ")
	}
}

func TestMigrateAddsRepoGoneAt(t *testing.T) {
	src := readSourceFile(t, "migrate.go")
	if !strings.Contains(src, `"aveloxis_data.repos", "repo_gone_at", "TIMESTAMPTZ"`) {
		t.Error("migrate.go must addColumnIfMissing repos.repo_gone_at for existing fleets")
	}
}

// MarkRepoGone sets BOTH columns in ONE statement — splitting them
// would mint a partial state the v0.27.39 sideline ordering
// (archive-before-dequeue) can't classify.
func TestMarkRepoGoneIsSingleStatement(t *testing.T) {
	src := readSourceFile(t, "repo_gone.go")
	i := strings.Index(src, "func (s *PostgresStore) MarkRepoGone(")
	if i < 0 {
		t.Fatal("MarkRepoGone missing")
	}
	body := src[i:]
	if end := strings.Index(body, "\nfunc "); end > 0 {
		body = body[:end]
	}
	if !strings.Contains(body, "repo_archived = TRUE") || !strings.Contains(body, "repo_gone_at = NOW()") {
		t.Error("MarkRepoGone must set repo_archived AND repo_gone_at in the same UPDATE")
	}
	if strings.Count(body, "UPDATE") != 1 {
		t.Error("MarkRepoGone must be exactly ONE statement")
	}
}

// ClearRepoGone is guarded so it's a 0-row no-op for the normal
// fleet (prelim calls it on every healthy probe).
func TestClearRepoGoneIsGuarded(t *testing.T) {
	src := readSourceFile(t, "repo_gone.go")
	if !strings.Contains(src, "AND repo_gone_at IS NOT NULL") {
		t.Error("ClearRepoGone must carry the IS NOT NULL guard")
	}
}

// Candidate selection: queueless only (genuinely-archived repos KEEP
// queue rows and keep cycling), and dataless stranded rows fall out.
func TestGoneProbeCandidatesShape(t *testing.T) {
	src := readSourceFile(t, "repo_gone.go")
	for _, needle := range []string{
		"LEFT JOIN aveloxis_ops.collection_queue",
		"WHERE q.repo_id IS NULL",
		"COALESCE(r.repo_archived, FALSE)",
		"r.repo_gone_at IS NOT NULL",
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("GetGoneProbeCandidates must contain %q", needle)
		}
	}
}

// Prelim's 404/410 branch stamps gone (not the bare ArchiveRepo),
// and its healthy path clears a stale stamp — set/clear live in the
// SAME probe so resurrection is symmetric.
func TestPrelimStampsAndClearsGone(t *testing.T) {
	data, err := os.ReadFile("../collector/prelim.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	i := strings.Index(src, "func RunPrelim(")
	if i < 0 {
		t.Fatal("RunPrelim missing")
	}
	body := src[i:]
	if end := strings.Index(body, "\nfunc "); end > 0 {
		body = body[:end]
	}
	if !strings.Contains(body, "store.MarkRepoGone(") {
		t.Error("prelim's 404/410 sideline must stamp via MarkRepoGone")
	}
	if strings.Contains(body, "store.ArchiveRepo(") {
		t.Error("the bare ArchiveRepo call must be gone from RunPrelim — MarkRepoGone supersedes it in the sideline")
	}
	if !strings.Contains(body, "store.ClearRepoGone(") {
		t.Error("prelim's healthy path must clear a stale gone stamp (symmetric resurrection)")
	}
	// v0.28.4 (review-lens finding, SR-16): the clear fires ONLY on a
	// DEFINITIVE 2xx — a transient 403/429/5xx during an outage must
	// not flip a gone-stamped repo back to the false queued banner.
	// Pin the gate directly ahead of the clear call, mirroring
	// mark-gone-repos' probe rule so the two consumers agree.
	clearPos := strings.Index(body, "store.ClearRepoGone(")
	lo := clearPos - 400
	if lo < 0 {
		lo = 0
	}
	if !strings.Contains(body[lo:clearPos], "statusCode >= 200 && statusCode < 300") {
		t.Error("ClearRepoGone must be gated on a definitive 2xx probe (SR-16) — indeterminate statuses prove nothing")
	}
}

// v0.28.4 (review-lens finding, the decorative-gate class): the
// ledgered timestamp cleanup must FAIL CLOSED — only the typed
// definitive absences (42P01 undefined_table / 42703 undefined_column)
// skip; every other per-column error propagates so runOnce cannot
// record "complete" over uncleaned columns.
func TestTimestampCleanupFailsClosed(t *testing.T) {
	src := readSourceFile(t, "migrate.go")
	i := strings.Index(src, "func cleanupBadTimestamps(")
	if i < 0 {
		t.Fatal("cleanupBadTimestamps missing")
	}
	body := src[i:]
	if end := strings.Index(body, "\nfunc "); end > 0 {
		body = body[:end]
	}
	for _, needle := range []string{`"42P01"`, `"42703"`, "timestamp cleanup %s.%s: %w"} {
		if !strings.Contains(body, needle) {
			t.Errorf("cleanupBadTimestamps must skip ONLY typed 42P01/42703 and propagate everything else (missing %q) — the blanket swallow made the ledger record decorative", needle)
		}
	}
	if strings.Contains(body, "skip silently") {
		t.Error("the blanket swallow-everything comment/shape must be gone from cleanupBadTimestamps")
	}
}

// GetRepoStats surfaces gone_at + metadata_as_of, and the queueless
// live-count fallback is GATED: the aggregates live in the separate
// queuelessLiveCounts method reached only from the ErrNoRows branch —
// the v0.27.85 cached read stays the only tracked-repo path.
func TestRepoStatsGoneSurface(t *testing.T) {
	src := readSourceFile(t, "repo_stats.go")
	for _, needle := range []string{
		`json:"gone_at,omitempty"`,
		`json:"metadata_as_of,omitempty"`,
		"repo_gone_at FROM aveloxis_data.repos",
		"func (s *PostgresStore) queuelessLiveCounts(",
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("repo_stats.go must contain %q", needle)
		}
	}
	// The fallback call must sit inside the ErrNoRows branch of the
	// queue-cached read.
	i := strings.Index(src, "pgx.ErrNoRows) {")
	j := strings.Index(src, "queuelessLiveCounts(ctx, repoID, st)")
	if i < 0 || j < 0 || j < i {
		t.Error("queuelessLiveCounts must be called from the queue-row ErrNoRows branch only")
	}
}

// ─── Integration (AVELOXIS_TEST_DB) ─────────────────────────────

func TestRepoGoneEndToEnd(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)

	repoID := seedRepoForDeps(t, store, ctx, "_avgone", "fixture")
	// Seed real collected rows so the queueless fallback has data to
	// count: 1 issue + 1 commit stored over TWO file rows (the
	// DISTINCT-hash contract).
	mustExecRetry(ctx, t, store, `
		INSERT INTO aveloxis_data.issues (repo_id, platform_issue_id, issue_number, issue_title, created_at)
		VALUES ($1, 990001, 1, '_avgone issue', NOW())`, repoID)
	t.Cleanup(func() {
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_data.issues WHERE repo_id = $1`, repoID)
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_data.commits WHERE repo_id = $1`, repoID)
	})
	for _, f := range []string{"a.go", "b.go"} {
		mustExecRetry(ctx, t, store, `
			INSERT INTO aveloxis_data.commits (repo_id, cmt_commit_hash, cmt_filename, cmt_author_name, cmt_author_email, cmt_author_date, cmt_author_timestamp)
			VALUES ($1, 'avgonehash1', $2, 'x', 'x@x', '2026-01-01', NOW())`, repoID, f)
	}

	// No queue row exists (seedRepoForDeps doesn't enqueue) — the
	// prelim-dequeued shape. Stats must fall back to live counts.
	st, err := store.GetRepoStats(ctx, repoID)
	if err != nil {
		t.Fatal(err)
	}
	if st.GatheredIssues != 1 || st.GatheredCommits != 1 {
		t.Errorf("queueless fallback: want issues=1 commits=1 (distinct hash), got issues=%d commits=%d",
			st.GatheredIssues, st.GatheredCommits)
	}
	if st.GoneAt != nil {
		t.Error("gone_at must be nil before MarkRepoGone")
	}

	// Stamp gone → surfaced; archived flag set too.
	if err := store.MarkRepoGone(ctx, repoID); err != nil {
		t.Fatal(err)
	}
	st, err = store.GetRepoStats(ctx, repoID)
	if err != nil {
		t.Fatal(err)
	}
	if st.GoneAt == nil || time.Since(*st.GoneAt) > time.Minute {
		t.Errorf("gone_at must surface a fresh stamp, got %v", st.GoneAt)
	}

	// Clear → nil again; second clear is a no-op.
	if err := store.ClearRepoGone(ctx, repoID); err != nil {
		t.Fatal(err)
	}
	if err := store.ClearRepoGone(ctx, repoID); err != nil {
		t.Fatal(err)
	}
	ga, err := store.GetRepoGoneAt(ctx, repoID)
	if err != nil {
		t.Fatal(err)
	}
	if ga != nil {
		t.Error("ClearRepoGone must null the stamp")
	}

	// Candidate selection: this queueless repo (has data) appears;
	// after stamping it stays (GoneStamped=true).
	if err := store.MarkRepoGone(ctx, repoID); err != nil {
		t.Fatal(err)
	}
	cands, err := store.GetGoneProbeCandidates(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	var found *GoneProbeCandidate
	for i := range cands {
		if cands[i].RepoID == repoID {
			found = &cands[i]
		}
	}
	if found == nil {
		t.Fatal("stamped queueless repo must be a gone-probe candidate")
	}
	if !found.GoneStamped || !found.HasData {
		t.Errorf("candidate flags wrong: %+v", *found)
	}
}
