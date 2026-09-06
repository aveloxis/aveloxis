// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.5 — scorecard_mode marker on repo_deps_scorecard. 'remote'
// (--repo, ~18 checks) and 'local' (--local, ~11 checks) overall
// scores are NOT comparable, so every row records which mode produced
// it. Source-contract pins for the schema/migration/insert wiring +
// AVELOXIS_TEST_DB integration coverage including the history-rotation
// column-parity contract (INSERT INTO history SELECT * requires both
// tables to gain the column).

package db

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"
)

func TestSchemaDeclaresScorecardModeColumn(t *testing.T) {
	schema, err := os.ReadFile("schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	src := string(schema)

	idx := strings.Index(src, "CREATE TABLE IF NOT EXISTS aveloxis_data.repo_deps_scorecard (")
	if idx < 0 {
		t.Fatal("cannot find repo_deps_scorecard CREATE TABLE in schema.sql")
	}
	block := src[idx:]
	if end := strings.Index(block, ");"); end > 0 {
		block = block[:end]
	}
	if !strings.Contains(block, "scorecard_mode") || !strings.Contains(block, "TEXT DEFAULT ''") {
		t.Error("repo_deps_scorecard must declare `scorecard_mode TEXT DEFAULT ''` — " +
			"the v0.27.5 execution-mode marker ('remote' vs 'local' check sets are not comparable)")
	}
}

func TestMigrateAddsScorecardModeToBothTables(t *testing.T) {
	src, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// BOTH tables must gain the column: RotateScorecardToHistory does
	// `INSERT INTO ..._history SELECT * FROM ...`, which requires
	// identical column sets. Adding scorecard_mode to only the main
	// table would break every rotation on existing fleets.
	for _, table := range []string{
		`"aveloxis_data.repo_deps_scorecard", "scorecard_mode"`,
		`"aveloxis_data.repo_deps_scorecard_history", "scorecard_mode"`,
	} {
		if !strings.Contains(code, table) {
			t.Errorf("migrate.go must addColumnIfMissing(%s, ...) — history rotation "+
				"uses SELECT * so main and history column sets must stay identical", table)
		}
	}
}

func TestInsertScorecardResultWritesMode(t *testing.T) {
	src, err := os.ReadFile("analysis_store.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	idx := strings.Index(code, "func (s *PostgresStore) InsertScorecardResult(")
	if idx < 0 {
		t.Fatal("cannot find InsertScorecardResult")
	}
	body := code[idx:]
	if next := strings.Index(body[1:], "\nfunc "); next > 0 {
		body = body[:next+1]
	}
	if !strings.Contains(body, "mode string") {
		t.Error("InsertScorecardResult must take a `mode string` parameter (v0.27.5)")
	}
	if !strings.Contains(body, "scorecard_mode") {
		t.Error("InsertScorecardResult's INSERT must include the scorecard_mode column")
	}
}

// --- integration tier (AVELOXIS_TEST_DB) ---

func scorecardModeConnect(t *testing.T) (*PostgresStore, context.Context) {
	t.Helper()
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set — skipping integration test")
	}
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := NewPostgresStore(ctx, dsn, logger)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	testMigrate(ctx, t, store)
	return store, ctx
}

// TestScorecardModePersistsAndRotates seeds a repo, inserts checks +
// the __overall__ row with a mode marker, reads the mode back, then
// rotates to history — the rotation is the load-bearing assertion:
// it fails with a column-count mismatch if main and history tables
// ever diverge on scorecard_mode.
func TestScorecardModePersistsAndRotates(t *testing.T) {
	store, ctx := scorecardModeConnect(t)
	t.Cleanup(store.pool.Close)

	var repoID int64
	err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.repos (platform_id, repo_git, repo_owner, repo_name)
		VALUES (1, 'https://github.com/_avscmode/it-' || floor(random()*1e9)::text, '_avscmode', 'it')
		RETURNING repo_id`).Scan(&repoID)
	if err != nil {
		t.Fatalf("seed repo: %v", err)
	}
	t.Cleanup(func() {
		store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repo_deps_scorecard_history WHERE repo_id = $1`, repoID)
		store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repo_deps_scorecard WHERE repo_id = $1`, repoID)
		store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, repoID)
	})

	if err := store.InsertScorecardResult(ctx, repoID, "Code-Review", "8", nil, "remote"); err != nil {
		t.Fatalf("insert check: %v", err)
	}
	if err := store.InsertScorecardResult(ctx, repoID, ScorecardOverallName, "5.6", nil, "remote"); err != nil {
		t.Fatalf("insert overall: %v", err)
	}

	var modes []string
	rows, err := store.pool.Query(ctx, `
		SELECT scorecard_mode FROM aveloxis_data.repo_deps_scorecard WHERE repo_id = $1`, repoID)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var m string
		if err := rows.Scan(&m); err != nil {
			t.Fatal(err)
		}
		modes = append(modes, m)
	}
	rows.Close()
	if len(modes) != 2 {
		t.Fatalf("expected 2 scorecard rows, got %d", len(modes))
	}
	for _, m := range modes {
		if m != "remote" {
			t.Errorf("scorecard_mode = %q, want remote on every row (checks AND __overall__)", m)
		}
	}

	// GetRepoScorecard's __overall__ contract must survive the new
	// column: overall extracted, never mixed into checks.
	checks, overall, _, err := store.GetRepoScorecard(ctx, repoID)
	if err != nil {
		t.Fatalf("GetRepoScorecard: %v", err)
	}
	if overall == nil || *overall != 5.6 {
		t.Errorf("overall = %v, want 5.6", overall)
	}
	for _, c := range checks {
		if c.Name == ScorecardOverallName {
			t.Error("__overall__ leaked into the checks list")
		}
	}

	// The rotation contract: SELECT * into history must succeed with
	// the new column present on both sides.
	if err := store.RotateScorecardToHistory(ctx, repoID); err != nil {
		t.Fatalf("RotateScorecardToHistory with scorecard_mode column: %v", err)
	}
	var histCount int
	if err := store.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM aveloxis_data.repo_deps_scorecard_history WHERE repo_id = $1 AND scorecard_mode = 'remote'`,
		repoID).Scan(&histCount); err != nil {
		t.Fatal(err)
	}
	if histCount != 2 {
		t.Errorf("history rows with mode=remote = %d, want 2", histCount)
	}
}

// TestListScorecardBacklogOrdering seeds three GitHub repos — never
// scanned / stale scan / fresh scan — and pins: NULLs first, oldest
// first, older-than filter, limit, and the last_collected gate.
func TestListScorecardBacklogOrdering(t *testing.T) {
	store, ctx := scorecardModeConnect(t)
	t.Cleanup(store.pool.Close)

	mk := func(name string, collected bool) int64 {
		var id int64
		err := store.pool.QueryRow(ctx, `
			INSERT INTO aveloxis_data.repos (platform_id, repo_git, repo_owner, repo_name)
			VALUES (1, 'https://github.com/_avscbacklog/' || $1 || '-' || floor(random()*1e9)::text, '_avscbacklog', $1)
			RETURNING repo_id`, name).Scan(&id)
		if err != nil {
			t.Fatalf("seed repo %s: %v", name, err)
		}
		lc := "NOW()"
		if !collected {
			lc = "NULL"
		}
		if _, err := store.pool.Exec(ctx, `
			INSERT INTO aveloxis_ops.collection_queue (repo_id, status, last_collected)
			VALUES ($1, 'queued', `+lc+`)
			ON CONFLICT (repo_id) DO UPDATE SET last_collected = EXCLUDED.last_collected`, id); err != nil {
			t.Fatalf("seed queue %s: %v", name, err)
		}
		t.Cleanup(func() {
			// Fresh bounded ctx + deadlock retry: raw Exec on the test
			// ctx silently dropped rows when a 40P01 or cancellation hit
			// (153 leaked _avscbacklog repos found 2026-08-31).
			cctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			cleanupExecRetry(cctx, store, `DELETE FROM aveloxis_data.repo_deps_scorecard WHERE repo_id = $1`, id)
			cleanupExecRetry(cctx, store, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, id)
			cleanupExecRetry(cctx, store, `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, id)
		})
		return id
	}
	scoreAt := func(id int64, age time.Duration) {
		if _, err := store.pool.Exec(ctx, `
			INSERT INTO aveloxis_data.repo_deps_scorecard (repo_id, name, score, scorecard_mode, data_collection_date)
			VALUES ($1, 'Maintained', '10', 'local', NOW() - $2::interval)`,
			id, age.String()); err != nil {
			t.Fatalf("seed scorecard: %v", err)
		}
	}

	neverScanned := mk("never", true)
	staleScanned := mk("stale", true)
	freshScanned := mk("fresh", true)
	uncollected := mk("uncollected", false)
	scoreAt(staleScanned, 90*24*time.Hour)
	scoreAt(freshScanned, 1*time.Hour)

	got, err := store.ListScorecardBacklog(ctx, 0, 0)
	if err != nil {
		t.Fatalf("ListScorecardBacklog: %v", err)
	}
	pos := map[int64]int{}
	for i, r := range got {
		if _, seen := pos[r.RepoID]; !seen {
			pos[r.RepoID] = i
		}
	}
	// The scratch DB may contain other repos; assert relative order of
	// OUR rows only.
	pNever, okN := pos[neverScanned]
	pStale, okS := pos[staleScanned]
	pFresh, okF := pos[freshScanned]
	if !okN || !okS || !okF {
		t.Fatalf("backlog missing seeded repos (never=%v stale=%v fresh=%v)", okN, okS, okF)
	}
	if _, leaked := pos[uncollected]; leaked {
		t.Error("backlog must exclude repos with last_collected IS NULL")
	}
	if !(pNever < pStale && pStale < pFresh) {
		t.Errorf("order = never@%d stale@%d fresh@%d; want never < stale < fresh (NULLs first, oldest first)",
			pNever, pStale, pFresh)
	}
	if got[pNever].LastScorecard != nil {
		t.Error("never-scanned repo should carry a nil LastScorecard")
	}

	// older-than filter: 30 days excludes the fresh repo, keeps
	// never-scanned + stale.
	filtered, err := store.ListScorecardBacklog(ctx, 30, 0)
	if err != nil {
		t.Fatal(err)
	}
	fpos := map[int64]bool{}
	for _, r := range filtered {
		fpos[r.RepoID] = true
	}
	if !fpos[neverScanned] || !fpos[staleScanned] {
		t.Error("older-than=30d must keep never-scanned + 90d-stale repos")
	}
	if fpos[freshScanned] {
		t.Error("older-than=30d must exclude the 1h-fresh repo")
	}

	// limit caps the result.
	capped, err := store.ListScorecardBacklog(ctx, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(capped) != 1 {
		t.Errorf("limit=1 returned %d rows", len(capped))
	}
}
