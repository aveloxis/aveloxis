// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.7 — repo_labor becomes latest-snapshot-only.
//
// Pre-v0.27.7, every scc analysis run INSERTed a full fresh per-file
// snapshot into repo_labor and NOTHING was ever rotated or deleted —
// production reached 2.0M live rows / 29 GB, growing unboundedly. The
// house pattern (repo_info, repo_deps_scorecard, repo_distribution) is
// current-table = latest snapshot only, prior snapshots rotate to a
// _history companion. Downstream consumers (explorer_repo_files /
// explorer_repo_languages, LaborInvestmentSnapshot) already filter to
// "latest scan per repo", so output is unchanged by rotation.
//
// Test tiers:
//   - source-contract pins on the schema / migration / write-path
//     needles (always run)
//   - AVELOXIS_TEST_DB-gated integration tests exercising the actual
//     rotation SQL against a live Postgres

package db

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// Source-contract tier
// ---------------------------------------------------------------------------

func TestSchemaDeclaresRepoLaborHistory(t *testing.T) {
	data, err := os.ReadFile("schema.sql")
	if err != nil {
		t.Fatalf("read schema.sql: %v", err)
	}
	src := string(data)

	for _, needle := range []string{
		"CREATE TABLE IF NOT EXISTS aveloxis_data.repo_labor_history",
		"LIKE aveloxis_data.repo_labor INCLUDING ALL",
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("schema.sql must declare the repo_labor history table via LIKE INCLUDING ALL (keeps the PK). Missing needle: %q", needle)
		}
	}

	// The v0.25.1-class documentation must live next to the CREATE: if
	// a UNIQUE is ever added to repo_labor, its inherited copy must be
	// dropped from the history table. Pin the audit note so a future
	// editor sees the rule.
	idx := strings.Index(src, "CREATE TABLE IF NOT EXISTS aveloxis_data.repo_labor_history")
	if idx < 0 {
		t.Fatal("repo_labor_history CREATE not found")
	}
	regionStart := idx - 2000
	if regionStart < 0 {
		regionStart = 0
	}
	region := src[regionStart : idx+200]
	if !strings.Contains(region, "v0.25.1") {
		t.Error("schema.sql's repo_labor_history block must reference the v0.25.1 lesson (inherited natural-key UNIQUEs are wrong on history tables) so a future UNIQUE on repo_labor gets its history copy dropped")
	}
}

func TestMigrationCreatesRepoLaborHistoryAndRotates(t *testing.T) {
	data, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatalf("read migrate.go: %v", err)
	}
	src := string(data)

	for _, needle := range []string{
		// Belt-and-suspenders table create for existing fleets, via
		// the fail-closed execMigrationStep contract (v0.19.4).
		`"v0.27.7 create repo_labor_history (latest-snapshot-only pattern for repo_labor)"`,
		"LIKE aveloxis_data.repo_labor INCLUDING ALL",
		// The one-shot rotation must be invoked from RunMigrations
		// with the error collector so failures fail-closed.
		"migrateRepoLaborSnapshotsToHistory(ctx, pg, logger, &errs)",
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("migrate.go missing v0.27.7 needle: %q", needle)
		}
	}

	// Ordering: the rotation's per-row latest-cohort check is a
	// correlated MAX served by idx_repo_labor_repo_id_analysis_date;
	// the index build must appear before the rotation call.
	idxPos := strings.Index(src, `"idx_repo_labor_repo_id_analysis_date"`)
	rotPos := strings.Index(src, "migrateRepoLaborSnapshotsToHistory(ctx, pg, logger, &errs)")
	if idxPos < 0 || rotPos < 0 {
		t.Fatal("could not locate both the index build and the rotation call in migrate.go")
	}
	if idxPos > rotPos {
		t.Error("migrate.go must build idx_repo_labor_repo_id_analysis_date BEFORE migrateRepoLaborSnapshotsToHistory — the rotation's correlated MAX(rl_analysis_date) probe needs that index or every window seq-scans the 2M-row table")
	}
}

func TestRepoLaborRotationSQLIsKeysetWindowed(t *testing.T) {
	// v0.26.6 lesson (the 9h45m single-batch grind): bulk backfills
	// batch by PK ranges, never LIMIT-rescan loops, never per-batch
	// DISTINCT-ON global sorts.
	for _, needle := range []string{
		"repo_labor_id > $1",
		"repo_labor_id <= $2",
		"IS DISTINCT FROM",
		"MAX(l2.rl_analysis_date)",
		"INSERT INTO aveloxis_data.repo_labor_history",
	} {
		if !strings.Contains(RepoLaborRotateWindowSQL, needle) {
			t.Errorf("RepoLaborRotateWindowSQL missing keyset-window needle: %q", needle)
		}
	}
	// Negative pins on the v0.26.6 anti-patterns.
	up := strings.ToUpper(RepoLaborRotateWindowSQL)
	if strings.Contains(up, "LIMIT") {
		t.Error("RepoLaborRotateWindowSQL must not use LIMIT — LIMIT-rescan loops re-pay the full scan per batch (v0.26.6 lesson)")
	}
	if strings.Contains(up, "DISTINCT ON") {
		t.Error("RepoLaborRotateWindowSQL must not use DISTINCT ON — per-batch global sorts are the v0.26.6 anti-pattern")
	}
}

func TestRotateRepoRowsToHistoryHelperIsAllowlisted(t *testing.T) {
	data, err := os.ReadFile("history.go")
	if err != nil {
		t.Fatalf("read history.go: %v", err)
	}
	src := string(data)

	for _, needle := range []string{
		"func rotateRepoRowsToHistory(ctx context.Context, q rotationExecer, table, historyTable string, repoID int64) error",
		"historyRotationAllowlist",
		`"aveloxis_data.repo_labor":          "aveloxis_data.repo_labor_history"`,
		`"aveloxis_data.repo_deps_scorecard": "aveloxis_data.repo_deps_scorecard_history"`,
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("history.go missing generic-rotation needle: %q", needle)
		}
	}

	// The allowlist check must guard the fmt.Sprintf interpolation:
	// refuse-then-interpolate, in that order, inside the helper body.
	body := extractPlainFunctionBody(t, "history.go", "rotateRepoRowsToHistory")
	guard := strings.Index(body, "historyRotationAllowlist")
	interp := strings.Index(body, "fmt.Sprintf")
	if guard < 0 || interp < 0 {
		t.Fatal("rotateRepoRowsToHistory must consult historyRotationAllowlist and build SQL via fmt.Sprintf")
	}
	if guard > interp {
		t.Error("rotateRepoRowsToHistory must check the allowlist BEFORE interpolating table names into SQL — the allowlist is the injection guard")
	}
}

func TestRotateScorecardToHistoryUsesGenericHelper(t *testing.T) {
	// Behavior-preserving refactor pin: the scorecard rotation keeps
	// its withRetry + single-transaction shape and delegates the two
	// statements to the generic helper with the scorecard pair.
	body := extractFunctionBody(t, "history.go", "RotateScorecardToHistory")
	for _, needle := range []string{
		"s.withRetry(",
		"s.pool.Begin(",
		"rotateRepoRowsToHistory(ctx, tx,",
		`"aveloxis_data.repo_deps_scorecard"`,
		`"aveloxis_data.repo_deps_scorecard_history"`,
		"tx.Commit(",
	} {
		if !strings.Contains(body, needle) {
			t.Errorf("RotateScorecardToHistory missing needle %q — the v0.27.7 refactor must preserve the pre-existing transaction semantics while delegating the SQL to rotateRepoRowsToHistory", needle)
		}
	}
}

func TestReplaceRepoLaborSnapshotRotatesFirstAndHasNoOnConflict(t *testing.T) {
	body := extractFunctionBody(t, "analysis_store.go", "ReplaceRepoLaborSnapshot")

	// Rotation and insert must share ONE transaction, rotation first.
	rotate := strings.Index(body, "rotateRepoRowsToHistory(ctx, tx,")
	insert := strings.Index(body, "INSERT INTO aveloxis_data.repo_labor")
	if rotate < 0 {
		t.Fatal("ReplaceRepoLaborSnapshot must rotate via rotateRepoRowsToHistory on the SAME tx as the insert")
	}
	if insert < 0 {
		t.Fatal("ReplaceRepoLaborSnapshot must insert the fresh snapshot")
	}
	if rotate > insert {
		t.Error("ReplaceRepoLaborSnapshot must rotate the previous snapshot BEFORE inserting the fresh one")
	}
	if !strings.Contains(body, "s.withRetry(") || !strings.Contains(body, "s.pool.Begin(") {
		t.Error("ReplaceRepoLaborSnapshot must run inside withRetry + a single transaction so a mid-insert failure rolls the rotation back too")
	}

	// ON CONFLICT decision (v0.27.7): repo_labor has no unique
	// constraint besides the BIGSERIAL PK, so the old blanket
	// ON CONFLICT DO NOTHING was dead code that masked nothing and
	// prevented nothing. Post-rotation, a within-snapshot duplicate
	// would be a real bug that should surface loudly.
	if strings.Contains(body, "ON CONFLICT") {
		t.Error("ReplaceRepoLaborSnapshot must NOT carry an ON CONFLICT clause — see the v0.27.7 decision comment on the function; a duplicate within a fresh snapshot is a real bug to surface, not swallow")
	}
}

// stripLineComments removes // line comments so identifier mentions in
// comments (e.g. "InsertRepoLabor was removed") don't false-match the
// negative pins below. The v0.21.5 lesson, re-learned in v0.27.4.
func stripLineComments(src string) string {
	lines := strings.Split(src, "\n")
	for i, line := range lines {
		if idx := strings.Index(line, "//"); idx >= 0 {
			lines[i] = line[:idx]
		}
	}
	return strings.Join(lines, "\n")
}

// TestRepoLaborWritersCannotSkipRotation is the misuse tripwire: the
// ONLY code path that writes to aveloxis_data.repo_labor is
// ReplaceRepoLaborSnapshot, which fuses rotation + insert into one
// transaction. A second writer (or a revival of the removed
// InsertRepoLabor / InsertRepoLaborBatch) would let callers insert
// without rotating — re-creating the unbounded-growth bug (2.0M rows /
// 29 GB in production) that v0.27.7 fixed.
func TestRepoLaborWritersCannotSkipRotation(t *testing.T) {
	insertRe := regexp.MustCompile(`INSERT INTO aveloxis_data\.repo_labor\b`)

	type hit struct{ path string }
	var hits []hit

	for _, root := range []string{".", "../collector", "../api", "../scheduler", "../web", "../monitor", "../../cmd"} {
		err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() {
				return nil
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			data, err := os.ReadFile(path)
			if err != nil {
				return nil
			}
			src := stripLineComments(string(data))
			if insertRe.MatchString(src) {
				hits = append(hits, hit{path: path})
			}
			// The removed unrotated writers must not come back.
			for _, banned := range []string{"func (s *PostgresStore) InsertRepoLabor(", "func (s *PostgresStore) InsertRepoLaborBatch(", "InsertRepoLabor(ctx", "InsertRepoLaborBatch(ctx"} {
				if strings.Contains(src, banned) {
					t.Errorf("%s revives the removed unrotated repo_labor writer (%q) — use ReplaceRepoLaborSnapshot, which fuses rotation + insert so misuse is impossible", path, banned)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", root, err)
		}
	}

	if len(hits) != 1 || !strings.HasSuffix(hits[0].path, "analysis_store.go") {
		t.Fatalf("expected exactly ONE literal `INSERT INTO aveloxis_data.repo_labor` across non-test Go sources (inside analysis_store.go's ReplaceRepoLaborSnapshot); found %d: %+v", len(hits), hits)
	}
	body := extractFunctionBody(t, "analysis_store.go", "ReplaceRepoLaborSnapshot")
	if !insertRe.MatchString(body) {
		t.Error("the single repo_labor INSERT must live inside ReplaceRepoLaborSnapshot — anywhere else and rotation can be skipped")
	}
}

func TestScanSCCUsesAtomicSnapshotReplace(t *testing.T) {
	data, err := os.ReadFile("../collector/analysis.go")
	if err != nil {
		t.Fatalf("read analysis.go: %v", err)
	}
	src := stripLineComments(string(data))

	if !strings.Contains(src, "ReplaceRepoLaborSnapshot(ctx, repoID, laborRows)") {
		t.Error("scanSCC must call ReplaceRepoLaborSnapshot exactly once with the accumulated rows — rotation happens inside the store, once per analysis run, in the same tx as the insert")
	}
	if strings.Contains(src, "InsertRepoLabor(") {
		t.Error("scanSCC must not call the removed per-file InsertRepoLabor — per-file inserts without rotation are the unbounded-growth bug v0.27.7 fixed")
	}
}

// extractPlainFunctionBody mirrors extractFunctionBody (see
// contributor_batch_rename_recovery_test.go) for package-level
// functions that aren't PostgresStore methods.
func extractPlainFunctionBody(t *testing.T, filename, funcName string) string {
	t.Helper()
	src, err := os.ReadFile(filename)
	if err != nil {
		t.Fatalf("read %s: %v", filename, err)
	}
	code := string(src)
	marker := "func " + funcName + "("
	startIdx := strings.Index(code, marker)
	if startIdx < 0 {
		t.Fatalf("function %s not found in %s", funcName, filename)
	}
	tail := code[startIdx+1:]
	endRel := strings.Index(tail, "\nfunc ")
	if endRel < 0 {
		endRel = len(tail)
	}
	return code[startIdx : startIdx+1+endRel]
}

// ---------------------------------------------------------------------------
// Integration tier (gated on AVELOXIS_TEST_DB)
// ---------------------------------------------------------------------------

const (
	rlRepoMulti     = int64(991001) // 3 dated snapshots + 1 NULL-date legacy row
	rlRepoSingle    = int64(991002) // 1 snapshot — must be untouched
	rlRepoAllNull   = int64(991003) // only NULL-date rows — must be untouched
	rlRepoReplace   = int64(991004) // exercise ReplaceRepoLaborSnapshot
	rlRepoScorecard = int64(991005) // scorecard rotation behavior-preservation
)

func rlSeedRepo(t *testing.T, ctx context.Context, s *PostgresStore, repoID int64) {
	t.Helper()
	slug := fmt.Sprintf("repo-%d", repoID)
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO aveloxis_data.repos (repo_id, platform_id, repo_git, repo_owner, repo_name)
		VALUES ($1, 1, 'https://example.com/rl-test/' || $2, 'rl-test', $2)
		ON CONFLICT (repo_id) DO NOTHING`, repoID, slug); err != nil {
		t.Fatalf("seed repo %d: %v", repoID, err)
	}
}

func rlCleanup(t *testing.T, ctx context.Context, s *PostgresStore, repoIDs ...int64) {
	t.Helper()
	for _, id := range repoIDs {
		for _, tbl := range []string{
			"aveloxis_data.repo_labor", "aveloxis_data.repo_labor_history",
			"aveloxis_data.repo_deps_scorecard", "aveloxis_data.repo_deps_scorecard_history",
		} {
			if _, err := s.pool.Exec(ctx, `DELETE FROM `+tbl+` WHERE repo_id = $1`, id); err != nil {
				t.Fatalf("cleanup %s for repo %d: %v", tbl, id, err)
			}
		}
	}
}

func rlSeedLaborRow(t *testing.T, ctx context.Context, s *PostgresStore, repoID int64, date any, filePath string) {
	t.Helper()
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO aveloxis_data.repo_labor
			(repo_id, rl_analysis_date, programming_language, file_path, file_name, code_lines)
		VALUES ($1, $2, 'Go', $3, $3, 100)`, repoID, date, filePath); err != nil {
		t.Fatalf("seed repo_labor row (repo=%d date=%v file=%s): %v", repoID, date, filePath, err)
	}
}

func rlCount(t *testing.T, ctx context.Context, s *PostgresStore, table string, repoID int64) int {
	t.Helper()
	var n int
	if err := s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM `+table+` WHERE repo_id = $1`, repoID).Scan(&n); err != nil {
		t.Fatalf("count %s for repo %d: %v", table, repoID, err)
	}
	return n
}

// TestRepoLaborSnapshotMigrationEndToEnd is the v0.27.7 validation
// requirement: seed a repo with 3 snapshots + one NULL-date legacy row,
// one single-snapshot repo, and one all-NULL repo; run the migration's
// core function with a tiny window size (forcing multiple keyset
// windows); assert latest cohort stays, others moved, single-snapshot
// untouched, all-NULL untouched, and a re-run moves nothing.
func TestRepoLaborSnapshotMigrationEndToEnd(t *testing.T) {
	store, ctx := v0251Connect(t)
	defer store.pool.Close()

	rlCleanup(t, ctx, store, rlRepoMulti, rlRepoSingle, rlRepoAllNull)
	rlSeedRepo(t, ctx, store, rlRepoMulti)
	rlSeedRepo(t, ctx, store, rlRepoSingle)
	rlSeedRepo(t, ctx, store, rlRepoAllNull)

	// Multi-snapshot repo: 3 dated cohorts (2 files each) + 1 NULL-date
	// legacy row. Only the 2026-03-01 cohort must survive in current.
	for _, d := range []string{"2026-01-01", "2026-02-01", "2026-03-01"} {
		rlSeedLaborRow(t, ctx, store, rlRepoMulti, d, "main.go")
		rlSeedLaborRow(t, ctx, store, rlRepoMulti, d, "util.go")
	}
	rlSeedLaborRow(t, ctx, store, rlRepoMulti, nil, "legacy.go")

	// Single-snapshot repo: 2 files, one cohort. Nothing may move.
	rlSeedLaborRow(t, ctx, store, rlRepoSingle, "2026-02-15", "a.go")
	rlSeedLaborRow(t, ctx, store, rlRepoSingle, "2026-02-15", "b.go")

	// All-NULL repo: with no dates there is exactly one
	// indistinguishable cohort — it stays (documented rule).
	rlSeedLaborRow(t, ctx, store, rlRepoAllNull, nil, "x.go")
	rlSeedLaborRow(t, ctx, store, rlRepoAllNull, nil, "y.go")

	logger := store.logger
	// windowSize=3 forces several keyset windows over the seeded rows,
	// exercising the window loop, not just a single statement.
	moved, windows, err := rotateRepoLaborSnapshotWindows(ctx, store, logger, 3)
	if err != nil {
		t.Fatalf("rotation run 1: %v", err)
	}
	if windows < 2 {
		t.Errorf("windowSize=3 over 11+ seeded rows should execute multiple windows; got %d — the keyset loop isn't windowing", windows)
	}
	if moved < 5 {
		t.Errorf("expected at least 5 rows moved (2×2 old cohorts + 1 NULL legacy row); got %d", moved)
	}

	// Multi-snapshot repo: latest cohort (2 rows @ 2026-03-01) stays;
	// 4 older dated rows + 1 NULL row are in history.
	if n := rlCount(t, ctx, store, "aveloxis_data.repo_labor", rlRepoMulti); n != 2 {
		t.Errorf("multi-snapshot repo: want 2 current rows (latest cohort), got %d", n)
	}
	var latestOnly bool
	if err := store.pool.QueryRow(ctx, `
		SELECT COUNT(*) = 2 FROM aveloxis_data.repo_labor
		WHERE repo_id = $1 AND rl_analysis_date = '2026-03-01'::timestamptz`, rlRepoMulti).Scan(&latestOnly); err != nil {
		t.Fatalf("latest-cohort check: %v", err)
	}
	if !latestOnly {
		t.Error("multi-snapshot repo: surviving current rows must be exactly the 2026-03-01 (latest) cohort")
	}
	if n := rlCount(t, ctx, store, "aveloxis_data.repo_labor_history", rlRepoMulti); n != 5 {
		t.Errorf("multi-snapshot repo: want 5 history rows (2 cohorts × 2 files + 1 NULL-date legacy), got %d", n)
	}
	var nullInHistory int
	if err := store.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM aveloxis_data.repo_labor_history
		WHERE repo_id = $1 AND rl_analysis_date IS NULL`, rlRepoMulti).Scan(&nullInHistory); err != nil {
		t.Fatalf("NULL-in-history check: %v", err)
	}
	if nullInHistory != 1 {
		t.Errorf("NULL-date legacy row must rotate to history as 'oldest' when dated snapshots exist; found %d NULL history rows", nullInHistory)
	}

	// Single-snapshot repo untouched.
	if n := rlCount(t, ctx, store, "aveloxis_data.repo_labor", rlRepoSingle); n != 2 {
		t.Errorf("single-snapshot repo: want 2 current rows untouched, got %d", n)
	}
	if n := rlCount(t, ctx, store, "aveloxis_data.repo_labor_history", rlRepoSingle); n != 0 {
		t.Errorf("single-snapshot repo: want 0 history rows, got %d", n)
	}

	// All-NULL repo untouched (documented rule: no dates = one
	// indistinguishable cohort = it IS the latest).
	if n := rlCount(t, ctx, store, "aveloxis_data.repo_labor", rlRepoAllNull); n != 2 {
		t.Errorf("all-NULL repo: want 2 current rows untouched, got %d", n)
	}
	if n := rlCount(t, ctx, store, "aveloxis_data.repo_labor_history", rlRepoAllNull); n != 0 {
		t.Errorf("all-NULL repo: want 0 history rows, got %d", n)
	}

	// Re-run: idempotent, moves nothing, state unchanged.
	moved2, _, err := rotateRepoLaborSnapshotWindows(ctx, store, logger, 3)
	if err != nil {
		t.Fatalf("rotation run 2: %v", err)
	}
	// The shared scratch DB may contain other repos, but run 1 already
	// drained them; run 2 must be a global no-op.
	if moved2 != 0 {
		t.Errorf("re-run must move nothing (idempotency); moved %d rows", moved2)
	}
	if n := rlCount(t, ctx, store, "aveloxis_data.repo_labor", rlRepoMulti); n != 2 {
		t.Errorf("re-run changed multi-snapshot repo's current rows: %d", n)
	}
	if n := rlCount(t, ctx, store, "aveloxis_data.repo_labor_history", rlRepoMulti); n != 5 {
		t.Errorf("re-run changed multi-snapshot repo's history rows: %d", n)
	}

	rlCleanup(t, ctx, store, rlRepoMulti, rlRepoSingle, rlRepoAllNull)
}

// TestReplaceRepoLaborSnapshotEndToEnd exercises the forward write
// path: each replace rotates the previous snapshot to history and
// installs the fresh one atomically; an empty replace (successful scan,
// zero source files) leaves an empty current snapshot.
func TestReplaceRepoLaborSnapshotEndToEnd(t *testing.T) {
	store, ctx := v0251Connect(t)
	defer store.pool.Close()

	rlCleanup(t, ctx, store, rlRepoReplace)
	rlSeedRepo(t, ctx, store, rlRepoReplace)

	mk := func(paths ...string) []*RepoLaborRow {
		rows := make([]*RepoLaborRow, 0, len(paths))
		for _, p := range paths {
			rows = append(rows, &RepoLaborRow{Language: "Go", FilePath: p, FileName: p, CodeLines: 10})
		}
		return rows
	}

	// Snapshot 1: two files.
	if err := store.ReplaceRepoLaborSnapshot(ctx, rlRepoReplace, mk("a.go", "b.go")); err != nil {
		t.Fatalf("replace 1: %v", err)
	}
	if n := rlCount(t, ctx, store, "aveloxis_data.repo_labor", rlRepoReplace); n != 2 {
		t.Fatalf("after replace 1: want 2 current rows, got %d", n)
	}
	if n := rlCount(t, ctx, store, "aveloxis_data.repo_labor_history", rlRepoReplace); n != 0 {
		t.Fatalf("after replace 1: want 0 history rows, got %d", n)
	}

	// Snapshot 2: three files — snapshot 1 rotates to history.
	if err := store.ReplaceRepoLaborSnapshot(ctx, rlRepoReplace, mk("a.go", "b.go", "c.go")); err != nil {
		t.Fatalf("replace 2: %v", err)
	}
	if n := rlCount(t, ctx, store, "aveloxis_data.repo_labor", rlRepoReplace); n != 3 {
		t.Errorf("after replace 2: want 3 current rows, got %d", n)
	}
	if n := rlCount(t, ctx, store, "aveloxis_data.repo_labor_history", rlRepoReplace); n != 2 {
		t.Errorf("after replace 2: want 2 history rows (snapshot 1), got %d", n)
	}

	// Empty replace: successful scan observing zero files — the
	// current truth is "no files"; snapshot 2 rotates out.
	if err := store.ReplaceRepoLaborSnapshot(ctx, rlRepoReplace, nil); err != nil {
		t.Fatalf("empty replace: %v", err)
	}
	if n := rlCount(t, ctx, store, "aveloxis_data.repo_labor", rlRepoReplace); n != 0 {
		t.Errorf("after empty replace: want 0 current rows, got %d", n)
	}
	if n := rlCount(t, ctx, store, "aveloxis_data.repo_labor_history", rlRepoReplace); n != 5 {
		t.Errorf("after empty replace: want 5 history rows total, got %d", n)
	}

	rlCleanup(t, ctx, store, rlRepoReplace)
}

// TestRepoLaborHistoryHasNoNaturalKeyUniques is the v0.25.1-class
// negative tripwire: the history table must keep its PRIMARY KEY (via
// LIKE INCLUDING ALL) and must NOT carry any other unique index or
// constraint — history holds many snapshots per logical key, and an
// inherited UNIQUE would 23505 the second rotation of any repo (the
// exact v0.25.1 production failure on the distribution history tables).
// If a UNIQUE is ever added to repo_labor, this test fails on fresh
// installs until its inherited copy is dropped next to the CREATE.
func TestRepoLaborHistoryHasNoNaturalKeyUniques(t *testing.T) {
	store, ctx := v0251Connect(t)
	defer store.pool.Close()

	var hasPK bool
	if err := store.pool.QueryRow(ctx, `
		SELECT EXISTS (
		    SELECT 1 FROM information_schema.table_constraints
		    WHERE table_schema = 'aveloxis_data'
		      AND table_name = 'repo_labor_history'
		      AND constraint_type = 'PRIMARY KEY'
		)`).Scan(&hasPK); err != nil {
		t.Fatalf("introspect PK: %v", err)
	}
	if !hasPK {
		t.Error("repo_labor_history has no PRIMARY KEY — LIKE INCLUDING ALL must be kept so the PK on repo_labor_id survives")
	}

	rows, err := store.pool.Query(ctx, `
		SELECT c.relname
		FROM pg_index i
		JOIN pg_class c ON c.oid = i.indexrelid
		JOIN pg_class tbl ON tbl.oid = i.indrelid
		JOIN pg_namespace n ON n.oid = tbl.relnamespace
		WHERE n.nspname = 'aveloxis_data'
		  AND tbl.relname = 'repo_labor_history'
		  AND i.indisunique
		  AND NOT i.indisprimary`)
	if err != nil {
		t.Fatalf("introspect unique indexes: %v", err)
	}
	defer rows.Close()
	var offenders []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan: %v", err)
		}
		offenders = append(offenders, name)
	}
	if len(offenders) > 0 {
		t.Errorf("repo_labor_history carries non-PK UNIQUE indexes %v — history tables hold many snapshots per logical key (v0.25.1 lesson); drop the inherited copies by their auto-generated (63-char-truncated) names next to the CREATE in schema.sql", offenders)
	}
}

// TestRotateScorecardToHistoryBehaviorPreserved pins the operator
// directive on the v0.27.7 helper extraction: migrating
// RotateScorecardToHistory onto rotateRepoRowsToHistory is a
// behavior-PRESERVING transform — identical rows moved, main table
// cleared, history accumulates across rotations.
func TestRotateScorecardToHistoryBehaviorPreserved(t *testing.T) {
	store, ctx := v0251Connect(t)
	defer store.pool.Close()

	rlCleanup(t, ctx, store, rlRepoScorecard)
	rlSeedRepo(t, ctx, store, rlRepoScorecard)

	seed := func(name string) {
		if _, err := store.pool.Exec(ctx, `
			INSERT INTO aveloxis_data.repo_deps_scorecard (repo_id, name, score)
			VALUES ($1, $2, '7')`, rlRepoScorecard, name); err != nil {
			t.Fatalf("seed scorecard %s: %v", name, err)
		}
	}
	seed("Maintained")
	seed("License")

	if err := store.RotateScorecardToHistory(ctx, rlRepoScorecard); err != nil {
		t.Fatalf("rotate 1: %v", err)
	}
	if n := rlCount(t, ctx, store, "aveloxis_data.repo_deps_scorecard", rlRepoScorecard); n != 0 {
		t.Errorf("after rotate: want 0 current scorecard rows, got %d", n)
	}
	if n := rlCount(t, ctx, store, "aveloxis_data.repo_deps_scorecard_history", rlRepoScorecard); n != 2 {
		t.Errorf("after rotate: want 2 history scorecard rows, got %d", n)
	}

	// Second rotation with the SAME check names must accumulate in
	// history (no inherited-UNIQUE 23505) — the scorecard analog of
	// the v0.25.1 distribution incident.
	seed("Maintained")
	if err := store.RotateScorecardToHistory(ctx, rlRepoScorecard); err != nil {
		t.Fatalf("rotate 2: %v", err)
	}
	if n := rlCount(t, ctx, store, "aveloxis_data.repo_deps_scorecard_history", rlRepoScorecard); n != 3 {
		t.Errorf("after rotate 2: want 3 accumulated history rows, got %d", n)
	}

	rlCleanup(t, ctx, store, rlRepoScorecard)
}

// TestRotateRepoRowsToHistoryRefusesUnlistedTables pins the injection
// guard at runtime: a table pair outside historyRotationAllowlist is
// refused before any SQL is built.
func TestRotateRepoRowsToHistoryRefusesUnlistedTables(t *testing.T) {
	err := rotateRepoRowsToHistory(context.Background(), nil, "aveloxis_data.repos; DROP TABLE x", "aveloxis_data.repo_labor_history", 1)
	if err == nil || !strings.Contains(err.Error(), "allowlist") {
		t.Fatalf("unlisted table pair must be refused with an allowlist error; got %v", err)
	}
	err = rotateRepoRowsToHistory(context.Background(), nil, "aveloxis_data.repo_labor", "aveloxis_data.repos", 1)
	if err == nil || !strings.Contains(err.Error(), "allowlist") {
		t.Fatalf("mismatched history table must be refused with an allowlist error; got %v", err)
	}
}
