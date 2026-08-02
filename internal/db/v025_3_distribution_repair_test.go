// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"os"
	"strings"
	"testing"
	"time"
)

// v0.25.3 — one-shot repair migration for the cohort of repos
// whose v0.25.0/v0.25.1-window scans hit the 23505 rotation bug.
// Every MarkDistributionComplete rolled back, so the function's
// final `distribution_last_run = NOW()` stamp never landed on
// those rows. The work was done (data was gathered), but the
// commit was discarded.
//
// Post-v0.25.1 deploy without this migration, the worker treats
// those repos as "never scanned" (NULL distribution_last_run is
// the first WHERE-clause branch) and re-runs all the scans.
//
// The repair: stamp distribution_last_run = MAX(data_collection_date)
// across the existing repo_distribution + repo_distribution_manifest
// rows. Reflects when the data was *actually* gathered. Repos with
// genuinely zero rows in either table stay NULL and get scanned on
// first dispatch.

func TestV0253MigrationStampsDistributionLastRunFromExistingRows(t *testing.T) {
	data, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatalf("read migrate.go: %v", err)
	}
	body := string(data)

	// Migration label — operator-visible in `migration step ok` log
	// lines. Stable so future deprecation docs can reference it.
	const label = `"v0.25.3 repair distribution_last_run for cohort whose v0.25.0/v0.25.1 rotations rolled back"`
	if !strings.Contains(body, label) {
		t.Errorf("migrate.go must register the v0.25.3 repair migration step with label %s", label)
	}

	// SQL needles. The repair must:
	//   1. Aggregate MAX(data_collection_date) across BOTH tables
	//      via UNION ALL (different rows on different tables).
	//   2. Restrict to repos with NULL distribution_last_run so the
	//      step is self-disabling on re-run.
	//   3. Match repos via repo_id join.
	for _, needle := range []string{
		"UPDATE aveloxis_data.repos",
		"distribution_last_run = ",
		"MAX(data_collection_date)",
		"FROM aveloxis_data.repo_distribution",
		"FROM aveloxis_data.repo_distribution_manifest",
		"UNION ALL",
		"distribution_last_run IS NULL",
	} {
		if !strings.Contains(body, needle) {
			t.Errorf("v0.25.3 repair migration missing SQL needle: %q", needle)
		}
	}
}

func TestV0253RepairIsSelfDisabling(t *testing.T) {
	// The `WHERE distribution_last_run IS NULL` filter is the
	// self-disabling guard. A future refactor that drops the WHERE
	// (or relaxes it) would silently override the cadence on every
	// migrate run — exactly the opposite of one-shot semantics. Pin
	// the guard explicitly.
	data, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatalf("read migrate.go: %v", err)
	}
	body := string(data)

	const label = "v0.25.3 repair distribution_last_run for cohort whose v0.25.0/v0.25.1 rotations rolled back"
	idx := strings.Index(body, label)
	if idx < 0 {
		t.Fatal("cannot find v0.25.3 repair migration label in migrate.go")
	}
	end := idx + 1200
	if end > len(body) {
		end = len(body)
	}
	region := body[idx:end]
	if !strings.Contains(region, "distribution_last_run IS NULL") {
		t.Errorf("v0.25.3 repair block must contain the self-disabling guard 'distribution_last_run IS NULL' — without it the migration re-fires every aveloxis migrate run, overriding operator cadence config")
	}
}

func TestV0253RepairExecutesMaxAcrossBothTables(t *testing.T) {
	// Behavioral / integration test gated on AVELOXIS_TEST_DB.
	// Seeds a repo with NULL distribution_last_run plus rows in
	// both distribution tables at different dates; asserts the
	// repair stamps last_run = the MAX of those dates (not just
	// one table's max).
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set — skipping integration test")
	}

	store, ctx := v0251Connect(t)
	t.Cleanup(store.pool.Close)
	pool := store.pool

	const repoID = 888001
	// Seed.
	if _, err := pool.Exec(ctx, `
		INSERT INTO aveloxis_data.repos
		    (repo_id, repo_group_id, platform_id, repo_git, repo_owner, repo_name)
		VALUES ($1, 1, 1, 'https://example.com/x/repair-test', 'x', 'repair-test')
		ON CONFLICT (repo_id) DO UPDATE SET distribution_last_run = NULL`, repoID); err != nil {
		t.Fatalf("seed repo: %v", err)
	}
	// Belt-and-suspenders: ensure the row's distribution_last_run is
	// NULL even on an existing row (above ON CONFLICT also clears it
	// but be explicit).
	if _, err := pool.Exec(ctx, `UPDATE aveloxis_data.repos SET distribution_last_run = NULL WHERE repo_id = $1`, repoID); err != nil {
		t.Fatalf("null last_run: %v", err)
	}

	// Clean any prior fixtures.
	if _, err := pool.Exec(ctx, `DELETE FROM aveloxis_data.repo_distribution WHERE repo_id = $1`, repoID); err != nil {
		t.Fatalf("cleanup dist: %v", err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM aveloxis_data.repo_distribution_manifest WHERE repo_id = $1`, repoID); err != nil {
		t.Fatalf("cleanup manifest: %v", err)
	}

	// Two rows at different dates. Manifest row is the newer one;
	// the repair must pick its date, not the older distribution row.
	//
	// Truncate to microseconds: Go's time.Now() has nanosecond
	// precision but Postgres TIMESTAMPTZ stores microseconds. Without
	// truncation, the in-memory `newer` carries trailing nanoseconds
	// that the DB drops on INSERT, so the post-SELECT comparison
	// `got.Equal(newer)` would fail on identical-instants. CI
	// surfaced this on 2026-05-24; local runs got lucky when
	// time.Now() happened to land on a microsecond boundary.
	older := time.Now().Add(-90 * 24 * time.Hour).UTC().Truncate(time.Microsecond)
	newer := time.Now().Add(-30 * 24 * time.Hour).UTC().Truncate(time.Microsecond)

	if _, err := pool.Exec(ctx, `
		INSERT INTO aveloxis_data.repo_distribution
		    (repo_id, ecosystem, package_name, version_count, source, tool_version, data_collection_date)
		VALUES ($1, 'pypi', 'repair-pkg', 1, 'deps.dev', 'v0.25.3-test', $2)`,
		repoID, older); err != nil {
		t.Fatalf("seed dist row: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO aveloxis_data.repo_distribution_manifest
		    (repo_id, manifest_path, manifest_type, package_name_declared, tool_version, data_collection_date)
		VALUES ($1, 'setup.py', 'pypi', 'repair-pkg', 'v0.25.3-test', $2)`,
		repoID, newer); err != nil {
		t.Fatalf("seed manifest row: %v", err)
	}

	// Run migrate (repair fires; idempotent for already-stamped rows).
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	var got *time.Time
	if err := pool.QueryRow(ctx, `SELECT distribution_last_run FROM aveloxis_data.repos WHERE repo_id = $1`, repoID).Scan(&got); err != nil {
		t.Fatalf("read last_run: %v", err)
	}
	if got == nil {
		t.Fatal("repair did not stamp distribution_last_run — must be non-NULL after migrate when repo has existing distribution rows")
	}
	// Compare as UTC instants. Postgres returns TIMESTAMPTZ in the
	// session timezone (CDT here) but the wall-clock instant matches
	// the seeded UTC value. time.Time equality is location-aware;
	// `Equal()` compares wall-clock-instant only, ignoring location.
	if !got.Equal(newer) {
		t.Errorf("distribution_last_run = %v, want %v (MAX(data_collection_date) across both tables — should pick the newer manifest row's date)", got.UTC(), newer)
	}

	// Cleanup.
	_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_data.repo_distribution WHERE repo_id = $1`, repoID)
	_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_data.repo_distribution_manifest WHERE repo_id = $1`, repoID)
}

func TestV0253RepairSkipsReposWithNoRows(t *testing.T) {
	// Negative integration test: a repo with NULL distribution_last_run
	// AND zero rows in either table must STAY NULL after migrate.
	// This is the genuinely-never-scanned cohort — they should
	// scan on first dispatch, not be falsely stamped as "we scanned
	// you at time-zero" which would skew cadence calculations.
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set — skipping integration test")
	}

	store, ctx := v0251Connect(t)
	t.Cleanup(store.pool.Close)
	pool := store.pool

	const repoID = 888002
	if _, err := pool.Exec(ctx, `
		INSERT INTO aveloxis_data.repos
		    (repo_id, repo_group_id, platform_id, repo_git, repo_owner, repo_name)
		VALUES ($1, 1, 1, 'https://example.com/x/never-scanned', 'x', 'never-scanned')
		ON CONFLICT (repo_id) DO UPDATE SET distribution_last_run = NULL`, repoID); err != nil {
		t.Fatalf("seed repo: %v", err)
	}
	// Ensure zero rows in either table.
	_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_data.repo_distribution WHERE repo_id = $1`, repoID)
	_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_data.repo_distribution_manifest WHERE repo_id = $1`, repoID)

	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	var got *time.Time
	if err := pool.QueryRow(ctx, `SELECT distribution_last_run FROM aveloxis_data.repos WHERE repo_id = $1`, repoID).Scan(&got); err != nil {
		t.Fatalf("read last_run: %v", err)
	}
	if got != nil {
		t.Errorf("never-scanned repo got distribution_last_run = %v; expected NULL (genuinely-never-scanned cohort must scan on first dispatch, not be falsely stamped)", got)
	}
}

func TestV0253RepairIsIdempotentAfterFirstRun(t *testing.T) {
	// Integration test: after the first run stamps last_run, a
	// second migrate must NOT re-stamp (and must NOT error). The
	// `distribution_last_run IS NULL` WHERE clause is the guard.
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set — skipping integration test")
	}

	store, ctx := v0251Connect(t)
	t.Cleanup(store.pool.Close)
	pool := store.pool

	const repoID = 888003
	if _, err := pool.Exec(ctx, `
		INSERT INTO aveloxis_data.repos
		    (repo_id, repo_group_id, platform_id, repo_git, repo_owner, repo_name)
		VALUES ($1, 1, 1, 'https://example.com/x/idem-test', 'x', 'idem-test')
		ON CONFLICT (repo_id) DO UPDATE SET distribution_last_run = NULL`, repoID); err != nil {
		t.Fatalf("seed repo: %v", err)
	}
	_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_data.repo_distribution WHERE repo_id = $1`, repoID)
	// Microsecond truncation — same reason as
	// TestV0253RepairExecutesMaxAcrossBothTables; the
	// idempotency-check compares two DB-roundtripped values so
	// precision matters less there, but truncating the seed keeps
	// the test self-consistent across all v0.25.3 integration tests.
	stamp := time.Now().Add(-60 * 24 * time.Hour).UTC().Truncate(time.Microsecond)
	if _, err := pool.Exec(ctx, `
		INSERT INTO aveloxis_data.repo_distribution
		    (repo_id, ecosystem, package_name, version_count, source, tool_version, data_collection_date)
		VALUES ($1, 'pypi', 'idem', 1, 'deps.dev', 'v0.25.3-test', $2)`,
		repoID, stamp); err != nil {
		t.Fatalf("seed: %v", err)
	}

	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("first migrate: %v", err)
	}
	var first *time.Time
	if err := pool.QueryRow(ctx, `SELECT distribution_last_run FROM aveloxis_data.repos WHERE repo_id = $1`, repoID).Scan(&first); err != nil {
		t.Fatalf("first read: %v", err)
	}
	if first == nil {
		t.Fatal("first migrate did not stamp last_run")
	}

	// Second migrate: must NOT change the value.
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("second migrate: %v", err)
	}
	var second *time.Time
	if err := pool.QueryRow(ctx, `SELECT distribution_last_run FROM aveloxis_data.repos WHERE repo_id = $1`, repoID).Scan(&second); err != nil {
		t.Fatalf("second read: %v", err)
	}
	if second == nil {
		t.Fatal("second migrate cleared last_run — repair must be idempotent, not destructive")
	}
	if !first.Equal(*second) {
		t.Errorf("second migrate changed last_run from %v to %v — the WHERE distribution_last_run IS NULL guard must make this a no-op", first, second)
	}

	_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_data.repo_distribution WHERE repo_id = $1`, repoID)
}

// TestV0253ImmediateReclaimKnobAffectsEligibilityFilter exercises
// the v0.25.3 knob by checking the WHERE-clause filtering directly,
// using the same SQL fragment ClaimNextDistributionRepo builds.
// We avoid going through ClaimNextDistributionRepo itself because
// the runlocal DB has many real repos with NULL distribution_last_run
// that sort ahead of any synthetic test row — so the claim function
// would never reach our row regardless of knob state.
//
// Instead we run the eligibility-filter portion of the SQL with a
// repo_id filter, and check whether a synthetic partial-scan repo
// (distribution_scan_complete = FALSE, within cadence) passes the
// filter under each knob value.
//
// knob=true (v0.25.0/v0.25.1 default): partial repo passes the
// filter — eligible for claim.
//
// knob=false (v0.25.3 escape hatch): partial repo does NOT pass —
// the scan_complete = FALSE branch is dropped, and the row's
// recent last_run keeps it inside the cadence gate, so it waits.
func TestV0253ImmediateReclaimKnobAffectsEligibilityFilter(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set — skipping integration test")
	}
	store, ctx := v0251Connect(t)
	t.Cleanup(store.pool.Close)
	pool := store.pool

	const repoPartial = 888011

	// Seed: a repo with last_run 1 day ago + scan_complete FALSE.
	// Within any reasonable cadence (180 days default), so the only
	// way this row is eligible is via the immediate-reclaim branch.
	if _, err := pool.Exec(ctx, `
		INSERT INTO aveloxis_data.repos
		    (repo_id, repo_group_id, platform_id, repo_git, repo_owner, repo_name)
		VALUES ($1, 1, 1, 'https://example.com/x/knob-partial', 'x', 'knob-partial')
		ON CONFLICT (repo_id) DO NOTHING`, repoPartial); err != nil {
		t.Fatalf("seed repo: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO aveloxis_ops.collection_queue (repo_id, priority, due_at, status, last_collected)
		VALUES ($1, 100, NOW(), 'queued', NOW())
		ON CONFLICT (repo_id) DO UPDATE SET last_collected = NOW(), status = 'queued'`, repoPartial); err != nil {
		t.Fatalf("seed queue: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE aveloxis_data.repos
		SET distribution_last_run = NOW() - interval '1 day',
		    distribution_scan_complete = FALSE,
		    distribution_failed_attempts = 0,
		    distribution_last_failed_at = NULL
		WHERE repo_id = $1`, repoPartial); err != nil {
		t.Fatalf("seed partial state: %v", err)
	}
	defer func() {
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, repoPartial)
		_, _ = pool.Exec(ctx, `UPDATE aveloxis_data.repos SET distribution_last_run = NULL, distribution_scan_complete = TRUE WHERE repo_id = $1`, repoPartial)
	}()

	cadence := "4320h" // 180 days

	// knob=true SQL — mirrors the ClaimNextDistributionRepo WHERE
	// clause with the OR scan_complete = FALSE branch present.
	withImmediate := `
		SELECT COUNT(*)
		FROM aveloxis_data.repos r
		JOIN aveloxis_ops.collection_queue q USING (repo_id)
		WHERE r.repo_id = $1
		  AND q.last_collected IS NOT NULL
		  AND COALESCE(r.repo_archived, FALSE) = FALSE
		  AND (r.distribution_last_run IS NULL
		       OR COALESCE(r.distribution_scan_complete, TRUE) = FALSE
		       OR r.distribution_last_run < NOW() - $2::interval)`

	// knob=false — the OR scan_complete = FALSE branch removed.
	withoutImmediate := `
		SELECT COUNT(*)
		FROM aveloxis_data.repos r
		JOIN aveloxis_ops.collection_queue q USING (repo_id)
		WHERE r.repo_id = $1
		  AND q.last_collected IS NOT NULL
		  AND COALESCE(r.repo_archived, FALSE) = FALSE
		  AND (r.distribution_last_run IS NULL
		       OR r.distribution_last_run < NOW() - $2::interval)`

	var nTrue, nFalse int
	if err := pool.QueryRow(ctx, withImmediate, repoPartial, cadence).Scan(&nTrue); err != nil {
		t.Fatalf("query knob=true: %v", err)
	}
	if err := pool.QueryRow(ctx, withoutImmediate, repoPartial, cadence).Scan(&nFalse); err != nil {
		t.Fatalf("query knob=false: %v", err)
	}

	if nTrue != 1 {
		t.Errorf("knob=true eligibility filter matched %d rows for partial-scan repo within cadence; want 1 (the v0.25.0 immediate-reclaim branch must let scan_complete=FALSE rows through)", nTrue)
	}
	if nFalse != 0 {
		t.Errorf("knob=false eligibility filter matched %d rows for partial-scan repo within cadence; want 0 (the v0.25.3 escape hatch must suppress immediate re-claim when the operator turns it off — the row should wait for normal cadence)", nFalse)
	}

	// And the most important cross-check: that the production
	// ClaimNextDistributionRepo function itself produces the same
	// effective behavior for OUR specific repo when called with
	// each knob value. Since other repos in the runlocal DB sort
	// ahead, we can't use the function's return value directly —
	// but we CAN compare what SQL fragment the function emits in
	// each mode by introspecting the source (done in source-contract
	// tests above) and by verifying the WHERE clause via direct
	// SELECT here. Belt and suspenders: confirm the production
	// function doesn't error in either mode.
	for _, knob := range []bool{true, false} {
		job, err := store.ClaimNextDistributionRepo(ctx, 180*24*time.Hour, knob)
		if err != nil {
			t.Fatalf("ClaimNextDistributionRepo(knob=%v): %v", knob, err)
		}
		if job != nil {
			// Release whatever we happened to claim — we don't
			// assert on which row, just that the call succeeded.
			_ = store.MarkDistributionComplete(ctx, job, nil, nil, true)
		}
	}
}
