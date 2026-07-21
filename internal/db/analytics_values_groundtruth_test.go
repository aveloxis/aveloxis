// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// v0.27.31 (audit Phase 3, C1/C4) — metric VALUE tests with seeded
// ground truth, CI-executable. The audit found the base-metric SQL was
// covered only by "produces a non-empty series on live data"
// (TestAnalyticsQueriesOnRealData) — non-empty proves the query runs,
// not that it counts correctly. These tests seed known rows and assert
// the EXACT values, with every expected number derived by hand here,
// never by running the code under test.
//
// The seeds deliberately include the two known failure shapes:
//   - per-file commit rows (one hash, 4 files) — DISTINCT-hash dedup
//   - a timestamp at Monday 03:00 UTC, which is still SUNDAY in
//     America/Chicago. Without the AT TIME ZONE 'UTC' anchor a
//     non-UTC session buckets it into the PREVIOUS week — a
//     date-level error this test catches directly (the DB CI tier
//     runs under TZ/PGTZ=America/Chicago for exactly this reason).

import (
	"testing"
	"time"
)

// bucketDate returns the value whose bucket falls on the given UTC
// date, or -1 when the bucket is absent from the series.
func bucketDate(points []WeeklyPoint, y int, m time.Month, d int) float64 {
	for _, p := range points {
		bu := p.Bucket.UTC()
		if bu.Year() == y && bu.Month() == m && bu.Day() == d {
			return p.Value
		}
	}
	return -1
}

func TestMetricSeriesExactValuesFromSeededData(t *testing.T) {
	store, ctx := retentionConnect(t)
	defer store.Close()
	seed := newRetentionSeed(ctx, t, store)
	carol := seed.contributor("_avgt-carol", "User", 0)

	// Week of Monday 2025-03-03 (all timestamps UTC):
	wed := time.Date(2025, 3, 5, 12, 0, 0, 0, time.UTC)
	// Monday 03:00 UTC = Sunday 21:00 in America/Chicago — the
	// session-TZ trap. Must land in the 2025-03-03 bucket.
	monEdge := time.Date(2025, 3, 3, 3, 0, 0, 0, time.UTC)
	// A second week (Monday 2025-03-10) proves bucket separation.
	nextWed := time.Date(2025, 3, 12, 12, 0, 0, 0, time.UTC)

	seed.issue(carol, wed)
	seed.issue(carol, wed)
	seed.issue(carol, monEdge)
	seed.issue(carol, nextWed)
	seed.pr(carol, wed)
	seed.commit(carol, wed, 4) // ONE hash, four per-file rows
	seed.commit(carol, wed, 1)

	since := time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC)
	until := time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC)
	ids := []int64{seed.repoID}

	issues, err := store.MetricWeeklySeries(ctx, ids, "issues", "week", since, until)
	if err != nil {
		t.Fatalf("issues series: %v", err)
	}
	// Hand-derived: 2 (Wed) + 1 (Mon-edge) = 3 in the Mar-3 week.
	if got := bucketDate(issues, 2025, time.March, 3); got != 3 {
		t.Errorf("issues week 2025-03-03 = %v, want 3 — if this is 2 with the Mon-edge issue in 2025-02-24, the session-TZ anchor regressed", got)
	}
	if got := bucketDate(issues, 2025, time.March, 10); got != 1 {
		t.Errorf("issues week 2025-03-10 = %v, want 1", got)
	}
	if got := bucketDate(issues, 2025, time.February, 24); got != -1 {
		t.Errorf("issues week 2025-02-24 present (=%v) — the Monday-03:00-UTC issue leaked into the previous week (local-time bucketing)", got)
	}

	commits, err := store.MetricWeeklySeries(ctx, ids, "code_change_commits", "week", since, until)
	if err != nil {
		t.Fatalf("commits series: %v", err)
	}
	// Hand-derived: 2 DISTINCT hashes (the 4-file commit is ONE).
	if got := bucketDate(commits, 2025, time.March, 3); got != 2 {
		t.Errorf("code_change_commits week 2025-03-03 = %v, want 2 (per-file rows must dedup by hash — 5 would mean row-counting)", got)
	}

	prs, err := store.MetricWeeklySeries(ctx, ids, "change_requests", "week", since, until)
	if err != nil {
		t.Fatalf("prs series: %v", err)
	}
	if got := bucketDate(prs, 2025, time.March, 3); got != 1 {
		t.Errorf("change_requests week 2025-03-03 = %v, want 1", got)
	}

	// Month bucketing on the same seeds: all 4 issues are in March.
	issuesM, err := store.MetricWeeklySeries(ctx, ids, "issues", "month", since, until)
	if err != nil {
		t.Fatalf("issues month series: %v", err)
	}
	if got := bucketDate(issuesM, 2025, time.March, 1); got != 4 {
		t.Errorf("issues month 2025-03 = %v, want 4", got)
	}
}

// TestLaborInvestmentMatchesPublishedCOCOMO (audit C4): the COCOMO-II
// basic estimate validated against a hand-computed value of the
// PUBLISHED formula, not against the Go expression it implements.
// PM = 2.94 · KLOC^1.0997; at 100 KSLOC:
//
//	ln(100) = 4.605170, × 1.0997 = 5.064305, e^5.064305 ≈ 158.27
//	× 2.94 ≈ 465.3 person-months
//
// (computed by hand above — the tolerance below covers the manual
// rounding, not implementation slack).
func TestLaborInvestmentMatchesPublishedCOCOMO(t *testing.T) {
	store, ctx := retentionConnect(t)
	defer store.Close()
	seed := newRetentionSeed(ctx, t, store)

	_, err := store.pool.Exec(ctx, `
		INSERT INTO aveloxis_data.repo_labor (repo_id, rl_analysis_date, file_path, code_lines)
		VALUES ($1, NOW(), 'main.go', 100000)`, seed.repoID)
	if err != nil {
		t.Fatalf("seed repo_labor: %v", err)
	}

	snap, err := store.LaborInvestmentSnapshot(ctx, []int64{seed.repoID})
	if err != nil {
		t.Fatalf("LaborInvestmentSnapshot: %v", err)
	}
	if snap.Value < 464.5 || snap.Value > 466.2 {
		t.Errorf("COCOMO at 100 KSLOC = %.2f person-months, want ≈465.3 (hand-computed 2.94·100^1.0997)", snap.Value)
	}

	// Zero-KLOC repo → zero estimate, no error.
	empty := newRetentionSeed(ctx, t, store)
	snap0, err := store.LaborInvestmentSnapshot(ctx, []int64{empty.repoID})
	if err != nil {
		t.Fatalf("empty snapshot: %v", err)
	}
	if snap0.Value != 0 {
		t.Errorf("zero-KLOC COCOMO = %v, want 0", snap0.Value)
	}
}
