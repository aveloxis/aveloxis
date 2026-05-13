package db

import (
	"os"
	"strings"
	"testing"
)

// v0.20.5: Repos affected by the pre-v0.20.5 gap-fill silent-failure
// bug have force_full_collect=FALSE and last_collected=NOW() in
// aveloxis_ops.collection_queue. The Fix A plumbing change makes
// FUTURE gap-fill failures auto-flag, but existing affected rows are
// already stuck — their next cycle would still run incremental and
// never close the historical PR gap. The migration backfill closes
// the loop by setting force_full_collect=TRUE for any queue row whose
// latest repo_info.pr_count materially exceeds last_prs. Idempotent:
// CompleteJob clears the flag on the next successful collection so
// subsequent migrate runs are no-ops for already-recovered repos.

func TestMigrationBackfillsGapFillForceFullCollect(t *testing.T) {
	data, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	// Pin the SQL shape: an UPDATE on the queue, joined against the
	// latest repo_info row per repo_id, filtered by a 5% gap threshold
	// matching the live gap detector (5% in scheduler — see
	// gap_fill.go's gapPctThreshold). Wrapped in execMigrationStep per
	// the v0.19.4 fail-closed contract.
	needles := []string{
		"UPDATE aveloxis_ops.collection_queue",
		"SET force_full_collect = TRUE",
		"aveloxis_data.repo_info",
		"pr_count",
		"last_prs",
		"execMigrationStep",
	}
	for _, n := range needles {
		if !strings.Contains(src, n) {
			t.Errorf("migrate.go missing backfill needle %q — the v0.20.5 migration must include a one-shot UPDATE that sets force_full_collect=TRUE for queue rows with a material PR gap so already-affected repos auto-recover on the next cycle without operator intervention", n)
		}
	}
}

// TestBackfillIsIdempotent pins the WHERE clause shape that prevents
// the migration from doing anything on subsequent runs once the
// affected repos have been re-collected. After a successful full
// re-collection, last_prs catches up to pr_count and force_full_collect
// is already cleared — both conditions filter the row out.
func TestBackfillIsIdempotent(t *testing.T) {
	data, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	// Locate the backfill block by anchoring on the UPDATE.
	idx := strings.Index(src, "UPDATE aveloxis_ops.collection_queue\n\t\tSET force_full_collect = TRUE")
	if idx < 0 {
		// More forgiving anchor for whitespace variation.
		idx = strings.Index(src, "SET force_full_collect = TRUE")
		if idx < 0 {
			t.Fatal("cannot locate backfill UPDATE in migrate.go")
		}
	}
	// Scan forward to the end of the SQL string literal (closing backtick).
	tail := src[idx:]
	end := strings.Index(tail, "`")
	if end < 0 {
		t.Fatal("cannot locate end of backfill SQL literal")
	}
	sqlBlock := tail[:end]

	// The WHERE clause must include the gap threshold AND skip rows
	// where the flag is already set. Two separate signals together
	// guarantee idempotency.
	if !strings.Contains(sqlBlock, "last_prs") {
		t.Error("backfill SQL must compare last_prs against the metadata PR count — otherwise every queue row would get flagged on every migrate run")
	}
	// Either an explicit `force_full_collect = FALSE` filter or a
	// `force_full_collect IS DISTINCT FROM TRUE` filter is acceptable;
	// both make a repo with the flag already set a no-op for the row.
	if !strings.Contains(sqlBlock, "force_full_collect = FALSE") &&
		!strings.Contains(sqlBlock, "force_full_collect IS DISTINCT FROM TRUE") &&
		!strings.Contains(sqlBlock, "NOT force_full_collect") {
		t.Error("backfill SQL must skip rows where force_full_collect is already TRUE — otherwise repeated migrate runs would repeatedly re-write the same flag, churning the queue updated_at unnecessarily")
	}
}

// TestBackfillUsesLatestRepoInfoRow pins the DISTINCT-ON-by-repo
// pattern. repo_info accumulates one row per collection cycle (the
// previous snapshot rotates to repo_info_history). The backfill must
// compare against the LATEST snapshot, not the first or an arbitrary
// row, otherwise old metadata could falsely match the gap threshold.
func TestBackfillUsesLatestRepoInfoRow(t *testing.T) {
	data, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	idx := strings.Index(src, "SET force_full_collect = TRUE")
	if idx < 0 {
		t.Fatal("cannot locate backfill UPDATE")
	}
	// Look backward and forward within a window for the latest-row
	// selector — typically `DISTINCT ON (repo_id) ... ORDER BY repo_id,
	// data_collection_date DESC` or an equivalent MAX subquery.
	window := src[max(0, idx-200):min(len(src), idx+1200)]
	hasDistinctOn := strings.Contains(window, "DISTINCT ON (repo_id)") &&
		strings.Contains(window, "data_collection_date DESC")
	hasMaxSubquery := strings.Contains(window, "MAX(data_collection_date)") ||
		strings.Contains(window, "max(data_collection_date)")
	if !hasDistinctOn && !hasMaxSubquery {
		t.Error("backfill must select the latest repo_info row per repo — either DISTINCT ON (repo_id) ORDER BY repo_id, data_collection_date DESC, or a MAX(data_collection_date) subquery. Without this, the backfill could compare against stale metadata.")
	}
}
