// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

// The dependency inventory is replaced WHOLE, in one transaction (summary/70,
// 4.7). Before 0.29.157 the rotation deleted every current row in its own
// transaction and the new rows were then inserted one at a time, each in its
// own transaction, with a registry lookup between them: an analysis
// interrupted part-way left a partial inventory, and a vulnerability scan
// that read it closed the findings of every dependency not yet re-inserted.

func currentLibyearNames(t *testing.T, store *PostgresStore, ctx context.Context, repoID int64) map[string]int64 {
	t.Helper()
	rows, err := store.pool.Query(ctx,
		`SELECT name, repo_deps_libyear_id FROM aveloxis_data.repo_deps_libyear WHERE repo_id = $1`, repoID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]int64{}
	for rows.Next() {
		var name string
		var id int64
		if err := rows.Scan(&name, &id); err != nil {
			t.Fatal(err)
		}
		out[name] = id
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func libyearRow(name, version string) *model.LibyearRow {
	return &model.LibyearRow{Name: name, PackageManager: "npm", CurrentVersion: version, Type: "runtime",
		Requirement: "^" + version, Purl: "pkg:npm/" + name + "@" + version, License: "MIT"}
}

func TestReplaceRepoLibyearReplacesTheInventoryAndRotatesTheOldOne(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	repoID := seedLibyearRotationRepo(t, store, ctx, "replace-whole")

	if err := store.ReplaceRepoLibyear(ctx, repoID, []*model.LibyearRow{libyearRow("a", "1.0.0"), libyearRow("b", "2.0.0")}, nil, model.InventoryCoverage{}); err != nil {
		t.Fatalf("first replace: %v", err)
	}
	first := currentLibyearNames(t, store, ctx, repoID)
	if len(first) != 2 || libyearHistoryRows(t, store, ctx, repoID) != 0 {
		t.Fatalf("after the first replace: current %v, history %d; want a and b, and nothing rotated", first, libyearHistoryRows(t, store, ctx, repoID))
	}

	// b stays, a goes, c arrives.
	if err := store.ReplaceRepoLibyear(ctx, repoID, []*model.LibyearRow{libyearRow("b", "2.1.0"), libyearRow("c", "3.0.0")}, nil, model.InventoryCoverage{}); err != nil {
		t.Fatalf("second replace: %v", err)
	}
	second := currentLibyearNames(t, store, ctx, repoID)
	if len(second) != 2 || second["b"] == 0 || second["c"] == 0 {
		t.Fatalf("after the second replace: current %v; want exactly b and c", second)
	}
	// A row that stays is a NEW row with a new key: the history table copies
	// the primary key, so a row kept in place would fail the next rotation.
	if second["b"] == first["b"] {
		t.Error("the row that stayed kept its key; the next rotation would collide in the history table")
	}
	if got := libyearHistoryRows(t, store, ctx, repoID); got != 2 {
		t.Errorf("history rows = %d, want the 2 rows of the first inventory", got)
	}
	var historyKey int64
	if err := store.pool.QueryRow(ctx, `
		SELECT repo_deps_libyear_id FROM aveloxis_data.repo_deps_libyear_history
		WHERE repo_id = $1 AND name = 'a'`, repoID).Scan(&historyKey); err != nil || historyKey != first["a"] {
		t.Errorf("the rotated row's key = %d (%v), want the key it had, %d", historyKey, err, first["a"])
	}

	// A third replace over rows that were themselves written by a replace.
	if err := store.ReplaceRepoLibyear(ctx, repoID, []*model.LibyearRow{libyearRow("b", "2.2.0")}, nil, model.InventoryCoverage{}); err != nil {
		t.Fatalf("third replace: %v", err)
	}
	if got := libyearHistoryRows(t, store, ctx, repoID); got != 4 {
		t.Errorf("history rows after three replaces = %d, want 4", got)
	}
}

// An empty inventory is a whole inventory: the repository declares nothing
// now, and the previous rows are history.
func TestReplaceRepoLibyearWithNoRowsEmptiesTheInventory(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	repoID := seedLibyearRotationRepo(t, store, ctx, "replace-empty")
	if err := store.ReplaceRepoLibyear(ctx, repoID, []*model.LibyearRow{libyearRow("a", "1.0.0")}, nil, model.InventoryCoverage{}); err != nil {
		t.Fatal(err)
	}
	if err := store.ReplaceRepoLibyear(ctx, repoID, nil, nil, model.InventoryCoverage{}); err != nil {
		t.Fatalf("replace with no rows: %v", err)
	}
	if got := currentLibyearNames(t, store, ctx, repoID); len(got) != 0 {
		t.Errorf("current rows = %v, want none", got)
	}
	if got := libyearHistoryRows(t, store, ctx, repoID); got != 1 {
		t.Errorf("history rows = %d, want 1", got)
	}
}

// TestReplaceRepoLibyearIsAllOrNothing is the red test for the whole replace:
// when any row cannot be written, the previous inventory stands whole and
// nothing was rotated. (The collector validates rows before it calls this, so
// in production the failure is a database error; a NUL byte is the cheapest
// way to make PostgreSQL refuse one row of several.)
func TestReplaceRepoLibyearIsAllOrNothing(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	repoID := seedLibyearRotationRepo(t, store, ctx, "replace-atomic")
	if err := store.ReplaceRepoLibyear(ctx, repoID, []*model.LibyearRow{libyearRow("a", "1.0.0"), libyearRow("b", "2.0.0")}, nil, model.InventoryCoverage{}); err != nil {
		t.Fatal(err)
	}
	before := currentLibyearNames(t, store, ctx, repoID)

	bad := libyearRow("d", "4.0.0")
	bad.License = "MIT\x00"
	err := store.ReplaceRepoLibyear(ctx, repoID, []*model.LibyearRow{libyearRow("c", "3.0.0"), bad, libyearRow("e", "5.0.0")}, nil, model.InventoryCoverage{})
	if err == nil {
		t.Fatal("a row PostgreSQL refuses must fail the replace")
	}
	after := currentLibyearNames(t, store, ctx, repoID)
	if len(after) != 2 || after["a"] != before["a"] || after["b"] != before["b"] {
		t.Errorf("after a failed replace: current %v, want the previous inventory untouched %v", after, before)
	}
	if got := libyearHistoryRows(t, store, ctx, repoID); got != 0 {
		t.Errorf("a failed replace rotated %d rows into history; it must rotate none", got)
	}
}

// The unknown libyear is stored as NULL, never as 0 (v0.29.57), through the
// replace as through the single insert.
func TestReplaceRepoLibyearStoresAnUnknownLibyearAsNull(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	repoID := seedLibyearRotationRepo(t, store, ctx, "replace-null")
	known := libyearRow("known", "1.0.0")
	known.Libyear = 1.5
	unknown := libyearRow("unknown", "1.0.0")
	unknown.NoLibyear = true
	if err := store.ReplaceRepoLibyear(ctx, repoID, []*model.LibyearRow{known, unknown}, nil, model.InventoryCoverage{}); err != nil {
		t.Fatal(err)
	}
	var knownIsNull, unknownIsNull bool
	var value float64
	if err := store.pool.QueryRow(ctx, `
		SELECT (SELECT libyear IS NULL FROM aveloxis_data.repo_deps_libyear WHERE repo_id = $1 AND name = 'known'),
		       (SELECT libyear FROM aveloxis_data.repo_deps_libyear WHERE repo_id = $1 AND name = 'known'),
		       (SELECT libyear IS NULL FROM aveloxis_data.repo_deps_libyear WHERE repo_id = $1 AND name = 'unknown')`,
		repoID).Scan(&knownIsNull, &value, &unknownIsNull); err != nil {
		t.Fatal(err)
	}
	if knownIsNull || value != 1.5 || !unknownIsNull {
		t.Errorf("known libyear NULL=%v value=%v, unknown NULL=%v; want 1.5 and NULL", knownIsNull, value, unknownIsNull)
	}
}

// More rows than one batch holds still go in one transaction.
func TestReplaceRepoLibyearAcrossBatches(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	repoID := seedLibyearRotationRepo(t, store, ctx, "replace-batches")
	n := libyearReplaceBatchSize*2 + 7
	rows := make([]*model.LibyearRow, 0, n)
	for i := range n {
		rows = append(rows, libyearRow("pkg-"+string(rune('a'+i%26))+"-"+itoa(i), "1.0.0"))
	}
	bad := libyearRow("last", "1.0.0")
	bad.Requirement = "x\x00"
	if err := store.ReplaceRepoLibyear(ctx, repoID, append(append([]*model.LibyearRow{}, rows...), bad), nil, model.InventoryCoverage{}); err == nil {
		t.Fatal("a bad row in the LAST batch must fail the whole replace")
	}
	if got := currentLibyearNames(t, store, ctx, repoID); len(got) != 0 {
		t.Errorf("%d rows of the earlier batches survived a failure in the last one", len(got))
	}
	if err := store.ReplaceRepoLibyear(ctx, repoID, rows, nil, model.InventoryCoverage{}); err != nil {
		t.Fatal(err)
	}
	if got := currentLibyearNames(t, store, ctx, repoID); len(got) != n {
		t.Errorf("current rows = %d, want %d", len(got), n)
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for ; i > 0; i /= 10 {
		b = append([]byte{byte('0' + i%10)}, b...)
	}
	return string(b)
}

// A row the registry did not answer for, with no licence, is outside the
// licence breakdown's universe (0.29.162): nothing was asked about its
// licence, so "Unknown" would be a claim about a question never put. Before
// that release such a dependency had no row, so the bucket does not grow.
func TestLicenseBreakdownLeavesOutRowsTheRegistryDidNotAnswerFor(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	repoID := seedLibyearRotationRepo(t, store, ctx, "license-universe")
	answered := libyearRow("answered", "1.0.0") // MIT
	answered.Status = model.LibyearResolved
	answeredNoLicense := libyearRow("answered-no-license", "1.0.0")
	answeredNoLicense.License, answeredNoLicense.Status = "", model.LibyearResolved
	legacy := libyearRow("legacy-no-license", "1.0.0") // no status recorded
	legacy.License = ""
	var unanswered []*model.LibyearRow
	for _, status := range []string{model.LibyearNotOnRegistry, model.LibyearCannotBeAsked, model.LibyearLookupFailed} {
		r := libyearRow("unanswered-"+status, "1.0.0")
		r.License, r.Status, r.NoLibyear = "", status, true
		unanswered = append(unanswered, r)
	}
	// A Go row whose lookup failed but whose licence came from the forge
	// stays in: it HAS a licence.
	goRow := libyearRow("go-with-forge-license", "1.0.0")
	goRow.License, goRow.Status, goRow.NoLibyear = "Apache-2.0", model.LibyearLookupFailed, true
	rows := append([]*model.LibyearRow{answered, answeredNoLicense, legacy, goRow}, unanswered...)
	if err := store.ReplaceRepoLibyear(ctx, repoID, rows, nil, model.InventoryCoverage{}); err != nil {
		t.Fatal(err)
	}
	counts, err := store.GetRepoLicensesScoped(ctx, repoID, false)
	if err != nil {
		t.Fatal(err)
	}
	total, unknown := 0, 0
	for _, c := range counts {
		total += c.Count
		if c.License == "Unknown" {
			unknown = c.Count
		}
	}
	if total != 4 || unknown != 2 {
		t.Errorf("licence breakdown = %+v: %d rows, %d unknown; want 4 rows (the 3 unanswered rows with no licence left out) and 2 unknown", counts, total, unknown)
	}
	// The stored data_source says who answered.
	var fromManifest int
	if err := store.pool.QueryRow(ctx, `SELECT COUNT(*) FROM aveloxis_data.repo_deps_libyear WHERE repo_id = $1 AND data_source = 'manifest'`, repoID).Scan(&fromManifest); err != nil {
		t.Fatal(err)
	}
	if fromManifest != 4 {
		t.Errorf("%d rows say data_source 'manifest'; want the 4 the registry did not answer for", fromManifest)
	}
}
