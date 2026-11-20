// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"reflect"
	"sort"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

// A manifest that cannot be read or parsed contributes no dependencies, and a
// project that dropped every dependency looks the same. Its previous rows are
// CARRIED by manifest_path for as long as the file exists and fails
// (summary/70, 4.7), inside the replace transaction, as new rows.

func rowAt(name, version, manifestPath string) *model.LibyearRow {
	r := libyearRow(name, version)
	r.ManifestPath = manifestPath
	return r
}

func inventoryByPath(t *testing.T, store *PostgresStore, ctx context.Context, repoID int64) map[string][]string {
	t.Helper()
	rows, err := store.pool.Query(ctx,
		`SELECT COALESCE(manifest_path, ''), name FROM aveloxis_data.repo_deps_libyear WHERE repo_id = $1`, repoID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[string][]string{}
	for rows.Next() {
		var path, name string
		if err := rows.Scan(&path, &name); err != nil {
			t.Fatal(err)
		}
		out[path] = append(out[path], name)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	for k := range out {
		sort.Strings(out[k])
	}
	return out
}

func TestAFailedManifestsRowsAreCarried(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	repoID := seedLibyearRotationRepo(t, store, ctx, "carry")

	// Two manifests parse.
	if err := store.ReplaceRepoLibyear(ctx, repoID, []*model.LibyearRow{
		rowAt("a", "1.0.0", "package.json"), rowAt("b", "1.0.0", "package.json"), rowAt("c", "1.0.0", "svc/package.json"),
	}, nil, model.InventoryCoverage{ManifestsFound: 2, ManifestsParsed: 2, DependenciesDeclared: 3}); err != nil {
		t.Fatal(err)
	}
	keys := currentLibyearNames(t, store, ctx, repoID)

	// Next analysis: package.json fails; svc/package.json now declares d.
	failing := model.InventoryCoverage{ManifestsFound: 2, ManifestsParsed: 1, DependenciesDeclared: 1,
		ManifestsFailed: []model.ManifestFailure{{Path: "package.json", Kind: "package.json", Reason: model.ManifestFailedParse}}}
	if err := store.ReplaceRepoLibyear(ctx, repoID, []*model.LibyearRow{rowAt("d", "1.0.0", "svc/package.json")}, nil, failing); err != nil {
		t.Fatal(err)
	}
	want := map[string][]string{"package.json": {"a", "b"}, "svc/package.json": {"d"}}
	if got := inventoryByPath(t, store, ctx, repoID); !reflect.DeepEqual(got, want) {
		t.Fatalf("inventory = %v, want %v: the failed manifest's rows carried, the parsed one's replaced", got, want)
	}
	// Carried rows are NEW rows: the history table copies the primary key.
	after := currentLibyearNames(t, store, ctx, repoID)
	if after["a"] == keys["a"] || after["b"] == keys["b"] {
		t.Error("a carried row kept its key; the next rotation would collide in the history table")
	}
	if got := libyearHistoryRows(t, store, ctx, repoID); got != 3 {
		t.Errorf("history rows = %d, want the previous inventory's 3", got)
	}
	cov, _, err := store.GetRepoDependencyCoverage(ctx, repoID)
	if err != nil {
		t.Fatal(err)
	}
	if len(cov.Inventory.ManifestsFailed) != 1 || cov.Inventory.ManifestsFailed[0].RowsCarried != 2 {
		t.Errorf("coverage = %+v; want the failed path recorded with 2 rows carried", cov.Inventory.ManifestsFailed)
	}

	// A second failing cycle: the replace succeeds again, and the history
	// table gains the carried rows a second time.
	if err := store.ReplaceRepoLibyear(ctx, repoID, []*model.LibyearRow{rowAt("d", "1.0.0", "svc/package.json")}, nil, failing); err != nil {
		t.Fatalf("second failing cycle: %v", err)
	}
	if got := inventoryByPath(t, store, ctx, repoID); !reflect.DeepEqual(got, want) {
		t.Errorf("after a second failing cycle: %v, want %v", got, want)
	}
	if got := libyearHistoryRows(t, store, ctx, repoID); got != 6 {
		t.Errorf("history rows = %d, want 6 (3, then the 2 carried and d)", got)
	}

	// The manifest parses again: its carried rows give way to what it says.
	if err := store.ReplaceRepoLibyear(ctx, repoID, []*model.LibyearRow{rowAt("a", "2.0.0", "package.json"), rowAt("d", "1.0.0", "svc/package.json")}, nil,
		model.InventoryCoverage{ManifestsFound: 2, ManifestsParsed: 2, DependenciesDeclared: 2}); err != nil {
		t.Fatal(err)
	}
	if got, want := inventoryByPath(t, store, ctx, repoID), (map[string][]string{"package.json": {"a"}, "svc/package.json": {"d"}}); !reflect.DeepEqual(got, want) {
		t.Errorf("after the manifest parses again: %v, want %v", got, want)
	}
}

// The two accepted limits: a row written before this release has no
// manifest_path and cannot be carried; a file that never parsed has nothing
// to carry.
func TestWhatCannotBeCarried(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	repoID := seedLibyearRotationRepo(t, store, ctx, "nocarry")
	// A legacy row: no manifest_path.
	if err := store.ReplaceRepoLibyear(ctx, repoID, []*model.LibyearRow{libyearRow("legacy", "1.0.0")}, nil, model.InventoryCoverage{}); err != nil {
		t.Fatal(err)
	}
	failing := model.InventoryCoverage{ManifestsFound: 2, ManifestsFailed: []model.ManifestFailure{
		{Path: "package.json", Kind: "package.json", Reason: model.ManifestFailedParse},
		{Path: "", Kind: "package.json", Reason: model.ManifestFailedRead}, // an empty path never matches the legacy rows
	}}
	if err := store.ReplaceRepoLibyear(ctx, repoID, nil, nil, failing); err != nil {
		t.Fatal(err)
	}
	if got := inventoryByPath(t, store, ctx, repoID); len(got) != 0 {
		t.Errorf("inventory = %v; a row with no manifest_path cannot be carried, and a file that never parsed has nothing", got)
	}
	cov, _, _ := store.GetRepoDependencyCoverage(ctx, repoID)
	for _, f := range cov.Inventory.ManifestsFailed {
		if f.RowsCarried != 0 {
			t.Errorf("%+v: nothing could be carried", f)
		}
	}
}
