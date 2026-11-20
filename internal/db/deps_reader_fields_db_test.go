// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

// What the reader sees of a dependency row (summary/70, 4.8; 0.29.198): an
// unknown libyear is null with the reason, never 0; the scope is on every
// row; the licences table says how many rows it leaves out; the snapshot
// says how many unknowns it skipped.
func TestDependencyRowsCarryStatusScopeAndUnknowns(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)
	repoID := seedRepoForDeps(t, store, ctx, "_avdepfields", "fixture")
	seed := func(name, typ, mgr string, libyear any, status, license, source string) {
		t.Helper()
		if _, err := store.pool.Exec(ctx, `
			INSERT INTO aveloxis_data.repo_deps_libyear
				(repo_id, name, requirement, type, package_manager, current_version, latest_version, libyear, license, libyear_status, version_source)
			VALUES ($1, $2, '*', $3, $4, '1.0.0', '1.0.0', $5, $6, NULLIF($7, ''), NULLIF($8, ''))`,
			repoID, name, typ, mgr, libyear, license, status, source); err != nil {
			t.Fatal(err)
		}
	}
	seed("resolved-runtime", "runtime", "npm", 1.5, model.LibyearResolved, "MIT", "locked")
	seed("zero-is-a-number", "", "npm", 0.0, model.LibyearResolved, "MIT", "")
	seed("no-version-runtime", "runtime", "npm", nil, model.LibyearNoVersion, "MIT", "")
	seed("not-on-registry", "runtime", "npm", nil, model.LibyearNotOnRegistry, "", "")
	seed("lookup-failed-dev", "dev", "npm", nil, model.LibyearLookupFailed, "", "")
	seed("cannot-be-asked-with-licence", "runtime", "go", nil, model.LibyearCannotBeAsked, "Apache-2.0", "")
	seed("no-date-test", "test", "pypi", nil, model.LibyearNoReleaseDate, "BSD-3-Clause", "managed")
	seed("an-action", "build", "githubactions", nil, model.LibyearNoTimeline, "", "")
	seed("legacy-no-status", "runtime", "npm", nil, "", "", "")
	// Not a state the collector writes, and still never counted as left
	// out: an Action has no licence data by nature.
	seed("an-action-with-a-failure-status", "build", "githubactions", nil, model.LibyearLookupFailed, "", "")

	rows, err := store.Deps(ctx, repoID)
	if err != nil {
		t.Fatal(err)
	}
	by := map[string]DepRow{}
	for _, r := range rows {
		by[r.Name] = r
	}
	if len(by) != 10 {
		t.Fatalf("%d rows; the plain list is every row, whatever its scope or status", len(by))
	}
	if r := by["resolved-runtime"]; r.Libyear == nil || *r.Libyear != 1.5 || r.LibyearStatus != model.LibyearResolved || r.Scope != "runtime" || r.VersionSource != "locked" {
		t.Errorf("a resolved row = %+v", r)
	}
	// A libyear of 0 is a number (up to date), and is sent as 0, not null.
	if r := by["zero-is-a-number"]; r.Libyear == nil || *r.Libyear != 0 || r.Scope != "runtime" {
		t.Errorf("a row at the latest version = %+v; want libyear 0 and scope runtime for an empty stored type", r)
	}
	for name, want := range map[string][2]string{
		"no-version-runtime": {model.LibyearNoVersion, "runtime"},
		"not-on-registry":    {model.LibyearNotOnRegistry, "runtime"},
		"lookup-failed-dev":  {model.LibyearLookupFailed, "dev"},
		"no-date-test":       {model.LibyearNoReleaseDate, "test"},
		"an-action":          {model.LibyearNoTimeline, "build"},
		"legacy-no-status":   {"", "runtime"},
	} {
		r := by[name]
		if r.Libyear != nil || r.LibyearStatus != want[0] || r.Scope != want[1] {
			t.Errorf("%s: libyear %v status %q scope %q; want null, %q, %q", name, r.Libyear, r.LibyearStatus, r.Scope, want[0], want[1])
		}
	}
	if by["no-date-test"].VersionSource != "managed" {
		t.Errorf("version_source = %q", by["no-date-test"].VersionSource)
	}

	// The drill-down sends the same fields.
	filtered, err := store.DepsFiltered(ctx, repoID, "", false)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range filtered {
		if w := by[r.Name]; r.LibyearStatus != w.LibyearStatus || r.Scope != w.Scope || (r.Libyear == nil) != (w.Libyear == nil) {
			t.Errorf("drill-down row %s = %+v, the plain list's = %+v", r.Name, r, w)
		}
	}

	// Left out of the licences table: the registry did not answer AND no
	// licence. Not the Action (never counted), not the Go row with a
	// licence from the forge, not the row with no status.
	all, err := store.CountRepoLicenseRowsLeftOut(ctx, repoID, false)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := store.CountRepoLicenseRowsLeftOut(ctx, repoID, true)
	if err != nil {
		t.Fatal(err)
	}
	if all != 2 || runtime != 1 {
		t.Errorf("left out: %d in every scope, %d in runtime; want 2 (not-on-registry, lookup-failed-dev) and 1", all, runtime)
	}
	// The table and the count add up to the rows that are not Actions.
	inTable := 0
	lics, err := store.GetRepoLicensesScoped(ctx, repoID, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, l := range lics {
		inTable += l.Count
	}
	if inTable+all != 8 {
		t.Errorf("the table holds %d and %d are left out; together they must be the 8 rows that are not Actions", inTable, all)
	}

	// The snapshot counts over rows that have a libyear, and says how many
	// it skipped, in its runtime and development split. The Action is not
	// an unknown: there is nothing to know.
	snap, err := store.UpstreamDependenciesSnapshot(ctx, []int64{repoID})
	if err != nil {
		t.Fatal(err)
	}
	if snap.Value != 2 || snap.Detail["libyear_unknown"] != 4 || snap.Detail["dev_libyear_unknown"] != 3 {
		t.Errorf("snapshot: value %v, detail %v; want 2 runtime rows with a libyear, 4 runtime unknowns, 3 development unknowns", snap.Value, snap.Detail)
	}
}

// Every status but resolved has one wording of its own.
func TestLibyearStatusWordings(t *testing.T) {
	seen := map[string]bool{}
	for _, s := range []string{model.LibyearNoVersion, model.LibyearNoReleaseDate, model.LibyearNotOnRegistry,
		model.LibyearCannotBeAsked, model.LibyearLookupFailed, model.LibyearNoTimeline} {
		w := model.LibyearStatusWording(s)
		if w == "" || seen[w] {
			t.Errorf("status %q has no wording of its own (%q)", s, w)
		}
		seen[w] = true
	}
	if model.LibyearStatusWording(model.LibyearNoTimeline) != "not applicable" {
		t.Error("no_timeline is the one shown as not applicable")
	}
	if model.LibyearStatusWording(model.LibyearResolved) != "" || len(seen) != 6 {
		t.Error("resolved has a number, not a wording; six statuses have one")
	}
	if !model.LibyearIsUnknown(false, model.LibyearLookupFailed) || !model.LibyearIsUnknown(false, "") ||
		model.LibyearIsUnknown(false, model.LibyearNoTimeline) || model.LibyearIsUnknown(true, model.LibyearResolved) {
		t.Error("LibyearIsUnknown: no libyear and a status other than no_timeline")
	}
}
