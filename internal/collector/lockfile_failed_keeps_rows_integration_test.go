// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

// A lockfile that fails keeps its previous answer (summary/70, 4.3). The
// lockfile phase replaces all of a repository's lockfile rows at once, so a
// lockfile that stopped parsing used to lose its rows: every transitive
// target from it vanished, its findings closed as if fixed, and its direct
// dependencies fell back from the locked version to the declared one.

type lockRow struct {
	Eco, Name, Version, Path, Namespace string
	Direct                              bool
}

func lockfileRows(t *testing.T, f *wholeReplaceFixture) []lockRow {
	t.Helper()
	rows, err := f.store.Pool().Query(context.Background(), `
		SELECT ecosystem, package_name, resolved_version, lockfile_path, purl_namespace, direct
		FROM aveloxis_data.repo_lockfile_packages WHERE repo_id = $1`, f.repoID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []lockRow
	for rows.Next() {
		var r lockRow
		if err := rows.Scan(&r.Eco, &r.Name, &r.Version, &r.Path, &r.Namespace, &r.Direct); err != nil {
			t.Fatal(err)
		}
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path+out[i].Name < out[j].Path+out[j].Name })
	return out
}

func lockFixture(t *testing.T, name string) *wholeReplaceFixture {
	t.Helper()
	f := newWholeReplaceFixture(t, name, func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, npmAnswer) })
	t.Cleanup(func() {
		for _, tbl := range []string{"repo_lockfile_edges", "repo_lockfile_packages", "repo_lockfiles"} {
			_, _ = f.store.Pool().Exec(context.Background(), "DELETE FROM aveloxis_data."+tbl+" WHERE repo_id = $1", f.repoID)
		}
	})
	f.ac.TransitiveLockfiles = true
	return f
}

func (f *wholeReplaceFixture) write(t *testing.T, rel, body string) {
	t.Helper()
	p := filepath.Join(f.dir, rel)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func (f *wholeReplaceFixture) lockPhase(t *testing.T) error {
	t.Helper()
	return f.ac.scanLockfiles(context.Background(), f.repoID, f.dir, collectLockfiles(f.dir, f.ac.logger), &AnalysisResult{})
}

const (
	goodNpmLock = `{"lockfileVersion": 3, "packages": {
		"": {"dependencies": {"express": "^4.0.0"}},
		"node_modules/express": {"version": "4.18.2", "dependencies": {"body-parser": "1.20.1"}},
		"node_modules/body-parser": {"version": "1.20.1"}}}`
	goodSwiftLock = `{"pins": [{"identity": "alamofire", "kind": "remoteSourceControl",
		"location": "https://github.com/Alamofire/Alamofire.git", "state": {"version": "5.8.0"}}], "version": 2}`
)

func TestAFailedLockfileKeepsItsPreviousRows(t *testing.T) {
	f := lockFixture(t, "lock-keep")
	ctx := context.Background()
	if err := f.store.ReplaceRepoLibyear(ctx, f.repoID, []*model.LibyearRow{
		{Name: "express", PackageManager: "npm", CurrentVersion: "4.0.0", Requirement: "^4.0.0", Purl: "pkg:npm/express@4.0.0"},
	}, nil, model.InventoryCoverage{}); err != nil {
		t.Fatal(err)
	}
	f.write(t, "package-lock.json", goodNpmLock)
	f.write(t, "ios/Package.resolved", goodSwiftLock)
	if err := f.lockPhase(t); err != nil {
		t.Fatal(err)
	}
	before := lockfileRows(t, f)
	if len(before) != 3 {
		t.Fatalf("first scan stored %+v; want express, body-parser and the Swift pin", before)
	}
	var swift lockRow
	for _, r := range before {
		if r.Eco == "swiftpm" {
			swift = r
		}
	}
	if swift.Namespace != "github.com/Alamofire" {
		t.Fatalf("the Swift pin's namespace is %q; the fixture is wrong", swift.Namespace)
	}
	edgesBefore := f.count(t, "repo_lockfile_edges")
	if edgesBefore == 0 {
		t.Fatal("the fixture stored no edge")
	}

	// Both lockfiles stop parsing.
	f.write(t, "package-lock.json", `{"lockfileVersion": 3, "packages": {`)
	f.write(t, "ios/Package.resolved", `not json`)
	if err := f.lockPhase(t); err != nil {
		t.Fatal(err)
	}
	after := lockfileRows(t, f)
	if len(after) != len(before) {
		t.Fatalf("after the lockfiles failed: %+v\nwant the previous rows kept: %+v", after, before)
	}
	for i := range before {
		if after[i] != before[i] {
			t.Errorf("row %d changed: %+v, was %+v (a Swift pin re-inserted without its namespace changes its purl)", i, after[i], before[i])
		}
	}
	if n := f.count(t, "repo_lockfile_edges"); n != edgesBefore {
		t.Errorf("%d edges after, %d before", n, edgesBefore)
	}
	if n := f.count(t, "repo_lockfiles"); n != 2 {
		t.Errorf("%d lockfile inventory rows, want the two kept", n)
	}
	// The coverage record says so: which lockfiles failed, and how many of
	// each one's rows were kept.
	cov, _, err := f.store.GetRepoDependencyCoverage(ctx, f.repoID)
	if err != nil {
		t.Fatal(err)
	}
	carried := map[string]int{}
	for _, lf := range cov.LockfilesFailed {
		carried[lf.Path] = lf.RowsCarried
		if lf.Reason != model.ManifestFailedParse {
			t.Errorf("%s: reason %q, want parse", lf.Path, lf.Reason)
		}
	}
	if len(carried) != 2 || carried["package-lock.json"] != 2 || carried[filepath.Join("ios", "Package.resolved")] != 1 {
		t.Errorf("coverage's failed lockfiles = %+v; want both, with 2 and 1 rows kept", cov.LockfilesFailed)
	}

	// The carried rows take their direct flag from THIS cycle's declared
	// set: the declaration of express is removed, so its carried row is no
	// longer a direct resolution (an old flag would leave it targeted as
	// one, by nothing).
	if err := f.store.ReplaceRepoLibyear(ctx, f.repoID, nil, nil, model.InventoryCoverage{}); err != nil {
		t.Fatal(err)
	}
	if err := f.lockPhase(t); err != nil {
		t.Fatal(err)
	}
	for _, r := range lockfileRows(t, f) {
		if r.Name == "express" && r.Direct {
			t.Error("the carried express row is still flagged direct after its declaration was removed")
		}
	}

	// It is kept for as long as the file exists and fails. Deleted, its
	// rows go; a lockfile that parses again is read fresh.
	if err := os.Remove(filepath.Join(f.dir, "ios/Package.resolved")); err != nil {
		t.Fatal(err)
	}
	f.write(t, "package-lock.json", `{"lockfileVersion": 3, "packages": {"": {}, "node_modules/only": {"version": "1.0.0"}}}`)
	if err := f.lockPhase(t); err != nil {
		t.Fatal(err)
	}
	final := lockfileRows(t, f)
	if len(final) != 1 || final[0].Name != "only" {
		t.Errorf("after one lockfile was deleted and the other repaired: %+v; want only the repaired one's entry", final)
	}
	if cov, _, err := f.store.GetRepoDependencyCoverage(ctx, f.repoID); err != nil || len(cov.LockfilesFailed) != 0 {
		t.Errorf("coverage still lists failed lockfiles %+v (err %v) after none failed", cov.LockfilesFailed, err)
	}
}

func (f *wholeReplaceFixture) count(t *testing.T, table string) int {
	t.Helper()
	var n int
	if err := f.store.Pool().QueryRow(context.Background(), "SELECT COUNT(*) FROM aveloxis_data."+table+" WHERE repo_id = $1", f.repoID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// A lockfile that has never parsed has nothing to keep, and is reported.
func TestALockfileThatNeverParsedKeepsNothing(t *testing.T) {
	f := lockFixture(t, "lock-never")
	f.write(t, "package-lock.json", `{"lockfileVersion": 3, "packages": {`)
	set := collectLockfiles(f.dir, f.ac.logger)
	if len(set.Failed) != 1 || set.Failed[0].Path != "package-lock.json" || set.Failed[0].Kind != "package-lock.json" || set.Failed[0].Reason != model.ManifestFailedParse {
		t.Fatalf("failed = %+v; want the one lockfile, as a parse failure", set.Failed)
	}
	if err := f.ac.scanLockfiles(context.Background(), f.repoID, f.dir, set, &AnalysisResult{}); err != nil {
		t.Fatal(err)
	}
	if rows := lockfileRows(t, f); len(rows) != 0 {
		t.Errorf("rows %+v; want none", rows)
	}
}

// A walk that cannot read the clone's root knows nothing about the
// repository's lockfiles. The snapshot is not replaced with "none".
func TestALockfileWalkFailingAtTheRootReplacesNothing(t *testing.T) {
	f := lockFixture(t, "lock-root")
	f.write(t, "package-lock.json", goodNpmLock)
	if err := f.lockPhase(t); err != nil {
		t.Fatal(err)
	}
	before := len(lockfileRows(t, f))
	gone := filepath.Join(f.dir, "does-not-exist")
	set := collectLockfiles(gone, f.ac.logger)
	if set.WalkErr == nil {
		t.Fatal("a walk of a missing root reported no error")
	}
	if err := f.ac.scanLockfiles(context.Background(), f.repoID, gone, set, &AnalysisResult{}); err == nil {
		t.Error("the lockfile phase must fail when its walk could not read the root")
	}
	if n := len(lockfileRows(t, f)); n != before {
		t.Errorf("%d rows after a failed walk, %d before: the snapshot was replaced", n, before)
	}
}
