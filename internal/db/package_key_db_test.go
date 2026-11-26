// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/pkgid"
)

// The package key is stored on the inventory and the lockfile packages
// (summary/70, 4.2): the one fold pkgid.Key makes, written on insert, so a
// reader can find "every repository holding this package" by an indexed
// equality where today each reader folds names again in SQL.

func TestInventoryRowsCarryThePackageKey(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	repoID := seedLibyearRotationRepo(t, store, ctx, "package-key")
	rows := []*model.LibyearRow{
		{Name: "Flask_SQLAlchemy", PackageManager: "pypi", CurrentVersion: "3.0", Purl: "pkg:pypi/flask-sqlalchemy@3.0", ManifestPath: "requirements.txt"},
		{Name: "Rails", PackageManager: "rubygems", CurrentVersion: "7.0", Purl: "pkg:gem/Rails@7.0", ManifestPath: "Gemfile"},
		{Name: "org.apache:Commons-Lang3", PackageManager: "maven", CurrentVersion: "3.12", Purl: "pkg:maven/org.apache/Commons-Lang3@3.12", ManifestPath: "pom.xml"},
	}
	if err := store.ReplaceRepoLibyear(ctx, repoID, rows, nil, model.InventoryCoverage{}); err != nil {
		t.Fatal(err)
	}
	keys := func(table string) map[string]string {
		t.Helper()
		out := map[string]string{}
		r, err := store.pool.Query(ctx, `SELECT name, COALESCE(package_key, '<NULL>') FROM aveloxis_data.`+table+` WHERE repo_id = $1`, repoID)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		for r.Next() {
			var n, k string
			if err := r.Scan(&n, &k); err != nil {
				t.Fatal(err)
			}
			out[n] = k
		}
		return out
	}
	want := map[string]string{
		"Flask_SQLAlchemy":         "pypi|flask-sqlalchemy",
		"Rails":                    "gem|rails",
		"org.apache:Commons-Lang3": "maven|org.apache:commons-lang3",
	}
	got := keys("repo_deps_libyear")
	for n, w := range want {
		if got[n] != w {
			t.Errorf("%s: package_key %q, want %q", n, got[n], w)
		}
		// The stored key is pkgid's, not a second spelling of the fold.
		for _, r := range rows {
			if r.Name == n && got[n] != pkgid.Key(r.PackageManager, r.Name) {
				t.Errorf("%s: stored %q, pkgid.Key gives %q", n, got[n], pkgid.Key(r.PackageManager, r.Name))
			}
		}
	}

	// A carried row (its manifest failed this cycle) keeps its key, and the
	// rotated copy in the history table has it.
	cov := model.InventoryCoverage{ManifestsFailed: []model.ManifestFailure{{Path: "Gemfile", Kind: "gemfile", Reason: "unreadable"}}}
	if err := store.ReplaceRepoLibyear(ctx, repoID, rows[:1], nil, cov); err != nil {
		t.Fatal(err)
	}
	if got := keys("repo_deps_libyear"); got["Rails"] != "gem|rails" {
		t.Errorf("the carried row's package_key is %q, want gem|rails", got["Rails"])
	}
	if got := keys("repo_deps_libyear_history"); got["Flask_SQLAlchemy"] != "pypi|flask-sqlalchemy" {
		t.Errorf("the history copy's package_key is %q", got["Flask_SQLAlchemy"])
	}
}

func TestLockfilePackagesCarryThePackageKey(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	repoID := seedLibyearRotationRepo(t, store, ctx, "lockfile-key")
	t.Cleanup(func() {
		for _, tbl := range []string{"repo_lockfile_edges", "repo_lockfile_packages", "repo_lockfiles"} {
			cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_data.`+tbl+` WHERE repo_id = $1`, repoID)
		}
	})
	if err := store.ReplaceRepoLockfileSnapshot(ctx, repoID, nil, []*model.RepoLockfilePackage{
		{Ecosystem: "swiftpm", PackageName: "Alamofire", ResolvedVersion: "5.0", LockfilePath: "Package.resolved", Namespace: "github.com/Alamofire"},
		{Ecosystem: "pypi", PackageName: "ruamel.yaml", ResolvedVersion: "0.17", LockfilePath: "poetry.lock"},
	}, nil, nil); err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	r, err := store.pool.Query(ctx, `SELECT package_name, COALESCE(package_key, '<NULL>') FROM aveloxis_data.repo_lockfile_packages WHERE repo_id = $1`, repoID)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	for r.Next() {
		var n, k string
		if err := r.Scan(&n, &k); err != nil {
			t.Fatal(err)
		}
		got[n] = k
	}
	// The key folds the ecosystem (swiftpm -> swift) and, for PyPI, the dot.
	if got["Alamofire"] != "swift|alamofire" || got["ruamel.yaml"] != "pypi|ruamel-yaml" {
		t.Errorf("lockfile package keys = %v", got)
	}
}

// The lookup index on each key exists after a migrate, and is the migrate's
// (SR-2): schema.sql must not create an index on a fleet-scale table.
func TestPackageKeyIndexesAreTheMigrates(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	for _, idx := range []string{"idx_repo_deps_libyear_package_key", "idx_repo_lockfile_packages_package_key"} {
		var n int
		if err := store.pool.QueryRow(ctx, `SELECT COUNT(*) FROM pg_indexes WHERE schemaname = 'aveloxis_data' AND indexname = $1`, idx).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 1 {
			t.Errorf("index %s: found %d, want 1", idx, n)
		}
	}
}
