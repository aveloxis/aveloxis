// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"io"
	"log/slog"
	"reflect"
	"sort"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/pkgid"
	"github.com/aveloxis/aveloxis/internal/scantarget"
	"github.com/aveloxis/aveloxis/internal/versionuse"
)

// The version in use (summary/70, 4.3): where the nearest lockfile resolves
// a declared dependency to exactly one version, the inventory row reports
// that version, where it used to report the lowest the declaration allows.

type invRow struct{ Version, Source, Purl, Requirement string }

func inventoryRows(t *testing.T, f *wholeReplaceFixture) map[string]invRow {
	t.Helper()
	rows, err := f.store.Pool().Query(context.Background(), `
		SELECT COALESCE(manifest_path, '') || ':' || name, current_version, COALESCE(version_source, ''), COALESCE(purl, ''), COALESCE(requirement, '')
		FROM aveloxis_data.repo_deps_libyear WHERE repo_id = $1`, f.repoID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]invRow{}
	for rows.Next() {
		var k string
		var r invRow
		if err := rows.Scan(&k, &r.Version, &r.Source, &r.Purl, &r.Requirement); err != nil {
			t.Fatal(err)
		}
		out[k] = r
	}
	return out
}

// analyse runs the three phases the way AnalyzeRepo orders them: the
// lockfiles are read, then the inventory is built, then the lockfiles are
// stored.
func (f *wholeReplaceFixture) analyse(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	read := f.ac.readClone(ctx, f.repoID, f.dir)
	locks := read.lockfiles()
	if err := f.ac.scanLibyear(ctx, f.repoID, f.dir, read, f.ac.lockfilesInUse(ctx, f.repoID, locks, read.declaredKeys()), &AnalysisResult{}); err != nil {
		t.Fatal(err)
	}
	if err := f.ac.scanLockfiles(ctx, f.repoID, f.dir, locks, &AnalysisResult{}); err != nil {
		t.Fatal(err)
	}
}

func targetPurls(t *testing.T, f *wholeReplaceFixture) []string {
	t.Helper()
	set, err := scantarget.Build(context.Background(), f.store, f.repoID, true)
	if err != nil {
		t.Fatal(err)
	}
	out := append([]string(nil), set.Purls...)
	sort.Strings(out)
	return out
}

func TestTheInventoryRowReportsTheLockedVersion(t *testing.T) {
	f := lockFixture(t, "version-in-use")
	f.write(t, "package.json", `{"dependencies": {"express": "^4.0.0", "lodash": "^4.17.0", "unlocked": "^1.0.0"}}`)
	f.write(t, "package-lock.json", `{"lockfileVersion": 3, "packages": {
		"": {"dependencies": {"express": "^4.0.0", "lodash": "^4.17.0"}},
		"node_modules/express": {"version": "4.18.2"},
		"node_modules/lodash": {"version": "4.17.21"},
		"node_modules/a/node_modules/lodash": {"version": "3.10.1"}}}`)
	f.analyse(t)
	got := inventoryRows(t, f)

	e := got["package.json:express"]
	if e.Version != "4.18.2" || e.Source != versionuse.SourceLocked || e.Purl != "pkg:npm/express@4.18.2" {
		t.Errorf("express = %+v; want the locked version 4.18.2, source locked, and the purl at it", e)
	}
	// The declaration is still what was declared.
	if e.Requirement != "^4.0.0" {
		t.Errorf("express's requirement is %q; the row keeps the declaration", e.Requirement)
	}
	// Several versions: the row keeps the declared one, and says why.
	if l := got["package.json:lodash"]; l.Version != "4.17.0" || l.Source != versionuse.SourceLockedSeveral {
		t.Errorf("lodash = %+v; want the declared 4.17.0 with source locked-several", l)
	}
	// Not in the lockfile: declared, no source.
	if u := got["package.json:unlocked"]; u.Version != "1.0.0" || u.Source != "" {
		t.Errorf("unlocked = %+v; want the declared 1.0.0 and no source", u)
	}
}

// THE constraint of this step: it changes what a row reports and never what
// the scan targets. Pinned as EQUALITY of the target set, not as a superset,
// between the row shape before this release (the declared version, no
// source) and the shape after, over the plan's four fixtures. A changed
// target would close findings across the fleet as if they were fixed.
func TestTheLockedVersionChangesNoScanTarget(t *testing.T) {
	type fixture struct {
		name  string
		files map[string]string
		self  string // a package the repository publishes, if any
	}
	fixtures := []fixture{
		{name: "two applications: one declares and locks, one only locks", files: map[string]string{
			"a/package.json":      `{"dependencies": {"express": "^4.0.0"}}`,
			"a/package-lock.json": `{"lockfileVersion": 3, "packages": {"": {"dependencies": {"express": "^4.0.0"}}, "node_modules/express": {"version": "4.18.2"}}}`,
			"b/package.json":      `{"dependencies": {"left-pad": "^1.0.0"}}`,
			"b/package-lock.json": `{"lockfileVersion": 3, "packages": {"": {"dependencies": {"left-pad": "^1.0.0"}}, "node_modules/left-pad": {"version": "1.3.0"}, "node_modules/express": {"version": "4.17.0"}}}`,
		}},
		{name: "a self-excluded name that has lockfile entries", self: "ownpkg", files: map[string]string{
			"package.json":      `{"name": "ownpkg", "dependencies": {"ownpkg": "^1.0.0", "express": "^4.0.0"}}`,
			"package-lock.json": `{"lockfileVersion": 3, "packages": {"": {"dependencies": {"ownpkg": "^1.0.0", "express": "^4.0.0"}}, "node_modules/ownpkg": {"version": "1.2.0"}, "node_modules/express": {"version": "4.18.2"}}}`,
		}},
		{name: "the nearest lockfile does not hold the name", files: map[string]string{
			"package.json":          `{"dependencies": {"express": "^4.0.0"}}`,
			"package-lock.json":     `{"lockfileVersion": 3, "packages": {"": {"dependencies": {"express": "^4.0.0"}}, "node_modules/express": {"version": "4.18.2"}}}`,
			"app/package.json":      `{"dependencies": {"express": "^4.0.0", "lodash": "^4.0.0"}}`,
			"app/package-lock.json": `{"lockfileVersion": 3, "packages": {"": {"dependencies": {"lodash": "^4.0.0"}}, "node_modules/lodash": {"version": "4.17.21"}}}`,
		}},
		{name: "a locked version the manifest hygiene would blank", files: map[string]string{
			"composer.json": `{"require": {"vendor/pkg": "dev-master", "vendor/other": "^1.0"}}`,
			"composer.lock": `{"packages": [{"name": "vendor/pkg", "version": "dev-master"}, {"name": "vendor/other", "version": "1.4.0"}]}`,
		}},
	}
	for _, fx := range fixtures {
		t.Run(fx.name, func(t *testing.T) {
			f := lockFixture(t, "target-equality")
			for rel, body := range fx.files {
				f.write(t, rel, body)
			}
			f.analyse(t)
			after := targetPurls(t, f)
			if len(after) == 0 {
				t.Fatal("the fixture produced no target")
			}
			locked := 0
			for _, r := range inventoryRows(t, f) {
				if r.Source == versionuse.SourceLocked {
					locked++
				}
			}
			if locked == 0 {
				t.Fatal("the fixture has no row with a locked version: it proves nothing")
			}

			// The same repository as the release before wrote it: every row
			// at its declared version, built by the same code with no
			// lockfile offered, and the same lockfile table.
			ctx := context.Background()
			if err := f.ac.scanLibyear(ctx, f.repoID, f.dir, f.ac.readClone(ctx, f.repoID, f.dir), nil, &AnalysisResult{}); err != nil {
				t.Fatal(err)
			}
			for _, r := range inventoryRows(t, f) {
				if r.Source != "" {
					t.Fatalf("the legacy-shape inventory has a row with a source: %+v", r)
				}
			}
			before := targetPurls(t, f)
			if !reflect.DeepEqual(before, after) {
				t.Errorf("the target set changed.\nbefore (declared versions): %v\nafter  (locked versions):   %v", before, after)
			}
		})
	}
}

// The purl a locked row stores is the purl the scan targets for that
// version: the same bytes, so no finding is re-keyed.
func TestALockedRowsPurlIsTheScansPurl(t *testing.T) {
	for _, c := range []struct{ manager, name, declared, locked string }{
		{"npm", "@scope/pkg", "1.0.0", "1.2.3"},
		{"packagist", "vendor/pkg", "", "dev-master"},
		{"pypi", "Flask_SQLAlchemy", "", "3.0.0"},
		{"maven", "org.apache:commons-lang3", "3.0", "3.12.0"},
		{"npm", "odd", "1.0.0", "1.0.0+build.5"},
		{"rubygems", "nokogiri", "1.0", "1.15.4-x86_64-linux"},
		// Where the two builders differ: the manifest's builder escapes a
		// reserved character in the version, the scan's does not. The row
		// must store what the scan targets, or the finding is re-keyed.
		{"npm", "reserved", "1.0.0", "2.0@beta"},
		{"npm", "hash", "1.0.0", "1.0.0#sha"},
	} {
		dep := model.LibyearDep{Name: c.name, Manager: c.manager, Version: c.declared}
		declaredPurl, ok := manifestPurl(dep)
		if !ok {
			t.Fatalf("%s: no purl", c.name)
		}
		dep.Version, dep.VersionSource = c.locked, versionuse.SourceLocked
		rowPurl, _ := manifestPurl(dep)
		row := &model.LibyearRow{Purl: rowPurl}
		stampVersionSource(dep, row)
		if want := pkgid.WithVersion(declaredPurl, c.locked); row.Purl != want {
			t.Errorf("%s %s: the row stores %q, the scan targets %q", c.manager, c.name, row.Purl, want)
		}
	}
}

// A cached registry answer is shared between repositories; every field that
// belongs to ONE dependency is copied from this caller's, the version's
// source included.
func TestACachedRegistryAnswerCarriesThisDependencysSource(t *testing.T) {
	cache := newTTLCache[*model.LibyearRow](registryCacheTTL)
	resolve := func(_ context.Context, d model.LibyearDep) (*model.LibyearRow, error) {
		return &model.LibyearRow{Name: d.Name, PackageManager: d.Manager, CurrentVersion: d.Version, VersionSource: "leaked from another repository"}, nil
	}
	ctx := withRegistryLogger(context.Background(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	first := model.LibyearDep{Name: "express", Manager: "npm", Version: "4.18.2", VersionSource: versionuse.SourceLocked, ManifestPath: "a/package.json"}
	second := model.LibyearDep{Name: "express", Manager: "npm", Version: "4.18.2", ManifestPath: "b/package.json"}
	a, err := resolveLibyearCached(ctx, cache, first, resolve)
	if err != nil {
		t.Fatal(err)
	}
	b, err := resolveLibyearCached(ctx, cache, second, resolve)
	if err != nil {
		t.Fatal(err)
	}
	if a.VersionSource != versionuse.SourceLocked || b.VersionSource != "" {
		t.Errorf("sources %q and %q; want each repository's own (locked, and none)", a.VersionSource, b.VersionSource)
	}
	if b.ManifestPath != "b/package.json" {
		t.Errorf("manifest path %q", b.ManifestPath)
	}
}

// Maven read as a tree, end to end (summary/70, 4.4): a dependency with no
// <version> whose parent manages it is stored at that version, with its
// source, and the scan targets it there, labelled "managed". Before
// 0.29.178 its purl had no version and OSV answered with the package's
// whole advisory history.
func TestAManagedMavenVersionReachesTheInventoryAndTheScan(t *testing.T) {
	f := lockFixture(t, "maven-tree")
	f.write(t, "pom.xml", `<project><groupId>org.example</groupId><artifactId>parent</artifactId><version>1.0</version>
	<properties><netty.version>4.1.100.Final</netty.version></properties>
	<dependencyManagement><dependencies>
	  <dependency><groupId>io.netty</groupId><artifactId>netty-all</artifactId><version>${netty.version}</version></dependency>
	</dependencies></dependencyManagement></project>`)
	f.write(t, "app/pom.xml", `<project><parent><groupId>org.example</groupId><artifactId>parent</artifactId><version>1.0</version></parent>
	<artifactId>app</artifactId><dependencies>
	  <dependency><groupId>io.netty</groupId><artifactId>netty-all</artifactId></dependency>
	  <dependency><groupId>com.acme</groupId><artifactId>nowhere</artifactId></dependency>
	</dependencies></project>`)
	f.analyse(t)
	rows := inventoryRows(t, f)
	if r := rows["app/pom.xml:io.netty:netty-all"]; r.Version != "4.1.100.Final" || r.Source != model.VersionSourceManaged || r.Purl != "pkg:maven/io.netty/netty-all@4.1.100.Final" {
		t.Errorf("the managed dependency's row = %+v", r)
	}
	if r := rows["app/pom.xml:com.acme:nowhere"]; r.Version != "" || r.Source != "" || r.Purl != "pkg:maven/com.acme/nowhere" {
		t.Errorf("a dependency managed nowhere = %+v; want no version, as before", r)
	}
	set, err := scantarget.Build(context.Background(), f.store, f.repoID, true)
	if err != nil {
		t.Fatal(err)
	}
	if tgt, ok := set.Targets["pkg:maven/io.netty/netty-all@4.1.100.Final"]; !ok || tgt.Resolution != versionuse.Managed {
		t.Errorf("the scan's target for the managed dependency: found %v, %+v; want it at the managed version, labelled managed", ok, tgt)
	}
	if tgt, ok := set.Targets["pkg:maven/com.acme/nowhere"]; !ok || tgt.Resolution != versionuse.Unpinned {
		t.Errorf("the scan's target for the unmanaged dependency: found %v, %+v; want versionless and unpinned", ok, tgt)
	}
}

// A Maven dependency's reason for having no version reaches the stored row
// (0.29.181), and a row with a version has none.
func TestAVersionReasonReachesTheInventory(t *testing.T) {
	f := lockFixture(t, "version-reason")
	ctx := context.Background()
	if err := f.store.ReplaceRepoLibyear(ctx, f.repoID, []*model.LibyearRow{
		{Name: "io.netty:netty-all", PackageManager: "maven", Purl: "pkg:maven/io.netty/netty-all", ManifestPath: "pom.xml",
			VersionReason: model.VersionReasonInheritedLookupFailed},
		{Name: "com.acme:lib", PackageManager: "maven", CurrentVersion: "1.0", Purl: "pkg:maven/com.acme/lib@1.0", ManifestPath: "pom.xml"},
	}, nil, model.InventoryCoverage{}); err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	rows, err := f.store.Pool().Query(ctx, `SELECT name, COALESCE(version_reason, '<NULL>') FROM aveloxis_data.repo_deps_libyear WHERE repo_id = $1`, f.repoID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var n, r string
		if err := rows.Scan(&n, &r); err != nil {
			t.Fatal(err)
		}
		got[n] = r
	}
	if got["io.netty:netty-all"] != model.VersionReasonInheritedLookupFailed || got["com.acme:lib"] != "<NULL>" {
		t.Errorf("stored reasons = %v", got)
	}
	// stampVersionSource carries the reader's reason onto the row.
	row := &model.LibyearRow{}
	stampVersionSource(model.LibyearDep{VersionReason: model.VersionReasonInheritedNotFound}, row)
	if row.VersionReason != model.VersionReasonInheritedNotFound {
		t.Errorf("the row's reason is %q", row.VersionReason)
	}
}
