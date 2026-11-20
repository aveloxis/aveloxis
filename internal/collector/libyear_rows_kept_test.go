// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

// The inventory no longer depends on the registry (summary/70, 4.2): a
// dependency keeps its row when its lookup fails, with the reason. For that
// the purl has to be built from the manifest, BEFORE the lookup, and it has
// to be the purl the resolver would have built: a finding's identity is
// (repository, advisory, purl), and one changed byte opens a new finding and
// closes the old.

func TestManifestPurlIsThePurlTheResolverBuilds(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		manager  string
		fixture  string
		base     *string
		resolver func(context.Context, model.LibyearDep) (*model.LibyearRow, error)
		name     string
		versions []string
	}{
		{"npm", "", &npmRegistryBase, resolveNPMLibyear, "@Scope/Left_Pad", []string{"4.17.0", ""}},
		{"pypi", "pypi_flask.json", &pypiRegistryBase, resolvePyPILibyear, "Flask_SQLAlchemy", []string{"2.0.1", ""}},
		{"go", "goproxy_cobra_latest.json", &goProxyBase, resolveGoLibyear, "github.com/Spf13/Cobra_X", []string{"v1.8.0", ""}},
		{"cargo", "crates_serde.json", &cratesRegistryBase, resolveCargoLibyear, "Serde_JSON", []string{"1.0.229", ""}},
		{"rubygems", "rubygems_rails.json", &rubygemsRegistryBase, resolveRubyGemsLibyear, "Rails_X", []string{"7.0.0", ""}},
		{"maven", "maven_commonslang3_metadata.xml", &mavenRepositoryBase, resolveMavenLibyear, "Org.Apache.Commons:Commons_Lang3", []string{"3.12.0", ""}},
		{"packagist", "packagist_monolog.json", &packagistRegistryBase, resolvePackagistLibyear, "monolog/monolog", []string{"3.10.0", ""}},
		{"hex", "hex_phoenix.json", &hexRegistryBase, resolveHexLibyear, "Phoenix_X", []string{"1.8.9", ""}},
		{"nuget", "nuget_newtonsoft.json", &nugetRegistryBase, resolveNuGetLibyear, "Newtonsoft.Json", []string{"13.0.1", ""}},
		{"pub", "pubdev_http.json", &pubDevRegistryBase, resolvePubDevLibyear, "Http_X", []string{"0.2.8+2", ""}},
		{"hackage", "hackage_aeson_preferred.json", &hackageRegistryBase, resolveHackageLibyear, "Aeson_X", []string{"2.0.0.0", ""}},
		{"cran", "crandb_commonmark.json", &cranRegistryBase, resolveCRANLibyear, "Common.Mark", []string{"1.9.2", ""}},
	}
	// Names carry capitals and underscores wherever the resolver tolerates
	// them (the captured fixtures answer for any name), so a fold applied on
	// one side and not the other shows. Packagist's is lower case because its
	// resolver looks the name up in the answer as declared.
	compared := 0
	for _, tc := range cases {
		t.Run(tc.manager, func(t *testing.T) {
			var srv *httptest.Server
			if tc.fixture != "" {
				srv = serveFixture(t, tc.fixture)
			} else {
				srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, npmAnswer) }))
			}
			defer srv.Close()
			old := *tc.base
			*tc.base = srv.URL
			defer func() { *tc.base = old }()
			for _, v := range tc.versions {
				dep := model.LibyearDep{Name: tc.name, Version: v, Manager: tc.manager}
				row, err := tc.resolver(ctx, dep)
				if err != nil || row == nil {
					t.Fatalf("%s@%q: row %v err %v", tc.name, v, row, err)
				}
				got, ok := manifestPurl(dep)
				compared++
				if !ok || got != row.Purl {
					t.Errorf("%s %s@%q: manifestPurl = (%q, %v), the resolver built %q", tc.manager, tc.name, v, got, ok, row.Purl)
				}
			}
		})
	}
	if compared != 24 {
		t.Errorf("compared %d purls, want 24 (12 resolvers, with and without a version)", compared)
	}

	// SwiftPM resolves through the forge client, and takes its purl's
	// namespace from the repository URL the manifest gives.
	data, err := os.ReadFile("testdata/registries/github_alamofire_latest.json")
	if err != nil {
		t.Fatal(err)
	}
	gh := &fakeGitHubAPI{answers: map[string]string{"/repos/Alamofire/Alamofire/releases/latest": string(data)}}
	swift := model.LibyearDep{Name: "Alamofire", Version: "5.8.0", Manager: "swiftpm", Requirement: "https://github.com/Alamofire/Alamofire.git"}
	row, err := resolveSwiftPMLibyear(ctx, gh, swift)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := manifestPurl(swift); !ok || got != row.Purl {
		t.Errorf("swiftpm: manifestPurl = (%q, %v), the resolver built %q", got, ok, row.Purl)
	}

	// A GitHub Action's purl carries its ref.
	if got, ok := manifestPurl(model.LibyearDep{Name: "actions/checkout", Version: "v4", Manager: "githubactions"}); !ok || got != "pkg:githubactions/actions/checkout@v4" {
		t.Errorf("githubactions: manifestPurl = (%q, %v)", got, ok)
	}
}

// What has no purl gets no row: a coordinate with no group where the purl
// type requires a namespace, a SwiftPM package whose address is not a
// repository URL, and a package manager with no purl type. A purl-less row is
// never written (it would block the scan-complete mark: tried and reverted,
// v0.28.5).
func TestManifestPurlRefusals(t *testing.T) {
	for _, dep := range []model.LibyearDep{
		{Name: "artifact-only", Manager: "maven", Version: "1.0"},
		{Name: ":artifact", Manager: "maven", Version: "1.0"},
		{Name: "php", Manager: "packagist", Version: "8.2"},
		{Name: "Alamofire", Manager: "swiftpm", Version: "5.8.0", Requirement: "not a url"},
		{Name: "numpy", Manager: "conda", Version: "1.26"},
		{Name: "", Manager: "npm", Version: "1.0.0"},
	} {
		if got, ok := manifestPurl(dep); ok || got != "" {
			t.Errorf("manifestPurl(%+v) = (%q, %v); want no purl", dep, got, ok)
		}
	}
}

// unanswered builds the row of a dependency the registry did not answer for.
func TestUnansweredLibyearRow(t *testing.T) {
	dep := model.LibyearDep{Name: "left-pad", Version: "1.3.0", Requirement: "^1.3.0", Type: "dev", Manager: "npm", ManifestPath: "web/package.json"}
	for _, c := range []struct {
		err  error
		want string
	}{
		{&registryStatusError{url: "u", status: http.StatusNotFound}, model.LibyearNotOnRegistry},
		{fmt.Errorf("x: %w", errRegistryCannotBeAsked), model.LibyearCannotBeAsked},
		{&registryStatusError{url: "u", status: http.StatusBadGateway}, model.LibyearLookupFailed},
		{errors.New("unexpected EOF"), model.LibyearLookupFailed},
		{nil, model.LibyearLookupFailed}, // no row and no error
	} {
		row, ok := unansweredLibyearRow(dep, c.err)
		if !ok {
			t.Fatalf("%v: no row", c.err)
		}
		want := model.LibyearRow{Name: "left-pad", Requirement: "^1.3.0", Type: "dev", PackageManager: "npm",
			CurrentVersion: "1.3.0", Purl: "pkg:npm/left-pad@1.3.0", NoLibyear: true, ManifestPath: "web/package.json", Status: c.want}
		if !reflect.DeepEqual(*row, want) {
			t.Errorf("%v:\n got %+v\nwant %+v", c.err, *row, want)
		}
	}
	if _, ok := unansweredLibyearRow(model.LibyearDep{Name: "artifact-only", Manager: "maven", Version: "1"}, errRegistryCannotBeAsked); ok {
		t.Error("a coordinate with no purl must get no row")
	}
}

// The integration: a registry that says "no such package" for one dependency
// and fails for another. Both keep their rows, with the reason; neither has
// a libyear; and the scan can now ask about them.
func TestDependenciesTheRegistryCannotAnswerForKeepTheirRows(t *testing.T) {
	missing, failing := uniq(t, "kept-missing"), uniq(t, "kept-failing")
	f := newWholeReplaceFixture(t, "rowskept", func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, missing):
			http.NotFound(w, r)
		case strings.Contains(r.URL.Path, failing):
			http.Error(w, "boom", http.StatusInternalServerError)
		default:
			_, _ = io.WriteString(w, npmAnswer)
		}
	})
	good := uniq(t, "kept-good")
	f.manifest(t, map[string]string{good: "1.0.0", missing: "2.0.0", failing: "3.0.0", "local-tool": "file:../tool"})
	res := &AnalysisResult{}
	if err := f.ac.scanLibyear(context.Background(), f.repoID, f.dir, nil, res); err != nil {
		t.Fatal(err)
	}
	type stored struct {
		version, purl, status, source string
		libyearNull                   bool
	}
	rows, err := f.store.Pool().Query(context.Background(), `
		SELECT name, current_version, purl, COALESCE(libyear_status, ''), data_source, libyear IS NULL
		FROM aveloxis_data.repo_deps_libyear WHERE repo_id = $1`, f.repoID)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]stored{}
	for rows.Next() {
		var name string
		var s stored
		if err := rows.Scan(&name, &s.version, &s.purl, &s.status, &s.source, &s.libyearNull); err != nil {
			t.Fatal(err)
		}
		got[name] = s
	}
	rows.Close()
	want := map[string]stored{
		good:    {"1.0.0", "pkg:npm/" + good + "@1.0.0", model.LibyearResolved, "package registry", false},
		missing: {"2.0.0", "pkg:npm/" + missing + "@2.0.0", model.LibyearNotOnRegistry, "manifest", true},
		failing: {"3.0.0", "pkg:npm/" + failing + "@3.0.0", model.LibyearLookupFailed, "manifest", true},
	}
	if len(got) != 3 {
		t.Fatalf("inventory has %d rows (%v); want 3: the path dependency has no row, the other three do", len(got), got)
	}
	for name, w := range want {
		if got[name] != w {
			t.Errorf("%s:\n got %+v\nwant %+v", name, got[name], w)
		}
	}
	if res.LibyearDeps != 3 {
		t.Errorf("counted %d rows, want 3", res.LibyearDeps)
	}
	cov, _ := f.coverage(t)
	if wr := cov.Inventory.DependenciesWithoutRow; len(wr) != 1 || wr[model.NoRowNonRegistry] != 1 {
		t.Errorf("dependencies without a row = %v; want only the path dependency: a failed lookup now keeps its row", wr)
	}

	// The scan asks about all three.
	queries := stubOSV(t, nil)
	if _, err := ScanVulnerabilities(context.Background(), f.store, f.repoID, f.ac.logger, nil, true); err != nil {
		t.Fatal(err)
	}
	if *queries != 3 {
		t.Errorf("the scan sent %d targets; want 3, the two the registry could not answer for included", *queries)
	}
}

// Packagist answers for a name written with capitals, and keys its answer in
// lower case (confirmed against repo.packagist.org on 2026-11-19:
// /p2/Monolog/Monolog.json is a 200 whose only key is "monolog/monolog").
// The resolver looked the answer up by the name as declared, so a Composer
// dependency declared with capitals was "not found on Packagist" on every
// analysis (found 0.29.156; summary/70, top of 4.2).
func TestPackagistResolverFindsANameDeclaredWithCapitals(t *testing.T) {
	srv := serveFixture(t, "packagist_monolog.json")
	defer srv.Close()
	old := packagistRegistryBase
	packagistRegistryBase = srv.URL
	defer func() { packagistRegistryBase = old }()

	lower, err := resolvePackagistLibyear(context.Background(), model.LibyearDep{Name: "monolog/monolog", Version: "3.10.0", Manager: "packagist"})
	if err != nil {
		t.Fatal(err)
	}
	for _, declared := range []string{"Monolog/Monolog", "MONOLOG/monolog"} {
		row, err := resolvePackagistLibyear(context.Background(), model.LibyearDep{Name: declared, Version: "3.10.0", Manager: "packagist"})
		if err != nil || row == nil {
			t.Fatalf("%s: row %v err %v; Packagist has this package", declared, row, err)
		}
		if row.Name != declared {
			t.Errorf("row name = %q; the name is stored as declared (%q)", row.Name, declared)
		}
		if row.Purl != lower.Purl || row.LatestVersion != lower.LatestVersion || row.CurrentReleaseDate != lower.CurrentReleaseDate || row.VersionFound != lower.VersionFound {
			t.Errorf("%s resolved to %+v; want the same purl, latest version, date and version-found as the lower-case spelling: %+v", declared, row, lower)
		}
	}
}

// A declared dependency that gets no row is recorded as a note, and a name
// that is not a package name is never looked up (0.29.164).
func TestDependenciesWithoutARowAreRecordedAsNotes(t *testing.T) {
	var asked []string
	f := newWholeReplaceFixture(t, "notes", func(w http.ResponseWriter, r *http.Request) {
		asked = append(asked, r.URL.Path)
		_, _ = io.WriteString(w, npmAnswer)
	})
	good := uniq(t, "note-good")
	f.manifest(t, map[string]string{good: "1.0.0", "local-tool": "file:../tool", "not a name!": "1.0.0"})
	// A Gradle coordinate with no group (the shorthand ':tests:1.0'): not
	// garbage, and no purl. (":tests" had an inventory row on production.)
	if err := os.WriteFile(f.dir+"/build.gradle", []byte("dependencies {\n  implementation ':tests:1.0'\n}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	res := &AnalysisResult{}
	if err := f.ac.scanLibyear(context.Background(), f.repoID, f.dir, nil, res); err != nil {
		t.Fatal(err)
	}
	if got := f.inventory(t); len(got) != 1 || !strings.HasPrefix(got[0], good) {
		t.Errorf("inventory = %v; want only the one real package", got)
	}
	for _, path := range asked {
		if strings.Contains(path, "not%20a%20name") || strings.Contains(path, "not a name") || strings.Contains(path, "local-tool") {
			t.Errorf("the registry was asked about %q; a name that is not a package name, and a path dependency, are not looked up", path)
		}
	}
	notes, err := f.store.GetRepoInventoryNotes(context.Background(), f.repoID)
	if err != nil {
		t.Fatal(err)
	}
	byName := map[string]model.InventoryNote{}
	for _, n := range notes {
		byName[n.Name] = n
	}
	want := map[string]model.InventoryNote{
		"local-tool": {Kind: model.NoRowNonRegistry, PackageManager: "npm", Name: "local-tool", PackageKey: "npm|local-tool", ManifestPath: "package.json"},
		// A grammar rejection carries the purl the name would have had, so a
		// finding at that purl can be labelled when it closes.
		"not a name!": {Kind: model.NoRowNameRejected, PackageManager: "npm", Name: "not a name!", PackageKey: "npm|not a name!",
			Purl: "pkg:npm/not%20a%20name!@1.0.0", ManifestPath: "package.json"},
	}
	for name, w := range want {
		if byName[name] != w {
			t.Errorf("note for %q:\n got %+v\nwant %+v", name, byName[name], w)
		}
	}
	var mavenNote *model.InventoryNote
	for i := range notes {
		if notes[i].PackageManager == "maven" {
			mavenNote = &notes[i]
		}
	}
	if mavenNote == nil || mavenNote.Kind != model.NoRowNoNamespace || mavenNote.Purl != "" || mavenNote.ManifestPath != "build.gradle" || mavenNote.Name != ":tests" {
		t.Errorf("the group-less Gradle coordinate's note = %+v; want kind no_namespace, no purl", mavenNote)
	}
	if len(notes) != 3 {
		t.Errorf("%d notes, want 3: %+v", len(notes), notes)
	}
	// The coverage record counts the same three, by the same reasons, and
	// the numbers add up.
	cov, _ := f.coverage(t)
	wr := cov.Inventory.DependenciesWithoutRow
	if wr[model.NoRowNonRegistry] != 1 || wr[model.NoRowNameRejected] != 1 || wr[model.NoRowNoNamespace] != 1 || len(wr) != 3 {
		t.Errorf("dependencies without a row = %v", wr)
	}
	if cov.Inventory.DependenciesDeclared != 4 || res.LibyearDeps != 1 {
		t.Errorf("declared %d, rows %d; want 4 and 1", cov.Inventory.DependenciesDeclared, res.LibyearDeps)
	}
}

func TestInventoryNote(t *testing.T) {
	// A name the sanitizer would change: stored sanitized, keyed from the
	// stored name, and with NO purl (identity is never repaired).
	n := inventoryNote(model.LibyearDep{Name: "bad\x00name", Manager: "pypi", Version: "1.0", ManifestPath: "requirements.txt"}, model.NoRowNameRejected)
	if n.Name != "badname" || n.PackageKey != "pypi|badname" || n.Purl != "" || n.ManifestPath != "requirements.txt" {
		t.Errorf("note = %+v", n)
	}
	// A name past the bound is cut to it, on a rune boundary, with no purl.
	long := strings.Repeat("é", 2000) // 4,000 bytes
	n = inventoryNote(model.LibyearDep{Name: long, Manager: "pypi"}, model.NoRowNameRejected)
	if len(n.Name) > 2704 || len(n.Name) < 2702 || !strings.HasPrefix(long, n.Name) || n.Purl != "" {
		t.Errorf("a %d-byte name was stored as %d bytes with purl %q", len(long), len(n.Name), n.Purl)
	}
	// A non-registry dependency never carries the registry package's purl.
	n = inventoryNote(model.LibyearDep{Name: "left-pad", Manager: "npm", Version: "1.0.0"}, model.NoRowNonRegistry)
	if n.Purl != "" || n.PackageKey != "npm|left-pad" {
		t.Errorf("non-registry note = %+v; it is NOT the registry package of the same name", n)
	}
}
