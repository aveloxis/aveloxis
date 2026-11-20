// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package pkgid

import (
	"bufio"
	"encoding/json"
	"os"
	"strings"
	"testing"
)

// TestPurlAgainstSpecCases tests the builder against the purl SPECIFICATION's
// canonical cases (testdata/purl_spec_cases.json, derived from
// github.com/package-url/purl-spec), not against our own output. Before
// v0.27.29 the tests asserted the implementation's non-canonical strings back
// at it (pkg:pypi/Flask_SQLAlchemy pinned as correct).
func TestPurlAgainstSpecCases(t *testing.T) {
	raw, err := os.ReadFile("testdata/purl_spec_cases.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Cases []struct {
			Type      string `json:"type"`
			Name      string `json:"name"`
			Version   string `json:"version"`
			Canonical string `json:"canonical"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	if len(fixture.Cases) < 10 {
		t.Fatalf("fixture has %d cases: truncated?", len(fixture.Cases))
	}
	for _, c := range fixture.Cases {
		if got := Purl(c.Type, c.Name, c.Version); got != c.Canonical {
			t.Errorf("Purl(%q, %q, %q) = %q, want spec-canonical %q",
				c.Type, c.Name, c.Version, got, c.Canonical)
		}
	}
}

// goldenRows reads testdata/identity_golden.tsv. The file was written by the
// code this package replaced (collector's buildPurl, purlForPackage and
// selfAdvisoryPurl; db's LockfileGraphKey; collector's lockfileMatchKey) over
// testdata/production_names_sample.tsv, a sample of the dependency names both
// production databases held on 2026-11-19, chosen to include every
// combination of unusual characters each package manager showed. It is a
// record of what was, never regenerated from this package: a row that no
// longer matches is a changed purl or key, which is a decision (see the
// package comment), not a test to update.
func goldenRows(t *testing.T, kind string, fields int) [][]string {
	t.Helper()
	f, err := os.Open("testdata/identity_golden.tsv")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var out [][]string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		cols := strings.Split(sc.Text(), "\t")
		if cols[0] != kind {
			continue
		}
		if len(cols) != fields {
			t.Fatalf("golden row has %d fields, want %d: %q", len(cols), fields, sc.Text())
		}
		out = append(out, cols)
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// The golden's own size is pinned so that a truncated file cannot pass.
const (
	goldenNameRows    = 2342
	goldenVersionRows = 120
)

// TestPurlsAreByteIdenticalToTheOldBuilders is the first invariant: for every
// sampled production name, the purl at a version, the versionless purl and the
// self purl are what they were.
func TestPurlsAreByteIdenticalToTheOldBuilders(t *testing.T) {
	rows := goldenRows(t, "N", 8)
	if len(rows) != goldenNameRows {
		t.Fatalf("golden has %d name rows, want %d", len(rows), goldenNameRows)
	}
	sendable := 0
	for _, r := range rows {
		manager, name, wantAt, wantBare, wantSelf := r[1], r[2], r[3], r[4], r[5]
		if got := PurlForPackage(manager, "", name, "1.2.3"); got != wantAt {
			t.Errorf("PurlForPackage(%q, \"\", %q, 1.2.3) = %q, want %q", manager, name, got, wantAt)
		}
		bare := ""
		if typ, ok := PurlTypeOfManager(manager); ok {
			bare = Purl(typ, name, "")
		}
		if bare != wantBare {
			t.Errorf("versionless Purl for (%q, %q) = %q, want %q", manager, name, bare, wantBare)
		}
		// "packagist" and "hackage" are what ecosyste.ms calls those two
		// registries, and so what the distribution subsystem stores. The
		// old builder knew them only as "composer" and "haskell", so the
		// golden has no self purl for them and no publisher to either was
		// ever asked about its own package (0.29.208, review round 1). The
		// self purl is the package's purl with no version.
		if manager == "packagist" || manager == "hackage" {
			wantSelf = strings.TrimSuffix(wantAt, "@1.2.3")
		}
		if got := SelfPurl(manager, name); got != wantSelf {
			t.Errorf("SelfPurl(%q, %q) = %q, want %q", manager, name, got, wantSelf)
		}
		if wantAt != "" {
			sendable++
		}
	}
	// Guard the denominator: most of the sample must exercise the builder,
	// not its refusals.
	if sendable < goldenNameRows*4/5 {
		t.Errorf("only %d of %d sampled names yield a purl; the sample no longer tests the builder", sendable, goldenNameRows)
	}
}

// TestVersionsAreEscapedAsBefore covers the version half over every purl type:
// build metadata, an npm alias, a space, and each reserved character.
func TestVersionsAreEscapedAsBefore(t *testing.T) {
	rows := goldenRows(t, "V", 4)
	if len(rows) != goldenVersionRows {
		t.Fatalf("golden has %d version rows, want %d", len(rows), goldenVersionRows)
	}
	for _, r := range rows {
		if got := Purl(r[1], "Group_Name/Some.Pkg", r[2]); got != r[3] {
			t.Errorf("Purl(%q, Group_Name/Some.Pkg, %q) = %q, want %q", r[1], r[2], got, r[3])
		}
	}
}

// TestKeysAreByteIdenticalToTheOldKeys is the second invariant.
func TestKeysAreByteIdenticalToTheOldKeys(t *testing.T) {
	for _, r := range goldenRows(t, "N", 8) {
		manager, name := r[1], r[2]
		if got := Key(manager, name); got != r[6] {
			t.Errorf("Key(%q, %q) = %q, want %q", manager, name, got, r[6])
		}
		if got := MatchKey(manager, name); got != r[7] {
			t.Errorf("MatchKey(%q, %q) = %q, want %q", manager, name, got, r[7])
		}
	}
}

// TestMatchKeyAndKeyMatchTheSamePairs: MatchKey does not fold the ecosystem,
// so its strings differ from Key's, but within one ecosystem string the two
// agree on which names are the same package. Checked over every pair of
// sampled names of each manager that either key calls equal.
func TestMatchKeyAndKeyMatchTheSamePairs(t *testing.T) {
	byManager := map[string][]string{}
	for _, r := range goldenRows(t, "N", 8) {
		byManager[r[1]] = append(byManager[r[1]], r[2])
	}
	compared := 0
	for manager, names := range byManager {
		byKey, byMatch := map[string][]string{}, map[string][]string{}
		for _, n := range names {
			byKey[Key(manager, n)] = append(byKey[Key(manager, n)], n)
			byMatch[MatchKey(manager, n)] = append(byMatch[MatchKey(manager, n)], n)
		}
		for _, n := range names {
			a, b := byKey[Key(manager, n)], byMatch[MatchKey(manager, n)]
			compared++
			if strings.Join(a, "\x00") != strings.Join(b, "\x00") {
				t.Errorf("%s: Key groups %q as %q, MatchKey as %q", manager, n, a, b)
			}
		}
	}
	if compared != goldenNameRows {
		t.Errorf("compared %d names, want %d", compared, goldenNameRows)
	}
}

func TestKeyFolds(t *testing.T) {
	cases := []struct{ eco, name, want string }{
		{"pypi", "Flask_SQLAlchemy", "pypi|flask-sqlalchemy"},
		{"pypi", "ruamel.yaml", "pypi|ruamel-yaml"},
		{"PyPI", " Requests ", "pypi|requests"},
		{"npm", "@Scope/Name", "npm|@scope/name"},
		{"npm", "lodash.merge", "npm|lodash.merge"}, // the PEP 503 fold is PyPI's alone
		{"rubygems", "Rails", "gem|rails"},
		{"gem", "rails", "gem|rails"},
		{"packagist", "Vendor/Pkg", "composer|vendor/pkg"},
		{"swiftpm", "Alamofire", "swift|alamofire"},
		{"elixir", "phoenix", "hex|phoenix"},
		{"dart", "http", "pub|http"},
		{"golang", "github.com/Foo/Bar", "go|github.com/foo/bar"},
		{"haskell", "aeson", "hackage|aeson"},
		// The KEY folds case, as every key does; CRAN refuses a package
		// whose name differs from another's only by case, so no two
		// packages share one. The PURL keeps the case (TestCranPurlKeepsCase).
		{"cran", "Rcpp", "cran|rcpp"},
		{"cran", "data.table", "cran|data.table"}, // the PEP 503 fold is PyPI's alone
		{"maven", "org.Apache:Commons_Lang", "maven|org.apache:commons_lang"},
		{"conda", "NumPy", "conda|numpy"}, // an unknown ecosystem is lower-cased, not refused
	}
	for _, c := range cases {
		if got := Key(c.eco, c.name); got != c.want {
			t.Errorf("Key(%q, %q) = %q, want %q", c.eco, c.name, got, c.want)
		}
	}
}

// TestKeyNameIsTheNameHalfOfKey: the SQL prefilter is built from KeyName, so
// it must be exactly what Key puts after the bar.
func TestKeyNameIsTheNameHalfOfKey(t *testing.T) {
	for _, r := range goldenRows(t, "N", 8) {
		e := FoldEcosystem(r[1])
		if got, want := e+"|"+KeyName(e, r[2]), Key(r[1], r[2]); got != want {
			t.Errorf("FoldEcosystem+KeyName = %q, Key = %q", got, want)
		}
	}
}

func TestSelfPurl(t *testing.T) {
	if got := SelfPurl("pypi", "NumPy"); got != "pkg:pypi/numpy" {
		t.Errorf("SelfPurl pypi NumPy = %q, want pkg:pypi/numpy", got)
	}
	if got := SelfPurl("PyPI", "NumPy"); got != "pkg:pypi/numpy" {
		t.Errorf("the distribution vocabulary folds case; got %q", got)
	}
	if got := SelfPurl("conda", "whatever"); got != "" {
		t.Errorf("an unmapped ecosystem must yield no purl, got %q", got)
	}
	if got := SelfPurl("maven", "org.apache.commons:commons-lang3"); got != "pkg:maven/org.apache.commons/commons-lang3" {
		t.Errorf("SelfPurl maven = %q", got)
	}
	if got := SelfPurl("maven", "commons-lang3"); got != "" {
		t.Errorf("a maven name without its group has no purl, got %q", got)
	}
}

func TestPurlForPackageRefusals(t *testing.T) {
	cases := []struct {
		name                           string
		eco, namespace, pkg, ver, want string
	}{
		{"unknown ecosystem", "conda", "", "numpy", "1.0", ""},
		{"the manager lookup is exact", "NPM", "", "left-pad", "1.0", ""},
		{"empty name", "npm", "", "", "1.0", ""},
		{"empty version", "npm", "", "left-pad", "", ""},
		{"swift without a namespace", "swiftpm", "", "alamofire", "5.0.0", ""},
		{"swift with the lockfile's namespace", "swiftpm", "github.com/Alamofire/", "alamofire", "5.0.0", "pkg:swift/github.com/Alamofire/alamofire@5.0.0"},
		{"composer platform package", "packagist", "", "php", "8.2", ""},
		{"composer with vendor", "packagist", "", "Monolog/Monolog", "3.0", "pkg:composer/monolog/monolog@3.0"},
		{"maven folds the first colon", "maven", "", "org.slf4j:slf4j-api", "2.0.9", "pkg:maven/org.slf4j/slf4j-api@2.0.9"},
		{"maven without a group", "maven", "", "slf4j-api", "2.0.9", ""},
		{"maven with an empty group", "maven", "", ":tests", "1.0", ""},
		{"npm scope", "npm", "", "@Babel/Core", "7.0.0", "pkg:npm/%40babel/core@7.0.0"},
		{"go module", "go", "", "github.com/Foo/Bar", "v1.2.3", "pkg:golang/github.com/foo/bar@v1.2.3"},
		{"rubygems alias", "rubygems", "", "rails", "7.1.0", "pkg:gem/rails@7.1.0"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := PurlForPackage(c.eco, c.namespace, c.pkg, c.ver); got != c.want {
				t.Errorf("PurlForPackage(%q, %q, %q, %q) = %q, want %q", c.eco, c.namespace, c.pkg, c.ver, got, c.want)
			}
		})
	}
}

// TestEveryLockfileRosterAliasHasAPurlType: the lockfile roster's ecosystem
// strings differ from the package managers'. Without the alias every such
// transitive package yielded no purl and was dropped from the scan without a
// word (v0.27.133).
func TestEveryLockfileRosterAliasHasAPurlType(t *testing.T) {
	for alias, want := range map[string]string{
		"rubygems": "gem", "packagist": "composer", "swiftpm": "swift", "hackage": "hackage",
	} {
		if got, ok := PurlTypeOfManager(alias); !ok || got != want {
			t.Errorf("PurlTypeOfManager(%q) = (%q, %v), want (%q, true)", alias, got, ok, want)
		}
	}
}

func TestNamespaceRules(t *testing.T) {
	for _, typ := range []string{"maven", "swift", "composer"} {
		if !NamespaceRequired(typ) {
			t.Errorf("NamespaceRequired(%q) = false", typ)
		}
	}
	for _, typ := range []string{"npm", "golang", "pypi", "cargo", "gem", "nuget", "hex", "pub", "hackage", "cran", ""} {
		if NamespaceRequired(typ) {
			t.Errorf("NamespaceRequired(%q) = true; its specification leaves the namespace optional", typ)
		}
	}
	for name, want := range map[string]bool{
		"group/artifact": true, "a/b/c": true, "artifact": false, "/artifact": false, "group/": false, "": false,
	} {
		if got := NameHasNamespace(name); got != want {
			t.Errorf("NameHasNamespace(%q) = %v, want %v", name, got, want)
		}
	}
}

func TestEscapeSegmentNeverEncodesTwice(t *testing.T) {
	if got := EscapeSegment("a%40b@c"); got != "a%2540b%40c" {
		t.Errorf("EscapeSegment = %q", got)
	}
	if got := EscapeSegment("1.0 beta#1?x"); got != "1.0%20beta%231%3Fx" {
		t.Errorf("EscapeSegment = %q", got)
	}
}
