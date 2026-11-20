// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package pkgid

import "testing"

// TestKeyOfPurlIsTheKeyOfItsRow is the third invariant: for every sampled
// production name, the key of the purl built from the row equals the key of
// the row, at a version, with no version, and for the self purl.
func TestKeyOfPurlIsTheKeyOfItsRow(t *testing.T) {
	compared := 0
	for _, r := range goldenRows(t, "N", 8) {
		manager, name, atVersion, bare, self, wantKey := r[1], r[2], r[3], r[4], r[5], r[6]
		for _, c := range []struct{ purl, version string }{{atVersion, "1.2.3"}, {bare, ""}, {self, ""}} {
			if c.purl == "" {
				continue
			}
			// The golden's versionless purl is the raw builder's, with no
			// namespace rule and no Maven fold; a Maven name keeps its ':'
			// there and is one segment, which no stored purl has.
			if manager == "maven" && c.purl == bare {
				continue
			}
			key, version, ok := KeyOfPurl(c.purl)
			compared++
			if !ok || key != wantKey || version != c.version {
				t.Errorf("KeyOfPurl(%q) = (%q, %q, %v); the row (%q, %q) has key %q, version %q", c.purl, key, version, ok, manager, name, wantKey, c.version)
			}
		}
	}
	if compared < 5000 {
		t.Errorf("compared %d purls; the sample no longer exercises the inverse", compared)
	}
}

func TestKeyOfPurl(t *testing.T) {
	cases := []struct {
		purl, key, version string
		ok                 bool
	}{
		{"pkg:npm/%40babel/core@7.0.0", "npm|@babel/core", "7.0.0", true},
		{"pkg:npm/left-pad", "npm|left-pad", "", true},
		{"pkg:pypi/flask-sqlalchemy@2.5.1", "pypi|flask-sqlalchemy", "2.5.1", true},
		{"pkg:pypi/ruamel.yaml@0.17", "pypi|ruamel-yaml", "0.17", true},
		{"pkg:golang/github.com/pkg/errors@v0.9.1", "go|github.com/pkg/errors", "v0.9.1", true},
		{"pkg:maven/org.slf4j/slf4j-api@2.0.9", "maven|org.slf4j:slf4j-api", "2.0.9", true},
		{"pkg:gem/rails@7.1.0", "gem|rails", "7.1.0", true},
		{"pkg:composer/monolog/monolog@3.0", "composer|monolog/monolog", "3.0", true},
		{"pkg:swift/github.com/Alamofire/Alamofire@5.8.0", "swift|alamofire", "5.8.0", true},
		{"pkg:githubactions/actions/checkout@v4", "githubactions|actions/checkout", "v4", true},
		{"pkg:nuget/newtonsoft.json@13.0.1", "nuget|newtonsoft.json", "13.0.1", true},
		// A version with an escaped character, qualifiers and a subpath.
		{"pkg:npm/x@1.0.0%20beta?vcs_url=git%2Bhttps://e.com/x@abc#sub/dir", "npm|x", "1.0.0 beta", true},
		{"pkg:pub/http@0.2.8+2", "pub|http", "0.2.8+2", true},
		// Not purls this package builds.
		{"", "", "", false},
		{"left-pad@1.0.0", "", "", false},
		{"pkg:npm", "", "", false},
		{"pkg:npm/", "", "", false},
		{"pkg:npm/@1.0.0", "", "", false},
		{"pkg:generic/owner/repo", "", "", false},
		{"pkg:conda/numpy@1.26", "", "", false},
	}
	for _, c := range cases {
		key, version, ok := KeyOfPurl(c.purl)
		if key != c.key || version != c.version || ok != c.ok {
			t.Errorf("KeyOfPurl(%q) = (%q, %q, %v), want (%q, %q, %v)", c.purl, key, version, ok, c.key, c.version, c.ok)
		}
	}
}

// Every purl type the builders emit can be read back.
func TestEveryPurlTypeHasAnEcosystem(t *testing.T) {
	types := map[string]bool{"githubactions": true}
	// The identities of findings for ecosystems asked by name (0.29.206).
	for eco := range osvEcosystemsByName {
		types[eco] = true
	}
	for _, typ := range managerPurlTypes {
		types[typ] = true
	}
	for _, typ := range distributionPurlTypes {
		types[typ] = true
	}
	for typ := range types {
		eco, ok := purlTypeEcosystems[typ]
		if !ok {
			t.Errorf("purl type %q has no ecosystem to key by", typ)
			continue
		}
		// And the round trip lands on the fold Key uses.
		if FoldEcosystem(eco) != eco {
			t.Errorf("purl type %q maps to %q, which Key would fold to %q", typ, eco, FoldEcosystem(eco))
		}
	}
	if len(purlTypeEcosystems) != len(types) {
		t.Errorf("purlTypeEcosystems has %d types, the builders emit %d", len(purlTypeEcosystems), len(types))
	}
}
