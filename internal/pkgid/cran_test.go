// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package pkgid

import "testing"

// R (summary/70, 4.9). OSV lists the CRAN ecosystem and matches
// pkg:cran/<name> BY CASE: asked live on 2026-11-19, pkg:cran/commonmark
// answered with three advisories and pkg:cran/Commonmark with none. A purl
// with the name folded would ask about a package that does not exist and be
// told, truthfully, that it has no advisories.
func TestCranPurlKeepsCase(t *testing.T) {
	for _, c := range []struct{ name, version, want string }{
		{"Rcpp", "1.0.12", "pkg:cran/Rcpp@1.0.12"},
		{"data.table", "1.14.8", "pkg:cran/data.table@1.14.8"},
		{"R6", "", "pkg:cran/R6"},
		{"MASS", "7.3-60", "pkg:cran/MASS@7.3-60"},
	} {
		if got := Purl("cran", c.name, c.version); got != c.want {
			t.Errorf("Purl(cran, %q, %q) = %q, want %q", c.name, c.version, got, c.want)
		}
	}
	if got := PurlForPackage("cran", "", "Rcpp", "1.0.12"); got != "pkg:cran/Rcpp@1.0.12" {
		t.Errorf("PurlForPackage = %q", got)
	}
	if typ, ok := PurlTypeOfManager("cran"); !ok || typ != "cran" {
		t.Errorf("PurlTypeOfManager(cran) = (%q, %v)", typ, ok)
	}
	// The key of the purl is the key of the row it was built from.
	key, version, ok := KeyOfPurl("pkg:cran/Rcpp@1.0.12")
	if !ok || key != Key("cran", "Rcpp") || version != "1.0.12" {
		t.Errorf("KeyOfPurl = (%q, %q, %v), want (%q, 1.0.12, true)", key, version, ok, Key("cran", "Rcpp"))
	}
}

// A repository that publishes to CRAN is asked about its own package
// (summary/70, 4.9), versionless and with the name's case kept: OSV has the
// package under one case only.
func TestCranSelfPurlKeepsCase(t *testing.T) {
	for _, eco := range []string{"cran", "CRAN"} {
		if got := SelfPurl(eco, "Rcpp"); got != "pkg:cran/Rcpp" {
			t.Errorf("SelfPurl(%q, Rcpp) = %q, want pkg:cran/Rcpp", eco, got)
		}
		if !SelfNameKeepsCase(eco) {
			t.Errorf("SelfNameKeepsCase(%q) = false", eco)
		}
	}
}

// Which ecosystems keep an own-package name's case is decided by how OSV
// matches that ecosystem's names, asked live on 2026-11-19 with a real
// package in two cases (advisories found as published / in another case):
//
//	maven    xerces/xercesImpl 5 / 0      gem     RedCloth 2 / 0
//	hex      phoenix 8 / 0                pub     http 1 / 0
//	hackage  aeson 2 / 0                  swift   github.com/vapor/vapor 7 / 0
//	cran     commonmark 3 / 0             npm     lodash 10 / 0
//	pypi     django 324 / 324             cargo   tokio 13 / 13
//	nuget    Newtonsoft.Json 1 / 1        golang  gin-gonic/gin 8 / 8
//	composer symfony/symfony 102 / 102
//
// Where OSV matches by case, a name folded to lower case asks about a
// package that does not exist whenever the real name has a capital (540
// repositories' Maven names on one production database did, and none of
// them had an own-package finding). So those keep the stored spelling.
// Where OSV ignores case, the fold stays, because the purl is a finding's
// identity and those identities exist (npm's is folded by Purl itself).
func TestOwnPackageNamesKeepCaseWhereOSVMatchesByCase(t *testing.T) {
	keep := []string{"cran", "maven", "gem", "rubygems", "hex", "elixir", "pub", "dart", "haskell", "hackage", "swift", "swiftpm", "bioconductor", "Bioconductor"}
	fold := []string{"pypi", "cargo", "nuget", "go", "composer", "packagist", "npm", "", "conda"}
	for _, eco := range keep {
		if !SelfNameKeepsCase(eco) {
			t.Errorf("SelfNameKeepsCase(%q) = false; OSV matches its names by case", eco)
		}
	}
	for _, eco := range fold {
		if SelfNameKeepsCase(eco) {
			t.Errorf("SelfNameKeepsCase(%q) = true; its own-package identities are lower-case and stay so", eco)
		}
	}
	for _, c := range []struct{ eco, name, want string }{
		{"maven", "xerces:xercesImpl", "pkg:maven/xerces/xercesImpl"},
		{"rubygems", "RedCloth", "pkg:gem/RedCloth"},
		{"haskell", "HsOpenSSL", "pkg:hackage/HsOpenSSL"},
		{"hackage", "HsOpenSSL", "pkg:hackage/HsOpenSSL"},
		{"packagist", "Monolog/Monolog", "pkg:composer/monolog/monolog"},
		{"Packagist", "novendor", ""}, // Composer's purl needs its vendor
		// Folded by the purl type's own rule, whatever is handed in.
		{"pypi", "Django", "pkg:pypi/django"},
		{"nuget", "Newtonsoft.Json", "pkg:nuget/newtonsoft.json"},
	} {
		if got := SelfPurl(c.eco, c.name); got != c.want {
			t.Errorf("SelfPurl(%q, %q) = %q, want %q", c.eco, c.name, got, c.want)
		}
	}
}

// Swift too (operator, 2026-11-19: "Keep case for Swift too"): OSV matches a
// Swift purl by case, and the lockfile side has kept it since 0.29.59. The
// distribution subsystem stores Swift packages as "swiftpm", which had no
// purl type at all, so no Swift publisher was asked about its own package.
func TestSwiftSelfPurlKeepsCase(t *testing.T) {
	for _, eco := range []string{"swift", "swiftpm", "SwiftPM"} {
		if got := SelfPurl(eco, "github.com/Alamofire/Alamofire"); got != "pkg:swift/github.com/Alamofire/Alamofire" {
			t.Errorf("SelfPurl(%q) = %q", eco, got)
		}
		if !SelfNameKeepsCase(eco) {
			t.Errorf("SelfNameKeepsCase(%q) = false", eco)
		}
	}
	// A Swift name with no namespace is still no purl.
	if got := SelfPurl("swiftpm", "Alamofire"); got != "" {
		t.Errorf("SelfPurl(swiftpm, Alamofire) = %q; the type requires a namespace", got)
	}
}

// Every registry OSV can be asked about is reachable from BOTH names it goes
// by: the package managers' and ecosyste.ms's (which is what the
// distribution subsystem stores). A spelling missing from the distribution
// table means no publisher to that registry is ever asked about its own
// package, and nothing says so: `swiftpm` was missing until 0.29.206,
// `packagist` and `hackage` until 0.29.208.
func TestEveryRegistrySpellingHasADistributionPurlType(t *testing.T) {
	// The package managers' table is the roster of spellings: each one that
	// is not a build tool's own name must be a distribution spelling too.
	notARegistryName := map[string]bool{
		"githubactions": true, // asked through OSV's GitHub Actions ecosystem, never as a published package
		"golang":        true, // the purl type; the registry is "go" in both vocabularies
	}
	examined := 0
	for spelling, typ := range managerPurlTypes {
		if notARegistryName[spelling] {
			continue
		}
		examined++
		got, ok := PurlTypeOfDistribution(spelling)
		if !ok || got != typ {
			t.Errorf("PurlTypeOfDistribution(%q) = %q, %v; the package managers' table maps it to %q", spelling, got, ok, typ)
		}
	}
	if examined < 12 {
		t.Errorf("only %d spellings examined; the package managers' table has shrunk or the exemptions have grown", examined)
	}
}
