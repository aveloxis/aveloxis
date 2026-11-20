// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/aveloxis/aveloxis/internal/manifest"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/versionuse"
)

// The CRAN resolver (summary/70, 4.9). CRAN has no JSON interface; release
// dates come from crandb (crandb.r-pkg.org, the R-hub project's database of
// CRAN, the operator's choice of 2026-11-19): one request answers with the
// latest version and a timeline of every version's publication time.

func cranRow(t *testing.T, version string) *model.LibyearRow {
	t.Helper()
	srv := serveFixture(t, "crandb_commonmark.json")
	defer srv.Close()
	defer swapBase(&cranRegistryBase, srv.URL)()
	row, err := resolveCRANLibyear(context.Background(), model.LibyearDep{Name: "commonmark", Version: version, Manager: "cran", Type: "runtime", Requirement: ">= " + version})
	if err != nil || row == nil {
		t.Fatalf("row %v, err %v", row, err)
	}
	return row
}

func TestCRANResolverReadsTheTimeline(t *testing.T) {
	row := cranRow(t, "1.9.2")
	if row.LatestVersion != "2.0.0" || row.LatestReleaseDate != "2025-07-07T12:40:02+00:00" {
		t.Errorf("latest = %q at %q", row.LatestVersion, row.LatestReleaseDate)
	}
	if row.CurrentVersion != "1.9.2" || row.CurrentReleaseDate == "" || row.VersionFound != model.VersionOnRegistry {
		t.Errorf("current = %q at %q, found %d", row.CurrentVersion, row.CurrentReleaseDate, row.VersionFound)
	}
	if row.Libyear <= 0 {
		t.Errorf("libyear = %v; 1.9.2 is older than 2.0.0", row.Libyear)
	}
	if row.PackageManager != "cran" || row.Purl != "pkg:cran/commonmark@1.9.2" || row.Type != "runtime" || row.Requirement != ">= 1.9.2" {
		t.Errorf("row = %+v", row)
	}
	// The licence as CRAN states it, in SPDX where it has a form there.
	if row.License != "BSD-2-Clause" {
		t.Errorf("licence = %q, want BSD-2-Clause (from %q)", row.License, "BSD_2_clause + file LICENSE")
	}
	// The latest version is zero libyears behind.
	if latest := cranRow(t, "2.0.0"); latest.Libyear != 0 || latest.VersionFound != model.VersionOnRegistry {
		t.Errorf("at the latest version: libyear %v, found %d", latest.Libyear, latest.VersionFound)
	}
	// A version CRAN never published: proved absent, and not dated.
	if absent := cranRow(t, "9.9.9"); absent.VersionFound != model.VersionNotOnRegistry || absent.CurrentReleaseDate != "" {
		t.Errorf("a version CRAN lacks: found %d, dated %q", absent.VersionFound, absent.CurrentReleaseDate)
	}
}

func TestCRANResolverRequestsThePackageByItsExactName(t *testing.T) {
	var asked string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		asked = r.URL.Path
		_, _ = io.WriteString(w, `{"name":"Rcpp","latest":"1.0.12","timeline":{"1.0.12":"2024-01-09T09:00:00+00:00"},"versions":{"1.0.12":{"License":"GPL (>= 2)"}}}`)
	}))
	defer srv.Close()
	defer swapBase(&cranRegistryBase, srv.URL)()
	row, err := resolveCRANLibyear(context.Background(), model.LibyearDep{Name: "Rcpp", Manager: "cran"})
	if err != nil {
		t.Fatal(err)
	}
	// crandb answers 404 for another case of the name (checked live).
	if asked != "/Rcpp/all" {
		t.Errorf("asked %q, want /Rcpp/all", asked)
	}
	if row.Purl != "pkg:cran/Rcpp" || row.License != "GPL-2.0-or-later" || row.VersionFound != model.VersionNotEstablished {
		t.Errorf("row = purl %q, licence %q, found %d", row.Purl, row.License, row.VersionFound)
	}
}

// A package crandb does not have is "not on the registry": typed, so that
// the row is kept with that status. An answer with no timeline is not a
// "no" (SR-5): it says nothing, and is asked again.
func TestCRANResolverFailures(t *testing.T) {
	missing := httptest.NewServer(http.NotFoundHandler())
	defer missing.Close()
	restore := swapBase(&cranRegistryBase, missing.URL)
	_, err := resolveCRANLibyear(context.Background(), model.LibyearDep{Name: "nosuchpackage", Manager: "cran"})
	restore()
	if !errors.Is(err, errRegistryNotFound) {
		t.Errorf("a 404: %v, want errRegistryNotFound", err)
	}
	for what, body := range map[string]string{
		"no timeline":            `{"name":"x","latest":"1.0"}`,
		"an empty timeline":      `{"name":"x","latest":"1.0","timeline":{}}`,
		"latest not in timeline": `{"name":"x","latest":"2.0","timeline":{"1.0":"2020-01-01T00:00:00+00:00"}}`,
		"no latest":              `{"name":"x","timeline":{"1.0":"2020-01-01T00:00:00+00:00"}}`,
		"not JSON":               `<html>maintenance</html>`,
	} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, body) }))
		restore := swapBase(&cranRegistryBase, srv.URL)
		row, err := resolveCRANLibyear(context.Background(), model.LibyearDep{Name: "x", Manager: "cran"})
		restore()
		srv.Close()
		if err == nil || row != nil || errors.Is(err, errRegistryNotFound) {
			t.Errorf("%s: row %v, err %v; want an error that is not a not-found", what, row, err)
		}
	}
}

func TestCRANLicence(t *testing.T) {
	for in, want := range map[string]string{
		"MIT + file LICENSE":                "MIT",
		"BSD_3_clause + file LICENSE":       "BSD-3-Clause",
		"BSD_2_clause + file LICENCE":       "BSD-2-Clause",
		"GPL-2":                             "GPL-2.0-only",
		"GPL-3":                             "GPL-3.0-only",
		"GPL (>= 2)":                        "GPL-2.0-or-later",
		"GPL (>= 3)":                        "GPL-3.0-or-later",
		"GPL-2 | GPL-3":                     "GPL-2.0-only OR GPL-3.0-only",
		"LGPL-2.1":                          "LGPL-2.1-only",
		"LGPL (>= 2.1)":                     "LGPL-2.1-or-later",
		"Apache License 2.0":                "Apache-2.0",
		"Apache License (== 2.0)":           "Apache-2.0",
		"Apache License 2.0 | file LICENSE": "Apache-2.0",
		"CC0":                               "CC0-1.0",
		"Artistic-2.0":                      "Artistic-2.0",
		// What has no SPDX form here is passed on as CRAN states it, and
		// "see the file" alone is no licence name.
		"Mozilla Public License 2.0": "Mozilla Public License 2.0",
		"file LICENSE":               "",
		"":                           "",
		"GPL":                        "GPL",
	} {
		if got := cranLicense(in); got != want {
			t.Errorf("cranLicense(%q) = %q, want %q", in, got, want)
		}
	}
}

// crandb is one of the fixed registries: requests to it go under the
// registry rule, and its base is the shipped one.
func TestCrandbIsAFixedRegistry(t *testing.T) {
	if shippedRegistryBases["cran"] != "https://crandb.r-pkg.org" {
		t.Errorf("shipped base for cran = %q", shippedRegistryBases["cran"])
	}
	found := false
	for _, h := range fixedRegistryHosts {
		found = found || h == "crandb.r-pkg.org"
	}
	if !found {
		t.Error("crandb.r-pkg.org is not among the fixed registry hosts")
	}
}

// How each form of a DESCRIPTION requirement is then classed (versionuse.Of,
// the one decision every ecosystem shares): a floor, a pin, or no version
// at all. An upper bound alone must not read as a bounded range with a
// floor it does not have: its finding would then be exposure at no version,
// where it is "version not determined". The requirement is kept as written.
func TestRDescriptionRequirementsClassify(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "DESCRIPTION")
	if err := os.WriteFile(path, []byte("Package: p\nImports: aa (>= 1.0), bb (== 2.0), cc, dd (<= 4.0), ee (!= 5.0), ff (> 6.0)\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"aa": versionuse.RangeFloor, "bb": versionuse.Exact, "cc": versionuse.Unpinned, "dd": versionuse.Unpinned, "ee": versionuse.Unpinned, "ff": versionuse.RangeFloor}
	deps := manifest.NewWalk(root, nil).Read(path, manifest.Options{}).Deps
	if len(deps) != len(want) {
		t.Fatalf("%d dependencies, want %d: %+v", len(deps), len(want), deps)
	}
	for _, d := range deps {
		uses := versionuse.Of(model.VulnScanDep{PackageManager: d.Manager, Name: d.Name, CurrentVersion: d.Version, Requirement: d.Requirement}, nil)
		if len(uses) != 1 || uses[0].Resolution != want[d.Name] {
			t.Errorf("%s (requirement %q, version %q) classed %+v, want %q", d.Name, d.Requirement, d.Version, uses, want[d.Name])
		}
		if d.Name == "dd" && d.Requirement != "<= 4.0" {
			t.Errorf("dd's requirement = %q; an upper bound is kept as written", d.Requirement)
		}
	}
}

// R compares 1.0-7 and 1.0.7 as one version; CRAN publishes either
// spelling. A declared version is found under the other.
func TestCRANResolverFindsAVersionUnderEitherSeparator(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"name":"MASS","latest":"7.3-61","timeline":{"7.3-60":"2023-05-04T00:00:00+00:00","7.3-61":"2024-06-13T00:00:00+00:00"},"versions":{}}`)
	}))
	defer srv.Close()
	defer swapBase(&cranRegistryBase, srv.URL)()
	row, err := resolveCRANLibyear(context.Background(), model.LibyearDep{Name: "MASS", Version: "7.3.60", Manager: "cran"})
	if err != nil {
		t.Fatal(err)
	}
	if row.VersionFound != model.VersionOnRegistry || row.CurrentReleaseDate != "2023-05-04T00:00:00+00:00" || row.Libyear <= 0 {
		t.Errorf("7.3.60 against a registry that lists 7.3-60: found %d, dated %q, libyear %v", row.VersionFound, row.CurrentReleaseDate, row.Libyear)
	}
	// The row keeps the version as declared: the purl is the finding's identity.
	if row.CurrentVersion != "7.3.60" || row.Purl != "pkg:cran/MASS@7.3.60" || row.License != "" {
		t.Errorf("row = version %q, purl %q, licence %q", row.CurrentVersion, row.Purl, row.License)
	}
}

// The live canary for crandb (network-gated, as every registry's is): the
// fixture is a capture, and this is what says the service still answers in
// that shape, by the exact name, and refuses another case of it.
func TestLiveCrandbForCommonmark(t *testing.T) {
	skipUnlessNetwork(t)
	lb, err := resolveCRANLibyear(context.Background(), model.LibyearDep{Name: "commonmark", Version: "1.9.2", Manager: "cran"})
	if err != nil {
		t.Fatalf("live crandb resolve failed: %v", err)
	}
	if lb.LatestVersion == "" || lb.CurrentReleaseDate == "" || lb.VersionFound != model.VersionOnRegistry || lb.Libyear <= 0 || lb.License == "" {
		t.Errorf("live crandb answer: %+v", lb)
	}
	if _, err := resolveCRANLibyear(context.Background(), model.LibyearDep{Name: "Commonmark", Manager: "cran"}); !errors.Is(err, errRegistryNotFound) {
		t.Errorf("another case of the name: %v; want not found (the name is case-sensitive)", err)
	}
}
