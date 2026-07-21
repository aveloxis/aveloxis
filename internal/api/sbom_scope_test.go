// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.46 (summary/19 P3) — the scope-aware consumer surfaces:
// ?scope= on the SBOM and licenses endpoints, SPDX SECURITY/advisory
// externalRefs (replacing the old spdx+vulns=1 400), the digest dev
// gate, and the runtime counts split.

package api

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
)

func TestAnnotateSPDXWithVulnsAttachesSecurityRefs(t *testing.T) {
	sbom := []byte(`{
	  "spdxVersion": "SPDX-2.3",
	  "packages": [
	    {"SPDXID": "SPDXRef-RootPackage", "name": "app"},
	    {"SPDXID": "SPDXRef-Package-1", "name": "flask",
	     "externalRefs": [{"referenceCategory": "PACKAGE-MANAGER",
	                       "referenceType": "purl",
	                       "referenceLocator": "pkg:pypi/flask@2.0.0"}]},
	    {"SPDXID": "SPDXRef-Package-2", "name": "clean-dep",
	     "externalRefs": [{"referenceCategory": "PACKAGE-MANAGER",
	                       "referenceType": "purl",
	                       "referenceLocator": "pkg:pypi/clean-dep@1.0.0"}]}
	  ]
	}`)
	resolved := time.Now()
	vulns := []*db.VulnerabilityRow{
		{VulnID: "GHSA-live-1", PackageName: "flask", PackagePurl: "pkg:pypi/flask@2.0.0"},
		// Resolved-historical: must not attach.
		{VulnID: "GHSA-old-1", PackageName: "flask", PackagePurl: "pkg:pypi/flask@2.0.0", ResolvedAt: &resolved},
		// Self-advisory: must not attach.
		{VulnID: "GHSA-self-1", PackageName: "app", PackagePurl: "pkg:pypi/app", DependencyKind: "self"},
	}
	out, err := annotateSPDXWithVulns(sbom, vulns)
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Packages []struct {
			SPDXID       string `json:"SPDXID"`
			ExternalRefs []struct {
				ReferenceCategory string `json:"referenceCategory"`
				ReferenceType     string `json:"referenceType"`
				ReferenceLocator  string `json:"referenceLocator"`
			} `json:"externalRefs"`
		} `json:"packages"`
	}
	if err := json.Unmarshal(out, &doc); err != nil {
		t.Fatal(err)
	}
	secRefs := map[string][]string{}
	for _, p := range doc.Packages {
		for _, r := range p.ExternalRefs {
			if r.ReferenceCategory == "SECURITY" {
				if r.ReferenceType != "advisory" {
					t.Errorf("SECURITY ref must use referenceType advisory, got %q", r.ReferenceType)
				}
				secRefs[p.SPDXID] = append(secRefs[p.SPDXID], r.ReferenceLocator)
			}
		}
	}
	if len(secRefs["SPDXRef-Package-1"]) != 1 {
		t.Errorf("flask package must carry exactly 1 SECURITY ref (live finding only), got %v", secRefs["SPDXRef-Package-1"])
	}
	if len(secRefs["SPDXRef-Package-1"]) > 0 && !strings.Contains(secRefs["SPDXRef-Package-1"][0], "GHSA-live-1") {
		t.Errorf("SECURITY ref must link the advisory: %v", secRefs["SPDXRef-Package-1"])
	}
	if len(secRefs["SPDXRef-Package-2"]) != 0 {
		t.Errorf("clean-dep must carry no SECURITY refs, got %v", secRefs["SPDXRef-Package-2"])
	}
	if len(secRefs["SPDXRef-RootPackage"]) != 0 {
		t.Errorf("root package must never carry finding refs (self excluded), got %v", secRefs["SPDXRef-RootPackage"])
	}
	// The purl ref must survive alongside the new SECURITY refs.
	purlSurvives := false
	for _, p := range doc.Packages {
		for _, r := range p.ExternalRefs {
			if r.ReferenceType == "purl" && r.ReferenceLocator == "pkg:pypi/flask@2.0.0" {
				purlSurvives = true
			}
		}
	}
	if !purlSurvives {
		t.Error("annotation must append SECURITY refs, never replace the PACKAGE-MANAGER purl ref")
	}
}

func TestAnnotateSPDXWithVulnsFallsBackToNameMatch(t *testing.T) {
	// Locked-version scanning can leave the finding's purl different
	// from the stored dep purl; name matching is the fallback.
	sbom := []byte(`{"packages": [
	  {"SPDXID": "SPDXRef-Package-1", "name": "lodash",
	   "externalRefs": [{"referenceCategory": "PACKAGE-MANAGER",
	                     "referenceType": "purl",
	                     "referenceLocator": "pkg:npm/lodash"}]}
	]}`)
	vulns := []*db.VulnerabilityRow{
		{VulnID: "GHSA-x", PackageName: "lodash", PackagePurl: "pkg:npm/lodash@4.17.20"},
	}
	out, err := annotateSPDXWithVulns(sbom, vulns)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(out), `"SECURITY"`) {
		t.Error("name-match fallback failed: finding with locked-version purl must still attach to its package")
	}
}

// TestSBOMHandlerScopeAndSPDXVulns pins the handler wiring: the
// ?scope= parameter routes through GenerateSBOMWithOptions, SPDX
// vulns=1 goes through annotateSPDXWithVulns (the old 400 is GONE),
// and filenames gain the -runtime / -with-vulns markers.
func TestSBOMHandlerScopeAndSPDXVulns(t *testing.T) {
	src, err := os.ReadFile("server.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	for _, needle := range []string{
		`Get("scope")`,
		"GenerateSBOMWithOptions(",
		"annotateSPDXWithVulns(",
		"-runtime.spdx.json",
		"-with-vulns.spdx.json",
	} {
		if !strings.Contains(code, needle) {
			t.Errorf("server.go SBOM handler missing v0.27.46 wiring %q", needle)
		}
	}
	if strings.Contains(code, "vulns=1 is only supported for format=cyclonedx") {
		t.Error("the spdx+vulns=1 400 must be GONE — SPDX gets SECURITY/advisory externalRefs (v0.27.46)")
	}
}

// TestLicensesHandlerScopeParam pins the licenses ?scope= wiring +
// the store-side runtime filter with the IsRuntimeScope-in-SQL shape.
func TestLicensesHandlerScopeParam(t *testing.T) {
	src, err := os.ReadFile("server.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(src), "GetRepoLicensesScoped(") {
		t.Error("handleLicenses must route through GetRepoLicensesScoped")
	}
	store, err := os.ReadFile("../db/timeseries.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(store), "NOT IN ('dev','test','build','optional','peer')") {
		t.Error("GetRepoLicensesScoped must express IsRuntimeScope in SQL — '' and unknown values count as runtime")
	}
}

// TestDigestDevGate pins the digest's $4 scope gate + the counts
// envelope's runtime derivation.
func TestDigestDevGate(t *testing.T) {
	store, err := os.ReadFile("../db/vuln_digest_store.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(store)
	if !strings.Contains(code, "includeDev bool") {
		t.Error("GetNewVulnerabilityFindings must take includeDev (mail.vuln_digest_include_dev)")
	}
	if !strings.Contains(code, `($4 OR COALESCE(v.dependency_scope, '') NOT IN ('dev','test','build','optional','peer'))`) {
		t.Error("digest SQL must gate non-runtime scopes behind $4 — runtime rows always pass")
	}
	api, err := os.ReadFile("vulnerabilities.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(api), `"runtime": current - devCount`) {
		t.Error("counts envelope must derive the runtime split (current - devCount)")
	}
}

// TestSnapshotHeadlineIsRuntimeOnly pins the v0.27.46 semantic bump:
// UpstreamDependenciesSnapshot's headline covers runtime deps only,
// with total/dev companions in detail.
func TestSnapshotHeadlineIsRuntimeOnly(t *testing.T) {
	store, err := os.ReadFile("../db/analytics_store.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(store)
	for _, needle := range []string{
		`FILTER (WHERE COALESCE(type, '') NOT IN ('dev','test','build','optional','peer'))`,
		`"dev_count"`,
		`"total_count"`,
		`"dev_median_libyear"`,
	} {
		if !strings.Contains(code, needle) {
			t.Errorf("UpstreamDependenciesSnapshot missing v0.27.46 shape %q", needle)
		}
	}
}
