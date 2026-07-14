// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
)

func TestAdvisoryURLs(t *testing.T) {
	osv, cve := advisoryURLs("GHSA-abcd-1234", "CVE-2023-34053")
	if osv != "https://osv.dev/vulnerability/GHSA-abcd-1234" {
		t.Errorf("osv url: %s", osv)
	}
	if cve != "https://app.opencve.io/cve/CVE-2023-34053" {
		t.Errorf("cve url: %s", cve)
	}
	if _, cve := advisoryURLs("PYSEC-2024-1", ""); cve != "" {
		t.Error("no cve id must mean no opencve link — never fabricate one")
	}
}

func TestAnnotateCycloneDXWithVulns(t *testing.T) {
	sbom := []byte(`{"bomFormat":"CycloneDX","specVersion":"1.5","components":[{"bom-ref":"pkg:npm/a@1","purl":"pkg:npm/a@1"}]}`)
	resolvedAt := time.Now()
	rows := []*db.VulnerabilityRow{
		{VulnID: "GHSA-live", CVEID: "CVE-2024-1", PackageName: "a", PackagePurl: "pkg:npm/a@1",
			Severity: "CRITICAL", CVSSScore: 9.8, Summary: "bad", FixedVersion: "2.0", Source: "osv.dev"},
		{VulnID: "GHSA-gone", PackagePurl: "pkg:npm/b@1", Severity: "LOW",
			Source: "osv.dev", ResolvedAt: &resolvedAt},
	}
	out, err := annotateCycloneDXWithVulns(sbom, rows)
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Vulnerabilities []struct {
			ID      string `json:"id"`
			Ratings []struct {
				Severity string  `json:"severity"`
				Score    float64 `json:"score"`
			} `json:"ratings"`
			Affects        []struct{ Ref string } `json:"affects"`
			Recommendation string                 `json:"recommendation"`
		} `json:"vulnerabilities"`
		Components []any `json:"components"`
	}
	if err := json.Unmarshal(out, &doc); err != nil {
		t.Fatal(err)
	}
	if len(doc.Components) != 1 {
		t.Error("annotation must preserve the original SBOM content")
	}
	if len(doc.Vulnerabilities) != 1 || doc.Vulnerabilities[0].ID != "GHSA-live" {
		t.Fatalf("only CURRENT findings belong in the SBOM annotation, got %+v", doc.Vulnerabilities)
	}
	v := doc.Vulnerabilities[0]
	if v.Ratings[0].Severity != "critical" || v.Ratings[0].Score != 9.8 {
		t.Errorf("rating mapping wrong: %+v", v.Ratings)
	}
	if v.Affects[0].Ref != "pkg:npm/a@1" {
		t.Errorf("affects.ref must be the component purl/bom-ref, got %s", v.Affects[0].Ref)
	}
	if !strings.Contains(v.Recommendation, "2.0") {
		t.Errorf("recommendation must carry the fixed version, got %q", v.Recommendation)
	}
}

func TestStarRequiresIdentityAndScope(t *testing.T) {
	store := &fakeSessionStore{userID: 7, scope: []int64{42}, valid: map[string]bool{"tok": true}}
	s := &Server{}
	s.auth = newAuthenticator(store, false)

	// No token → 401 even though require_auth is off.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("PUT", "/api/v1/repos/42/star", nil)
	req.SetPathValue("repoID", "42")
	s.handleStarRepo(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("star without identity must 401, got %d", rec.Code)
	}

	// Out-of-scope repo → 403.
	rec = httptest.NewRecorder()
	req = httptest.NewRequest("PUT", "/api/v1/repos/999/star", nil)
	req.SetPathValue("repoID", "999")
	req.Header.Set("Authorization", "Bearer tok")
	s.handleStarRepo(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Errorf("starring an out-of-scope repo must 403, got %d", rec.Code)
	}
}

func TestV0274RoutesRegistered(t *testing.T) {
	src := mustReadFile(t, "server.go")
	for _, route := range []string{
		`"GET /api/v1/repos/{repoID}/vulnerabilities"`,
		`"PUT /api/v1/repos/{repoID}/star"`,
		`"DELETE /api/v1/repos/{repoID}/star"`,
		`"GET /api/v1/home/repos"`,
	} {
		if !strings.Contains(src, route) {
			t.Errorf("server.go must register %s", route)
		}
	}
	// The licenses handler must ship the scanned flag (GUI empty-state
	// disambiguation) and the SBOM handler the vulns=1 annotation path.
	for _, needle := range []string{`"scanned":`, `HasDependencyData`, `annotateCycloneDXWithVulns`, `Get("vulns")`} {
		if !strings.Contains(src, needle) {
			t.Errorf("server.go missing v0.27.4 wiring %q", needle)
		}
	}
}
