// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

// v0.27.4 — per-repo vulnerability surface for the GUI:
//
//	GET /api/v1/repos/{repoID}/vulnerabilities
//
// Full finding list ordered most-critical-first, current findings
// before resolved-historical ones (the v0.27.4 lifecycle: rows are
// never deleted; a complete scan that stops reporting a finding
// stamps resolved_at). Each finding carries advisory links the GUI
// renders directly — osv.dev works for every id; opencve for CVEs.
//
// The same file houses the CycloneDX vulnerability annotation used by
// GET /api/v1/repos/{id}/sbom?vulns=1 (operator request: "a download
// of the SBOM that also includes an annotation of CVE's against parts
// of the CURRENT SBOM").

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
)

type vulnJSON struct {
	VulnID      string  `json:"vuln_id"`
	CVEID       string  `json:"cve_id,omitempty"`
	PackageName string  `json:"package_name"`
	PackagePurl string  `json:"package_purl,omitempty"`
	Ecosystem   string  `json:"ecosystem,omitempty"`
	Severity    string  `json:"severity"`
	CVSSScore   float64 `json:"cvss_score"`
	CVSSVector  string  `json:"cvss_vector,omitempty"`
	Summary     string  `json:"summary"`
	Details     string  `json:"details,omitempty"`
	// ScannedVersion (v0.27.14) is the version the scan actually ran
	// against, derived from the purl — the GUI's Version column pairs
	// it with the version-resolution badge so "scanned 4.17.21" is
	// never read as "installed 4.17.21" on range-declared deps.
	ScannedVersion    string          `json:"scanned_version,omitempty"`
	FixedVersion      string          `json:"fixed_version,omitempty"`
	IntroducedVersion string          `json:"introduced_version,omitempty"`
	Source            string          `json:"source"`
	Aliases           []string        `json:"aliases,omitempty"`
	References        json.RawMessage `json:"references,omitempty"`
	AdvisoryURL       string          `json:"advisory_url"`
	CVEURL            string          `json:"cve_url,omitempty"`
	FirstDetectedAt   time.Time       `json:"first_detected_at"`
	LastSeenAt        time.Time       `json:"last_seen_at"`
	ResolvedAt        *time.Time      `json:"resolved_at,omitempty"` // null = currently affected

	// v0.27.11 version-resolution accuracy: the raw manifest
	// requirement ("apache-airflow>=3.0.0") and how the scanned
	// version was chosen — locked / exact / bounded-range /
	// range-floor / unpinned. Absent on pre-v0.27.11 rows (heals on
	// the repo's next scan); the GUI renders e.g.
	// "≥3.0.0 declared — floor shown" for range-floor.
	DeclaredRequirement string `json:"declared_requirement,omitempty"`
	VersionResolution   string `json:"version_resolution,omitempty"`
	// v0.27.21 C1: 'direct' | 'transitive' ('' = pre-C1 row, rendered
	// as direct) + 'dev'/'runtime'/'' scope from the lockfile.
	DependencyKind  string `json:"dependency_kind,omitempty"`
	DependencyScope string `json:"dependency_scope,omitempty"`
}

// scannedVersionFromPurl derives the version a finding was scanned at
// from its package purl: everything after the LAST '@'. This is
// npm-scope-safe — a scope's '@' ("pkg:npm/@babel/traverse@7.23.2")
// always precedes the final '/', while the version separator always
// follows it, so a last-'@' that sits before the last '/' means the
// purl carries no version at all. Qualifiers ("?type=jar") and
// subpaths ("#lib") after the version are stripped. Empty when the
// purl has no version.
func scannedVersionFromPurl(purl string) string {
	// Strip qualifiers/subpath FIRST — a "#lib/utils" subpath contains
	// a '/' that would otherwise defeat the scope guard below.
	if i := strings.IndexAny(purl, "?#"); i >= 0 {
		purl = purl[:i]
	}
	lastAt := strings.LastIndex(purl, "@")
	if lastAt <= 0 || lastAt < strings.LastIndex(purl, "/") {
		return ""
	}
	return purl[lastAt+1:]
}

// advisoryURLs returns the canonical description pages for a finding:
// osv.dev resolves every id OSV emits (GHSA, PYSEC, GO-, RUSTSEC-…);
// opencve is the CVE-specific page the operator pointed at.
func advisoryURLs(vulnID, cveID string) (osv, cve string) {
	osv = "https://osv.dev/vulnerability/" + vulnID
	if cveID != "" {
		cve = "https://app.opencve.io/cve/" + cveID
	}
	return osv, cve
}

func (s *Server) handleRepoVulnerabilities(w http.ResponseWriter, r *http.Request) {
	repoID, err := strconv.ParseInt(r.PathValue("repoID"), 10, 64)
	if err != nil {
		http.Error(w, "invalid repo_id", http.StatusBadRequest)
		return
	}
	if !s.authorizeRepo(w, r, repoID) {
		return
	}
	rows, err := s.store.GetRepoVulnerabilities(r.Context(), repoID)
	if err != nil {
		http.Error(w, "vulnerability lookup failed", http.StatusInternalServerError)
		return
	}
	out := make([]vulnJSON, 0, len(rows))
	current, resolved, critical := 0, 0, 0
	// v0.27.21 C1 count split (CURRENT findings only): a repo with 3
	// direct and 400 transitive findings must never read as "403
	// vulnerabilities" — the GUI leads with direct. Pre-C1 rows
	// ('' kind) count as direct (they were, by construction).
	directCount, transitiveCount, devCount := 0, 0, 0
	for _, v := range rows {
		osvURL, cveURL := advisoryURLs(v.VulnID, v.CVEID)
		if v.ResolvedAt == nil {
			current++
			if v.Severity == "CRITICAL" || v.CVSSScore >= 9.0 {
				critical++
			}
			if v.DependencyKind == "transitive" {
				transitiveCount++
			} else {
				directCount++
			}
			if v.DependencyScope == "dev" {
				devCount++
			}
		} else {
			resolved++
		}
		out = append(out, vulnJSON{
			VulnID: v.VulnID, CVEID: v.CVEID,
			PackageName: v.PackageName, PackagePurl: v.PackagePurl,
			Ecosystem: v.Ecosystem, Severity: v.Severity,
			CVSSScore: v.CVSSScore, CVSSVector: v.CVSSVector,
			Summary: v.Summary, Details: v.Details,
			ScannedVersion: scannedVersionFromPurl(v.PackagePurl),
			FixedVersion:   v.FixedVersion, IntroducedVersion: v.IntroducedVersion,
			Source: v.Source, Aliases: v.Aliases,
			References:  json.RawMessage(v.ReferencesJSON),
			AdvisoryURL: osvURL, CVEURL: cveURL,
			FirstDetectedAt: v.FirstDetectedAt, LastSeenAt: v.LastSeenAt,
			ResolvedAt:          v.ResolvedAt,
			DeclaredRequirement: v.DeclaredRequirement,
			VersionResolution:   v.VersionResolution,
			DependencyKind:      v.DependencyKind,
			DependencyScope:     v.DependencyScope,
		})
	}
	// v0.27.11: repo-level lockfile certainty — derived at read time
	// (overall=full iff every ecosystem with dependencies also has a
	// lockfile; Go is locked by construction). Best-effort: a lookup
	// error degrades to "none" rather than failing the finding list.
	certainty, cerr := s.store.GetRepoLockfileCertainty(r.Context(), repoID)
	if cerr != nil {
		s.logger.Warn("lockfile certainty lookup failed", "repo_id", repoID, "error", cerr)
		certainty = &db.LockfileCertainty{Overall: "none", Ecosystems: []db.LockfileEcosystemCertainty{}}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"repo_id":            repoID,
		"vulnerabilities":    out,
		"lockfile_certainty": certainty,
		"counts": map[string]int{
			"current": current, "resolved": resolved, "critical": critical,
			"direct": directCount, "transitive": transitiveCount, "dev": devCount,
		},
	})
}

// annotateCycloneDXWithVulns appends a CycloneDX 1.5 `vulnerabilities`
// array to a generated SBOM, covering the repo's CURRENT (unresolved)
// findings. Components carry their purl as bom-ref, so affects.ref
// matches directly. Resolved-historical findings are excluded — the
// SBOM describes the repository as it stands.
func annotateCycloneDXWithVulns(sbom []byte, vulns []*db.VulnerabilityRow) ([]byte, error) {
	var doc map[string]any
	if err := json.Unmarshal(sbom, &doc); err != nil {
		return nil, err
	}
	entries := make([]map[string]any, 0, len(vulns))
	for _, v := range vulns {
		if v.ResolvedAt != nil {
			continue
		}
		osvURL, _ := advisoryURLs(v.VulnID, v.CVEID)
		entry := map[string]any{
			"id":     v.VulnID,
			"source": map[string]any{"name": v.Source, "url": osvURL},
			"ratings": []map[string]any{{
				"score":    v.CVSSScore,
				"severity": cdxSeverity(v.Severity),
				"method":   "CVSSv31",
				"vector":   v.CVSSVector,
			}},
			"description": v.Summary,
			"affects":     []map[string]any{{"ref": v.PackagePurl}},
		}
		if v.FixedVersion != "" {
			entry["recommendation"] = "Upgrade " + v.PackageName + " to " + v.FixedVersion + " or later"
		}
		entries = append(entries, entry)
	}
	doc["vulnerabilities"] = entries
	return json.MarshalIndent(doc, "", "  ")
}

// cdxSeverity maps OSV severity labels to CycloneDX's lowercase enum.
func cdxSeverity(s string) string {
	switch s {
	case "CRITICAL":
		return "critical"
	case "HIGH":
		return "high"
	case "MEDIUM", "MODERATE":
		return "medium"
	case "LOW":
		return "low"
	default:
		return "unknown"
	}
}

// handleRepoScorecard serves the current OpenSSF Scorecard checks for
// a repo (v0.27.4 — repo page + comparison page tables). Thresholds
// are a GUI concern; scores ship raw (0–10, -1 = not applicable).
func (s *Server) handleRepoScorecard(w http.ResponseWriter, r *http.Request) {
	repoID, err := strconv.ParseInt(r.PathValue("repoID"), 10, 64)
	if err != nil {
		http.Error(w, "invalid repo_id", http.StatusBadRequest)
		return
	}
	if !s.authorizeRepo(w, r, repoID) {
		return
	}
	checks, overall, asOf, err := s.store.GetRepoScorecard(r.Context(), repoID)
	if err != nil {
		http.Error(w, "scorecard lookup failed", http.StatusInternalServerError)
		return
	}
	resp := map[string]any{"repo_id": repoID, "checks": checks, "scanned": len(checks) > 0}
	if overall != nil {
		// The headline aggregate. Absent on scans that predate v0.27.4;
		// heals on the repo's next scorecard run.
		resp["overall"] = *overall
	}
	if !asOf.IsZero() {
		resp["as_of"] = asOf
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}
