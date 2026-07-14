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
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
)

type vulnJSON struct {
	VulnID            string          `json:"vuln_id"`
	CVEID             string          `json:"cve_id,omitempty"`
	PackageName       string          `json:"package_name"`
	PackagePurl       string          `json:"package_purl,omitempty"`
	Ecosystem         string          `json:"ecosystem,omitempty"`
	Severity          string          `json:"severity"`
	CVSSScore         float64         `json:"cvss_score"`
	CVSSVector        string          `json:"cvss_vector,omitempty"`
	Summary           string          `json:"summary"`
	Details           string          `json:"details,omitempty"`
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
	for _, v := range rows {
		osvURL, cveURL := advisoryURLs(v.VulnID, v.CVEID)
		if v.ResolvedAt == nil {
			current++
			if v.Severity == "CRITICAL" || v.CVSSScore >= 9.0 {
				critical++
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
			FixedVersion: v.FixedVersion, IntroducedVersion: v.IntroducedVersion,
			Source: v.Source, Aliases: v.Aliases,
			References:  json.RawMessage(v.ReferencesJSON),
			AdvisoryURL: osvURL, CVEURL: cveURL,
			FirstDetectedAt: v.FirstDetectedAt, LastSeenAt: v.LastSeenAt,
			ResolvedAt: v.ResolvedAt,
		})
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"repo_id":         repoID,
		"vulnerabilities": out,
		"counts": map[string]int{
			"current": current, "resolved": resolved, "critical": critical,
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
