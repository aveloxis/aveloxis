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
	"github.com/aveloxis/aveloxis/internal/model"
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
	DependencyKind string `json:"dependency_kind,omitempty"`
	// IntroducedBy — v0.27.133 (C2): for TRANSITIVE findings, the
	// direct roots that pull the package in, each with one shortest
	// chain root → … → vulnerable package. Absent when no edge data
	// exists (knob off, edge-less lockfile format) — the GUI must
	// treat absence as "attribution unavailable", never "no parents".
	IntroducedBy    []vulnChainJSON `json:"introduced_by,omitempty"`
	// IntroducedByTotalRoots — v0.27.148 (round 27): the TRUE count of
	// distinct direct roots pulling this package in. introduced_by is
	// capped at 3 emitted chains; without the total a consumer cannot
	// distinguish "exactly 3 roots" from "30 roots, showing 3" and may
	// present a truncated remediation set as complete. 0 when no chain
	// resolved (field omitted).
	IntroducedByTotalRoots int `json:"introduced_by_total_roots,omitempty"`
	DependencyScope string          `json:"dependency_scope,omitempty"`
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
	// v0.27.133 C2: attribution index, built ONCE per request and only
	// when a transitive finding exists (edges are absent with the knob
	// off — zero cost on the default path). Best-effort: a lookup
	// failure degrades to no attribution, never a 500.
	var chains *chainIndex
	for _, v := range rows {
		if v.DependencyKind == "transitive" && v.ResolvedAt == nil {
			edges, eerr := s.store.GetRepoLockfileEdges(r.Context(), repoID)
			if eerr != nil {
				s.logger.Warn("lockfile edge lookup failed — findings served without attribution", "repo_id", repoID, "error", eerr)
				break
			}
			if len(edges) == 0 {
				break
			}
			directSets, derr := s.store.GetRepoDirectPackageSets(r.Context(), repoID)
			if derr != nil {
				s.logger.Warn("direct-set lookup failed — findings served without attribution", "repo_id", repoID, "error", derr)
				break
			}
			chains = buildChainIndex(edges, directSets)
			break
		}
	}

	out := make([]vulnJSON, 0, len(rows))
	current, resolved, critical := 0, 0, 0
	// v0.27.21 C1 count split (CURRENT findings only): a repo with 3
	// direct and 400 transitive findings must never read as "403
	// vulnerabilities" — the GUI leads with direct. Pre-C1 rows
	// ('' kind) count as direct (they were, by construction).
	directCount, transitiveCount, devCount := 0, 0, 0
	// v0.27.29: kind='self' = advisories against the repo's OWN
	// published releases (versionless — the numpy fix). They are
	// lifecycle-current forever, so they get their own counter and
	// stay OUT of current/critical/direct — a project's historical
	// advisories must never read as live dependency exposure.
	selfCount := 0
	for _, v := range rows {
		osvURL, cveURL := advisoryURLs(v.VulnID, v.CVEID)
		if v.ResolvedAt == nil {
			if v.DependencyKind == "self" {
				selfCount++
			} else {
				current++
				if v.Severity == "CRITICAL" || v.CVSSScore >= 9.0 {
					critical++
				}
				if v.DependencyKind == "transitive" {
					transitiveCount++
				} else {
					directCount++
				}
				if !model.IsRuntimeScope(v.DependencyScope) {
					devCount++
				}
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
		// Round-19: CURRENT findings only. The chain index is built
		// from the CURRENT lockfile edges; a resolved-historical
		// finding describes an OLDER snapshot, so attaching today's
		// graph to it could name a path that never produced the
		// finding (and implies live exposure where none exists).
		// Historical edge snapshots are not retained — honest absence.
		if chains != nil && v.DependencyKind == "transitive" && v.ResolvedAt == nil {
			out[len(out)-1].IntroducedBy, out[len(out)-1].IntroducedByTotalRoots = chains.chainsFor(v.Ecosystem, v.PackageName)
		}
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
	jsonResponse(w, map[string]any{
		"repo_id":            repoID,
		"vulnerabilities":    out,
		"lockfile_certainty": certainty,
		"counts": map[string]int{
			"current": current, "resolved": resolved, "critical": critical,
			"direct": directCount, "transitive": transitiveCount, "dev": devCount,
			// v0.27.46: runtime = current findings on runtime-scope
			// deps (the headline the GUI leads with).
			"runtime": current - devCount,
			"self":    selfCount,
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
		// v0.27.29: self-advisories describe the repo's OWN releases,
		// not this SBOM's dependency components — their versionless
		// purls match no component bom-ref, so affects.ref would
		// dangle. The SBOM vulnerability array stays dependency-only.
		if v.DependencyKind == "self" {
			continue
		}
		osvURL, _ := advisoryURLs(v.VulnID, v.CVEID)
		entry := map[string]any{
			"id":     v.VulnID,
			"source": map[string]any{"name": v.Source, "url": osvURL},
			"ratings": []map[string]any{{
				"score":    v.CVSSScore,
				"severity": cdxSeverity(v.Severity),
				"method":   cvssMethodFromVector(v.CVSSVector),
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

// annotateSPDXWithVulns attaches the repo's CURRENT (unresolved)
// findings to a generated SPDX 2.3 document as package-level
// externalRefs (referenceCategory SECURITY, referenceType advisory —
// the spec's conformant vehicle; SPDX 2.3 has no native vulnerability
// section, which is why ?vulns=1 used to 400 for spdx). Findings
// match their package by purl first, then by package name; findings
// matching no package are skipped (an SPDX externalRef must hang off
// a real package — there is no dangling-ref form). Self-advisories
// and resolved-historical findings are excluded, mirroring the
// CycloneDX annotation. SPDX 3.0's security profile is the eventual
// richer home.
func annotateSPDXWithVulns(sbom []byte, vulns []*db.VulnerabilityRow) ([]byte, error) {
	var doc map[string]any
	if err := json.Unmarshal(sbom, &doc); err != nil {
		return nil, err
	}
	byPurl := map[string][]*db.VulnerabilityRow{}
	byName := map[string][]*db.VulnerabilityRow{}
	for _, v := range vulns {
		if v.ResolvedAt != nil || v.DependencyKind == "self" {
			continue
		}
		if v.PackagePurl != "" {
			byPurl[v.PackagePurl] = append(byPurl[v.PackagePurl], v)
		}
		if v.PackageName != "" {
			byName[v.PackageName] = append(byName[v.PackageName], v)
		}
	}
	pkgs, _ := doc["packages"].([]any)
	for _, p := range pkgs {
		pkg, ok := p.(map[string]any)
		if !ok || pkg["SPDXID"] == "SPDXRef-RootPackage" {
			continue
		}
		refs, _ := pkg["externalRefs"].([]any)
		purl := ""
		for _, r := range refs {
			if ref, ok := r.(map[string]any); ok && ref["referenceType"] == "purl" {
				purl, _ = ref["referenceLocator"].(string)
			}
		}
		matched := byPurl[purl]
		if len(matched) == 0 {
			if name, _ := pkg["name"].(string); name != "" {
				matched = byName[name]
			}
		}
		seen := map[string]bool{}
		for _, v := range matched {
			osvURL, _ := advisoryURLs(v.VulnID, v.CVEID)
			if osvURL == "" || seen[osvURL] {
				continue
			}
			seen[osvURL] = true
			refs = append(refs, map[string]any{
				"referenceCategory": "SECURITY",
				"referenceType":     "advisory",
				"referenceLocator":  osvURL,
			})
		}
		if len(refs) > 0 {
			pkg["externalRefs"] = refs
		}
	}
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
	jsonResponse(w, resp)
}

// cvssMethodFromVector derives the CycloneDX rating method from the
// stored vector's prefix. v0.27.39 (summary/18 Phase 2 + the operator's
// standards-conformance direction): the method was hardcoded "CVSSv31"
// even when the score was computed from a v3.0 or v2 vector — wrong
// provenance in the exported document.
func cvssMethodFromVector(vector string) string {
	switch {
	case strings.HasPrefix(vector, "CVSS:3.1"):
		return "CVSSv31"
	case strings.HasPrefix(vector, "CVSS:3.0"):
		return "CVSSv3"
	case strings.HasPrefix(vector, "CVSS:4"):
		return "CVSSv4"
	case strings.HasPrefix(vector, "AV:"): // v2 vectors have no CVSS: prefix
		return "CVSSv2"
	default:
		return "other"
	}
}
