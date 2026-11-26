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
	"strings"
	"time"

	"github.com/aveloxis/aveloxis/internal/collector"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/vulnclass"
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
	IntroducedBy []vulnChainJSON `json:"introduced_by,omitempty"`
	// IntroducedByTotalRoots — v0.27.148 (round 27): the TRUE count of
	// distinct direct roots pulling this package in. introduced_by is
	// capped at 3 emitted chains; without the total a consumer cannot
	// distinguish "exactly 3 roots" from "30 roots, showing 3" and may
	// present a truncated remediation set as complete. 0 when no chain
	// resolved (field omitted).
	IntroducedByTotalRoots int    `json:"introduced_by_total_roots,omitempty"`
	DependencyScope        string `json:"dependency_scope,omitempty"`

	// 0.29.196 (summary/70, 4.8): decided by vulnclass, not by the client.
	// Class is the finding's one class (closed, self,
	// version_not_determined, exposure). Evidence is an exposure finding's
	// grade (in_use, lowest_allowed, floating_ref, not_classified); absent
	// for every other class. ClosedReason is why a closed finding closed
	// when that was not a fix; absent for a current finding and for an
	// ordinary resolution.
	Class        string `json:"class"`
	Evidence     string `json:"evidence,omitempty"`
	ClosedReason string `json:"closed_reason,omitempty"`
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

func (s *Server) handleRepoVulnerabilities(w http.ResponseWriter, r *http.Request) {
	repoID, ok := s.repoFromPath(w, r)
	if !ok {
		return
	}
	rows, err := s.store.GetRepoVulnerabilities(r.Context(), repoID)
	if err != nil {
		s.serverError(w, r, "handleRepoVulnerabilities", err)
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
				s.partialAnswer(r, eerr, "lockfile edge lookup failed — findings served without attribution", "repo_id", repoID, "error", eerr)
				break
			}
			if len(edges) == 0 {
				break
			}
			directSets, derr := s.store.GetRepoDirectPackageSets(r.Context(), repoID)
			if derr != nil {
				s.partialAnswer(r, derr, "direct-set lookup failed — findings served without attribution", "repo_id", repoID, "error", derr)
				break
			}
			chains = buildChainIndex(edges, directSets)
			break
		}
	}

	out := make([]vulnJSON, 0, len(rows))
	counts := vulnCounts(rows)
	for _, v := range rows {
		out = append(out, vulnJSONOf(v))
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
		s.partialAnswer(r, cerr, "lockfile certainty lookup failed", "repo_id", repoID, "error", cerr)
		certainty = &db.LockfileCertainty{Overall: "none", Ecosystems: []db.LockfileEcosystemCertainty{}}
	}
	// v0.28.1 (A4): the completed-scan stamp. null = never scanned —
	// the GUI must render that distinctly from a dated clean scan.
	// Best-effort: a lookup failure degrades to null, never a 500.
	scannedAt, serr := s.store.GetVulnScanLastRun(r.Context(), repoID)
	if serr != nil {
		s.partialAnswer(r, serr, "vuln scan stamp lookup failed", "repo_id", repoID, "error", serr)
		scannedAt = nil
	}
	resp := map[string]any{
		"repo_id":            repoID,
		"vulnerabilities":    out,
		"scanned_at":         scannedAt,
		"lockfile_certainty": certainty,
		"counts":             counts,
	}
	// 0.29.197 (summary/70, 4.8): the coverage state, decided by the one
	// function the statistics answers use. When an input cannot be read the
	// object is left out and the answer is partial: a client then shows
	// "coverage not known", never "not yet scanned" (SR-5).
	inputs, cerr := s.store.GetRepoCoverageInputs(r.Context(), []int64{repoID})
	if cerr != nil {
		s.partialAnswer(r, cerr, "coverage lookup failed", "repo_id", repoID, "error", cerr)
	} else if in, found := inputs[repoID]; found {
		in.CurrentExposure = counts["current"]
		resp["coverage"] = vulnclass.Coverage(in)
	}
	s.writeJSON(w, r, resp)
}

// vulnJSONOf is one finding as the API sends it, less the chains the
// handler adds from the lockfile edges. Its class, its evidence grade and
// its closing reason are decided by vulnclass, so a client that keys on them
// cannot disagree with the server's counts (0.29.196).
func vulnJSONOf(v *model.VulnerabilityRow) vulnJSON {
	osvURL, cveURL := collector.AdvisoryURLs(v.VulnID, v.CVEID)
	return vulnJSON{
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
		Class:               string(vulnclass.Of(v)),
		Evidence:            vulnclass.Evidence(v),
		ClosedReason:        v.ClosedReason,
	}
}

// vulnCounts splits a repository's findings for the payload's counts.
// current/critical/direct/transitive/dev/runtime count EXPOSURE only
// (vulnclass.Exposure): a repo with 3 direct and 400 transitive findings
// never reads as "403" (v0.27.21 C1 — the GUI leads with direct; a pre-C1
// row with an empty kind counts as direct); the project's own release advisories count as
// self (v0.27.29); and the unknown-version advisories of unpinned
// dependencies (v0.29.70 — OSV was asked without a version, so every
// advisory for the package came back) count as unknown_version, with
// their critical subset apart. runtime = exposure on runtime-scope deps
// (v0.27.46, the GUI's headline).
func vulnCounts(rows []*model.VulnerabilityRow) map[string]int {
	c := map[string]int{"current": 0, "resolved": 0, "critical": 0, "direct": 0, "transitive": 0,
		"dev": 0, "runtime": 0, "self": 0, "unknown_version": 0, "unknown_version_critical": 0}
	// "resolved" is every closed finding; its parts say which are fixes
	// (resolved_unlabelled) and which closed for a reason that is not one.
	// The parts always sum to it.
	c[vulnclass.ResolvedUnlabelledKey], c[vulnclass.ResolvedOtherReasonKey] = 0, 0
	for _, reason := range vulnclass.ClosedReasons {
		c[vulnclass.ResolvedCountKey(reason)] = 0
	}
	for _, v := range rows {
		critical := vulnclass.IsCritical(v)
		switch vulnclass.Of(v) {
		case vulnclass.Closed:
			c["resolved"]++
			c[vulnclass.ResolvedCountKey(v.ClosedReason)]++
		case vulnclass.Self:
			c["self"]++
		case vulnclass.VersionNotDetermined:
			c["unknown_version"]++
			if critical {
				c["unknown_version_critical"]++
			}
		default:
			c["current"]++
			if critical {
				c["critical"]++
			}
			if v.DependencyKind == "transitive" {
				c["transitive"]++
			} else {
				c["direct"]++
			}
			if model.IsRuntimeScope(v.DependencyScope) {
				c["runtime"]++
			} else {
				c["dev"]++
			}
		}
	}
	return c
}

// handleRepoScorecard serves the current OpenSSF Scorecard checks for
// a repo (v0.27.4 — repo page + comparison page tables). Thresholds
// are a GUI concern; scores ship raw (0–10, -1 = not applicable).
func (s *Server) handleRepoScorecard(w http.ResponseWriter, r *http.Request) {
	repoID, ok := s.repoFromPath(w, r)
	if !ok {
		return
	}
	checks, overall, asOf, mode, err := s.store.GetRepoScorecard(r.Context(), repoID)
	if err != nil {
		s.serverError(w, r, "handleRepoScorecard", err)
		return
	}
	resp := map[string]any{"repo_id": repoID, "checks": checks, "scanned": len(checks) > 0}
	if len(checks) > 0 {
		// 2026-09-12: which check SET this is. "remote" is the complete
		// set; "local" is the subset a clone alone can answer (GitLab and
		// generic-git repos, or a GitHub repo whose only successful runs
		// were local) — a consumer must not read a missing Code-Review
		// row as "no code review" on a local set. "" = pre-v0.27.5 rows.
		resp["mode"] = mode
	}
	if overall != nil {
		// The headline aggregate. Absent on scans that predate v0.27.4;
		// heals on the repo's next scorecard run.
		resp["overall"] = *overall
	}
	if !asOf.IsZero() {
		resp["as_of"] = asOf
	}
	s.writeJSON(w, r, resp)
}
