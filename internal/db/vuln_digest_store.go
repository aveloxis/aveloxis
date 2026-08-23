// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// VulnDigestItem is one row of the operator vulnerability digest
// (v0.27.12): a finding first detected after the previous digest
// window, still unresolved, at or above the configured severity floor.
type VulnDigestItem struct {
	RepoID          int64
	RepoOwner       string
	RepoName        string
	VulnID          string
	Severity        string
	PackagePurl     string
	Summary         string
	FirstDetectedAt time.Time
}

// severityRank orders OSV severities for the digest floor. UNKNOWN
// ranks lowest — it only appears in a digest when the operator sets
// the floor to UNKNOWN (or the ALL alias) explicitly.
var severityRank = map[string]int{
	"CRITICAL": 4,
	"HIGH":     3,
	"MEDIUM":   2,
	"LOW":      1,
	"UNKNOWN":  0,
}

// SeveritiesAtOrAbove expands a minimum-severity floor into the set of
// severity labels it admits. Unrecognized input falls back to HIGH
// (CRITICAL+HIGH) — the conservative default for operator email. The
// "ALL" alias admits everything including UNKNOWN.
func SeveritiesAtOrAbove(min string) []string {
	min = strings.ToUpper(strings.TrimSpace(min))
	if min == "ALL" {
		min = "UNKNOWN"
	}
	floor, ok := severityRank[min]
	if !ok {
		floor = severityRank["HIGH"]
	}
	out := make([]string, 0, len(severityRank))
	for _, sev := range []string{"CRITICAL", "HIGH", "MEDIUM", "LOW", "UNKNOWN"} {
		if severityRank[sev] >= floor {
			out = append(out, sev)
		}
	}
	return out
}

// GetNewVulnerabilityFindings returns unresolved findings first
// detected AFTER since, at or above the minSeverity floor, most severe
// first. Feeds the operator digest email (v0.27.12). The digest never
// re-reports a finding: first_detected_at is stamped once at first
// observation and the caller advances `since` monotonically.
// v0.27.21: includeTransitive=false (the default) keeps
// dependency_kind='transitive' findings out of the digest — the first
// transitive-enabled cycles would otherwise blast a 50-item email of
// utility-package findings. Direct and pre-C1 ("") rows always pass.
// v0.27.46 (summary/19 P3, decision #1): includeDev=false (the
// default) additionally keeps non-runtime-scope findings
// (dev/test/build/optional/peer) out — the P2 Python expansion would
// otherwise flood the digest with dev-tooling findings the week the
// knob flips on. Runtime-scope ("" or unrecognized) rows always pass.
func (s *PostgresStore) GetNewVulnerabilityFindings(ctx context.Context, since time.Time, minSeverity string, includeTransitive, includeDev bool) ([]VulnDigestItem, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT v.repo_id, r.repo_owner, r.repo_name, v.vuln_id,
		       COALESCE(v.severity, 'UNKNOWN'), COALESCE(v.package_purl, ''),
		       COALESCE(v.summary, ''), v.first_detected_at
		FROM aveloxis_data.repo_deps_vulnerabilities v
		JOIN aveloxis_data.repos r ON r.repo_id = v.repo_id
		WHERE v.first_detected_at > $1
		  AND v.resolved_at IS NULL
		  AND ($3 OR COALESCE(v.dependency_kind, '') IS DISTINCT FROM 'transitive')
		  AND ($4 OR COALESCE(v.dependency_scope, '') NOT IN ('dev','test','build','optional','peer'))
		  -- v0.27.29: self-advisories (the repo's OWN releases,
		  -- versionless) never digest — they're historical record,
		  -- not new dependency exposure for the operator to act on.
		  AND COALESCE(v.dependency_kind, '') <> 'self'
		  AND UPPER(COALESCE(v.severity, 'UNKNOWN')) = ANY($2)
		ORDER BY CASE UPPER(COALESCE(v.severity, 'UNKNOWN'))
		           WHEN 'CRITICAL' THEN 4 WHEN 'HIGH' THEN 3
		           WHEN 'MEDIUM' THEN 2 WHEN 'LOW' THEN 1 ELSE 0
		         END DESC,
		         r.repo_owner, r.repo_name, v.vuln_id`,
		since, SeveritiesAtOrAbove(minSeverity), includeTransitive, includeDev)
	if err != nil {
		return nil, fmt.Errorf("query new vulnerability findings: %w", err)
	}
	defer rows.Close()

	var items []VulnDigestItem
	for rows.Next() {
		var it VulnDigestItem
		if err := rows.Scan(&it.RepoID, &it.RepoOwner, &it.RepoName, &it.VulnID,
			&it.Severity, &it.PackagePurl, &it.Summary, &it.FirstDetectedAt); err != nil {
			return nil, fmt.Errorf("scan digest item: %w", err)
		}
		items = append(items, it)
	}
	return items, rows.Err()
}
