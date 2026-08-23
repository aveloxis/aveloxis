// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"
)

// WeeklyDataPoint is a single week's aggregated count for a metric.
type WeeklyDataPoint struct {
	WeekStart time.Time `json:"week_start"`
	Count     int       `json:"count"`
}

// TimeSeriesResult holds weekly time-series data for multiple metrics.
type TimeSeriesResult struct {
	RepoID    int64             `json:"repo_id"`
	RepoName  string            `json:"repo_name"`
	RepoOwner string            `json:"repo_owner"`
	Commits   []WeeklyDataPoint `json:"commits"`
	PRsOpened []WeeklyDataPoint `json:"prs_opened"`
	PRsMerged []WeeklyDataPoint `json:"prs_merged"`
	Issues    []WeeklyDataPoint `json:"issues"`
}

// GetRepoTimeSeries returns weekly aggregated counts for a repo's key metrics
// between `since` and `until` (inclusive lower, exclusive upper).
// A zero `until` is treated as "no upper bound" (queries up to the latest data).
// Uses date_trunc('week', timestamp) for consistent Monday-aligned weeks.
//
// v0.27.36: every query/scan error propagates. The pre-fix structure
// swallowed all four query errors and always returned nil, so a DB
// failure rendered as an empty chart indistinguishable from a genuinely
// inactive repo (summary/18 Phase 0b).
func (s *PostgresStore) GetRepoTimeSeries(ctx context.Context, repoID int64, since, until time.Time) (*TimeSeriesResult, error) {
	result := &TimeSeriesResult{RepoID: repoID}

	// Get repo name for labels.
	if err := s.pool.QueryRow(ctx,
		`SELECT repo_name, repo_owner FROM aveloxis_data.repos WHERE repo_id = $1`,
		repoID).Scan(&result.RepoName, &result.RepoOwner); err != nil {
		return nil, fmt.Errorf("time series repo lookup: %w", err)
	}

	// A zero `until` is represented as a far-future timestamp so the SQL
	// queries can remain parameterized identically regardless of whether the
	// caller specified an upper bound.
	upper := until
	if until.IsZero() {
		upper = time.Now().AddDate(100, 0, 0)
	}

	var err error
	// Weekly commits (from the commits table — one row per file, so count distinct hashes).
	result.Commits, err = s.weeklySeries(ctx, `
		SELECT date_trunc('week', cmt_author_timestamp AT TIME ZONE 'UTC') AS week_start,
			COUNT(DISTINCT cmt_commit_hash) AS cnt
		FROM aveloxis_data.commits
		WHERE repo_id = $1 AND cmt_author_timestamp >= $2 AND cmt_author_timestamp < $3
		  AND cmt_author_timestamp IS NOT NULL
		GROUP BY week_start
		ORDER BY week_start`, repoID, since, upper)
	if err != nil {
		return nil, fmt.Errorf("weekly commits: %w", err)
	}

	result.PRsOpened, err = s.weeklySeries(ctx, `
		SELECT date_trunc('week', created_at AT TIME ZONE 'UTC') AS week_start,
			COUNT(*) AS cnt
		FROM aveloxis_data.pull_requests
		WHERE repo_id = $1 AND created_at >= $2 AND created_at < $3
		  AND created_at IS NOT NULL
		GROUP BY week_start
		ORDER BY week_start`, repoID, since, upper)
	if err != nil {
		return nil, fmt.Errorf("weekly PRs opened: %w", err)
	}

	result.PRsMerged, err = s.weeklySeries(ctx, `
		SELECT date_trunc('week', merged_at AT TIME ZONE 'UTC') AS week_start,
			COUNT(*) AS cnt
		FROM aveloxis_data.pull_requests
		WHERE repo_id = $1 AND merged_at >= $2 AND merged_at < $3
		  AND merged_at IS NOT NULL
		GROUP BY week_start
		ORDER BY week_start`, repoID, since, upper)
	if err != nil {
		return nil, fmt.Errorf("weekly PRs merged: %w", err)
	}

	result.Issues, err = s.weeklySeries(ctx, `
		SELECT date_trunc('week', created_at AT TIME ZONE 'UTC') AS week_start,
			COUNT(*) AS cnt
		FROM aveloxis_data.issues
		WHERE repo_id = $1 AND created_at >= $2 AND created_at < $3
		  AND created_at IS NOT NULL
		GROUP BY week_start
		ORDER BY week_start`, repoID, since, upper)
	if err != nil {
		return nil, fmt.Errorf("weekly issues: %w", err)
	}

	return result, nil
}

// weeklySeries runs one weekly-bucketed aggregate query and scans the
// points, surfacing every query, scan, and iteration error.
func (s *PostgresStore) weeklySeries(ctx context.Context, query string, repoID int64, since, upper time.Time) ([]WeeklyDataPoint, error) {
	rows, err := s.pool.Query(ctx, query, repoID, since, upper)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []WeeklyDataPoint
	for rows.Next() {
		var dp WeeklyDataPoint
		if err := rows.Scan(&dp.WeekStart, &dp.Count); err != nil {
			return nil, err
		}
		out = append(out, dp)
	}
	return out, rows.Err()
}

// LicenseCount is a single license with its count and OSI compliance status.
type LicenseCount struct {
	License string `json:"license"`
	Count   int    `json:"count"`
	IsOSI   bool   `json:"is_osi"`
}

// osiLicenses is the set of OSI-approved SPDX license identifiers.
// Only canonical SPDX forms are listed here — synonym normalization happens
// in NormalizeLicenseToSPDX() before this map is consulted.
// Source: https://opensource.org/licenses/
var osiLicenses = map[string]bool{
	"MIT": true, "Apache-2.0": true, "GPL-2.0-only": true,
	"GPL-3.0-only": true, "LGPL-2.1-only": true,
	"LGPL-3.0-only": true, "BSD-2-Clause": true, "BSD-3-Clause": true,
	"ISC": true, "MPL-2.0": true, "CDDL-1.0": true, "EPL-1.0": true, "EPL-2.0": true,
	"AGPL-3.0-only": true, "Artistic-2.0": true, "Zlib": true,
	"Unlicense": true, "0BSD": true, "BSL-1.0": true, "PostgreSQL": true,
	"OFL-1.1": true, "NCSA": true, "MulanPSL-2.0": true, "EUPL-1.2": true,
	"CC0-1.0": true, "BlueOak-1.0.0": true, "UPL-1.0": true, "PSF-2.0": true,
	// v0.28.8 (operator correction, SPDX-verified): EVERY LGPL
	// version is OSI-approved — the SPDX license list
	// (https://spdx.org/licenses/) marks LGPL-2.0/2.1/3.0 in both
	// -only and -or-later forms isOsiApproved=true (verified against
	// spdx/license-list-data 2026-08-23; only the unrelated LGPLLR is
	// not, and nothing here maps to it). That makes the bare "LGPL"
	// family bucket approved with NO caveat: whichever version the
	// unspecified metadata means, it is OSI-approved. An earlier
	// comment claimed LGPL-2.0 wasn't on the OSI list — wrong, and
	// the hedge invited a false compliance-gap review finding.
	"LGPL":              true,
	"LGPL-2.0-only":     true,
	"LGPL-2.0-or-later": true, "LGPL-2.1-or-later": true,
	"LGPL-3.0-or-later": true,
	"GPL-2.0-or-later":  true, "GPL-3.0-or-later": true,
	"AGPL-3.0-or-later": true,
	// v0.28.1: unversioned family labels for families with NO SPDX
	// "any version" expression (the or-later suffix exists only for
	// the GNU family). Produced by version-less upstream
	// declarations; safe to approve because every released version
	// of these families is OSI-approved.
	"EPL": true, "Artistic": true,
}

// normalizeLicense maps license strings to canonical SPDX identifiers.
// Unifies common synonyms (e.g., "MIT License" → "MIT", "Apache 2.0" → "Apache-2.0")
// and maps "no license" sentinels to "Unknown".
func normalizeLicense(license string) string {
	return NormalizeLicenseToSPDX(license)
}

// GetRepoLicenses returns a summary of dependency licenses for a repo,
// with counts and OSI compliance indicators. Dependencies with no declared
// license (empty, whitespace, or sentinel values like NOASSERTION) are
// grouped under "Unknown".
func (s *PostgresStore) GetRepoLicenses(ctx context.Context, repoID int64) ([]LicenseCount, error) {
	return s.GetRepoLicensesScoped(ctx, repoID, false)
}

// licenseRowUniverseSQL is the ONE spelling (SR-17) of the licenses
// table's row-universe predicates, shared by the aggregate
// (GetRepoLicensesScoped) and the drill-down (DepsFiltered) so the
// "clicking a bucket of N lists exactly N rows" reconciliation cannot
// drift when either side is edited. Positional contract: $1 = repo_id
// (consumed by the caller's WHERE), $2 = runtimeOnly. v0.27.47: the
// githubactions exclusion applies in BOTH scopes — those rows carry no
// license data, and counting them as 'Unknown' would pollute the
// compliance view (runtime already excludes them via type='build').
const licenseRowUniverseSQL = `
		  AND ($2 = FALSE OR COALESCE(type, '') NOT IN ('dev','test','build','optional','peer'))
		  AND package_manager <> 'githubactions'`

// GetRepoLicensesScoped is GetRepoLicenses with an optional
// runtime-scope filter (v0.27.46, summary/19 P3 — decision #8:
// license COMPLIANCE obligations attach overwhelmingly to distributed
// runtime deps; dev tooling licenses are informational). runtimeOnly
// excludes rows whose type is a known non-runtime scope; "" and
// unrecognized values count as runtime (the IsRuntimeScope semantic,
// expressed in SQL).
func (s *PostgresStore) GetRepoLicensesScoped(ctx context.Context, repoID int64, runtimeOnly bool) ([]LicenseCount, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT COALESCE(NULLIF(TRIM(license), ''), 'Unknown') AS lic,
			COUNT(*) AS cnt
		FROM aveloxis_data.repo_deps_libyear
		WHERE repo_id = $1`+licenseRowUniverseSQL+`
		GROUP BY lic
		ORDER BY cnt DESC`, repoID, runtimeOnly)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Aggregate: SQL handles empty/whitespace → 'Unknown', Go normalizes
	// sentinel values (NOASSERTION, NONE, etc.) that SQL doesn't catch.
	counts := make(map[string]int)
	for rows.Next() {
		var lic string
		var cnt int
		if err := rows.Scan(&lic, &cnt); err != nil {
			return nil, err
		}
		counts[normalizeLicense(lic)] += cnt
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	var result []LicenseCount
	for lic, cnt := range counts {
		result = append(result, LicenseCount{
			License: lic,
			Count:   cnt,
			IsOSI:   isOSILicense(lic),
		})
	}
	// Sort by count descending for stable output.
	slices.SortFunc(result, func(a, b LicenseCount) int {
		if a.Count != b.Count {
			return b.Count - a.Count // descending
		}
		return strings.Compare(a.License, b.License)
	})
	return result, nil
}

// isOSILicense checks if a license string matches a known OSI-approved license.
// The input should already be normalized via NormalizeLicenseToSPDX.
//
// v0.28.1: multi-license declarations are stored joined with " AND "
// (the v0.27.29 storage decision; the SBOM generator's
// makeCDXLicenses splits on the same separator). A compound
// expression is approved iff EVERY part — individually re-normalized,
// since synonym forms can appear inside a compound — is approved; an
// empty part never counts, so "MIT AND " can't slip through.
func isOSILicense(license string) bool {
	if osiLicenses[license] {
		return true
	}
	if !strings.Contains(license, " AND ") {
		return false
	}
	for _, part := range strings.Split(license, " AND ") {
		part = strings.TrimSpace(part)
		if part == "" || !osiLicenses[NormalizeLicenseToSPDX(part)] {
			return false
		}
	}
	return true
}
