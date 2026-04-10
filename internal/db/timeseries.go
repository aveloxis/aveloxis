package db

import (
	"context"
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
	RepoID     int64              `json:"repo_id"`
	RepoName   string             `json:"repo_name"`
	RepoOwner  string             `json:"repo_owner"`
	Commits    []WeeklyDataPoint  `json:"commits"`
	PRsOpened  []WeeklyDataPoint  `json:"prs_opened"`
	PRsMerged  []WeeklyDataPoint  `json:"prs_merged"`
	Issues     []WeeklyDataPoint  `json:"issues"`
}

// GetRepoTimeSeries returns weekly aggregated counts for a repo's key metrics.
// Uses date_trunc('week', timestamp) for consistent Monday-aligned weeks.
// Optimized: queries against indexed timestamp columns, limits to recent data.
func (s *PostgresStore) GetRepoTimeSeries(ctx context.Context, repoID int64, since time.Time) (*TimeSeriesResult, error) {
	result := &TimeSeriesResult{RepoID: repoID}

	// Get repo name for labels.
	s.pool.QueryRow(ctx,
		`SELECT repo_name, repo_owner FROM aveloxis_data.repos WHERE repo_id = $1`,
		repoID).Scan(&result.RepoName, &result.RepoOwner)

	// Weekly commits (from the commits table — one row per file, so count distinct hashes).
	rows, err := s.pool.Query(ctx, `
		SELECT date_trunc('week', cmt_author_timestamp) AS week_start,
			COUNT(DISTINCT cmt_commit_hash) AS cnt
		FROM aveloxis_data.commits
		WHERE repo_id = $1 AND cmt_author_timestamp >= $2
		  AND cmt_author_timestamp IS NOT NULL
		GROUP BY week_start
		ORDER BY week_start`, repoID, since)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var dp WeeklyDataPoint
			if rows.Scan(&dp.WeekStart, &dp.Count) == nil {
				result.Commits = append(result.Commits, dp)
			}
		}
	}

	// Weekly PRs opened.
	rows2, err := s.pool.Query(ctx, `
		SELECT date_trunc('week', created_at) AS week_start,
			COUNT(*) AS cnt
		FROM aveloxis_data.pull_requests
		WHERE repo_id = $1 AND created_at >= $2
		  AND created_at IS NOT NULL
		GROUP BY week_start
		ORDER BY week_start`, repoID, since)
	if err == nil {
		defer rows2.Close()
		for rows2.Next() {
			var dp WeeklyDataPoint
			if rows2.Scan(&dp.WeekStart, &dp.Count) == nil {
				result.PRsOpened = append(result.PRsOpened, dp)
			}
		}
	}

	// Weekly PRs merged.
	rows3, err := s.pool.Query(ctx, `
		SELECT date_trunc('week', merged_at) AS week_start,
			COUNT(*) AS cnt
		FROM aveloxis_data.pull_requests
		WHERE repo_id = $1 AND merged_at >= $2
		  AND merged_at IS NOT NULL
		GROUP BY week_start
		ORDER BY week_start`, repoID, since)
	if err == nil {
		defer rows3.Close()
		for rows3.Next() {
			var dp WeeklyDataPoint
			if rows3.Scan(&dp.WeekStart, &dp.Count) == nil {
				result.PRsMerged = append(result.PRsMerged, dp)
			}
		}
	}

	// Weekly issues opened.
	rows4, err := s.pool.Query(ctx, `
		SELECT date_trunc('week', created_at) AS week_start,
			COUNT(*) AS cnt
		FROM aveloxis_data.issues
		WHERE repo_id = $1 AND created_at >= $2
		  AND created_at IS NOT NULL
		GROUP BY week_start
		ORDER BY week_start`, repoID, since)
	if err == nil {
		defer rows4.Close()
		for rows4.Next() {
			var dp WeeklyDataPoint
			if rows4.Scan(&dp.WeekStart, &dp.Count) == nil {
				result.Issues = append(result.Issues, dp)
			}
		}
	}

	return result, nil
}

// LicenseCount is a single license with its count and OSI compliance status.
type LicenseCount struct {
	License     string `json:"license"`
	Count       int    `json:"count"`
	IsOSI       bool   `json:"is_osi"`
}

// osiLicenses is the set of OSI-approved SPDX license identifiers.
// Source: https://opensource.org/licenses/
var osiLicenses = map[string]bool{
	"MIT": true, "Apache-2.0": true, "GPL-2.0": true, "GPL-2.0-only": true,
	"GPL-3.0": true, "GPL-3.0-only": true, "LGPL-2.1": true, "LGPL-2.1-only": true,
	"LGPL-3.0": true, "LGPL-3.0-only": true, "BSD-2-Clause": true, "BSD-3-Clause": true,
	"ISC": true, "MPL-2.0": true, "CDDL-1.0": true, "EPL-1.0": true, "EPL-2.0": true,
	"AGPL-3.0": true, "AGPL-3.0-only": true, "Artistic-2.0": true, "Zlib": true,
	"Unlicense": true, "0BSD": true, "BSL-1.0": true, "PostgreSQL": true,
	"OFL-1.1": true, "NCSA": true, "MulanPSL-2.0": true, "EUPL-1.2": true,
	"CC0-1.0": true, "BlueOak-1.0.0": true, "UPL-1.0": true,
	// Common short forms that appear in package registries.
	"MIT License": true, "Apache 2.0": true, "Apache License 2.0": true,
	"BSD": true, "ISC License": true, "Artistic-2.0 OR GPL-2.0-or-later": true,
}

// normalizeLicense maps empty, whitespace-only, and common "no license"
// sentinel values to "Unknown". Package registries return various representations
// of "no license declared": empty string, "NOASSERTION" (SPDX), "NONE", "N/A",
// "(none)" (RubyGems), or whitespace-only. This normalizer unifies them so the
// license page shows a single "Unknown" count instead of cryptic registry values.
func normalizeLicense(license string) string {
	trimmed := strings.TrimSpace(license)
	upper := strings.ToUpper(trimmed)
	switch upper {
	case "", "NOASSERTION", "NONE", "N/A", "(NONE)", "UNKNOWN":
		return "Unknown"
	}
	return trimmed
}

// GetRepoLicenses returns a summary of dependency licenses for a repo,
// with counts and OSI compliance indicators. Dependencies with no declared
// license (empty, whitespace, or sentinel values like NOASSERTION) are
// grouped under "Unknown".
func (s *PostgresStore) GetRepoLicenses(ctx context.Context, repoID int64) ([]LicenseCount, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT COALESCE(NULLIF(TRIM(license), ''), 'Unknown') AS lic,
			COUNT(*) AS cnt
		FROM aveloxis_data.repo_deps_libyear
		WHERE repo_id = $1
		GROUP BY lic
		ORDER BY cnt DESC`, repoID)
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
		if rows.Scan(&lic, &cnt) == nil {
			normalized := normalizeLicense(lic)
			counts[normalized] += cnt
		}
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
// Handles both exact SPDX identifiers and common variations.
func isOSILicense(license string) bool {
	if osiLicenses[license] {
		return true
	}
	// Try common normalizations.
	// Many registries return "MIT" or "Apache-2.0" but some use full names.
	return false
}
