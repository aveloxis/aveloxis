// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Package exposure (v0.29.60) — the software-supply-chain view of the
// vulnerability findings, keyed by PACKAGE rather than by repository:
// for one dependency, how many repositories in a cohort are exposed,
// how much of that exposure is transitive (invisible to a manifest
// reader), how long it has been open, which advisory leads, and how many
// distinct versions are in simultaneous use. The GUI's dependencies page
// draws the "what the non-expert cannot see" panel from it.
//
// One SQL body serves two scopes (SR-17): the FLEET, materialized as
// aveloxis_data.explorer_package_exposure and explorer_package_advisory
// (built from this same text without the repo filter by
// CreateSupplyChainViews in supply_chain_views.go, v0.29.61), and a
// COHORT — a user's scope or one group's repositories — computed live
// with `repo_id = ANY($1)`. Fleet-wide the aggregate walks every finding
// (5.5M rows on the 2026-09-22 production fleet, ~3 s), which is why it
// is materialized; a cohort of hundreds of repositories aggregates in
// milliseconds through idx_repo_deps_vulns_repo_id.
//
// The live path is a per-request aggregate on a listing surface, which
// the web-api rules warn against (the v0.27.4 nginx-timeout class). It is
// kept because the alternative — a per-user materialization — has no
// refresh story, and because the cost is bounded and measured: one pass
// (the total rides on the rows as a window count), a 60 s response cache
// per caller and query in the API, page sizes clamped here, and the
// worst case on the production fleet (a 62,000-repository scope) 3.2 s —
// far inside any proxy timeout.

// PackageExposure is one package's cohort profile.
type PackageExposure struct {
	Ecosystem   string `json:"ecosystem"`
	PackageName string `json:"package_name"`
	// CohortRepos is how many repositories in the cohort have EVER had a
	// finding on this package; ReposUnresolved how many have one now.
	CohortRepos     int `json:"cohort_repos"`
	ReposUnresolved int `json:"repos_unresolved"`
	// Findings counts finding rows (one per repo × advisory × version).
	Findings           int     `json:"findings"`
	FindingsUnresolved int     `json:"findings_unresolved"`
	PctUnresolved      float64 `json:"pct_unresolved"`
	PctTransitive      float64 `json:"pct_transitive"`
	// MedianDaysOpen is the median age of the CURRENT findings.
	MedianDaysOpen         *float64 `json:"median_days_open"`
	WorstSeverity          string   `json:"worst_severity"`
	MaxCVSS                float64  `json:"max_cvss"`
	Advisories             int      `json:"n_advisories"`
	DistinctVersionsInUse  int      `json:"distinct_versions_in_use"`
	ModalVersion           string   `json:"modal_version"`
	ModalVersionSharePct   *float64 `json:"modal_version_share_pct"`
	LeadAdvisory           string   `json:"lead_advisory"`
	LeadCVE                string   `json:"lead_cve"`
	LeadSeverity           string   `json:"lead_severity"`
	LeadFixedVersion       string   `json:"lead_fixed_version"`
	LeadAdvisoryRepos      int      `json:"lead_advisory_repos"`
	ReposWithKnownVersions int      `json:"repos_with_known_versions"`
}

// PackageAdvisory is one advisory's footprint on a package in the cohort.
type PackageAdvisory struct {
	VulnID          string     `json:"vuln_id"`
	CVEID           string     `json:"cve_id"`
	Severity        string     `json:"severity"`
	MaxCVSS         float64    `json:"max_cvss"`
	FixedVersion    string     `json:"fixed_version"`
	Repos           int        `json:"repos"`
	ReposUnresolved int        `json:"repos_unresolved"`
	FirstSeen       *time.Time `json:"first_seen"`
}

// PackageVersionUse is one scanned version's footprint among CURRENT
// exposures.
type PackageVersionUse struct {
	Version string `json:"version"`
	Repos   int    `json:"repos"`
}

// PackageExposedRepo is one exposed repository, for the drill-down list.
type PackageExposedRepo struct {
	RepoID             int64  `json:"repo_id"`
	Owner              string `json:"repo_owner"`
	Name               string `json:"repo_name"`
	FindingsUnresolved int    `json:"findings_unresolved"`
	Transitive         bool   `json:"transitive"`
	// Versions is the distinct scanned versions in text order (the
	// original field, kept for compatibility).
	Versions []string `json:"versions"`
	// VersionDetail is the same versions with their weight: open findings
	// and lockfile count per version, most findings first, then
	// CompareVersionish order. A versionless finding (a purl with no
	// version) counts in FindingsUnresolved but has no entry here.
	VersionDetail []PackageRepoVersion `json:"version_detail"`
}

// PackageRepoVersion is one scanned version in one exposed repository.
type PackageRepoVersion struct {
	Version            string `json:"version"`
	FindingsUnresolved int    `json:"findings_unresolved"`
	// Lockfiles is how many distinct lockfiles in the repository's
	// CURRENT lockfile snapshot (repo_lockfile_packages) resolve the
	// package to exactly this version. 0 is a real answer: a direct
	// dependency scanned at its manifest floor, or a lockfile-less
	// ecosystem.
	Lockfiles int `json:"lockfiles"`
}

// PackageExposureQuery scopes and pages the leaderboard.
type PackageExposureQuery struct {
	// RepoIDs restricts the cohort; nil means the whole fleet (served from
	// the materialized view).
	RepoIDs   []int64
	Ecosystem string
	Search    string // substring of package_name, case-insensitive
	Sort      string // one of PackageExposureSorts
	Limit     int
	Offset    int
}

// PackageExposureSorts is the sort-key allowlist (the listing-table
// grammar: names are bound to columns here, never interpolated from the
// request).
var PackageExposureSorts = map[string]string{
	"repos":      "cohort_repos DESC, package_name",
	"unresolved": "repos_unresolved DESC, package_name",
	"cvss":       "max_cvss DESC NULLS LAST, cohort_repos DESC, package_name",
	"transitive": "pct_transitive DESC, cohort_repos DESC, package_name",
	"versions":   "distinct_versions_in_use DESC, cohort_repos DESC, package_name",
	"days":       "median_days_open DESC NULLS LAST, cohort_repos DESC, package_name",
	"name":       "package_name, ecosystem",
}

const packageExposureDefaultLimit = 50
const packageExposureMaxLimit = 500

// notSelfFindingSQL excludes "self" rows: advisories against a repository's
// OWN published package, versionless and never resolved by a later scan.
// They are not exposure (PR #212 review: one made a package read CRITICAL
// and its publisher an exposed repository); CountRepoVulnerabilities, the
// repo stats and the digest exclude them the same way. One spelling for
// every supply-chain read in this file (SR-17).
const notSelfFindingSQL = ` AND COALESCE(v.dependency_kind, '') <> 'self'`

// packageFindingsSQL is the per-finding base every package-level
// aggregate reads: one row per finding with the scanned version pulled
// out of the purl (the version is stored only inside package_purl) and
// the "current" flag. %s is the repo filter — "" for the fleet, or
// " AND v.repo_id = ANY($1)" for a cohort.
const packageFindingsSQL = `
	SELECT v.ecosystem, v.package_name, v.repo_id, v.vuln_id, v.cve_id,
	       v.severity, v.cvss_score, v.fixed_version, v.dependency_kind,
	       v.first_detected_at, v.resolved_at,
	       (v.resolved_at IS NULL) AS current,
	       ` + purlVersionSQL + ` AS scanned_version
	  FROM aveloxis_data.repo_deps_vulnerabilities v
	 WHERE v.ecosystem <> '' AND v.package_name <> ''` + notSelfFindingSQL + `%s`

// purlVersionSQL reads the scanned version out of v.package_purl: the text
// after the LAST '@' when it contains no '/' (review round 1 on v0.29.60).
// A canonical purl escapes a scope's '@' as %40, but the scan preserves a
// legacy raw scoped purl (pkg:npm/@scope/name@1.0.0) minted before
// v0.27.29, whose FIRST '@' is the scope marker; a versionless one
// (pkg:npm/@scope/name) yields NULL, never "scope/name". One expression
// for every reader (SR-17).
const purlVersionSQL = `NULLIF(substring(v.package_purl FROM '@([^@/]+)$'), '')`

// severityRankSQL ranks a severity label so "worst" is CRITICAL > HIGH >
// MEDIUM > LOW, not alphabetical; %s is the column.
const severityRankSQL = `CASE upper(%s) WHEN 'CRITICAL' THEN 4 WHEN 'HIGH' THEN 3 WHEN 'MEDIUM' THEN 2 WHEN 'MODERATE' THEN 2 WHEN 'LOW' THEN 1 ELSE 0 END`

// advisorySeveritySQL and advisoryFixedVersionSQL pick one advisory's
// label across its rows the way the writer's upsert does — prefer the
// KNOWN value (review round 1 on v0.29.60): a failed detail fetch stores a
// stub (severity UNKNOWN or the empty string, no fixed version), and frequency voting
// (mode()) let stubs outvote known rows, with the empty label winning a
// tie. Severity is the highest-RANKED label; the fixed version is the most
// common NON-EMPTY one.
var advisorySeveritySQL = `COALESCE((array_agg(severity ORDER BY ` + fmt.Sprintf(severityRankSQL, "severity") + ` DESC, severity DESC))[1], '')`

const advisoryFixedVersionSQL = `COALESCE(mode() WITHIN GROUP (ORDER BY fixed_version) FILTER (WHERE fixed_version <> ''), '')`

// packageExposureSQL is the cohort profile per (ecosystem, package): the
// body of explorer_package_exposure. The modal version share is over
// repositories whose current exposure carries a known version.
func packageExposureSQL(repoFilter string) string {
	return `
	WITH f AS (` + fmt.Sprintf(packageFindingsSQL, repoFilter) + `),
	per_pkg AS (
	    SELECT ecosystem, package_name,
	           COUNT(DISTINCT repo_id)                                   AS cohort_repos,
	           COUNT(DISTINCT repo_id) FILTER (WHERE current)            AS repos_unresolved,
	           COUNT(*)                                                  AS findings,
	           COUNT(*) FILTER (WHERE current)                           AS findings_unresolved,
	           ROUND(100.0 * COUNT(*) FILTER (WHERE current) / COUNT(*), 1)                          AS pct_unresolved,
	           ROUND(100.0 * COUNT(*) FILTER (WHERE dependency_kind = 'transitive') / COUNT(*), 1)  AS pct_transitive,
	           percentile_cont(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (NOW() - first_detected_at)) / 86400.0)
	               FILTER (WHERE current)                                AS median_days_open,
	           MAX(cvss_score)                                           AS max_cvss,
	           MAX(` + fmt.Sprintf(severityRankSQL, "severity") + `)     AS worst_rank,
	           COUNT(DISTINCT vuln_id)                                   AS n_advisories,
	           COUNT(DISTINCT scanned_version) FILTER (WHERE current)    AS distinct_versions_in_use,
	           COUNT(DISTINCT repo_id) FILTER (WHERE current AND scanned_version IS NOT NULL) AS repos_with_known_versions
	      FROM f
	     GROUP BY ecosystem, package_name
	),
	versions AS (
	    SELECT ecosystem, package_name, scanned_version, COUNT(DISTINCT repo_id) AS repos
	      FROM f WHERE current AND scanned_version IS NOT NULL
	     GROUP BY ecosystem, package_name, scanned_version
	),
	modal AS (
	    SELECT DISTINCT ON (ecosystem, package_name)
	           ecosystem, package_name, scanned_version AS modal_version, repos AS modal_repos
	      FROM versions
	     ORDER BY ecosystem, package_name, repos DESC, scanned_version
	),
	advisories AS (
	    SELECT ecosystem, package_name, vuln_id,
	           MAX(cve_id) AS cve_id,
	           ` + advisorySeveritySQL + ` AS severity,
	           ` + advisoryFixedVersionSQL + ` AS fixed_version,
	           COUNT(DISTINCT repo_id) AS repos
	      FROM f
	     GROUP BY ecosystem, package_name, vuln_id
	),
	lead AS (
	    SELECT DISTINCT ON (ecosystem, package_name)
	           ecosystem, package_name, vuln_id, cve_id, severity, fixed_version, repos
	      FROM advisories
	     ORDER BY ecosystem, package_name, repos DESC, vuln_id
	)
	SELECT p.ecosystem, p.package_name, p.cohort_repos, p.repos_unresolved,
	       p.findings, p.findings_unresolved, p.pct_unresolved, p.pct_transitive,
	       p.median_days_open, p.max_cvss,
	       CASE p.worst_rank WHEN 4 THEN 'CRITICAL' WHEN 3 THEN 'HIGH' WHEN 2 THEN 'MEDIUM' WHEN 1 THEN 'LOW' ELSE '' END AS worst_severity,
	       p.n_advisories, p.distinct_versions_in_use, p.repos_with_known_versions,
	       COALESCE(m.modal_version, '') AS modal_version,
	       CASE WHEN p.repos_with_known_versions > 0 THEN ROUND(100.0 * m.modal_repos / p.repos_with_known_versions, 1) END AS modal_version_share_pct,
	       COALESCE(l.vuln_id, '') AS lead_advisory, COALESCE(l.cve_id, '') AS lead_cve,
	       COALESCE(l.severity, '') AS lead_severity, COALESCE(l.fixed_version, '') AS lead_fixed_version,
	       COALESCE(l.repos, 0) AS lead_advisory_repos
	  FROM per_pkg p
	  LEFT JOIN modal m USING (ecosystem, package_name)
	  LEFT JOIN lead  l USING (ecosystem, package_name)`
}

// packageAdvisorySQL is one row per (ecosystem, package, advisory): the
// body of explorer_package_advisory.
func packageAdvisorySQL(repoFilter string) string {
	return `
	WITH f AS (` + fmt.Sprintf(packageFindingsSQL, repoFilter) + `)
	SELECT ecosystem, package_name, vuln_id,
	       MAX(cve_id) AS cve_id,
	       ` + advisorySeveritySQL + ` AS severity,
	       MAX(cvss_score) AS max_cvss,
	       ` + advisoryFixedVersionSQL + ` AS fixed_version,
	       COUNT(DISTINCT repo_id) AS repos,
	       COUNT(DISTINCT repo_id) FILTER (WHERE current) AS repos_unresolved,
	       MIN(first_detected_at) AS first_seen
	  FROM f
	 GROUP BY ecosystem, package_name, vuln_id`
}

// PackageExposureMatviewSQL and PackageAdvisoryMatviewSQL are the
// fleet-wide bodies as matviews.sql must carry them (no repo filter).
func PackageExposureMatviewSQL() string { return packageExposureSQL("") }
func PackageAdvisoryMatviewSQL() string { return packageAdvisorySQL("") }

const cohortFilter = " AND v.repo_id = ANY($1)"

// isUndefinedTable reports PostgreSQL's 42P01 (undefined_table): the ONE
// error that means a relation does not exist yet — here, a supply-chain
// view on a fleet whose migrate has not run, or whose view build failed
// (since v0.29.61 the pair is built whatever collection.materialized_views
// says). The fleet readers then
// aggregate live over the whole table (the same SQL, no filter) instead of
// failing; any other error is returned (SR-5).
func isUndefinedTable(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "42P01"
}

// likeLiteral escapes LIKE's metacharacters (backslash first, then % and
// _) for a pattern whose ESCAPE character is backslash.
func likeLiteral(s string) string {
	return strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(s)
}

func exposureSelectColumns() string {
	return `ecosystem, package_name, cohort_repos, repos_unresolved, findings, findings_unresolved,
	        pct_unresolved, pct_transitive, median_days_open, max_cvss, worst_severity, n_advisories,
	        distinct_versions_in_use, repos_with_known_versions, modal_version, modal_version_share_pct,
	        lead_advisory, lead_cve, lead_severity, lead_fixed_version, lead_advisory_repos`
}

func exposureScanTargets(e *PackageExposure, maxCVSS **float64) []any {
	return []any{&e.Ecosystem, &e.PackageName, &e.CohortRepos, &e.ReposUnresolved, &e.Findings, &e.FindingsUnresolved,
		&e.PctUnresolved, &e.PctTransitive, &e.MedianDaysOpen, maxCVSS, &e.WorstSeverity, &e.Advisories,
		&e.DistinctVersionsInUse, &e.ReposWithKnownVersions, &e.ModalVersion, &e.ModalVersionSharePct,
		&e.LeadAdvisory, &e.LeadCVE, &e.LeadSeverity, &e.LeadFixedVersion, &e.LeadAdvisoryRepos}
}

func scanExposure(row pgx.Row) (*PackageExposure, error) {
	var e PackageExposure
	var maxCVSS *float64
	if err := row.Scan(exposureScanTargets(&e, &maxCVSS)...); err != nil {
		return nil, err
	}
	if maxCVSS != nil {
		e.MaxCVSS = *maxCVSS
	}
	return &e, nil
}

func scanExposureWithTotal(row pgx.Row) (*PackageExposure, int, error) {
	var e PackageExposure
	var maxCVSS *float64
	var total int
	if err := row.Scan(append(exposureScanTargets(&e, &maxCVSS), &total)...); err != nil {
		return nil, 0, err
	}
	if maxCVSS != nil {
		e.MaxCVSS = *maxCVSS
	}
	return &e, total, nil
}

// PackageExposurePage is one leaderboard page: the rows, the total number
// of packages in the cohort, and whether the answer was computed live (a
// cohort, or the fleet without its views) or read from the view.
type PackageExposurePage struct {
	Rows  []*PackageExposure
	Total int
	Live  bool
}

// ListPackageExposure returns the leaderboard page. A nil RepoIDs reads the
// materialized view; a cohort aggregates live. One pass: the total rides
// on every row as COUNT(*) OVER () (the aggregated set is one row per
// package, so the window count is free — review round 1: the live path ran
// the whole aggregate twice per request); a page past the end counts the
// set once more.
func (s *PostgresStore) ListPackageExposure(ctx context.Context, q PackageExposureQuery) (*PackageExposurePage, error) {
	orderBy, ok := PackageExposureSorts[q.Sort]
	if !ok {
		orderBy = PackageExposureSorts["repos"]
	}
	limit := q.Limit
	if limit <= 0 {
		limit = packageExposureDefaultLimit
	}
	if limit > packageExposureMaxLimit {
		limit = packageExposureMaxLimit
	}
	offset := max(q.Offset, 0)

	page := &PackageExposurePage{Live: q.RepoIDs != nil}
	var source string
	args := []any{}
	if q.RepoIDs != nil {
		source = "(" + packageExposureSQL(cohortFilter) + ") x"
		args = append(args, q.RepoIDs)
	} else {
		source = "aveloxis_data.explorer_package_exposure x"
	}
	where := ""
	if q.Ecosystem != "" {
		args = append(args, q.Ecosystem)
		where += fmt.Sprintf(" AND x.ecosystem = $%d", len(args))
	}
	if q.Search != "" {
		// A literal substring (PR #212 review): LIKE's wildcards in the
		// search text are escaped, so "_" (common in package names) and "%"
		// match only themselves.
		args = append(args, "%"+likeLiteral(strings.ToLower(q.Search))+"%")
		where += fmt.Sprintf(` AND lower(x.package_name) LIKE $%d ESCAPE '\'`, len(args))
	}
	pageArgs := append(append([]any{}, args...), limit, offset)
	pageSQL := func(src string) string {
		return "SELECT " + exposureSelectColumns() + ", COUNT(*) OVER () AS total FROM " + src + " WHERE TRUE" + where +
			" ORDER BY " + orderBy + fmt.Sprintf(" LIMIT $%d OFFSET $%d", len(pageArgs)-1, len(pageArgs))
	}
	rows, err := s.pool.Query(ctx, pageSQL(source), pageArgs...)
	if err != nil && q.RepoIDs == nil && isUndefinedTable(err) {
		// The fleet view is not built: aggregate live over the whole table.
		source = "(" + packageExposureSQL("") + ") x"
		page.Live = true
		rows, err = s.pool.Query(ctx, pageSQL(source), pageArgs...)
	}
	if err != nil {
		return nil, fmt.Errorf("package exposure list: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		e, total, err := scanExposureWithTotal(rows)
		if err != nil {
			return nil, err
		}
		page.Total = total
		page.Rows = append(page.Rows, e)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(page.Rows) == 0 && offset > 0 {
		// A page past the end carries no window count; count the set once.
		if err := s.pool.QueryRow(ctx, "SELECT COUNT(*) FROM "+source+" WHERE TRUE"+where, args...).Scan(&page.Total); err != nil {
			return nil, fmt.Errorf("package exposure count: %w", err)
		}
	}
	return page, nil
}

// ErrPackageNotFound is the typed not-found for a package with no
// finding in the cohort (SR-5: only this means "absent").
var ErrPackageNotFound = errors.New("package has no findings in this cohort")

// GetPackageExposure returns one package's cohort profile and whether it
// was computed live (a cohort, or the fleet without its view) rather than
// read from the view.
func (s *PostgresStore) GetPackageExposure(ctx context.Context, ecosystem, name string, repoIDs []int64) (*PackageExposure, bool, error) {
	live := repoIDs != nil
	var row pgx.Row
	if repoIDs != nil {
		row = s.pool.QueryRow(ctx, "SELECT "+exposureSelectColumns()+" FROM ("+packageExposureSQL(cohortFilter)+") x WHERE x.ecosystem = $2 AND x.package_name = $3",
			repoIDs, ecosystem, name)
	} else {
		row = s.pool.QueryRow(ctx, "SELECT "+exposureSelectColumns()+" FROM aveloxis_data.explorer_package_exposure x WHERE x.ecosystem = $1 AND x.package_name = $2",
			ecosystem, name)
	}
	e, err := scanExposure(row)
	if err != nil && repoIDs == nil && isUndefinedTable(err) {
		// The fleet view is not built: aggregate live over the whole table.
		live = true
		e, err = scanExposure(s.pool.QueryRow(ctx, "SELECT "+exposureSelectColumns()+" FROM ("+packageExposureSQL("")+") x WHERE x.ecosystem = $1 AND x.package_name = $2",
			ecosystem, name))
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, live, ErrPackageNotFound
	}
	if err != nil {
		return nil, live, fmt.Errorf("package exposure %s/%s: %w", ecosystem, name, err)
	}
	return e, live, nil
}

// GetPackageAdvisories lists the advisories on one package, most
// widespread first.
func (s *PostgresStore) GetPackageAdvisories(ctx context.Context, ecosystem, name string, repoIDs []int64) ([]PackageAdvisory, error) {
	var rows pgx.Rows
	var err error
	if repoIDs != nil {
		rows, err = s.pool.Query(ctx, "SELECT vuln_id, cve_id, severity, max_cvss, fixed_version, repos, repos_unresolved, first_seen FROM ("+
			packageAdvisorySQL(cohortFilter)+") x WHERE x.ecosystem = $2 AND x.package_name = $3 ORDER BY repos DESC, vuln_id",
			repoIDs, ecosystem, name)
	} else {
		rows, err = s.pool.Query(ctx, `SELECT vuln_id, cve_id, severity, max_cvss, fixed_version, repos, repos_unresolved, first_seen
			FROM aveloxis_data.explorer_package_advisory WHERE ecosystem = $1 AND package_name = $2 ORDER BY repos DESC, vuln_id`,
			ecosystem, name)
		if err != nil && isUndefinedTable(err) {
			// The fleet view is not built: aggregate live over the whole table.
			rows, err = s.pool.Query(ctx, "SELECT vuln_id, cve_id, severity, max_cvss, fixed_version, repos, repos_unresolved, first_seen FROM ("+
				packageAdvisorySQL("")+") x WHERE x.ecosystem = $1 AND x.package_name = $2 ORDER BY repos DESC, vuln_id",
				ecosystem, name)
		}
	}
	if err != nil {
		return nil, fmt.Errorf("package advisories %s/%s: %w", ecosystem, name, err)
	}
	defer rows.Close()
	var out []PackageAdvisory
	for rows.Next() {
		var a PackageAdvisory
		var cvss *float64
		if err := rows.Scan(&a.VulnID, &a.CVEID, &a.Severity, &cvss, &a.FixedVersion, &a.Repos, &a.ReposUnresolved, &a.FirstSeen); err != nil {
			return nil, err
		}
		if cvss != nil {
			a.MaxCVSS = *cvss
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// GetPackageVersionsInUse is the version-agreement panel: each scanned
// version among CURRENT exposures and how many repositories run it. Live
// in both scopes (indexed by (ecosystem, package_name)).
func (s *PostgresStore) GetPackageVersionsInUse(ctx context.Context, ecosystem, name string, repoIDs []int64) ([]PackageVersionUse, error) {
	filter := ""
	args := []any{ecosystem, name}
	if repoIDs != nil {
		filter = " AND v.repo_id = ANY($3)"
		args = append(args, repoIDs)
	}
	rows, err := s.pool.Query(ctx, `
		SELECT `+purlVersionSQL+` AS ver, COUNT(DISTINCT v.repo_id)
		  FROM aveloxis_data.repo_deps_vulnerabilities v
		 WHERE v.ecosystem = $1 AND v.package_name = $2 AND v.resolved_at IS NULL`+notSelfFindingSQL+filter+`
		 GROUP BY 1 HAVING `+purlVersionSQL+` IS NOT NULL
		 ORDER BY 2 DESC, 1`, args...)
	if err != nil {
		return nil, fmt.Errorf("package versions %s/%s: %w", ecosystem, name, err)
	}
	defer rows.Close()
	var out []PackageVersionUse
	for rows.Next() {
		var u PackageVersionUse
		if err := rows.Scan(&u.Version, &u.Repos); err != nil {
			return nil, err
		}
		out = append(out, u)
	}
	return out, rows.Err()
}

// GetPackageExposedRepos lists the repositories currently exposed to one
// package, most findings first, each with its per-version detail.
func (s *PostgresStore) GetPackageExposedRepos(ctx context.Context, ecosystem, name string, repoIDs []int64, limit int) ([]PackageExposedRepo, error) {
	if limit <= 0 {
		limit = packageExposureDefaultLimit
	}
	if limit > packageExposureMaxLimit {
		limit = packageExposureMaxLimit // the leaderboard's clamp (review round 2)
	}
	filter := ""
	args := []any{ecosystem, name, limit}
	if repoIDs != nil {
		filter = " AND v.repo_id = ANY($4)"
		args = append(args, repoIDs)
	}
	// f is one row per CURRENT finding with its scanned version; per_ver
	// weighs each (repository, version). The versions array keeps its
	// original text order; the per-version arrays ride in the same order
	// and are re-sorted in Go (CompareVersionish is not SQL).
	rows, err := s.pool.Query(ctx, `
		WITH f AS (
		    SELECT v.repo_id, v.dependency_kind, `+purlVersionSQL+` AS ver
		      FROM aveloxis_data.repo_deps_vulnerabilities v
		     WHERE v.ecosystem = $1 AND v.package_name = $2 AND v.resolved_at IS NULL`+notSelfFindingSQL+filter+`
		),
		per_repo AS (
		    SELECT repo_id, COUNT(*) AS findings_unresolved,
		           bool_and(dependency_kind = 'transitive') AS transitive
		      FROM f GROUP BY repo_id
		),
		per_ver AS (
		    SELECT repo_id, ver, COUNT(*) AS findings
		      FROM f WHERE ver IS NOT NULL GROUP BY repo_id, ver
		)
		SELECT p.repo_id, r.repo_owner, r.repo_name, p.findings_unresolved, p.transitive,
		       COALESCE(array_agg(pv.ver ORDER BY pv.ver) FILTER (WHERE pv.ver IS NOT NULL), '{}') AS versions,
		       COALESCE(array_agg(pv.findings ORDER BY pv.ver) FILTER (WHERE pv.ver IS NOT NULL), '{}') AS version_findings
		  FROM per_repo p
		  JOIN aveloxis_data.repos r USING (repo_id)
		  LEFT JOIN per_ver pv USING (repo_id)
		 GROUP BY p.repo_id, r.repo_owner, r.repo_name, p.findings_unresolved, p.transitive
		 ORDER BY p.findings_unresolved DESC, r.repo_owner, r.repo_name
		 LIMIT $3`, args...)
	if err != nil {
		return nil, fmt.Errorf("package repos %s/%s: %w", ecosystem, name, err)
	}
	defer rows.Close()
	var out []PackageExposedRepo
	for rows.Next() {
		var e PackageExposedRepo
		var counts []int64
		if err := rows.Scan(&e.RepoID, &e.Owner, &e.Name, &e.FindingsUnresolved, &e.Transitive, &e.Versions, &counts); err != nil {
			return nil, fmt.Errorf("package repos %s/%s: scan: %w", ecosystem, name, err)
		}
		if e.Versions == nil {
			e.Versions = []string{}
		}
		if len(counts) != len(e.Versions) {
			return nil, fmt.Errorf("package repos %s/%s: repo %d has %d versions but %d finding counts", ecosystem, name, e.RepoID, len(e.Versions), len(counts))
		}
		e.VersionDetail = make([]PackageRepoVersion, len(e.Versions))
		for i, v := range e.Versions {
			e.VersionDetail[i] = PackageRepoVersion{Version: v, FindingsUnresolved: int(counts[i])}
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("package repos %s/%s: %w", ecosystem, name, err)
	}
	if err := s.attachLockfileCounts(ctx, ecosystem, name, out); err != nil {
		return nil, err
	}
	for i := range out {
		d := out[i].VersionDetail
		sort.SliceStable(d, func(a, b int) bool {
			if d[a].FindingsUnresolved != d[b].FindingsUnresolved {
				return d[a].FindingsUnresolved > d[b].FindingsUnresolved
			}
			if c := CompareVersionish(d[a].Version, d[b].Version); c != 0 {
				return c < 0
			}
			return d[a].Version < d[b].Version // "1.0" and "1.0.0" compare equal; keep a total order
		})
	}
	return out, nil
}

// attachLockfileCounts fills each version's Lockfiles from the listed
// repositories' current lockfile snapshot. The match is the lockfile-graph
// key (LockfileGraphKey: the ecosystem alias fold — a finding stored under
// "gem" matches a Gemfile.lock row under "rubygems" — the lowercase name
// and PyPI's PEP 503 fold), decided in Go. The SQL only narrows the rows
// read to candidates and must never drop a row the key accepts: an ASCII
// row is lowercased under the "C" collation (ASCII-only, the same mapping
// as Go's for ASCII, whatever the database locale — a Turkish one maps
// 'I' elsewhere) and matched as a substring built from the same fold
// (lockfileGraphName; LIKE's metacharacters escaped, PyPI's folded '-' a
// one-character wildcard for '-', '_' or '.'); a row with any non-ASCII
// character is always a candidate (Go's ToLower maps the Kelvin sign to
// 'k'), and a non-ASCII target narrows nothing.
//
// Versions are matched as text against the scanned version the purl
// carries. A direct locked target's purl carries the lockfile's
// resolved_version verbatim; a transitive's purl percent-escapes '%', '@',
// '?', '#' and space (purlEscapeSegment), so the escaped spelling is also
// tried decoded. Accepted residue (L10 review): a DIRECT version that
// itself contains a valid %XX ("1.0%2B1") is also credited with the raw
// spelling's lockfiles, and one version seen both verbatim and escaped
// shows as two chips counting the same lockfile; both need versions no
// ecosystem is known to publish, and the finding rows do not record which
// spelling their purl used.
//
// Cost: the rows read are every lockfile row of the listed repositories
// (the repo_id index; the substring cannot use the name index). The list
// is capped at packageExposureMaxLimit and the GUI asks for 25; time a
// fleet-scope ?repos=500 on a popular transitive before relying on it.
func (s *PostgresStore) attachLockfileCounts(ctx context.Context, ecosystem, name string, repos []PackageExposedRepo) error {
	if len(repos) == 0 {
		return nil
	}
	ids := make([]int64, len(repos))
	for i, r := range repos {
		ids[i] = r.RepoID
	}
	target := LockfileGraphKey(ecosystem, name)
	folded := lockfileGraphName(lockfileEcoFold(ecosystem), name)
	pattern := "%"
	if isASCII(folded) {
		lit := likeLiteral(folded)
		if lockfileEcoFold(ecosystem) == "pypi" {
			lit = strings.ReplaceAll(lit, "-", "_")
		}
		pattern = "%" + lit + "%"
	}
	rows, err := s.pool.Query(ctx, `
		SELECT DISTINCT repo_id, ecosystem, package_name, resolved_version, lockfile_path
		  FROM aveloxis_data.repo_lockfile_packages
		 WHERE repo_id = ANY($1)
		   AND (lower(package_name COLLATE "C") LIKE $2 ESCAPE '\' OR package_name ~ '[^[:ascii:]]')`, ids, pattern)
	if err != nil {
		return fmt.Errorf("package repos %s/%s: lockfile counts: %w", ecosystem, name, err)
	}
	defer rows.Close()
	// repo -> version -> set of lockfile paths
	paths := map[int64]map[string]map[string]bool{}
	for rows.Next() {
		var repoID int64
		var eco, pkg, version, path string
		if err := rows.Scan(&repoID, &eco, &pkg, &version, &path); err != nil {
			return fmt.Errorf("package repos %s/%s: lockfile counts: scan: %w", ecosystem, name, err)
		}
		if LockfileGraphKey(eco, pkg) != target {
			continue
		}
		if paths[repoID] == nil {
			paths[repoID] = map[string]map[string]bool{}
		}
		if paths[repoID][version] == nil {
			paths[repoID][version] = map[string]bool{}
		}
		paths[repoID][version][path] = true
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("package repos %s/%s: lockfile counts: %w", ecosystem, name, err)
	}
	for i := range repos {
		for j := range repos[i].VersionDetail {
			v := repos[i].VersionDetail[j].Version
			found := paths[repos[i].RepoID][v]
			if raw, err := url.PathUnescape(v); err == nil && raw != v {
				// A transitive purl's escaped version: count the raw
				// spelling's lockfiles too (a path holding both is one).
				merged := map[string]bool{}
				for p := range found {
					merged[p] = true
				}
				for p := range paths[repos[i].RepoID][raw] {
					merged[p] = true
				}
				found = merged
			}
			repos[i].VersionDetail[j].Lockfiles = len(found)
		}
	}
	return nil
}

func isASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= utf8.RuneSelf {
			return false
		}
	}
	return true
}

// ErrGroupNotFound is the typed not-found for a cohort group: it does not
// exist, or a non-admin caller does not own it (the existence of another
// user's group is not disclosed).
var ErrGroupNotFound = errors.New("group not found")

// GetGroupRepoIDsForUser returns a group's repository ids for a cohort
// scope. The group must exist for everyone (review round 1: an admin
// asking for an unknown id got an empty cohort, not a 404); a non-admin
// must also own it (the portal's rule).
func (s *PostgresStore) GetGroupRepoIDsForUser(ctx context.Context, groupID int64, userID int, isAdmin bool) ([]int64, error) {
	var owner int
	err := s.pool.QueryRow(ctx, `SELECT user_id FROM aveloxis_ops.user_groups WHERE group_id = $1`, groupID).Scan(&owner)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && !isAdmin && owner != userID) {
		return nil, ErrGroupNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("group owner: %w", err)
	}
	rows, err := s.pool.Query(ctx, `SELECT repo_id FROM aveloxis_ops.user_repos WHERE group_id = $1 ORDER BY repo_id`, groupID)
	if err != nil {
		return nil, fmt.Errorf("group repos: %w", err)
	}
	defer rows.Close()
	ids := []int64{}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}
