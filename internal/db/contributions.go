// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
	"time"
)

// RepoContributor is a single contributor returned by GetRepoContributors.
// All "may be empty" string fields are passed through NULLIF/COALESCE in
// SQL so the Go-side value is either the meaningful text or "" — callers
// can render it directly without per-field null checks.
type RepoContributor struct {
	CntrbID        string `json:"cntrb_id"`
	Login          string `json:"login"`
	FullName       string `json:"full_name"`
	Email          string `json:"email"`
	ProfileCompany string `json:"profile_company"`
	Location       string `json:"location"`
	// v0.27.57 activity classification (GitHub contributionsCollection;
	// empty class = never checked or GitLab-side contributor — GitLab
	// has no restricted-contributions equivalent). Class vocabulary in
	// model.Activity*; the "no-observable-activity" class must be
	// rendered honestly (truly-inactive is indistinguishable from
	// private-with-disclosure-off by GitHub's design).
	ActivityClass      string `json:"activity_class"`
	PublicContribsYear int    `json:"public_contribs_year"`
	RestrictedContribs int    `json:"restricted_contribs_year"`
	LastContributionYr int    `json:"last_contribution_year"`
}

// AffiliationCount is a single row in the affiliation breakdown returned
// by GetRepoAffiliationCounts. ContributorCount is the number of DISTINCT
// contributors (not the number of contributions) tagged with the
// affiliation in the requested window.
type AffiliationCount struct {
	Affiliation      string `json:"affiliation"`
	ContributorCount int    `json:"contributor_count"`
}

// contributorsInWindowCTE is the shared SQL fragment that enumerates every
// distinct cntrb_id that produced any kind of contribution to repoID
// between since and until. Defined once here and embedded into both
// GetRepoContributors and GetRepoAffiliationCounts so the two endpoints
// can never drift on which actions count as "contribution."
//
// The contribution kinds covered are documented in docs/guide/api.md
// under "Contributors in a window" / "Affiliation breakdown." Adding a
// new kind means adding a UNION arm here AND updating that doc — both
// pinned by source-contract tests.
//
// All arms reference the same three positional parameters: $1=repoID,
// $2=since, $3=until.
const contributorsInWindowCTE = `
contributors_in_window AS (
    -- Commit authorship: cmt_author_timestamp is the TIMESTAMPTZ field
    -- (cmt_author_date is TEXT and not safe to range-filter).
    SELECT DISTINCT c.cmt_ght_author_id AS cntrb_id
    FROM aveloxis_data.commits c
    WHERE c.repo_id = $1
      AND c.cmt_ght_author_id IS NOT NULL
      AND c.cmt_author_timestamp >= $2
      AND c.cmt_author_timestamp <  $3

    UNION
    SELECT DISTINCT i.reporter_id
    FROM aveloxis_data.issues i
    WHERE i.repo_id = $1 AND i.reporter_id IS NOT NULL
      AND i.created_at >= $2 AND i.created_at < $3

    UNION
    SELECT DISTINCT i.closed_by_id
    FROM aveloxis_data.issues i
    WHERE i.repo_id = $1 AND i.closed_by_id IS NOT NULL
      AND i.closed_at >= $2 AND i.closed_at < $3

    UNION
    SELECT DISTINCT e.cntrb_id
    FROM aveloxis_data.issue_events e
    WHERE e.repo_id = $1 AND e.cntrb_id IS NOT NULL
      AND e.created_at >= $2 AND e.created_at < $3

    UNION
    SELECT DISTINCT pr.author_id
    FROM aveloxis_data.pull_requests pr
    WHERE pr.repo_id = $1 AND pr.author_id IS NOT NULL
      AND pr.created_at >= $2 AND pr.created_at < $3

    UNION
    SELECT DISTINCT prr.cntrb_id
    FROM aveloxis_data.pull_request_reviews prr
    WHERE prr.repo_id = $1 AND prr.cntrb_id IS NOT NULL
      AND prr.submitted_at >= $2 AND prr.submitted_at < $3

    UNION
    SELECT DISTINCT pe.cntrb_id
    FROM aveloxis_data.pull_request_events pe
    WHERE pe.repo_id = $1 AND pe.cntrb_id IS NOT NULL
      AND pe.created_at >= $2 AND pe.created_at < $3

    UNION
    -- Unified messages: issue comments + PR conversation comments +
    -- inline review comment bodies all live here. cntrb_id is the
    -- author across all three message kinds.
    SELECT DISTINCT m.cntrb_id
    FROM aveloxis_data.messages m
    WHERE m.repo_id = $1 AND m.cntrb_id IS NOT NULL
      AND m.msg_timestamp >= $2 AND m.msg_timestamp < $3
)`

// resolveWindow normalizes a (since, until) pair the same way
// GetRepoTimeSeries does: a zero until is treated as "no upper bound"
// by substituting a far-future timestamp so the SQL stays parameterized.
// A zero since is treated as "since the beginning of time" (1970-01-01).
// since must be strictly less than until; the caller validates this and
// surfaces a 400 if violated.
func resolveWindow(since, until time.Time) (time.Time, time.Time) {
	lower := since
	if lower.IsZero() {
		lower = time.Unix(0, 0)
	}
	upper := until
	if upper.IsZero() {
		upper = time.Now().AddDate(100, 0, 0)
	}
	return lower, upper
}

// GetRepoContributors returns every distinct contributor who made any
// kind of contribution to repoID between since and until.
//
// "Contribution" covers: commit authorship, opening or closing an issue,
// opening a PR, submitting a PR review, any issue or PR event (label /
// assignment / reference), and any message (issue comment, PR
// conversation comment, inline review comment body — all unified in
// aveloxis_data.messages per the architecture doc).
//
// Soft-deleted contributors (cntrb_deleted != 0, set by the v0.20.2
// rename-merge path) are excluded; the operator wants active identities,
// not merge tombstones.
//
// Commits whose cmt_ght_author_id is NULL — author email never matched
// a known contributor — are excluded. Those people don't have a cntrb_id
// to return; surfacing them would require a separate "unresolved commit
// authors" endpoint that returns name+email tuples instead.
func (s *PostgresStore) GetRepoContributors(ctx context.Context, repoID int64, since, until time.Time) ([]RepoContributor, error) {
	lower, upper := resolveWindow(since, until)

	sql := `
WITH ` + contributorsInWindowCTE + `
SELECT
    c.cntrb_id::text,
    COALESCE(NULLIF(c.cntrb_login, ''), c.gh_login, c.gl_username, '') AS login,
    COALESCE(NULLIF(c.cntrb_full_name, ''), '')                        AS full_name,
    COALESCE(NULLIF(c.cntrb_email, ''), '')                            AS email,
    COALESCE(NULLIF(c.cntrb_company, ''), '')                          AS profile_company,
    COALESCE(NULLIF(c.cntrb_location, ''), '')                         AS location,
    COALESCE(c.gh_activity_class, '')                                  AS activity_class,
    COALESCE(c.gh_public_contribs_year, 0)                             AS public_contribs_year,
    COALESCE(c.gh_restricted_contribs_year, 0)                         AS restricted_contribs_year,
    COALESCE(c.gh_last_contribution_year, 0)                           AS last_contribution_year
FROM contributors_in_window ciw
JOIN aveloxis_data.contributors c USING (cntrb_id)
WHERE COALESCE(c.cntrb_deleted, 0) = 0
ORDER BY login NULLS LAST, full_name NULLS LAST`

	rows, err := s.pool.Query(ctx, sql, repoID, lower, upper)
	if err != nil {
		return nil, fmt.Errorf("GetRepoContributors query: %w", err)
	}
	defer rows.Close()

	out := make([]RepoContributor, 0, 128)
	for rows.Next() {
		var rc RepoContributor
		if err := rows.Scan(&rc.CntrbID, &rc.Login, &rc.FullName, &rc.Email, &rc.ProfileCompany, &rc.Location,
			&rc.ActivityClass, &rc.PublicContribsYear, &rc.RestrictedContribs, &rc.LastContributionYr); err != nil {
			return nil, fmt.Errorf("GetRepoContributors scan: %w", err)
		}
		out = append(out, rc)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("GetRepoContributors rows: %w", err)
	}
	return out, nil
}

// ContributionsCoverage answers "is the data I'm about to render
// complete?" for a given (repo, window) pair. The other two
// contributions endpoints expose the cohort and its affiliation
// breakdown; this one exposes what the cohort's enrichment STATE looks
// like so the operator can tell whether an "(unknown)" bucket
// represents true unaffiliated contributors vs. people the v0.18.29
// enrichment ticker simply hasn't reached yet.
//
// All count fields are integers; the two timestamp fields are nullable
// (encoded as the Go zero time when the cohort has no matching rows
// — JSON-serialized as the omitempty default to keep the response
// shape stable).
type ContributionsCoverage struct {
	WindowSince            time.Time `json:"window_since"`
	WindowUntil            time.Time `json:"window_until"`
	TotalContributors      int       `json:"total_contributors"`
	Enriched               int       `json:"enriched"`
	CanonicalEmail         int       `json:"canonical_email"`
	GHUserIDResolved       int       `json:"gh_user_id_resolved"`
	SearchResolveAttempted int       `json:"search_resolve_attempted"`
	BreadthAttempted       int       `json:"breadth_attempted"`
	AffiliationResolved    int       `json:"affiliation_resolved"`
	AffiliationUnknown     int       `json:"affiliation_unknown"`
	// Nullable: pointer + omitempty so JSON omits the field entirely
	// when the cohort has no rows in the relevant state (rather than
	// emitting the zero time, which is operator-confusing).
	EnrichmentOldestPending *time.Time `json:"enrichment_oldest_pending,omitempty"`
	EnrichmentStalest       *time.Time `json:"enrichment_stalest,omitempty"`
}

// GetRepoContributionsCoverage returns the data-completeness state for
// the cohort that contributed to repoID in [since, until). Same window
// CTE as the other two contributions endpoints, so the cohort is
// identical and operators can correlate counts across all three.
//
// The query uses FILTER (WHERE ...) aggregates rather than separate
// COUNT(column) calls where the predicate isn't simple "column IS NOT
// NULL" — empty-string and JOIN-derived conditions need explicit
// predicates. COUNT(column) alone counts non-NULL values and is fine
// for the timestamp / nullable-int columns.
//
// EnrichmentOldestPending is the oldest data_collection_date among
// contributors with NULL cntrb_last_enriched_at — translates to "this
// person has been waiting N days for enrichment." Operators compare
// against their enrich_interval_minutes cadence to spot a stuck ticker.
//
// EnrichmentStalest is the oldest cntrb_last_enriched_at among
// contributors that HAVE been enriched at least once — surfaces the
// long tail of "enriched 18 months ago and never refreshed." The
// 30-day re-enrichment cooldown in v0.18.29 caps how stale this can
// get in steady state.
//
// AffiliationUnknown is derived in Go as Total − AffiliationResolved
// rather than re-counted in SQL — avoids a redundant FILTER scan.
func (s *PostgresStore) GetRepoContributionsCoverage(ctx context.Context, repoID int64, since, until time.Time) (*ContributionsCoverage, error) {
	lower, upper := resolveWindow(since, until)

	sql := `
WITH ` + contributorsInWindowCTE + `,
enriched_state AS (
    SELECT
        c.cntrb_id,
        c.cntrb_last_enriched_at,
        c.cntrb_canonical,
        c.gh_user_id,
        c.cntrb_last_search_attempted_at,
        c.cntrb_last_breadth_at,
        c.cntrb_company,
        c.data_collection_date,
        ca.ca_affiliation
    FROM contributors_in_window ciw
    JOIN aveloxis_data.contributors c USING (cntrb_id)
    LEFT JOIN aveloxis_data.contributor_affiliations ca
        ON LOWER(SPLIT_PART(c.cntrb_canonical, '@', 2)) = LOWER(ca.ca_domain)
        AND COALESCE(ca.ca_active, 1) = 1
    WHERE COALESCE(c.cntrb_deleted, 0) = 0
)
SELECT
    COUNT(*)::int                                                        AS total_contributors,
    COUNT(cntrb_last_enriched_at)::int                                   AS enriched,
    COUNT(*) FILTER (WHERE cntrb_canonical != '')::int                   AS canonical_email,
    COUNT(gh_user_id)::int                                               AS gh_user_id_resolved,
    COUNT(cntrb_last_search_attempted_at)::int                           AS search_resolve_attempted,
    COUNT(cntrb_last_breadth_at)::int                                    AS breadth_attempted,
    COUNT(*) FILTER (WHERE
        COALESCE(
            NULLIF(ca_affiliation, ''),
            NULLIF(REGEXP_REPLACE(cntrb_company, '^@', ''), '')
        ) IS NOT NULL
    )::int                                                               AS affiliation_resolved,
    MIN(data_collection_date) FILTER (WHERE cntrb_last_enriched_at IS NULL)
                                                                         AS enrichment_oldest_pending,
    MIN(cntrb_last_enriched_at)                                          AS enrichment_stalest
FROM enriched_state`

	out := &ContributionsCoverage{
		WindowSince: lower,
		WindowUntil: upper,
	}

	// Use pointer destinations for the nullable timestamp columns —
	// COUNT can never be NULL but MIN over a filtered empty set
	// returns NULL, which pgx rejects scanning into time.Time directly.
	var pending, stalest *time.Time

	err := s.pool.QueryRow(ctx, sql, repoID, lower, upper).Scan(
		&out.TotalContributors,
		&out.Enriched,
		&out.CanonicalEmail,
		&out.GHUserIDResolved,
		&out.SearchResolveAttempted,
		&out.BreadthAttempted,
		&out.AffiliationResolved,
		&pending,
		&stalest,
	)
	if err != nil {
		return nil, fmt.Errorf("GetRepoContributionsCoverage query: %w", err)
	}
	out.EnrichmentOldestPending = pending
	out.EnrichmentStalest = stalest
	out.AffiliationUnknown = out.TotalContributors - out.AffiliationResolved
	return out, nil
}

// TopContributor is one ranked row from TopContributors: a contributor
// with their per-kind activity counts inside the requested window.
type TopContributor struct {
	CntrbID       string `json:"cntrb_id"`
	Login         string `json:"login"`
	FullName      string `json:"full_name"`
	ActivityClass string `json:"activity_class"`
	Commits       int    `json:"commits"`
	Issues        int    `json:"issues"`
	PRs           int    `json:"prs"`
	Reviews       int    `json:"reviews"`
	Comments      int    `json:"comments"`
	Total         int    `json:"total"`
}

// TopContributors (v0.27.61) returns the top-N contributors to repoID
// in [since, until), ranked by total activity, with the per-kind
// breakdown that produced the rank.
//
// What counts, per kind (each arm windowed on its own event time):
//   - commits:  DISTINCT commit hashes authored (the commits table is
//     one row per FILE per commit — COUNT(*) would weight a 30-file
//     commit 30×). Windowed on cmt_author_timestamp (TIMESTAMPTZ);
//     cmt_author_date is TEXT and unsafe to range-filter.
//   - issues:   issues OPENED (reporter_id / created_at).
//   - prs:      pull requests OPENED (author_id / created_at).
//   - reviews:  PR reviews SUBMITTED (cntrb_id / submitted_at) — the
//     arm that makes maintainer review load visible; it is invisible
//     in commit/PR counts alone.
//   - comments: unified messages (issue comments + PR conversation +
//     inline review comment bodies) by msg_timestamp.
//
// Issue/PR events (labels, assignments, closes) are deliberately NOT
// counted — they are process noise for a "who does the work here"
// ranking, though they DO count for cohort membership in
// contributorsInWindowCTE. The two surfaces answer different
// questions and the difference is documented in docs/guide/api.md.
//
// Identity resolution matches GetRepoContributors exactly: the shared
// COALESCE login chain and the cntrb_deleted=0 soft-delete filter, so
// the two contributor surfaces can never disagree about who a
// cntrb_id is. Commits with NULL cmt_ght_author_id (unresolved
// authors) are excluded — there is no identity to rank.
func (s *PostgresStore) TopContributors(ctx context.Context, repoID int64, since, until time.Time, limit int) ([]TopContributor, error) {
	lower, upper := resolveWindow(since, until)
	if limit <= 0 {
		limit = 20
	}
	// Hard upper backstop AT THE STORE LAYER (CodeQL
	// go/uncontrolled-allocation-size, 2026-07-31): the API handler
	// caps limit at 100 too, but the store is the shared surface —
	// a future caller that forgets its own cap must not reach the
	// slice allocation (or the SQL LIMIT) with an unbounded value.
	if limit > 100 {
		limit = 100
	}

	sql := `
WITH per_kind AS (
    SELECT c.cmt_ght_author_id AS cntrb_id,
           COUNT(DISTINCT c.cmt_commit_hash) AS commits,
           0::bigint AS issues, 0::bigint AS prs, 0::bigint AS reviews, 0::bigint AS comments
    FROM aveloxis_data.commits c
    WHERE c.repo_id = $1 AND c.cmt_ght_author_id IS NOT NULL
      AND c.cmt_author_timestamp >= $2 AND c.cmt_author_timestamp < $3
    GROUP BY 1

    UNION ALL
    SELECT i.reporter_id, 0, COUNT(*), 0, 0, 0
    FROM aveloxis_data.issues i
    WHERE i.repo_id = $1 AND i.reporter_id IS NOT NULL
      AND i.created_at >= $2 AND i.created_at < $3
    GROUP BY 1

    UNION ALL
    SELECT pr.author_id, 0, 0, COUNT(*), 0, 0
    FROM aveloxis_data.pull_requests pr
    WHERE pr.repo_id = $1 AND pr.author_id IS NOT NULL
      AND pr.created_at >= $2 AND pr.created_at < $3
    GROUP BY 1

    UNION ALL
    SELECT prr.cntrb_id, 0, 0, 0, COUNT(*), 0
    FROM aveloxis_data.pull_request_reviews prr
    WHERE prr.repo_id = $1 AND prr.cntrb_id IS NOT NULL
      AND prr.submitted_at >= $2 AND prr.submitted_at < $3
    GROUP BY 1

    UNION ALL
    SELECT m.cntrb_id, 0, 0, 0, 0, COUNT(*)
    FROM aveloxis_data.messages m
    WHERE m.repo_id = $1 AND m.cntrb_id IS NOT NULL
      AND m.msg_timestamp >= $2 AND m.msg_timestamp < $3
    GROUP BY 1
),
totals AS (
    SELECT cntrb_id,
           SUM(commits) AS commits, SUM(issues) AS issues, SUM(prs) AS prs,
           SUM(reviews) AS reviews, SUM(comments) AS comments
    FROM per_kind
    GROUP BY cntrb_id
)
SELECT
    t.cntrb_id::text,
    COALESCE(NULLIF(c.cntrb_login, ''), c.gh_login, c.gl_username, '') AS login,
    COALESCE(NULLIF(c.cntrb_full_name, ''), '')                        AS full_name,
    COALESCE(c.gh_activity_class, '')                                  AS activity_class,
    t.commits::int, t.issues::int, t.prs::int, t.reviews::int, t.comments::int,
    (t.commits + t.issues + t.prs + t.reviews + t.comments)::int       AS total
FROM totals t
JOIN aveloxis_data.contributors c ON c.cntrb_id = t.cntrb_id
WHERE COALESCE(c.cntrb_deleted, 0) = 0
ORDER BY total DESC, login
LIMIT $4`

	rows, err := s.pool.Query(ctx, sql, repoID, lower, upper, limit)
	if err != nil {
		return nil, fmt.Errorf("TopContributors query: %w", err)
	}
	defer rows.Close()

	out := make([]TopContributor, 0, limit)
	for rows.Next() {
		var tc TopContributor
		if err := rows.Scan(&tc.CntrbID, &tc.Login, &tc.FullName, &tc.ActivityClass,
			&tc.Commits, &tc.Issues, &tc.PRs, &tc.Reviews, &tc.Comments, &tc.Total); err != nil {
			return nil, fmt.Errorf("TopContributors scan: %w", err)
		}
		out = append(out, tc)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("TopContributors rows: %w", err)
	}
	return out, nil
}

// GetRepoAffiliationCounts returns the number of DISTINCT contributors
// per affiliation in the same window as GetRepoContributors.
//
// Affiliation derivation priority (per-contributor):
//  1. contributor_affiliations[domain_of(cntrb_canonical)] — the curated
//     email-domain → org map populated by PopulateAffiliations from
//     observed company strings. Most reliable signal because it covers
//     people whose profile is blank but whose email domain is well-known.
//  2. cntrb_company (with leading "@" stripped to handle the GitHub
//     "@org" reference style) — what the user typed into their profile.
//     Freeform and often empty.
//  3. "(unknown)" — fallback bucket for contributors with neither a
//     canonical-email match nor a company string.
//
// The "(unknown)" row is included in the result so the count is
// honest about coverage; callers can hide or surface it as desired.
func (s *PostgresStore) GetRepoAffiliationCounts(ctx context.Context, repoID int64, since, until time.Time) ([]AffiliationCount, error) {
	lower, upper := resolveWindow(since, until)

	sql := `
WITH ` + contributorsInWindowCTE + `,
with_affiliation AS (
    SELECT
        c.cntrb_id,
        COALESCE(
            NULLIF(ca.ca_affiliation, ''),
            NULLIF(REGEXP_REPLACE(c.cntrb_company, '^@', ''), ''),
            '(unknown)'
        ) AS affiliation
    FROM contributors_in_window ciw
    JOIN aveloxis_data.contributors c USING (cntrb_id)
    LEFT JOIN aveloxis_data.contributor_affiliations ca
        ON LOWER(SPLIT_PART(c.cntrb_canonical, '@', 2)) = LOWER(ca.ca_domain)
        AND COALESCE(ca.ca_active, 1) = 1
    WHERE COALESCE(c.cntrb_deleted, 0) = 0
)
SELECT affiliation, COUNT(*)::int AS contributor_count
FROM with_affiliation
GROUP BY affiliation
ORDER BY contributor_count DESC, affiliation`

	rows, err := s.pool.Query(ctx, sql, repoID, lower, upper)
	if err != nil {
		return nil, fmt.Errorf("GetRepoAffiliationCounts query: %w", err)
	}
	defer rows.Close()

	out := make([]AffiliationCount, 0, 32)
	for rows.Next() {
		var ac AffiliationCount
		if err := rows.Scan(&ac.Affiliation, &ac.ContributorCount); err != nil {
			return nil, fmt.Errorf("GetRepoAffiliationCounts scan: %w", err)
		}
		out = append(out, ac)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("GetRepoAffiliationCounts rows: %w", err)
	}
	return out, nil
}
