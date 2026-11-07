// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"time"
)

// RepoStats holds gathered (actual row counts) and metadata (from repo_info API snapshot)
// counts for PRs, issues, and commits. Used by both the monitoring dashboard and the
// web GUI to show collection completeness at a glance.
type RepoStats struct {
	RepoID          int64 `json:"repo_id"`
	GatheredPRs     int   `json:"gathered_prs"`
	GatheredIssues  int   `json:"gathered_issues"`
	GatheredCommits int   `json:"gathered_commits"`
	MetadataPRs     int   `json:"metadata_prs"`     // pr_count from repo_info (GitHub API reported total)
	MetadataIssues  int   `json:"metadata_issues"`  // issues_count from repo_info
	MetadataCommits int   `json:"metadata_commits"` // commit_count from repo_info
	Vulnerabilities int   `json:"vulnerabilities"`  // current (unresolved, non-self) CVEs from OSV.dev scan
	CriticalVulns   int   `json:"critical_vulns"`   // current CVEs with severity CRITICAL or cvss_score >= 9.0
	// VulnsVersionUnknown (v0.29.70) counts the current advisories of
	// dependencies that declare no version — OSV was asked without one, so
	// exposure is unknown. Not in Vulnerabilities (exposurePredicateSQL).
	VulnsVersionUnknown int `json:"vulnerabilities_version_unknown"`
	// Archived (v0.27.50) is the forge's read-only status from the
	// latest repo_info snapshot (status='Archived') — the ACCURATE
	// signal, distinct from the repos.repo_archived boolean (which
	// prelim sets only for DEAD/404 repos). The GUI reads THIS.
	Archived bool `json:"archived"`
	// LastActivityAt (v0.27.50) is the most recent observed activity
	// (MAX commit/issue/PR timestamp), driving the chart last-active
	// ceiling + the dormant/archived chip. Nil = no activity yet.
	LastActivityAt *time.Time `json:"last_activity_at,omitempty"`
	// ForkedFrom (v0.27.79) is the upstream "owner/name" when the repo
	// is a fork (repos.forked_from — captured by Phase 0 since
	// v0.27.78; model.UnknownForkParent when the upstream was deleted).
	// Empty = not a fork. Drives the GUI's "Forked from X" chip.
	ForkedFrom string `json:"forked_from,omitempty"`
	// LastCollected (v0.27.84) is collection_queue.last_collected —
	// the GUI's ONLY way to distinguish "queued for first collection"
	// (nil → prominent queued banner) from "collected, zero activity"
	// (honest zeros). Absent when the repo has no queue row too.
	LastCollected *time.Time `json:"last_collected,omitempty"`
	// GoneAt (v0.28.1 A6) — repos.repo_gone_at: the repo no longer
	// resolves on its forge (prelim's probe got a definitive 404/410/451;
	// privatized or deleted upstream). The GUI must suppress the
	// queued banner and render the no-longer-available notice, with
	// gone taking precedence over the archived chip.
	GoneAt *time.Time `json:"gone_at,omitempty"`
	// UnavailableReason and UnavailableURL (v0.29.70, worklist 82) —
	// the forge's own message for a blocked or disabled repository and
	// its notice link (https only; the store refuses anything else). The
	// GUI repeats them on the repository page. Independent of GoneAt: a
	// repository disabled by staff keeps its queue row.
	UnavailableReason string `json:"unavailable_reason,omitempty"`
	UnavailableURL    string `json:"unavailable_url,omitempty"`
	// MetadataAsOf (v0.28.1 A6) — the repo_info snapshot date behind
	// the Metadata* counts. On gone repos the metadata is a frozen
	// pre-disappearance snapshot; this field lets the GUI date it
	// honestly ("metadata N (as of Jul 24, 2026)").
	MetadataAsOf *time.Time `json:"metadata_as_of,omitempty"`
	// ForgeIDChanges (v0.29.62) — adopted forge-ID changes: the upstream
	// repository was deleted and re-created under the same URL and the
	// operator adopted the new one as a continuation, so this row holds
	// data from both. The GUI shows a notice; statistics may be affected.
	ForgeIDChanges []ForgeIDChange `json:"forge_id_changes,omitempty"`
	// GoneHistory (0.29.140) — every stretch during which the repository
	// did not resolve on its forge (aveloxis_data.repo_gone_history),
	// oldest first; an episode without restored_at is the current one
	// (GoneAt). The GUI lists them under the gone notice, so a repository
	// that was unavailable for a day (re-created upstream, probed while
	// still 404) keeps that on its page after it is back.
	GoneHistory []GoneEpisode `json:"gone_history,omitempty"`
	// HasMetadataSnapshot (v0.28.11, Copilot round 10) — TRUE when a
	// repo_info ROW exists, independent of its (nullable)
	// data_collection_date. MetadataAsOf presence is NOT a snapshot-
	// presence signal: a legacy NULL-dated snapshot returns real
	// counts with a nil date, and consumers gating on the date would
	// re-conflate "zero" with "no snapshot".
	HasMetadataSnapshot bool `json:"has_metadata_snapshot,omitempty"`
}

// RepoStatsBatchMaxIDs bounds GetRepoStatsBatch's id list (v0.28.10):
// the queueless live-count fallback makes per-id cost non-trivial for
// the gone cohort, so the batch must stay bounded. 500 covers every
// real caller (monitor 200/page, portal/group pages 100/page) with
// headroom.
const RepoStatsBatchMaxIDs = 500

// SearchRepoResult is a minimal repo record for search results.
type SearchRepoResult struct {
	ID    int64  `json:"id"`
	Owner string `json:"owner"`
	Name  string `json:"name"`
	// Starred is annotated by the API layer when the caller presents a
	// Bearer identity (v0.27.4 — star toggles on search results).
	Starred bool `json:"starred"`
}

// SearchRepos searches repos by name or owner (case-insensitive). Used by the
// comparison page's repo search dropdown.
func (s *PostgresStore) SearchRepos(ctx context.Context, query string, limit int) ([]SearchRepoResult, error) {
	pattern := LikeContains(strings.ToLower(query)) // % and _ in the term are literal (stage 1b)
	rows, err := s.pool.Query(ctx, `
		SELECT repo_id, repo_owner, repo_name
		FROM aveloxis_data.repos
		WHERE LOWER(repo_name) LIKE $1 ESCAPE '\' OR LOWER(repo_owner) LIKE $1 ESCAPE '\' OR LOWER(repo_git) LIKE $1 ESCAPE '\'
		ORDER BY repo_owner, repo_name
		LIMIT $2`, pattern, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []SearchRepoResult{} // D6: empty is []
	for rows.Next() {
		var r SearchRepoResult
		if err := rows.Scan(&r.ID, &r.Owner, &r.Name); err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, rows.Err()
}

// GetRepoStats returns gathered vs metadata counts for a single repo: the
// batch reader's answer for that id (stage 1b, summary/58 §7 #16 — the
// two were hand-kept twins, and the batch had lost archived, forked_from
// and last_activity_at). Everything the answer carries is documented on
// GetRepoStatsBatch.
func (s *PostgresStore) GetRepoStats(ctx context.Context, repoID int64) (*RepoStats, error) {
	batch, err := s.GetRepoStatsBatch(ctx, []int64{repoID})
	if err != nil {
		return nil, err
	}
	st, ok := batch[repoID]
	if !ok {
		return nil, fmt.Errorf("repo %d: %w", repoID, ErrRepoNotFound)
	}
	return st, nil
}

// queuelessLiveCountsBatch fills the ids that had no collection_queue
// row (v0.28.7, Copilot round 3; since stage 1b the one fallback, the
// single reader's twin folded in): one query over ONLY those ids, counting
// the actual stored rows — the QUEUELESS cohort: prelim dequeues gone
// repos (privatized/deleted upstream), taking the cached counts with the
// queue row, so the tiles showed fabricated zeros over real collected data
// (the department-of-veterans-affairs report: 477 repos; v0.28.1 A6) —
// plus gone_at, the forge notice, the latest repo_info metadata, archived
// and forked_from. The live aggregates are exactly what v0.27.85 removed
// from the hot path (~23,500 buffer pages on a kernel-class repo), which is
// why this lives OUTSIDE GetRepoStatsBatch's body: the dashboard-alignment
// pin keeps banning the aggregates from the main path, and this gated
// fallback runs only for the rare queueless cohort (hundreds fleet-wide,
// none kernel-class), never for a tracked repository.
func (s *PostgresStore) queuelessLiveCountsBatch(ctx context.Context, repoIDs []int64, result map[int64]*RepoStats, seen map[int64]bool) error {
	rows, err := s.pool.Query(ctx, `
		SELECT r.repo_id, r.repo_gone_at,
		       COALESCE(r.repo_unavailable_reason, ''), COALESCE(r.repo_unavailable_url, ''), COALESCE(r.forked_from, ''),
		       ri.data_collection_date,
		       COALESCE(ri.has_snapshot, FALSE), COALESCE(ri.status, ''),
		       COALESCE(ri.pr_count, 0), COALESCE(ri.issues_count, 0), COALESCE(ri.commit_count, 0),
		       (SELECT COUNT(*) FROM aveloxis_data.issues i WHERE i.repo_id = r.repo_id),
		       (SELECT COUNT(*) FROM aveloxis_data.pull_requests p WHERE p.repo_id = r.repo_id),
		       (SELECT COUNT(DISTINCT c.cmt_commit_hash) FROM aveloxis_data.commits c WHERE c.repo_id = r.repo_id)
		FROM aveloxis_data.repos r
		LEFT JOIN LATERAL (
		    SELECT pr_count, issues_count, commit_count, data_collection_date, status,
		           TRUE AS has_snapshot
		    FROM aveloxis_data.repo_info
		    WHERE repo_id = r.repo_id
		    ORDER BY data_collection_date DESC NULLS LAST, repo_info_id DESC
		    LIMIT 1
		) ri ON TRUE
		WHERE r.repo_id = ANY($1)`, repoIDs)
	if err != nil {
		return fmt.Errorf("batch queueless fallback: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		var goneAt, metaAsOf *time.Time
		var unavailReason, unavailURL, forkedFrom, status string
		var hasSnap bool
		var mPRs, mIssues, mCommits, gIssues, gPRs, gCommits int
		if err := rows.Scan(&id, &goneAt, &unavailReason, &unavailURL, &forkedFrom, &metaAsOf, &hasSnap, &status, &mPRs, &mIssues, &mCommits, &gIssues, &gPRs, &gCommits); err != nil {
			return fmt.Errorf("batch queueless scan: %w", err)
		}
		if st, ok := result[id]; ok {
			seen[id] = true
			st.GoneAt = goneAt
			st.UnavailableReason = unavailReason
			st.UnavailableURL = unavailURL
			st.ForkedFrom = forkedFrom
			st.Archived = status == "Archived"
			st.MetadataAsOf = metaAsOf
			st.HasMetadataSnapshot = hasSnap
			st.MetadataPRs = mPRs
			st.MetadataIssues = mIssues
			st.MetadataCommits = mCommits
			st.GatheredIssues = gIssues
			st.GatheredPRs = gPRs
			st.GatheredCommits = gCommits
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("batch queueless iteration: %w", err)
	}
	return nil
}

// GetRepoStatsBatch returns stats for multiple repos in fewer queries: the
// ONE stats reader (stage 1b, summary/58 §7 #16) — the single-repository
// endpoint, the web GUI group detail page and the monitor dashboard. Per
// id: the queue's cached gathered counts and last_collected (v0.27.85 —
// the live COUNTs cost ~23,500 buffer pages per call on a big repo), the
// latest repo_info snapshot's metadata counts, date, and archived status
// (status = 'Archived', the forge's read-only flag: the accurate signal,
// distinct from repos.repo_archived), the repository row's gone_at, forge
// notice and forked_from, the last observed activity, the adopted forge-ID
// changes and the vulnerability classes.
//
// v0.18.30 rewrite: instead of issuing 5 separate aggregate queries
// against the heavy child tables (pull_requests / issues / commits /
// repo_info / repo_deps_vulnerabilities), this reads gathered counts
// directly from collection_queue.last_* — those columns are populated
// at CompleteJob time, so they're always in sync with the actual rows
// without requiring a COUNT(*) over millions of rows on every
// dashboard render. Metadata counts come from repo_info via a single
// LATERAL-style DISTINCT ON join, and vulnerability counts use a
// scoped subquery filtered by `WHERE repo_id = ANY($1)` so the GROUP
// BY only touches relevant rows. Total: two queries instead of five,
// no million-row scans.
//
// v0.27.36: (a) query/scan errors propagate instead of silently
// serving zeros; (b) the vulnerability counts apply the SAME
// predicates as CountRepoVulnerabilities — `resolved_at IS NULL AND
// COALESCE(dependency_kind, "") <> 'self'` — so the batch endpoint can
// never disagree with the single-repo endpoint again (summary/18
// Phase 1e; the pre-fix batch counted resolved + self rows and
// reported systematically higher, ever-growing totals).
func (s *PostgresStore) GetRepoStatsBatch(ctx context.Context, repoIDs []int64) (map[int64]*RepoStats, error) {
	// v0.28.10 (Copilot round 7): hard backstop on the id-list size —
	// the queueless fallback below runs live aggregates for the gone
	// cohort, so an unbounded list from a future caller would be an
	// unbounded-cost path (the v0.27.65 store-clamp rule: the store
	// owns the backstop; the API layer 400s as UX). Ids beyond the cap
	// are dropped from the result map — absent, not fabricated.
	if len(repoIDs) > RepoStatsBatchMaxIDs {
		repoIDs = repoIDs[:RepoStatsBatchMaxIDs]
	}
	result := make(map[int64]*RepoStats, len(repoIDs))
	if len(repoIDs) == 0 {
		return result, nil
	}

	// Initialize all entries.
	for _, id := range repoIDs {
		result[id] = &RepoStats{RepoID: id}
	}

	// Gathered counts (last_issues / last_prs / last_commits) come from
	// collection_queue's pre-computed cache. Metadata counts come from
	// the latest repo_info snapshot. Single query, single scan of the
	// queue index, single index lookup per repo into repo_info.
	// v0.28.7 (Copilot round 3): the batch rows carry gone_at +
	// metadata_as_of like the single-repo endpoint, and ids with NO
	// queue row fall through to the batch live-count fallback below —
	// pre-fix, /repos/stats?ids= served the queueless gone cohort as
	// all-zeros and could not expose the gone state, disagreeing with
	// the fixed single-repo endpoint.
	seen := make(map[int64]bool, len(repoIDs))
	rows, err := s.pool.Query(ctx, `
		SELECT q.repo_id,
		       COALESCE(q.last_issues, 0),
		       COALESCE(q.last_prs, 0),
		       COALESCE(q.last_commits, 0),
		       q.last_collected,
		       COALESCE(ri.pr_count, 0),
		       COALESCE(ri.issues_count, 0),
		       COALESCE(ri.commit_count, 0),
		       ri.data_collection_date,
		       COALESCE(ri.has_snapshot, FALSE), COALESCE(ri.status, ''),
		       r.repo_gone_at,
		       COALESCE(r.repo_unavailable_reason, ''), COALESCE(r.repo_unavailable_url, ''), COALESCE(r.forked_from, '')
		FROM aveloxis_ops.collection_queue q
		JOIN aveloxis_data.repos r ON r.repo_id = q.repo_id
		LEFT JOIN LATERAL (
		    SELECT pr_count, issues_count, commit_count, data_collection_date, status,
		           TRUE AS has_snapshot
		    FROM aveloxis_data.repo_info
		    WHERE repo_id = q.repo_id
		    ORDER BY data_collection_date DESC NULLS LAST, repo_info_id DESC
		    LIMIT 1
		) ri ON TRUE
		WHERE q.repo_id = ANY($1)`, repoIDs)
	if err != nil {
		return nil, fmt.Errorf("batch gathered/metadata counts: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		var gIssues, gPRs, gCommits, mPRs, mIssues, mCommits int
		var hasSnap bool
		var lastCollected, metaAsOf, goneAt *time.Time
		var unavailReason, unavailURL, forkedFrom, status string
		if err := rows.Scan(&id, &gIssues, &gPRs, &gCommits, &lastCollected, &mPRs, &mIssues, &mCommits, &metaAsOf, &hasSnap, &status, &goneAt, &unavailReason, &unavailURL, &forkedFrom); err != nil {
			return nil, fmt.Errorf("batch stats scan: %w", err)
		}
		if st, ok := result[id]; ok {
			seen[id] = true
			st.GatheredIssues = gIssues
			st.GatheredPRs = gPRs
			st.GatheredCommits = gCommits
			st.LastCollected = lastCollected
			st.MetadataPRs = mPRs
			st.MetadataIssues = mIssues
			st.MetadataCommits = mCommits
			st.MetadataAsOf = metaAsOf
			st.HasMetadataSnapshot = hasSnap
			st.Archived = status == "Archived"
			st.GoneAt = goneAt
			st.UnavailableReason = unavailReason
			st.UnavailableURL = unavailURL
			st.ForkedFrom = forkedFrom
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("batch stats iteration: %w", err)
	}
	var missing []int64
	for _, id := range repoIDs {
		if !seen[id] {
			missing = append(missing, id)
		}
	}
	if len(missing) > 0 {
		if err := s.queuelessLiveCountsBatch(ctx, missing, result, seen); err != nil {
			return nil, err
		}
	}
	// An id neither read returned names no repository: absent, never a
	// fabricated zero record (the v0.28.10 rule for the cap; stage 1b
	// review round 1 — the single route is a 404 and the two surfaces are
	// one reader).
	for _, id := range repoIDs {
		if !seen[id] {
			delete(result, id)
		}
	}

	// The forge-ID notice degrades ONLY when its table does not exist yet
	// (v0.29.62 review round 2: an api running before the migrate that
	// creates repo_forge_id_changes, worklist 49's incident, must still
	// serve the rest of the page). Any other failure fails the stats: a
	// page that silently drops "this may affect statistics" is a wrong
	// answer, not a degraded one (v0.29.63 review, SR-5). A caller leaving
	// (cancelled or past its deadline) is not a failure.
	changes, ferr := s.repoForgeIDChangesAdoptedBatch(ctx, repoIDs)
	if ferr != nil {
		if errors.Is(ferr, context.Canceled) || errors.Is(ferr, context.DeadlineExceeded) {
			return nil, ferr
		}
		if !isUndefinedTable(ferr) {
			return nil, fmt.Errorf("forge-ID changes: %w", ferr)
		}
		s.logger.Error("repo stats: repo_forge_id_changes does not exist — run aveloxis migrate; the page renders without the forge-ID notice", "repo_ids", len(repoIDs), "error", ferr)
	}
	for id, st := range result {
		st.ForgeIDChanges = changes[id]
	}
	// The gone history (0.29.140) degrades the same way, for the same
	// reason: an api running before the migrate that creates
	// repo_gone_history must still serve the page.
	episodes, gerr := s.repoGoneHistoryBatch(ctx, repoIDs)
	if gerr != nil {
		if errors.Is(gerr, context.Canceled) || errors.Is(gerr, context.DeadlineExceeded) {
			return nil, gerr
		}
		if !isUndefinedTable(gerr) {
			return nil, fmt.Errorf("gone history: %w", gerr)
		}
		s.logger.Error("repo stats: repo_gone_history does not exist — run aveloxis migrate; the page renders without the gone history", "repo_ids", len(repoIDs), "error", gerr)
	}
	for id, st := range result {
		st.GoneHistory = episodes[id]
	}

	// v0.27.50: last observed activity — drives the chart last-active
	// ceiling and the dormant/archived chip. Non-fatal: a failure leaves
	// LastActivityAt nil (the chip/ceiling degrade to off), logged.
	if activity, aerr := s.lastActivityByRepo(ctx, repoIDs); aerr != nil {
		if errors.Is(aerr, context.Canceled) || errors.Is(aerr, context.DeadlineExceeded) {
			return nil, aerr
		}
		s.logger.Warn("repo stats: last activity unreadable — answering without it", "repo_ids", len(repoIDs), "error", aerr)
	} else {
		for id, at := range activity {
			if st, ok := result[id]; ok {
				t := at
				st.LastActivityAt = &t
			}
		}
	}

	// Vulnerability counts. Scoped subquery: only scans rows whose
	// repo_id is in the requested set, so this stays cheap. Predicates
	// mirror CountRepoVulnerabilities exactly (see doc comment above).
	rows5, err := s.pool.Query(ctx, `
		SELECT repo_id,
		       COUNT(*) FILTER (WHERE TRUE`+exposurePredicateSQL+`),
		       COUNT(*) FILTER (WHERE `+criticalSQL+exposurePredicateSQL+`),
		       COUNT(*) FILTER (WHERE `+unknownVersionSQL+`)
		FROM aveloxis_data.repo_deps_vulnerabilities
		WHERE repo_id = ANY($1)
		  AND resolved_at IS NULL
		GROUP BY repo_id`, repoIDs)
	if err != nil {
		return nil, fmt.Errorf("batch vulnerability counts: %w", err)
	}
	defer rows5.Close()
	for rows5.Next() {
		var id int64
		var total, critical, unknown int
		if err := rows5.Scan(&id, &total, &critical, &unknown); err != nil {
			return nil, fmt.Errorf("batch vulnerability scan: %w", err)
		}
		if st, ok := result[id]; ok {
			st.Vulnerabilities = total
			st.CriticalVulns = critical
			st.VulnsVersionUnknown = unknown
		}
	}
	if err := rows5.Err(); err != nil {
		return nil, fmt.Errorf("batch vulnerability iteration: %w", err)
	}

	return result, nil
}
