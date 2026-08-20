// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// v0.26.5 — identity-attribution backfill (plan:
// summary/identity-attribution-audit-2026-07-09.md).
//
// The 2026-07-09 audit found cntrb_id 0% populated on
// issue_assignees (2.41M rows), pull_request_assignees (2.39M),
// pull_request_reviewers (8.06M), pull_request_meta (26.2M), and
// issues.closed_by_id at 0.015%. The identity material was stored all
// along (platform user ids at ~100% fill), so most of the repair is
// pure SQL derivation — measured coverage 99.87–99.98% on production.
//
// All functions are idempotent (predicates stop matching once filled)
// and batched by KEYSET WINDOWS over the table's primary key, so every
// batch is a bounded index-range scan. The first version batched by
// `candidates LIMIT N`, which re-executed the FULL join per batch —
// and DISTINCT ON forced a global sort before the LIMIT could apply.
// On the 26M-row production pull_request_meta that meant one batch
// ground for 9h45m without writing a row (observed 2026-07-10). A
// 1K-row scratch validation cannot surface this class; window math is
// sized against production PK ranges.

import (
	"context"
	"fmt"
	"strings"
)

// assignmentBackfillTables enumerates the three assignment-class
// tables whose cntrb_id derives from (platform_id, platform user id)
// via contributor_identities.
var assignmentBackfillTables = []struct {
	table, pk, platCol string
}{
	{"issue_assignees", "issue_assignee_id", "platform_assignee_id"},
	{"pull_request_assignees", "pr_assignee_id", "platform_assignee_id"},
	{"pull_request_reviewers", "pr_reviewer_id", "platform_reviewer_id"},
}

// BackfillAssignmentIdentities fills cntrb_id on the three
// assignment-class tables by joining the stored platform user id
// against contributor_identities. Returns rows updated. dryRun
// reports the candidate count without writing. limit (0 = unbounded)
// caps updated rows per table.
func (s *PostgresStore) BackfillAssignmentIdentities(ctx context.Context, batchSize int, limit int64, dryRun bool) (int64, error) {
	if batchSize <= 0 {
		batchSize = 100000
	}
	var total int64
	for _, t := range assignmentBackfillTables {
		if dryRun {
			var n int64
			err := s.pool.QueryRow(ctx, fmt.Sprintf(`
				SELECT COUNT(*)
				FROM aveloxis_data.%s a
				JOIN aveloxis_data.repos r USING (repo_id)
				JOIN aveloxis_data.contributor_identities ci
				  ON ci.platform_id = r.platform_id AND ci.platform_user_id = a.%s
				WHERE a.cntrb_id IS NULL AND a.%s <> 0`, t.table, t.platCol, t.platCol)).Scan(&n)
			if err != nil {
				return total, fmt.Errorf("dry-run count %s: %w", t.table, err)
			}
			s.logger.Info("backfill-identities dry-run", "table", t.table, "derivable", n)
			total += n
			continue
		}
		var maxID int64
		if err := s.pool.QueryRow(ctx, fmt.Sprintf(
			`SELECT COALESCE(MAX(%s), 0) FROM aveloxis_data.%s`, t.pk, t.table)).Scan(&maxID); err != nil {
			return total, fmt.Errorf("max id %s: %w", t.table, err)
		}
		var tableTotal int64
		window := int64(batchSize)
		for lo := int64(0); lo < maxID; lo += window {
			tag, err := s.pool.Exec(ctx, fmt.Sprintf(`
				UPDATE aveloxis_data.%[1]s a
				SET cntrb_id = ci.cntrb_id
				FROM aveloxis_data.repos r, aveloxis_data.contributor_identities ci
				WHERE a.repo_id = r.repo_id
				  AND ci.platform_id = r.platform_id AND ci.platform_user_id = a.%[3]s
				  AND a.cntrb_id IS NULL AND a.%[3]s <> 0
				  AND a.%[2]s > $1 AND a.%[2]s <= $2`, t.table, t.pk, t.platCol),
				lo, lo+window)
			if err != nil {
				return total, fmt.Errorf("backfill %s window %d: %w", t.table, lo, err)
			}
			n := tag.RowsAffected()
			tableTotal += n
			total += n
			if n > 0 {
				s.logger.Info("backfill-identities progress",
					"table", t.table, "updated", tableTotal, "window_hi", lo+window, "max_id", maxID)
			}
			if limit > 0 && tableTotal >= limit {
				break
			}
		}
	}
	return total, nil
}

// BackfillPRMetaOwners fills pull_request_meta.cntrb_id from the
// head/base ref owner: the paired pull_request_repo row's full name
// carries "owner/repo", and the owner login is matched
// case-insensitively against contributors.gh_login (soft-deleted
// merge losers excluded). Operator decision 2026-07-09: this identity
// is high-value; rows whose owner never appears in contributors stay
// NULL (no fabricated data).
func (s *PostgresStore) BackfillPRMetaOwners(ctx context.Context, batchSize int, limit int64, dryRun bool) (int64, error) {
	if batchSize <= 0 {
		batchSize = 100000
	}
	// Window predicate %s slots the pr_meta_id range bounds in.
	const candidates = `
		SELECT DISTINCT ON (m.pr_meta_id) m.pr_meta_id, c.cntrb_id
		FROM aveloxis_data.pull_request_meta m
		JOIN aveloxis_data.pull_request_repo pr
		  ON pr.pr_repo_meta_id = m.pr_meta_id
		 AND pr.pr_repo_head_or_base = m.head_or_base
		JOIN aveloxis_data.contributors c
		  ON LOWER(COALESCE(NULLIF(c.gh_login, ''), c.cntrb_login)) =
		     LOWER(split_part(pr.pr_repo_full_name, '/', 1))
		WHERE m.cntrb_id IS NULL
		  AND split_part(COALESCE(pr.pr_repo_full_name, ''), '/', 1) <> ''
		  AND COALESCE(c.cntrb_deleted, 0) = 0%s`
	if dryRun {
		// The dry-run count intentionally runs the join UNWINDOWED but
		// without the DISTINCT-ON sort (COUNT of distinct meta ids).
		q := `SELECT COUNT(DISTINCT m.pr_meta_id)` + strings.TrimPrefix(
			fmt.Sprintf(candidates, ""), `
		SELECT DISTINCT ON (m.pr_meta_id) m.pr_meta_id, c.cntrb_id`)
		var n int64
		if err := s.pool.QueryRow(ctx, q).Scan(&n); err != nil {
			return 0, fmt.Errorf("dry-run count pull_request_meta: %w", err)
		}
		s.logger.Info("backfill-identities dry-run", "table", "pull_request_meta", "derivable", n)
		return n, nil
	}
	var maxID int64
	if err := s.pool.QueryRow(ctx,
		`SELECT COALESCE(MAX(pr_meta_id), 0) FROM aveloxis_data.pull_request_meta`).Scan(&maxID); err != nil {
		return 0, fmt.Errorf("max pr_meta_id: %w", err)
	}
	var total int64
	window := int64(batchSize)
	for lo := int64(0); lo < maxID; lo += window {
		cand := fmt.Sprintf(candidates, " AND m.pr_meta_id > $1 AND m.pr_meta_id <= $2")
		tag, err := s.pool.Exec(ctx, `
			WITH cand AS (`+cand+`)
			UPDATE aveloxis_data.pull_request_meta m
			SET cntrb_id = cand.cntrb_id
			FROM cand WHERE m.pr_meta_id = cand.pr_meta_id`, lo, lo+window)
		if err != nil {
			return total, fmt.Errorf("backfill pull_request_meta window %d: %w", lo, err)
		}
		n := tag.RowsAffected()
		total += n
		if n > 0 {
			s.logger.Info("backfill-identities progress",
				"table", "pull_request_meta", "updated", total, "window_hi", lo+window, "max_id", maxID)
		}
		if limit > 0 && total >= limit {
			break
		}
	}
	return total, nil
}

// BackfillPRRepoOwners fills pull_request_repo.pr_cntrb_id — the fork/
// upstream repo OWNER identity, 0 of 41.2M rows populated when the
// 2026-08-19 fill audit ran (the owner object was decoded on both rails
// and dropped before storage; v0.27.104's OwnerRef mappers fix the
// forward path). Two passes per keyset window over pr_repo_id:
//
//  1. The cheap PK-join: the paired pull_request_meta row's cntrb_id
//     (v0.26.5 filled those FROM this table's owner segment, and
//     forward-collected meta carries deterministic platform-ID
//     resolutions).
//  2. The login-expression sweep for rows the pair couldn't cover:
//     case-insensitive owner-segment match against contributors
//     (gh_login with cntrb_login fallback, soft-deleted merge losers
//     excluded — the exact BackfillPRMetaOwners shape).
//
// Rows whose owner never appears in contributors (deleted forks) stay
// NULL — no fabricated data.
func (s *PostgresStore) BackfillPRRepoOwners(ctx context.Context, batchSize int, limit int64, dryRun bool) (int64, error) {
	if batchSize <= 0 {
		batchSize = 100000
	}
	// v0.27.110 (Copilot PR #184 round 5): BOTH passes are restricted to
	// GITHUB-platform rows (meta → repos join, platform_id = 1). The
	// login sweep matches the GLOBAL contributor table, and v0.26.5-era
	// pull_request_meta.cntrb_id values came from the same global login
	// join — so on GitLab rows either pass could attribute a group-owned
	// project to an unrelated same-name GitHub user, the exact
	// cross-platform fabrication v0.27.109 banned from the forward path.
	// GitLab rows stay NULL here and heal FORWARD-ONLY via the
	// ID-qualified owner-object refs on re-collection. The sweep's
	// bare-cntrb_login fallback arm additionally disqualifies
	// GitLab-native contributors (gl_username set): a GitLab user's
	// cntrb_login colliding with a GitHub owner name must not win.
	// GitHub-vs-GitHub login matching keeps the documented v0.26.5
	// accepted risk, unchanged.
	const pairSQL = `
		UPDATE aveloxis_data.pull_request_repo pr
		SET pr_cntrb_id = m.cntrb_id
		FROM aveloxis_data.pull_request_meta m
		JOIN aveloxis_data.repos r
		  ON r.repo_id = m.repo_id AND r.platform_id = 1
		WHERE m.pr_meta_id = pr.pr_repo_meta_id
		  AND m.head_or_base = pr.pr_repo_head_or_base
		  AND m.cntrb_id IS NOT NULL
		  AND pr.pr_cntrb_id IS NULL
		  AND pr.pr_repo_id > $1 AND pr.pr_repo_id <= $2`
	const candidates = `
		SELECT DISTINCT ON (pr.pr_repo_id) pr.pr_repo_id, c.cntrb_id
		FROM aveloxis_data.pull_request_repo pr
		JOIN aveloxis_data.pull_request_meta pm
		  ON pm.pr_meta_id = pr.pr_repo_meta_id
		JOIN aveloxis_data.repos r
		  ON r.repo_id = pm.repo_id AND r.platform_id = 1
		JOIN aveloxis_data.contributors c
		  ON LOWER(COALESCE(NULLIF(c.gh_login, ''), c.cntrb_login)) =
		     LOWER(split_part(pr.pr_repo_full_name, '/', 1))
		WHERE pr.pr_cntrb_id IS NULL
		  AND split_part(COALESCE(pr.pr_repo_full_name, ''), '/', 1) <> ''
		  AND COALESCE(c.cntrb_deleted, 0) = 0
		  AND (c.gh_login <> '' OR COALESCE(c.gl_username, '') = '')%s`
	if dryRun {
		// v0.27.107 (ultrareview bug_003): count BOTH derivation passes —
		// the pair PK-join fills rows the login sweep cannot see (renamed
		// owners whose meta row was resolved by platform id), so a
		// login-only count systematically under-reports the job's scope.
		var pair int64
		if err := s.pool.QueryRow(ctx, `
			SELECT COUNT(*)
			FROM aveloxis_data.pull_request_repo pr
			JOIN aveloxis_data.pull_request_meta m
			  ON m.pr_meta_id = pr.pr_repo_meta_id
			 AND m.head_or_base = pr.pr_repo_head_or_base
			JOIN aveloxis_data.repos r
			  ON r.repo_id = m.repo_id AND r.platform_id = 1
			WHERE m.cntrb_id IS NOT NULL
			  AND pr.pr_cntrb_id IS NULL`).Scan(&pair); err != nil {
			return 0, fmt.Errorf("dry-run pair count pull_request_repo: %w", err)
		}
		q := `SELECT COUNT(DISTINCT pr.pr_repo_id)` + strings.TrimPrefix(
			fmt.Sprintf(candidates, ""), `
		SELECT DISTINCT ON (pr.pr_repo_id) pr.pr_repo_id, c.cntrb_id`)
		var n int64
		if err := s.pool.QueryRow(ctx, q).Scan(&n); err != nil {
			return 0, fmt.Errorf("dry-run count pull_request_repo: %w", err)
		}
		// pair+n is an upper bound (the sets overlap); precise enough
		// for operator scoping and cheaper than a UNION-DISTINCT sort.
		s.logger.Info("backfill-identities dry-run", "table", "pull_request_repo",
			"pair_derivable", pair, "login_derivable", n, "derivable_max", pair+n)
		return pair + n, nil
	}
	var maxID int64
	if err := s.pool.QueryRow(ctx,
		`SELECT COALESCE(MAX(pr_repo_id), 0) FROM aveloxis_data.pull_request_repo`).Scan(&maxID); err != nil {
		return 0, fmt.Errorf("max pr_repo_id: %w", err)
	}
	var total int64
	window := int64(batchSize)
	for lo := int64(0); lo < maxID; lo += window {
		// Pass 1: the paired-meta PK join (far cheaper than the login
		// expression; covers everything v0.26.5 + forward collection
		// already resolved on the meta side).
		pairTag, err := s.pool.Exec(ctx, pairSQL, lo, lo+window)
		if err != nil {
			return total, fmt.Errorf("backfill pull_request_repo (meta pair) window %d: %w", lo, err)
		}
		total += pairTag.RowsAffected()

		// Pass 2: the login-expression sweep for the remainder.
		cand := fmt.Sprintf(candidates, " AND pr.pr_repo_id > $1 AND pr.pr_repo_id <= $2")
		tag, err := s.pool.Exec(ctx, `
			WITH cand AS (`+cand+`)
			UPDATE aveloxis_data.pull_request_repo pr
			SET pr_cntrb_id = cand.cntrb_id
			FROM cand WHERE pr.pr_repo_id = cand.pr_repo_id`, lo, lo+window)
		if err != nil {
			return total, fmt.Errorf("backfill pull_request_repo (owner login) window %d: %w", lo, err)
		}
		n := pairTag.RowsAffected() + tag.RowsAffected()
		total += tag.RowsAffected()
		if n > 0 {
			s.logger.Info("backfill-identities progress",
				"table", "pull_request_repo", "updated", total, "window_hi", lo+window, "max_id", maxID)
		}
		if limit > 0 && total >= limit {
			break
		}
	}
	return total, nil
}

// closedByCandidates is the shared derivation: the LATEST 'closed'
// event's actor per issue. Used by both the fleet backfill and the
// per-repo forward derivation so the two can never disagree.
const closedByCandidates = `
	SELECT DISTINCT ON (issue_id) e.issue_id, e.cntrb_id
	FROM aveloxis_data.issue_events e
	JOIN aveloxis_data.issues i USING (issue_id)
	WHERE e.action = 'closed' AND e.cntrb_id IS NOT NULL
	  AND i.closed_by_id IS NULL AND i.closed_at IS NOT NULL
	  %s
	ORDER BY issue_id, e.created_at DESC`

// BackfillClosedByFromEvents fills issues.closed_by_id fleet-wide from
// each issue's latest 'closed' event. Measured on production
// 2026-07-09: 2.03M of 5.26M closed issues derivable today, growing
// as the v0.26.3 event-healing cohort re-collects.
func (s *PostgresStore) BackfillClosedByFromEvents(ctx context.Context, batchSize int, limit int64, dryRun bool) (int64, error) {
	if batchSize <= 0 {
		batchSize = 100000
	}
	if dryRun {
		cand := fmt.Sprintf(closedByCandidates, "")
		q := `SELECT COUNT(DISTINCT e.issue_id)` + strings.TrimPrefix(cand, `
	SELECT DISTINCT ON (issue_id) e.issue_id, e.cntrb_id`)
		// Strip the ORDER BY — COUNT doesn't need it.
		q = strings.Replace(q, "ORDER BY issue_id, e.created_at DESC", "", 1)
		var n int64
		if err := s.pool.QueryRow(ctx, q).Scan(&n); err != nil {
			return 0, fmt.Errorf("dry-run count closed_by: %w", err)
		}
		s.logger.Info("backfill-identities dry-run", "table", "issues.closed_by_id", "derivable", n)
		return n, nil
	}
	var maxID int64
	if err := s.pool.QueryRow(ctx,
		`SELECT COALESCE(MAX(issue_id), 0) FROM aveloxis_data.issues`).Scan(&maxID); err != nil {
		return 0, fmt.Errorf("max issue_id: %w", err)
	}
	var total int64
	window := int64(batchSize)
	for lo := int64(0); lo < maxID; lo += window {
		cand := fmt.Sprintf(closedByCandidates, "AND i.issue_id > $1 AND i.issue_id <= $2")
		tag, err := s.pool.Exec(ctx, `
			WITH cand AS (`+cand+`)
			UPDATE aveloxis_data.issues i
			SET closed_by_id = cand.cntrb_id
			FROM cand WHERE i.issue_id = cand.issue_id`, lo, lo+window)
		if err != nil {
			return total, fmt.Errorf("backfill closed_by window %d: %w", lo, err)
		}
		n := tag.RowsAffected()
		total += n
		if n > 0 {
			s.logger.Info("backfill-identities progress",
				"table", "issues.closed_by_id", "updated", total, "window_hi", lo+window, "max_id", maxID)
		}
		if limit > 0 && total >= limit {
			break
		}
	}
	return total, nil
}

// DeriveIssueClosedByFromEvents is the per-repo forward step invoked
// at the end of Processor.ProcessRepo: same derivation as the fleet
// backfill, scoped to one repo, unbatched (per-repo volumes are
// small). Returns rows filled.
func (s *PostgresStore) DeriveIssueClosedByFromEvents(ctx context.Context, repoID int64) (int64, error) {
	cand := fmt.Sprintf(closedByCandidates, "AND i.repo_id = $1")
	tag, err := s.pool.Exec(ctx, `
		WITH cand AS (`+cand+`)
		UPDATE aveloxis_data.issues i
		SET closed_by_id = cand.cntrb_id
		FROM cand WHERE i.issue_id = cand.issue_id`, repoID)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// SweepIssue is one closed issue lacking a closer, for the Phase-3
// GraphQL timeline sweep (GitHub only — the repo-wide events feed is
// history-capped on large repos; the per-issue timeline is not).
type SweepIssue struct {
	IssueID int64
	RepoID  int64
	Owner   string
	Repo    string
	Number  int
}

// IssuesNeedingClosedBySweep returns closed GitHub issues with no
// closer, ordered by repo so the sweep batches per repository.
func (s *PostgresStore) IssuesNeedingClosedBySweep(ctx context.Context, limit int64) ([]SweepIssue, error) {
	q := `
		SELECT i.issue_id, i.repo_id, r.repo_owner, r.repo_name, i.issue_number
		FROM aveloxis_data.issues i
		JOIN aveloxis_data.repos r USING (repo_id)
		WHERE r.platform_id = 1
		  AND i.closed_at IS NOT NULL AND i.closed_by_id IS NULL
		  AND i.issue_number > 0
		ORDER BY i.repo_id, i.issue_number`
	if limit > 0 {
		q += fmt.Sprintf(" LIMIT %d", limit)
	}
	rows, err := s.pool.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SweepIssue
	for rows.Next() {
		var si SweepIssue
		if err := rows.Scan(&si.IssueID, &si.RepoID, &si.Owner, &si.Repo, &si.Number); err != nil {
			return nil, err
		}
		out = append(out, si)
	}
	return out, rows.Err()
}

// SetIssueClosedBy writes a resolved closer onto one issue.
func (s *PostgresStore) SetIssueClosedBy(ctx context.Context, issueID int64, cntrbID string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_data.issues SET closed_by_id = $2::uuid
		WHERE issue_id = $1 AND closed_by_id IS NULL`, issueID, cntrbID)
	return err
}
