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
// and batched by candidate LIMIT so no single transaction holds
// millions of row locks on the production fleet.

import (
	"context"
	"fmt"
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
		var tableTotal int64
		for {
			batch := batchSize
			if limit > 0 && int64(batch) > limit-tableTotal {
				batch = int(limit - tableTotal)
			}
			if batch <= 0 {
				break
			}
			tag, err := s.pool.Exec(ctx, fmt.Sprintf(`
				WITH cand AS (
					SELECT a.%[2]s AS id, ci.cntrb_id
					FROM aveloxis_data.%[1]s a
					JOIN aveloxis_data.repos r USING (repo_id)
					JOIN aveloxis_data.contributor_identities ci
					  ON ci.platform_id = r.platform_id AND ci.platform_user_id = a.%[3]s
					WHERE a.cntrb_id IS NULL AND a.%[3]s <> 0
					LIMIT $1
				)
				UPDATE aveloxis_data.%[1]s t
				SET cntrb_id = cand.cntrb_id
				FROM cand WHERE t.%[2]s = cand.id`, t.table, t.pk, t.platCol), batch)
			if err != nil {
				return total, fmt.Errorf("backfill %s: %w", t.table, err)
			}
			n := tag.RowsAffected()
			tableTotal += n
			total += n
			if n > 0 {
				s.logger.Info("backfill-identities progress", "table", t.table, "updated", tableTotal)
			}
			if n < int64(batch) {
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
		  AND COALESCE(c.cntrb_deleted, 0) = 0`
	if dryRun {
		var n int64
		if err := s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM (`+candidates+`) sub`).Scan(&n); err != nil {
			return 0, fmt.Errorf("dry-run count pull_request_meta: %w", err)
		}
		s.logger.Info("backfill-identities dry-run", "table", "pull_request_meta", "derivable", n)
		return n, nil
	}
	var total int64
	for {
		batch := batchSize
		if limit > 0 && int64(batch) > limit-total {
			batch = int(limit - total)
		}
		if batch <= 0 {
			break
		}
		tag, err := s.pool.Exec(ctx, `
			WITH cand AS (`+candidates+` LIMIT $1)
			UPDATE aveloxis_data.pull_request_meta m
			SET cntrb_id = cand.cntrb_id
			FROM cand WHERE m.pr_meta_id = cand.pr_meta_id`, batch)
		if err != nil {
			return total, fmt.Errorf("backfill pull_request_meta: %w", err)
		}
		n := tag.RowsAffected()
		total += n
		if n > 0 {
			s.logger.Info("backfill-identities progress", "table", "pull_request_meta", "updated", total)
		}
		if n < int64(batch) {
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
	cand := fmt.Sprintf(closedByCandidates, "")
	if dryRun {
		var n int64
		if err := s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM (`+cand+`) sub`).Scan(&n); err != nil {
			return 0, fmt.Errorf("dry-run count closed_by: %w", err)
		}
		s.logger.Info("backfill-identities dry-run", "table", "issues.closed_by_id", "derivable", n)
		return n, nil
	}
	var total int64
	for {
		batch := batchSize
		if limit > 0 && int64(batch) > limit-total {
			batch = int(limit - total)
		}
		if batch <= 0 {
			break
		}
		tag, err := s.pool.Exec(ctx, `
			WITH cand AS (`+cand+` LIMIT $1)
			UPDATE aveloxis_data.issues i
			SET closed_by_id = cand.cntrb_id
			FROM cand WHERE i.issue_id = cand.issue_id`, batch)
		if err != nil {
			return total, fmt.Errorf("backfill closed_by: %w", err)
		}
		n := tag.RowsAffected()
		total += n
		if n > 0 {
			s.logger.Info("backfill-identities progress", "table", "issues.closed_by_id", "updated", total)
		}
		if n < int64(batch) {
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
