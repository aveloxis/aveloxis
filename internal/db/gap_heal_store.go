// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// gap_heal_store.go — v0.27.140: candidate selection for
// `aveloxis heal-collection-gaps`, the isolated healer for the
// blind-window losses the v0.27.139 since fix stopped creating.
//
// Candidates are repos whose latest repo_info metadata counts exceed
// the queue's cached stored counts (last_issues/last_prs — cumulative
// since v0.21.2). Fleet sizing on aveloxis_large (2026-08-21): 6,815
// of 141,031 collected repos, ≥175,590 missing items — 4.8% of the
// fleet, which is the whole point: the healer targets ONLY these, not
// a 100% rescan. Count-gap is a LOWER bound (stored rows for
// upstream-deleted items net against missing ones), so the command
// also offers --all for a completeness sweep; the per-repo listing
// set-diff finds the TRUE missing numbers either way.

package db

import (
	"context"

	"github.com/aveloxis/aveloxis/internal/model"
)

// GapHealCandidate is one repo the healer should visit, with the
// metadata counts the gap fill compares against.
type GapHealCandidate struct {
	RepoID     int64
	Owner      string
	Name       string
	Platform   model.Platform
	MetaIssues int64
	MetaPRs    int64
	Gap        int64 // GREATEST(meta−stored,0) summed — reporting only
}

// GetGapHealCandidates pages collected repos (keyset by repo_id) whose
// metadata exceeds stored counts. all=true drops the gap predicate
// (the completeness sweep). Generic-git repos never appear — they have
// no API listing to heal from.
//
// v0.27.147 (round 26, suppressed finding): repo_info has NO unique on
// repo_id and its rotation to _history is warn-and-continue, so
// multiple live snapshots CAN coexist. A bare join then duplicates a
// candidate and — worse — a stale high count keeps a HEALED repo in
// the candidate set forever, breaking the SR-19 "rerun until 0
// candidates" convergence. The LATERAL picks the latest snapshot per
// repo (indexed via idx_repo_info_repo_id; repo_info_id DESC breaks
// same-timestamp ties toward the newer insert).
// nativeGatheredIssuesSQL derives the NATIVE gathered-issue count from
// the queue cache minus the live synthetic population, clamped at zero
// (round 15): the cache is refreshed best-effort by the drains, so
// synthetics can transiently outnumber it. Shared by the candidate
// predicate and the reported gap size so they can never disagree.
const nativeGatheredIssuesSQL = `GREATEST(COALESCE(q.last_issues, 0) - sy.synth, 0)`

func (s *PostgresStore) GetGapHealCandidates(ctx context.Context, afterRepoID int64, limit int, all bool) ([]GapHealCandidate, error) {
	// Review 2026-08-30 #3: q.last_issues is COUNT(*) INCLUDING the
	// mail-projected Jira synthetics (negative platform_issue_id), so an
	// Apache repo's cached count outruns the forge's meta count and a
	// genuine NATIVE gap is masked. The sy lateral subtracts the live
	// synthetic count (cheap via the partial idx_issues_synthetic_repo);
	// PRs have no synthetic population and stay as-is.
	// Round 15 (Copilot, suppressed ×2): the drain-side activity
	// refresh is best-effort (round 10), so the cached count can LAG
	// the live synthetic population — an unclamped subtraction went
	// negative and made even a zero-issue forge repo a candidate (and
	// the reported gap size counted phantom missing issues). Clamp
	// the derived native count at zero and coalesce the cache; ONE
	// spelling for the predicate and the size (SR-17).
	gapPredicate := `AND (COALESCE(ri.issues_count, 0) > ` + nativeGatheredIssuesSQL + ` OR COALESCE(ri.pr_count, 0) > COALESCE(q.last_prs, 0))`
	if all {
		gapPredicate = ""
	}
	// v0.27.150 (round 29, suppressed): LEFT lateral + COALESCE — a
	// collected repo with NO repo_info snapshot (the legacy
	// stamped-but-empty cohort) must not vanish from the --all sweep,
	// whose force-list mode needs no metadata counts. Normal gap mode
	// still excludes them naturally (0 > last_* is never true).
	rows, err := s.pool.Query(ctx, `
		SELECT q.repo_id, r.repo_owner, r.repo_name, r.platform_id,
		       COALESCE(ri.issues_count, 0), COALESCE(ri.pr_count, 0),
		       GREATEST(COALESCE(ri.issues_count, 0) - `+nativeGatheredIssuesSQL+`, 0) + GREATEST(COALESCE(ri.pr_count, 0) - COALESCE(q.last_prs, 0), 0)
		FROM aveloxis_ops.collection_queue q
		JOIN aveloxis_data.repos r ON r.repo_id = q.repo_id
		LEFT JOIN LATERAL (
			SELECT COUNT(*)::bigint AS synth
			FROM aveloxis_data.issues si
			WHERE si.repo_id = q.repo_id AND si.platform_issue_id < 0
		) sy ON TRUE
		LEFT JOIN LATERAL (
			SELECT ri0.issues_count, ri0.pr_count
			FROM aveloxis_data.repo_info ri0
			WHERE ri0.repo_id = q.repo_id
			ORDER BY ri0.data_collection_date DESC NULLS LAST, ri0.repo_info_id DESC
			LIMIT 1
		) ri ON TRUE
		WHERE q.last_collected IS NOT NULL
		  AND q.repo_id > $1
		  AND r.platform_id IN (1, 2)
		  `+gapPredicate+`
		ORDER BY q.repo_id
		LIMIT $2`, afterRepoID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []GapHealCandidate
	for rows.Next() {
		var c GapHealCandidate
		var plat int
		if err := rows.Scan(&c.RepoID, &c.Owner, &c.Name, &plat, &c.MetaIssues, &c.MetaPRs, &c.Gap); err != nil {
			return nil, err
		}
		c.Platform = model.Platform(plat)
		out = append(out, c)
	}
	return out, rows.Err()
}

// RefreshQueueGatheredCounts re-derives the queue row's cached
// last_issues/last_prs from the data tables — the SAME cumulative
// subqueries CompleteJob writes (v0.21.2). Round-23: the healer fills
// rows WITHOUT a CompleteJob pass, so without this refresh a healed
// repo satisfied the candidate predicate forever and "rerun until 0
// candidates" never converged. Touches ONLY the two count columns —
// never last_collected, status, or due_at.
func (s *PostgresStore) RefreshQueueGatheredCounts(ctx context.Context, repoID int64) error {
	return s.withRetry(ctx, func(ctx context.Context) error {
		_, err := s.pool.Exec(ctx, `
			UPDATE aveloxis_ops.collection_queue
			SET last_issues = (SELECT COUNT(*) FROM aveloxis_data.issues WHERE repo_id = $1),
				last_prs = (SELECT COUNT(*) FROM aveloxis_data.pull_requests WHERE repo_id = $1),
				last_activity_90d = (SELECT COUNT(*) FROM aveloxis_data.issues
				                     WHERE repo_id = $1 AND created_at >= NOW() - INTERVAL '90 days')
				                  + (SELECT COUNT(*) FROM aveloxis_data.pull_requests
				                     WHERE repo_id = $1 AND created_at >= NOW() - INTERVAL '90 days'),
				updated_at = NOW()
			WHERE repo_id = $1`, repoID)
		return err
	})
}
