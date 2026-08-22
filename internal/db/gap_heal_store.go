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
func (s *PostgresStore) GetGapHealCandidates(ctx context.Context, afterRepoID int64, limit int, all bool) ([]GapHealCandidate, error) {
	gapPredicate := `AND (ri.issues_count > q.last_issues OR ri.pr_count > q.last_prs)`
	if all {
		gapPredicate = ""
	}
	rows, err := s.pool.Query(ctx, `
		SELECT q.repo_id, r.repo_owner, r.repo_name, r.platform_id,
		       ri.issues_count, ri.pr_count,
		       GREATEST(ri.issues_count - q.last_issues, 0) + GREATEST(ri.pr_count - q.last_prs, 0)
		FROM aveloxis_ops.collection_queue q
		JOIN aveloxis_data.repo_info ri USING (repo_id)
		JOIN aveloxis_data.repos r ON r.repo_id = q.repo_id
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
