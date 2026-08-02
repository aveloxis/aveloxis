// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import "context"

// PublicFleetStats is the anonymous landing-page payload — fleet-scale
// vanity numbers, served through the api layer's 60s stale-on-error
// cache. JSON field names are the public contract (docs/guide/api.md).
//
// v0.27.77 supersedes v0.27.59's single repo count (CountActiveRepos,
// removed — this method's repos arm carries its exact semantics and
// the v0.27.68 archived-included pin).
type PublicFleetStats struct {
	Repos        int64 `json:"repos"`
	Commits      int64 `json:"commits"`
	Issues       int64 `json:"issues"`
	PRs          int64 `json:"prs"`
	Contributors int64 `json:"contributors"`
}

// GetPublicFleetStats returns the landing page's numbers.
//
// Definitions and cost class (each deliberate):
//   - Repos: TOTAL catalog count, archived INCLUDED (v0.27.68 operator
//     decision — the landing number must agree with the monitor's
//     total). ~100K rows, exact COUNT(*) in tens of milliseconds.
//   - Commits/Issues/PRs: SUMs of collection_queue's CACHED cumulative
//     per-repo totals (v0.19.11/v0.21.2 — the same values the monitor
//     renders). One pass over ~100K queue rows. NEVER a count of the
//     474M-row commits table (the v0.27.4 timeout class); the sums
//     are distinct-commit counts by construction (CompleteJob writes
//     COUNT(DISTINCT cmt_commit_hash) per repo).
//   - Contributors: exact COUNT(*) of the contributors catalog (~1.7M
//     rows, ~100-300ms) — acceptable behind the 60s cache.
func (s *PostgresStore) GetPublicFleetStats(ctx context.Context) (PublicFleetStats, error) {
	var out PublicFleetStats
	err := s.pool.QueryRow(ctx, `
		SELECT
			(SELECT COUNT(*) FROM aveloxis_data.repos),
			(SELECT COALESCE(SUM(last_commits), 0) FROM aveloxis_ops.collection_queue),
			(SELECT COALESCE(SUM(last_issues), 0) FROM aveloxis_ops.collection_queue),
			(SELECT COALESCE(SUM(last_prs), 0) FROM aveloxis_ops.collection_queue),
			(SELECT COUNT(*) FROM aveloxis_data.contributors)`).
		Scan(&out.Repos, &out.Commits, &out.Issues, &out.PRs, &out.Contributors)
	return out, err
}
