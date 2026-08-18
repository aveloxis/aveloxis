// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import "context"

// ReconcileOrgRepoLinks links every TRACKED repo whose URL falls under a
// registered org into that org's groups (v0.27.93).
//
// Why this exists: the org scan links only what the forge's org listing
// ENUMERATES. Repos that enter the catalog through any other door —
// mailing-list loaders, foundation importers, renames, GitLab orgs the
// scanner doesn't enumerate — were never linked into the groups tracking
// their org. The 2026-08-18 production drift check found 9 live repos
// stranded this way (including gitlab.com/petsc/petsc, which GitHub-only
// enumeration can never reach). This set-based pass self-heals the class
// once per full org-scan cycle, regardless of which path created the repo.
//
// Load-bearing properties:
//   - The collection_queue join is the "tracked" gate (v0.27.20: tracked =
//     queue row exists). It structurally excludes dead/sidelined catalog
//     residue — 218 of the 227 drift repos were GitHub-404 rows that must
//     NOT be pushed into users' groups.
//   - Rejected groups' orgs are never linked (the v0.27.20 abuse lever;
//     same gate the enumeration path applies).
//   - Prefix matching uses starts_with, not LIKE — org paths can contain
//     LIKE metacharacters ('_' is legal in GitLab namespaces) — and is
//     case-insensitive per the v0.25.32 forge-URL rule. GitLab nested
//     subgroup repos prefix-match their top-level group: deliberate (they
//     belong to the tracked org).
//   - Pure user_repos INSERT ... ON CONFLICT DO NOTHING: idempotent, and
//     ZERO collection-machinery reachability (no enqueue, no add-requests)
//     so the v0.27.20 approval invariant holds by construction — pinned by
//     TestReconcileOrgRepoLinksNeverTouchesCollectionMachinery.
//
// Returns the number of link rows inserted.
func (s *PostgresStore) ReconcileOrgRepoLinks(ctx context.Context) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		INSERT INTO aveloxis_ops.user_repos (group_id, repo_id)
		SELECT DISTINCT o.group_id, r.repo_id
		FROM aveloxis_ops.user_org_requests o
		JOIN aveloxis_ops.user_groups g ON g.group_id = o.group_id
		JOIN aveloxis_data.repos r
		  ON starts_with(LOWER(r.repo_git), LOWER(rtrim(o.org_url, '/')) || '/')
		JOIN aveloxis_ops.collection_queue q ON q.repo_id = r.repo_id
		WHERE COALESCE(g.status, 'approved') <> 'rejected'
		ON CONFLICT DO NOTHING`)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}
