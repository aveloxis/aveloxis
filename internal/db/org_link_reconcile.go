// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"

	"github.com/aveloxis/aveloxis/internal/capacity"
	"github.com/jackc/pgx/v5"
)

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
	// The candidates (links the org registrations imply and that do not
	// exist yet), then each group filled within its owner's repository
	// allocation (v0.29.89, summary/53 A8): an organization larger than
	// the room left links what fits; the rest is logged once a day per
	// account, never retried into a refusal loop.
	rows, err := s.pool.Query(ctx, `
		SELECT DISTINCT o.group_id, r.repo_id
		FROM aveloxis_ops.user_org_requests o
		JOIN aveloxis_ops.user_groups g ON g.group_id = o.group_id
		JOIN aveloxis_data.repos r
		  -- v0.29.71: the host/owner key both URLs share turns this into a
		  -- hash join; starts_with alone was a nested loop of every org x
		  -- every repository (kate: 323-338 s per run; keyed 0.7 s, the same
		  -- 222,699 rows). Every repository the prefix matches has the org's
		  -- key, so the key only prunes; starts_with still decides.
		  ON split_part(LOWER(r.repo_git), '/', 3) || '/' || split_part(LOWER(r.repo_git), '/', 4)
		   = split_part(LOWER(rtrim(o.org_url, '/')), '/', 3) || '/' || split_part(LOWER(rtrim(o.org_url, '/')), '/', 4)
		 AND starts_with(LOWER(r.repo_git), LOWER(rtrim(o.org_url, '/')) || '/')
		JOIN aveloxis_ops.collection_queue q ON q.repo_id = r.repo_id
		WHERE COALESCE(g.status, 'approved') <> 'rejected'
		  AND NOT EXISTS (SELECT 1 FROM aveloxis_ops.user_repos ur WHERE ur.group_id = o.group_id AND ur.repo_id = r.repo_id)
		ORDER BY o.group_id, r.repo_id`)
	if err != nil {
		return 0, err
	}
	// Exported fields: pgx.RowToStructByPos maps only exported ones.
	type pair struct{ Group, Repo int64 }
	pairs, err := pgx.CollectRows(rows, pgx.RowToStructByPos[pair])
	if err != nil {
		return 0, err
	}
	var total int64
	for start := 0; start < len(pairs); {
		end := start
		ids := []int64{}
		for end < len(pairs) && pairs[end].Group == pairs[start].Group {
			ids = append(ids, pairs[end].Repo)
			end++
		}
		n, err := s.linkWithinCap(ctx, pairs[start].Group, ids, linkOrgFill)
		total += int64(n)
		if ex, capped := capacity.AsExceeded(err); capped {
			if s.capacityNotifier().Due("org-reconcile|" + ex.Subject.ID) {
				s.logger.Warn("organization repositories not linked: the account's repository quota is reached",
					"group_id", pairs[start].Group, "user_id", ex.Subject.ID, "reach", ex.Used, "allowed", ex.Allowed, "new_repositories_wanted", ex.Wanted)
			}
		} else if err != nil {
			return total, err
		}
		start = end
	}
	return total, nil
}
