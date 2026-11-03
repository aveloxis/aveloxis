// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
)

// CollectionGeneration names the collection state of a set of repositories
// (O11 option 4, v0.29.71). The API keys its per-repository caches on it, so
// an answer cached before a job over any member finished is not served after
// it. It digests every member's queue row: updated_at, which the claim,
// CompleteJob (success AND failure), the gap healer's drain lock and
// release, the count refresh when a count changes (A2 of the 0.29.124
// review) and a re-queue all stamp, and last_collected. The latest last_collected alone
// (the first shape) missed a failed job, which writes rows without advancing
// it, and a member finishing after a member with a newer start anchor
// (review round 1). Since 0.29.120 it also digests repos.data_changed_at,
// the stamp a writer outside a job moves (stampRepoCacheStateSQL): the
// series cache keyed on it has no age limit (D6, summary/63), so such a
// write must move the key itself. Heartbeats stamp only locked_at, so a
// running job does not churn the key. Every member counts, a repository
// without a queue row (a gone one: dequeued, its data kept) by its
// data_changed_at alone (round 1 finding 4 of 0.29.123: read from the queue,
// a stamp on such a member never moved an organisation's series); a set of
// ids no repository has reads "none". One index scan over each table's
// primary key.
func (s *PostgresStore) CollectionGeneration(ctx context.Context, repoIDs []int64) (string, error) {
	var gen string
	if err := s.pool.QueryRow(ctx, `
		SELECT COALESCE(COUNT(*)::text || ':' || md5(string_agg(
			r.repo_id::text || '@' || COALESCE(to_char(q.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US'), '') ||
			'@' || COALESCE(to_char(q.last_collected AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US'), '') ||
			'@' || COALESCE(to_char(r.data_changed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US'), ''),
			',' ORDER BY r.repo_id)), 'none')
		FROM aveloxis_data.repos r
		LEFT JOIN aveloxis_ops.collection_queue q ON q.repo_id = r.repo_id
		WHERE r.repo_id = ANY($1::bigint[])`, repoIDs).Scan(&gen); err != nil {
		return "", fmt.Errorf("collection generation: %w", err)
	}
	return gen, nil
}
