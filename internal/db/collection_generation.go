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
// CompleteJob (success AND failure), the gap healer's count refresh and a
// re-queue all stamp, and last_collected. The latest last_collected alone
// (the first shape) missed a failed job, which writes rows without advancing
// it, and a member finishing after a member with a newer start anchor
// (review round 1). Heartbeats stamp only locked_at, so a running job does
// not churn the key. Repositories without a queue row contribute nothing; a
// set with none reads "none". One index scan over the queue's primary key.
func (s *PostgresStore) CollectionGeneration(ctx context.Context, repoIDs []int64) (string, error) {
	var gen string
	if err := s.pool.QueryRow(ctx, `
		SELECT COALESCE(COUNT(*)::text || ':' || md5(string_agg(
			repo_id::text || '@' || COALESCE(to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US'), '') ||
			'@' || COALESCE(to_char(last_collected AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US'), ''),
			',' ORDER BY repo_id)), 'none')
		FROM aveloxis_ops.collection_queue WHERE repo_id = ANY($1::bigint[])`, repoIDs).Scan(&gen); err != nil {
		return "", fmt.Errorf("collection generation: %w", err)
	}
	return gen, nil
}
